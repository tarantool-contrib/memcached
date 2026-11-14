-- Child Tarantool processes: what survives a restart, and a read-only
-- replica that waits for its space to arrive by replication.

local t = require('luatest')
local server = require('luatest.server')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local function create(srv, name, port, opts)
    return srv:exec(function(name_, port_, opts_)
        local mcc = require('memcached')
        opts_.listen = '127.0.0.1:' .. port_
        return mcc.create(name_, opts_).status
    end, {name, port, opts or {}})
end

local function status(srv, name)
    return srv:exec(function(name_)
        local inst = require('memcached').get(name_)
        return inst and inst.status
    end, {name})
end

--
-- Restart.
--

local g = t.group('restart')

g.before_all(function(cg)
    cg.server = server:new({alias = 'mcc_restart', env = env.lua_env()})
    cg.server:start()
    cg.port = env.free_port()
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.test_restart = function(cg)
    -- No sweeper: a flushed item stays in the space.
    local opts = {expire = {enabled = false}}
    t.assert_equals(create(cg.server, 'r', cg.port, opts), 'running')
    local c = client.must_connect(cg.port)
    t.assert_equals(c:set('a', 'old'), 'STORED\r\n')
    t.assert_equals(c:call('flush_all\r\n'), 'OK\r\n')
    t.assert_equals(c:set('b', 'new'), 'STORED\r\n')
    t.assert_equals(c:set('n', '41'), 'STORED\r\n')
    local _, _, cas = c:get('b')
    c:close()

    cg.server:restart()
    t.assert_equals(create(cg.server, 'r', cg.port, opts), 'running')
    c = client.must_connect(cg.port)
    -- The flush time is persisted: the old item is still dead.
    t.assert_equals(
        cg.server:exec(function()
            return box.space.__mcc_r:get('a') ~= nil
        end),
        true
    )
    t.assert_equals(c:get('a'), nil)
    t.assert_equals(c:get('b'), 'new')
    t.assert_equals(c:call('incr n 1\r\n'), '42\r\n')
    -- CAS values keep growing after a restart.
    t.assert_equals(c:set('b', 'newer'), 'STORED\r\n')
    local _, _, cas2 = c:get('b')
    t.assert(tonumber64(cas2) > tonumber64(cas))
    -- A delayed flush survives a restart too.
    t.assert_equals(c:call('flush_all 2\r\n'), 'OK\r\n')
    c:close()
    cg.server:restart()
    t.assert_equals(create(cg.server, 'r', cg.port, opts), 'running')
    c = client.must_connect(cg.port)
    t.assert_equals(c:get('b'), 'newer')
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(c:get('b'), nil)
    end)
    c:close()
end

-- The meta win tokens are in the items: they survive a restart.
g.test_win_tokens = function(cg)
    local port = env.free_port()
    t.assert_equals(create(cg.server, 'w', port), 'running')
    local c = client.must_connect(port)
    t.assert_equals(c:call('mg viv s N30\r\n'), 'HD s0 W\r\n')
    t.assert_equals(c:set('s', 'v'), 'STORED\r\n')
    t.assert_equals(c:call('md s I\r\n'), 'HD\r\n')
    t.assert_equals(c:call('mg s\r\n'), 'HD X W\r\n')
    t.assert_equals(c:set('r', 'v', {exptime = 100}), 'STORED\r\n')
    t.assert_equals(c:call('mg r R200\r\n'), 'HD W\r\n')
    t.assert_equals(c:set('n', 'v'), 'STORED\r\n')
    t.assert_equals(c:call('md n I\r\n'), 'HD\r\n')
    c:close()

    cg.server:restart()
    t.assert_equals(create(cg.server, 'w', port), 'running')
    c = client.must_connect(port)
    t.assert_equals(c:call('mg viv s\r\n'), 'HD s0 Z\r\n')
    t.assert_equals(c:call('mg s\r\n'), 'HD Z X\r\n')
    t.assert_equals(c:call('mg r R200\r\n'), 'HD Z\r\n')
    -- The one not taken is still there to win.
    t.assert_equals(c:call('mg n\r\n'), 'HD X W\r\n')
    c:close()
end

--
-- Replication.
--

local r = t.group('replica')

r.before_all(function(cg)
    cg.master = server:new({alias = 'mcc_master', env = env.lua_env()})
    cg.master:start()
    cg.master:exec(function()
        box.session.su(
            'admin',
            box.schema.user.grant,
            'guest',
            'replication',
            nil,
            nil,
            {if_not_exists = true}
        )
    end)
    cg.replica = server:new({
        alias = 'mcc_replica',
        env = env.lua_env(),
        box_cfg = {
            replication = {cg.master.net_box_uri},
            read_only = true,
        },
    })
    cg.replica:start()
    cg.master_port = env.free_port()
    cg.replica_port = env.free_port()
end)

r.after_all(function(cg)
    cg.replica:drop()
    cg.master:drop()
end)

r.test_space_arrives_by_replication = function(cg)
    -- The replica can't create the space: it waits.
    t.assert_equals(create(cg.replica, 'rep', cg.replica_port), 'waiting')
    t.assert_not(client.is_listening(cg.replica_port))
    -- The master creates it.
    t.assert_equals(create(cg.master, 'rep', cg.master_port), 'running')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(status(cg.replica, 'rep'), 'running')
    end)

    local m = client.must_connect(cg.master_port)
    local rc = client.must_connect(cg.replica_port)
    t.assert_equals(m:set('a', 'x'), 'STORED\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(rc:get('a'), 'x')
    end)
    -- Reads on the replica don't write; writes fail.
    t.assert_equals(rc:get('a'), 'x')
    t.assert_str_contains(rc:set('b', 'y'), 'SERVER_ERROR')
    -- A flush is replicated.
    t.assert_equals(m:call('flush_all\r\n'), 'OK\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(rc:get('a'), nil)
    end)
    m:close()
    rc:close()
end

-- A read-only replica can't write the win token of an item: it keeps its
-- own in memory. The ones the master hands out reach it with the items.
r.test_win_tokens_on_replica = function(cg)
    local mport, rport = env.free_port(), env.free_port()
    t.assert_equals(create(cg.master, 'tok', mport), 'running')
    create(cg.replica, 'tok', rport)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(status(cg.replica, 'tok'), 'running')
    end)
    local m = client.must_connect(mport)
    local rc = client.must_connect(rport)
    for _, key in ipairs({'s', 's2'}) do
        t.assert_equals(m:set(key, 'v'), 'STORED\r\n')
        t.assert_equals(m:call(('md %s I\r\n'):format(key)), 'HD\r\n')
    end
    t.assert_equals(m:call('mg s\r\n'), 'HD X W\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(
            cg.replica:exec(function()
                local space = box.space.__mcc_tok
                return {space:get('s').token, space:get('s2').stale}
            end),
            {true, true}
        )
    end)
    t.assert_equals(rc:call('mg s\r\n'), 'HD Z X\r\n')
    t.assert_equals(rc:call('mg s2\r\n'), 'HD X W\r\n')
    t.assert_equals(rc:call('mg s2\r\n'), 'HD Z X\r\n')
    t.assert_equals(
        cg.replica:exec(function()
            return box.space.__mcc_tok:get('s2').token
        end),
        false
    )
    -- Per replica: the master hasn't seen it.
    t.assert_equals(m:call('mg s2\r\n'), 'HD X W\r\n')
    -- A win that needs a write fails on the replica.
    t.assert_str_contains(rc:call('mg viv N30\r\n'), 'SERVER_ERROR')
    m:close()
    rc:close()
end

-- A space of format v2 (made by the code before the token field) is used
-- as it is on a read-only replica, upgraded by the master, and the upgrade
-- reaches the replica.
r.test_format_v2_upgrade = function(cg)
    local mport, rport = env.free_port(), env.free_port()
    cg.master:exec(function()
        local schema = require('memcached.schema')
        local format = {}
        for i = 1, schema.BUCKET_ID do
            format[i] = schema.FORMAT[i]
        end
        local space = box.schema.space.create('__mcc_v2', {format = format})
        space:create_index('primary', {type = 'HASH', parts = {{1, 'string'}}})
        space:create_index('exptime', {
            unique = false,
            parts = {{4, 'number'}},
        })
        local now = require('fiber').time()
        space:replace({'s', 'v', 0, 0, 1000, now, now, false, true, 0})
        space:replace({'s2', 'v', 0, 0, 1001, now, now, false, true, 0})
    end)
    local function fields(srv)
        return srv:exec(function()
            local space = box.space.__mcc_v2
            return space and space.index.primary and #space:format() or nil
        end)
    end
    local function has_exptime(srv)
        return srv:exec(function()
            return box.space.__mcc_v2.index.exptime ~= nil
        end)
    end
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(fields(cg.replica), 10)
        t.assert_equals(
            cg.replica:exec(function()
                return box.space.__mcc_v2:len()
            end),
            2
        )
    end)
    -- The replica serves the v2 space at once, with the indexes it has.
    t.assert_equals(create(cg.replica, 'v2', rport), 'running')
    t.assert_equals(fields(cg.replica), 10)
    t.assert(has_exptime(cg.replica))
    local rc = client.must_connect(rport)
    t.assert_equals(rc:call('mg s\r\n'), 'HD X W\r\n')
    t.assert_equals(rc:call('mg s\r\n'), 'HD Z X\r\n')

    -- The master upgrades the format and drops the exptime index
    -- (expire.mode = 'scan', the default); both reach the replica.
    t.assert_equals(create(cg.master, 'v2', mport), 'running')
    t.assert_equals(fields(cg.master), 11)
    t.assert_not(has_exptime(cg.master))
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(fields(cg.replica), 11)
        t.assert_not(has_exptime(cg.replica))
    end)
    local m = client.must_connect(mport)
    t.assert_equals(m:call('mg s2\r\n'), 'HD X W\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(rc:call('mg s2\r\n'), 'HD Z X\r\n')
    end)
    -- The replica's own token of the v2 tuple still counts.
    t.assert_equals(rc:call('mg s\r\n'), 'HD Z X\r\n')
    m:close()
    rc:close()
end
