-- The roles.memcached role on top of the Tarantool 3 declarative
-- config: a single instance, a replicaset (rw + ro) and a vshard cluster
-- configured through the role only.

local socket = require('socket')
local t = require('luatest')
local cbuilder = require('luatest.cbuilder')

local env = require('test.helpers.env')
local client = require('test.helpers.client')
local rh = require('test.helpers.role')
local vc = require('test.helpers.vshard_cluster')

local ROLE = rh.ROLE

local function value_reply(key, value)
    return ('VALUE %s 0 %d\r\n%s\r\nEND\r\n'):format(key, #value, value)
end

--------------------------------------------------------------------------------
-- A single instance.
--------------------------------------------------------------------------------

local g = t.group('role')

local function single_config(roles_cfg, roles)
    return cbuilder:new()
        :set_global_option('threads.groups', {{name = 'fe', size = 2}})
        :set_global_option('roles', roles or {ROLE})
        :set_global_option('roles_cfg', {[ROLE] = roles_cfg})
        :add_instance('instance-001', {database = {mode = 'rw'}})
        :config()
end

local function base_cfg(cg)
    return {
        cache_a = {listen = cg.ports[1]},
        cache_b = {listen = '127.0.0.1:' .. cg.ports[2]},
    }
end

local function reload(cg, roles_cfg, roles)
    rh.reload(cg.cluster, single_config(roles_cfg, roles), {cg.server})
end

local function reload_error(cg, roles_cfg)
    return rh.reload_error(cg.cluster, single_config(roles_cfg), cg.server)
end

g.before_all(function(cg)
    cg.ports = {}
    for i = 1, 6 do
        cg.ports[i] = env.free_port()
    end
    cg.cluster = rh.start(single_config(base_cfg(cg)))
    cg.server = cg.cluster['instance-001']
end)

g.after_all(function(cg)
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)

g.before_each(function(cg)
    -- Every test starts from the base config.
    cg.server:exec(function()
        require('roles.memcached').internal.multi_result = nil
    end)
    reload(cg, base_cfg(cg))
end)

g.test_serve = function(cg)
    t.assert_equals(client.set(cg.ports[1], 'a', 'x'), 'STORED\r\n')
    t.assert_equals(client.set(cg.ports[2], 'b', 'y'), 'STORED\r\n')
    t.assert_equals(client.get(cg.ports[1], 'a'), value_reply('a', 'x'))
    t.assert_equals(client.get(cg.ports[2], 'a'), 'END\r\n')
    t.assert_equals(
        cg.server:exec(function()
            return {
                box.space.__mcc_cache_a:get('a').value,
                box.space.__mcc_cache_b:get('b').value,
            }
        end),
        {'x', 'y'}
    )
    t.assert_equals(rh.status(cg.server, 'cache_a'), 'running')
    t.assert_equals(rh.status(cg.server, 'cache_b'), 'running')
end

g.test_reload = function(cg)
    local p = cg.ports
    t.assert_equals(client.set(p[1], 'a', 'x'), 'STORED\r\n')
    local conn = client.must_connect(p[1])

    -- Rebind cache_a, make its items small, drop cache_b, add cache_c.
    reload(cg, {
        -- The item size: 68 is 'small' with 4 bytes.
        cache_a = {listen = p[3], max_item_size = 68},
        cache_c = {listen = p[4], expire = {enabled = false}},
    })
    t.assert_not(client.is_listening(p[1]))
    t.assert_not(client.is_listening(p[2]))
    t.assert_equals(rh.status(cg.server, 'cache_b'), nil)
    -- The connection made before the rebind survives it.
    t.assert_equals(conn:call('get a\r\n', 'END\r\n'), value_reply('a', 'x'))
    conn:close()
    -- The new limit is in effect for the protocol.
    t.assert_equals(
        client.set(p[3], 'big', '123456789'),
        'SERVER_ERROR object too large for cache\r\n'
    )
    t.assert_equals(client.set(p[3], 'small', '1234'), 'STORED\r\n')
    t.assert_equals(client.set(p[4], 'c', 'z'), 'STORED\r\n')
    t.assert_equals(
        cg.server:exec(function()
            local state = require('memcached.state')
            return {
                require('memcached').get('cache_c').opts.expire.enabled,
                state.sweepers.cache_c == nil,
            }
        end),
        {false, true}
    )

    -- Options removed from the config fall back to their defaults.
    reload(cg, {
        cache_a = {listen = p[1]},
        cache_c = {listen = p[4]},
    })
    t.assert_equals(client.set(p[1], 'big', '12345'), 'STORED\r\n')
    t.assert_equals(
        cg.server:exec(function()
            local mcc = require('memcached')
            local state = require('memcached.state')
            return {
                mcc.get('cache_a').opts.max_item_size,
                mcc.get('cache_c').opts.expire.enabled,
                state.sweepers.cache_c ~= nil,
            }
        end),
        {1048576, true, true}
    )

    -- A new space_name recreates the instance; the old space is kept.
    local before = cg.server:exec(function()
        return require('memcached').get('cache_a').space_name
    end)
    t.assert_equals(before, '__mcc_cache_a')
    reload(cg, {
        cache_a = {listen = p[1], space_name = 'custom_a'},
    })
    t.assert_equals(client.get(p[1], 'a'), 'END\r\n')
    t.assert_equals(client.set(p[1], 'a', 'new'), 'STORED\r\n')
    t.assert_equals(
        cg.server:exec(function()
            return {
                box.space.custom_a:get('a').value,
                box.space.__mcc_cache_a:get('a').value,
            }
        end),
        {'new', 'x'}
    )
    t.assert_not(client.is_listening(p[4]))

    -- A new storage recreates it as well.
    reload(cg, {
        cache_a = {listen = p[1], space_name = 'custom_a2', storage = 'disk'},
    })
    t.assert_equals(
        cg.server:exec(function()
            return box.space.custom_a2.engine
        end),
        'vinyl'
    )
    t.assert_equals(client.set(p[1], 'd', 'disk'), 'STORED\r\n')
    t.assert_equals(client.get(p[1], 'd'), value_reply('d', 'disk'))
end

-- The instance behind a port: the space a client's set goes to.
local function owner(cg, port)
    local key = 'owner_' .. port
    t.assert_equals(client.set(port, key, tostring(port)), 'STORED\r\n')
    return cg.server:exec(function(key_)
        for _, name in ipairs({'cache_a', 'cache_b', 'cache_c'}) do
            local space = box.space['__mcc_' .. name]
            if space ~= nil and space:get(key_) ~= nil then
                space:delete(key_)
                return name
            end
        end
    end, {key})
end

-- Listen URIs swapped, or moved along a cycle, in one reload; the
-- connections stay with their instances.
g.test_swap_listen = function(cg)
    local p = cg.ports
    local a = client.must_connect(p[1])
    local b = client.must_connect(p[2])
    t.assert_equals(a:set('a', 'x'), 'STORED\r\n')
    t.assert_equals(b:set('b', 'y'), 'STORED\r\n')
    reload(cg, {
        cache_a = {listen = '127.0.0.1:' .. p[2]},
        cache_b = {listen = p[1]},
    })
    t.assert_equals(owner(cg, p[1]), 'cache_b')
    t.assert_equals(owner(cg, p[2]), 'cache_a')
    -- The connections stay.
    t.assert_equals(a:call('get a\r\n', 'END\r\n'), value_reply('a', 'x'))
    t.assert_equals(b:call('get b\r\n', 'END\r\n'), value_reply('b', 'y'))
    t.assert_equals(rh.health(cg.server, 'cache_a'), {status = 'ready'})
    t.assert_equals(rh.health(cg.server, 'cache_b'), {status = 'ready'})

    -- A cycle of three, one of them in the threads.
    local cfg = {
        cache_a = {listen = '127.0.0.1:' .. p[2]},
        cache_b = {listen = '127.0.0.1:' .. p[1], threads = 'fe'},
        cache_c = {listen = p[3]},
    }
    reload(cg, cfg)
    b:close()
    b = client.must_connect(p[1])
    t.assert_equals(b:set('b', 'z'), 'STORED\r\n')
    local c = client.must_connect(p[3])
    t.assert_equals(c:set('c', 'w'), 'STORED\r\n')
    cfg.cache_a.listen = '127.0.0.1:' .. p[3]
    cfg.cache_b.listen = '127.0.0.1:' .. p[2]
    cfg.cache_c.listen = p[1]
    reload(cg, cfg)
    t.assert_equals(owner(cg, p[1]), 'cache_c')
    t.assert_equals(owner(cg, p[2]), 'cache_b')
    t.assert_equals(owner(cg, p[3]), 'cache_a')
    t.assert_equals(a:call('get a\r\n', 'END\r\n'), value_reply('a', 'x'))
    t.assert_equals(b:call('get b\r\n', 'END\r\n'), value_reply('b', 'z'))
    t.assert_equals(c:call('get c\r\n', 'END\r\n'), value_reply('c', 'w'))
    for _, conn in ipairs({a, b, c}) do
        conn:close()
    end
    -- Back to the base config in one reload, too.
    reload(cg, base_cfg(cg))
    t.assert_equals(owner(cg, p[1]), 'cache_a')
    t.assert_equals(owner(cg, p[2]), 'cache_b')
    t.assert_not(client.is_listening(p[3]))
end

-- A swap whose other address is taken: the errors are reported, and each
-- instance keeps listening on its own URI.
g.test_swap_listen_error = function(cg)
    local p = cg.ports
    local busy = socket.tcp_server('127.0.0.1', p[5], function() end)
    local err = reload_error(cg, {
        cache_a = {listen = '127.0.0.1:' .. p[5]},
        cache_b = {listen = p[1]},
    })
    busy:close()
    t.assert_str_contains(err, "memcached instance 'cache_a': can't bind")
    t.assert_str_contains(err, "memcached instance 'cache_b': can't bind")
    t.assert_equals(owner(cg, p[1]), 'cache_a')
    t.assert_equals(owner(cg, p[2]), 'cache_b')
    t.assert_equals(rh.health(cg.server, 'cache_a').status, 'degraded')
    t.assert_equals(rh.health(cg.server, 'cache_b').status, 'degraded')
    -- Once the address is free, a reload applies it.
    reload(cg, {
        cache_a = {listen = '127.0.0.1:' .. p[5]},
        cache_b = {listen = p[1]},
    })
    t.assert_equals(owner(cg, p[5]), 'cache_a')
    t.assert_equals(owner(cg, p[1]), 'cache_b')
    t.assert_equals(rh.health(cg.server, 'cache_a'), {status = 'ready'})
end

-- read_mode is a runtime option: snapshot reads serve plain gets from a
-- read view, back to rw on the next reload.
g.test_reload_read_mode = function(cg)
    local p = cg.ports
    reload(cg, {
        cache_a = {
            listen = p[1],
            read_mode = 'snapshot',
            snapshot_refresh = 0.1,
        },
    })
    t.assert_equals(client.set(p[1], 'rv', '1'), 'STORED\r\n')
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(client.get(p[1], 'rv'), value_reply('rv', '1'))
    end)
    t.assert(
        cg.server:exec(function()
            return require('memcached.state').readviews.cache_a ~= nil
        end)
    )
    reload(cg, {cache_a = {listen = p[1]}})
    t.assert(
        cg.server:exec(function()
            return require('memcached.state').readviews.cache_a == nil
        end)
    )
end

g.test_threads = function(cg)
    local p = cg.ports
    reload(cg, {cache_t = {listen = '127.0.0.1:' .. p[5], threads = 'fe'}})
    t.assert_equals(client.set(p[5], 't', 'thread'), 'STORED\r\n')
    t.assert_equals(client.get(p[5], 't'), value_reply('t', 'thread'))
    t.assert_equals(
        cg.server:exec(function()
            return require('memcached').get('cache_t').fe_group
        end),
        'fe'
    )
    -- Health doesn't yield with threads.
    t.assert_equals(rh.health(cg.server, 'cache_t'), {status = 'ready'})
    cg.server:exec(function()
        rawset(require('memcached').get('cache_t'), 'mcc_test_mark', true)
    end)
    local c = client.must_connect(p[5])
    t.assert_equals(c:set('u', 'x'), 'STORED\r\n')

    -- Without threads the same instance moves back to TX (cfg() with
    -- threads = false), which closes the connections; the data stays.
    reload(cg, {cache_t = {listen = '127.0.0.1:' .. p[5]}})
    t.assert(c:is_closed())
    c:close()
    t.assert_equals(
        cg.server:exec(function()
            local inst = require('memcached').get('cache_t')
            return {
                inst.fe_group == nil,
                inst.opts.threads == nil,
                inst.server.running,
                rawget(inst, 'mcc_test_mark'),
            }
        end),
        {true, true, true, true}
    )
    t.assert_equals(client.get(p[5], 't'), value_reply('t', 'thread'))
    t.assert_equals(rh.health(cg.server, 'cache_t'), {status = 'ready'})
end

g.test_restart_stopped_and_destroyed = function(cg)
    local p = cg.ports
    cg.server:exec(function()
        require('memcached').get('cache_a'):stop()
        require('memcached').destroy('cache_b')
    end)
    t.assert_not(client.is_listening(p[1]))
    t.assert_not(client.is_listening(p[2]))
    local a = rh.health(cg.server, 'cache_a')
    -- 'degraded' when it was ready before, 'not_ready' otherwise.
    t.assert_not_equals(a.status, 'ready')
    t.assert_equals(a.reason, 'stopped')
    t.assert_equals(rh.health(cg.server, 'cache_b').reason, 'destroyed')

    -- A reload brings both back.
    rh.reload(cg.cluster, nil, {cg.server})
    t.assert(client.is_listening(p[1]))
    t.assert(client.is_listening(p[2]))
    t.assert_equals(rh.health(cg.server, 'cache_a'), {status = 'ready'})
    t.assert_equals(rh.health(cg.server, 'cache_b'), {status = 'ready'})
end

g.test_health_check = function(cg)
    t.assert_equals(rh.check_names(cg.server), {
        rh.CHECK .. '.cache_a',
        rh.CHECK .. '.cache_b',
    })
    t.assert_equals(rh.health(cg.server, 'cache_a'), {status = 'ready'})
    t.assert_equals(
        cg.server:exec(function()
            return require('internal.healthcheck').info().readiness.status
        end),
        true
    )

    -- The health check doesn't yield (the healthcheck module checks it,
    -- and so do we).
    t.assert(
        cg.server:exec(function()
            local fiber = require('fiber')
            local role = require('roles.memcached')
            local csw = fiber.self():csw()
            role.health_check()
            return fiber.self():csw() == csw
        end)
    )

    -- The fallback for Tarantool versions without multi_result.
    cg.server:exec(function()
        local role = require('roles.memcached')
        role.internal.multi_result = false
        require('memcached').get('cache_b'):stop()
    end)
    t.assert_equals(rh.check_names(cg.server), {rh.CHECK})
    local check = rh.health(cg.server)
    t.assert_not_equals(check.status, 'ready')
    t.assert_equals(
        check.reason,
        'memcached instances are not ready: cache_b: stopped'
    )
    cg.server:exec(function()
        require('memcached').get('cache_b'):start()
    end)
    t.assert_equals(rh.health(cg.server), {status = 'ready'})
    cg.server:exec(function()
        require('roles.memcached').internal.multi_result = nil
    end)
end

g.test_validation = function(cg)
    local p = cg.ports
    local cases = {
        {
            {a = {listen = p[5]}, b = {listen = p[5]}},
            "instances 'a' and 'b' have the same listen URI '" .. p[5] .. "'",
        },
        {
            {
                a = {listen = '0.0.0.0:' .. p[5]},
                b = {listen = '127.0.0.1:' .. p[5]},
            },
            "instances 'a' and 'b' have overlapping listen URIs",
        },
        {
            {a = {max_connections = 10}},
            "a: Option 'listen' is required",
        },
        {
            {a = {listen = 'unix/:'}},
            'a.listen: Unable to parse the URI',
        },
        {
            {a = {listen = p[5], storage = 'tape'}},
            'a.storage: Got tape',
        },
        {
            {a = {listen = p[5], unknown = 1}},
            'a: Unexpected field "unknown"',
        },
        {
            {a = {listen = p[5], max_item_size = 0}},
            'a.max_item_size: Must be in range [1, 1073741824], got 0',
        },
        {
            {a = {listen = p[5], expire = {batch = 'x'}}},
            'a.expire.batch: Unexpected data for scalar "integer"',
        },
        {
            {a = {listen = p[5], expire = {mode = 'tree'}}},
            'a.expire.mode: Got tree',
        },
        {
            {a = {listen = p[5], storage = 'disk', expire = {mode = 'scan'}}},
            "a: expire: mode 'scan' needs storage = 'memory'",
        },
        {
            -- The default of memory storage is 'scan'.
            {
                a = {listen = p[5]},
                b = {
                    listen = p[6],
                    space_name = '__mcc_a',
                    expire = {mode = 'index'},
                },
            },
            "instances 'a' and 'b' use the space '__mcc_a' with different " .. 'expire.mode',
        },
        {
            {a = {listen = p[5], backend = 'nope'}},
            'a.backend: Unknown backend "nope"',
        },
        {
            {a = {listen = p[5], threads = 'nope'}},
            "a: Application thread group 'nope' does not exist",
        },
        {
            {a = {listen = p[5], read_mode = 'ro'}},
            "a: read_mode 'ro' is not supported by the local backend",
        },
        {
            {a = {listen = p[5], read_mode = 'snapshot', storage = 'disk'}},
            "a: read_mode 'snapshot' needs storage 'memory'",
        },
        {
            {a = {listen = p[5], backend = 'vshard'}},
            "a: backend 'vshard' needs a vshard router",
        },
        {
            {
                a = {listen = p[5]},
                b = {listen = p[6], space_name = '__mcc_a', storage = 'disk'},
            },
            "instances 'a' and 'b' use the space '__mcc_a' with different storage",
        },
        {
            {a = {listen = p[5], track_access = 'no'}},
            'a.track_access: Unexpected data for scalar "boolean"',
        },
        {
            {a = {listen = p[5], idle_timeout = -1}},
            'a.idle_timeout: Must be a number of seconds >= 0, got -1',
        },
        {
            {a = {listen = p[5], commit_wait = 'async'}},
            'a.commit_wait: Got async',
        },
        {
            {a = {listen = p[5], evict = {high = 2}}},
            'a.evict.high: Must be in range (0, 1], got 2',
        },
        {
            {a = {listen = p[5], evict = {samples = 100}}},
            'a.evict.samples: Must be in range [1, 64], got 100',
        },
        {
            {a = {listen = p[5], evict = {low = 0.95}}},
            'a: evict.low (0.95) must be less than evict.high (0.95)',
        },
        {
            {
                a = {listen = p[5]},
                b = {
                    listen = p[6],
                    space_name = '__mcc_a',
                    evict = {enabled = false},
                },
            },
            "instances 'a' and 'b' use the space '__mcc_a' with different "
                .. 'evict options',
        },
    }
    for _, case in ipairs(cases) do
        local err = reload_error(cg, case[1])
        t.assert_str_contains(
            err,
            'Wrong config for role ' .. ROLE .. ': [' .. ROLE .. '] '
        )
        t.assert_str_contains(err, case[2])
    end
    -- The running instances are not affected.
    t.assert_equals(rh.status(cg.server, 'cache_a'), 'running')
    t.assert_equals(client.set(cg.ports[1], 'a', 'x'), 'STORED\r\n')
    t.assert_equals(rh.status(cg.server, 'a'), nil)
end

-- track_access from the config: off, then removed (back to the default).
g.test_track_access = function(cg)
    local cfg = base_cfg(cg)
    cfg.cache_a.track_access = false
    reload(cg, cfg)
    local function fetched()
        return cg.server:exec(function()
            return box.space.__mcc_cache_a:get('k').fetched
        end)
    end
    t.assert_equals(client.set(cg.ports[1], 'k', 'v'), 'STORED\r\n')
    t.assert_equals(client.get(cg.ports[1], 'k'), value_reply('k', 'v'))
    t.assert_equals(fetched(), false)
    reload(cg, base_cfg(cg))
    t.assert_equals(client.get(cg.ports[1], 'k'), value_reply('k', 'v'))
    t.assert_equals(fetched(), true)
end

-- idle_timeout from the config, then removed (back to 0, no timeout).
g.test_idle_timeout = function(cg)
    local cfg = base_cfg(cg)
    cfg.cache_a.idle_timeout = 0.3
    reload(cg, cfg)
    local c = client.must_connect(cg.ports[1])
    t.assert(c:is_closed(2), 'an idle connection is closed')
    c:close()
    t.assert_equals(
        cg.server:exec(function()
            local inst = require('memcached').get('cache_a')
            return {inst.opts.idle_timeout, inst:info().idle_kicks}
        end),
        {0.3, 1}
    )
    reload(cg, base_cfg(cg))
    t.assert_equals(
        cg.server:exec(function()
            return require('memcached').get('cache_a').opts.idle_timeout
        end),
        0
    )
    c = client.must_connect(cg.ports[1])
    t.assert_equals(c.sock:read(1, 0.6), nil, 'no timeout')
    c:close()
end

-- commit_wait from the config: submit, then removed (back to the default).
g.test_commit_wait = function(cg)
    local function mode()
        return cg.server:exec(function()
            local inst = require('memcached').get('cache_a')
            return {inst.opts.commit_wait, inst.backend.ctx.commit_wait}
        end)
    end
    local cfg = base_cfg(cg)
    cfg.cache_a.commit_wait = 'submit'
    reload(cg, cfg)
    t.assert_equals(mode(), {'submit', 'submit'})
    t.assert_equals(client.set(cg.ports[1], 'k', 'v'), 'STORED\r\n')
    t.assert_equals(client.get(cg.ports[1], 'k'), value_reply('k', 'v'))
    reload(cg, base_cfg(cg))
    t.assert_equals(mode(), {'complete', 'complete'})
end

-- An instance that can't start fails the reload; the others are applied.
g.test_apply_error = function(cg)
    local p = cg.ports
    local busy = socket.tcp_server('127.0.0.1', p[5], function() end)
    t.assert(busy)
    local cfg = base_cfg(cg)
    cfg.cache_x = {listen = '127.0.0.1:' .. p[5]}
    cfg.cache_y = {listen = p[6]}
    local err = reload_error(cg, cfg)
    t.assert_str_contains(
        err,
        'Error applying role ' .. ROLE
            .. ": memcached instance 'cache_x': can't bind"
    )
    t.assert_equals(rh.status(cg.server, 'cache_x'), nil)
    t.assert_equals(rh.status(cg.server, 'cache_y'), 'running')
    local x = rh.health(cg.server, 'cache_x')
    t.assert_equals(x.status, 'not_ready')
    t.assert_str_contains(x.reason, "can't bind to 127.0.0.1:" .. p[5])

    -- Once the port is free, a reload creates it.
    busy:close()
    rh.reload(cg.cluster, nil, {cg.server})
    t.assert_equals(rh.status(cg.server, 'cache_x'), 'running')
    t.assert_equals(rh.health(cg.server, 'cache_x'), {status = 'ready'})
    t.assert_equals(client.set(p[5], 'x', '1'), 'STORED\r\n')
end

g.test_not_managed_instance = function(cg)
    local p = cg.ports
    cg.server:exec(function(port)
        require('memcached').create('manual', {listen = port})
    end, {p[5]})
    local cfg = base_cfg(cg)
    cfg.manual = {listen = p[6]}
    local err = reload_error(cg, cfg)
    t.assert_str_contains(
        err,
        "memcached instance 'manual' already exists "
            .. 'and is not managed by '
            .. ROLE
    )
    -- The instance is left alone.
    t.assert(client.is_listening(p[5]))
    t.assert_not(client.is_listening(p[6]))
    cg.server:exec(function()
        require('memcached').destroy('manual')
        box.space.__mcc_manual:drop()
    end)
end

-- The role finds memcached in package.loaded on every call, so it
-- uses the code of the last mcc.reload().
g.test_code_reload = function(cg)
    local p = cg.ports
    t.assert_equals(client.set(p[1], 'a', 'before'), 'STORED\r\n')
    cg.server:exec(function()
        local old = require('memcached')
        old.reload()
        local new = package.loaded.memcached
        t.assert_not_equals(new, old)
        -- Count the calls into the new module.
        rawset(_G, 'mcc_test_creates', 0)
        rawset(_G, 'mcc_test_create', new.create)
        new.create = function(...)
            _G.mcc_test_creates = _G.mcc_test_creates + 1
            return _G.mcc_test_create(...)
        end
    end)
    local cfg = base_cfg(cg)
    cfg.cache_n = {listen = p[6]}
    reload(cg, cfg)
    t.assert_equals(
        cg.server:exec(function()
            return _G.mcc_test_creates
        end),
        1
    )
    t.assert_equals(client.get(p[1], 'a'), value_reply('a', 'before'))
    t.assert_equals(client.set(p[6], 'n', '1'), 'STORED\r\n')
    cg.server:exec(function()
        package.loaded.memcached.create = _G.mcc_test_create
        rawset(_G, 'mcc_test_create', nil)
        rawset(_G, 'mcc_test_creates', nil)
    end)
end

g.test_metrics = function(cg)
    local cfg = base_cfg(cg)
    cfg.cache_m = {listen = cg.ports[5], metrics = true}
    reload(cg, cfg)
    t.assert_equals(client.set(cg.ports[5], 'm', 'v'), 'STORED\r\n')
    t.assert_equals(client.get(cg.ports[5], 'm'), value_reply('m', 'v'))
    local obs = rh.collect(cg.server)
    t.assert_equals(
        rh.value(
            obs,
            'tnt_memcached_hits_total',
            {name = 'cache_m', op = 'get'}
        ),
        1
    )
    t.assert_equals(
        rh.value(
            obs,
            'tnt_memcached_running',
            {name = 'cache_m'}
        ),
        1
    )
    -- Only the instances with metrics = true.
    t.assert_equals(
        rh.series(
            obs,
            'tnt_memcached_running',
            {name = 'cache_a'}
        ),
        {}
    )
    -- metrics: false (the default again) removes the series.
    reload(cg, base_cfg(cg))
    obs = rh.collect(cg.server)
    t.assert_equals(rh.series(obs, 'tnt_memcached_running'), {})
end

g.test_role_removed = function(cg)
    reload(cg, base_cfg(cg), {})
    t.assert_not(client.is_listening(cg.ports[1]))
    t.assert_not(client.is_listening(cg.ports[2]))
    t.assert_equals(rh.status(cg.server, 'cache_a'), nil)
    t.assert_equals(rh.status(cg.server, 'cache_b'), nil)
    t.assert_equals(rh.check_names(cg.server), {})
    t.assert_equals(
        cg.server:exec(function()
            return next(require('memcached.state').instances)
        end),
        nil
    )
    -- Back: the role creates them again, with the data of their spaces.
    reload(cg, base_cfg(cg))
    t.assert_equals(rh.status(cg.server, 'cache_a'), 'running')
end

--------------------------------------------------------------------------------
-- A replicaset: a writable instance and a read-only replica.
--------------------------------------------------------------------------------

local rs = t.group('role_replicaset')

-- Each instance listens on its own ports, so roles_cfg is per instance.
local function replicaset_config(cg, names)
    local function roles_cfg(ports)
        local cfg = {}
        for i, name in ipairs(names) do
            cfg[name] = {listen = '127.0.0.1:' .. ports[i]}
        end
        return {[ROLE] = cfg}
    end
    return cbuilder:new()
        :set_global_option('roles', {ROLE})
        :add_instance('instance-001', {
            database = {mode = 'rw'},
            roles_cfg = roles_cfg(cg.rw_ports),
        })
        :add_instance('instance-002', {
            database = {mode = 'ro'},
            roles_cfg = roles_cfg(cg.ro_ports),
        })
        :config()
end

rs.before_all(function(cg)
    cg.rw_ports = {env.free_port(), env.free_port()}
    cg.ro_ports = {env.free_port(), env.free_port()}
    cg.cluster = rh.start(replicaset_config(cg, {'cache'}))
    cg.rw = cg.cluster['instance-001']
    cg.ro = cg.cluster['instance-002']
end)

rs.after_all(function(cg)
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)

rs.test_replica_serves_reads = function(cg)
    t.assert_equals(client.set(cg.rw_ports[1], 'a', 'x'), 'STORED\r\n')
    t.helpers.retrying({}, function()
        t.assert_equals(client.get(cg.ro_ports[1], 'a'), value_reply('a', 'x'))
    end)
    -- Writes on the replica fail.
    local reply = client.set(cg.ro_ports[1], 'b', 'y')
    t.assert_str_contains(reply, 'SERVER_ERROR')
    t.assert_str_contains(reply, 'read-only')
    t.assert_equals(rh.health(cg.ro, 'cache'), {status = 'ready'})
end

rs.test_replica_waits_for_space = function(cg)
    -- A new instance appears on the replica first: its space doesn't exist
    -- yet, so the replica waits for it.
    rh.reload(cg.cluster, replicaset_config(cg, {'cache', 'late'}), {cg.ro})
    t.assert_equals(rh.status(cg.ro, 'late'), 'waiting')
    t.assert_not(client.is_listening(cg.ro_ports[2]))
    local check = rh.health(cg.ro, 'late')
    t.assert_equals(check.status, 'not_ready')
    t.assert_equals(
        check.reason,
        "waiting for the space '__mcc_late' to be replicated"
    )

    -- The writable instance creates the space, the replica gets it.
    rh.reload(cg.cluster, nil, {cg.rw})
    rh.wait_status(cg.ro, 'late', 'running')
    t.assert_equals(client.set(cg.rw_ports[2], 'l', 'v'), 'STORED\r\n')
    t.helpers.retrying({}, function()
        t.assert_equals(client.get(cg.ro_ports[2], 'l'), value_reply('l', 'v'))
    end)
    t.assert_equals(rh.health(cg.ro, 'late'), {status = 'ready'})
end

--------------------------------------------------------------------------------
-- vshard: 1 router + 2 storage replicasets, configured through the role.
--------------------------------------------------------------------------------

local vg = t.group('role_vshard')

local function vshard_opts(cg, instances)
    return {port = cg.port, instances = instances}
end

local function router_instance(cg, extra)
    local opts = {
        listen = '127.0.0.1:' .. cg.port,
        backend = 'vshard',
        vshard = {timeout = 2},
    }
    for k, v in pairs(extra or {}) do
        opts[k] = v
    end
    return opts
end

local function vshard_reload(cg, instances, roles)
    local opts = vshard_opts(cg, instances)
    opts.roles = roles
    cg.cluster:reload(rh.vshard_config(opts))
end

vg.before_all(function(cg)
    cg.port = env.free_port()
    cg.port2 = env.free_port()
    cg.cluster = rh.vshard_start(
        vshard_opts(cg, {
            [vc.NAME] = router_instance(cg, {metrics = true}),
        })
    )
end)

vg.after_all(function(cg)
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)

vg.before_each(function(cg)
    vshard_reload(cg, {[vc.NAME] = router_instance(cg, {metrics = true})})
end)

vg.test_serve = function(cg)
    local keys = {
        vc.keys_on(cg.cluster, 'rs-1', 1, 'a')[1],
        vc.keys_on(cg.cluster, 'rs-2', 1, 'b')[1],
    }
    for _, key in ipairs(keys) do
        t.assert_equals(client.set(cg.port, key, 'v-' .. key), 'STORED\r\n')
        t.assert_equals(client.get(cg.port, key), value_reply(key, 'v-' .. key))
    end
    t.assert_equals(
        vc.tuple(cg.cluster['s1-a'], keys[1]).value,
        'v-' .. keys[1]
    )
    t.assert_equals(
        vc.tuple(cg.cluster['s2-a'], keys[2]).value,
        'v-' .. keys[2]
    )
    t.assert_equals(vc.tuple(cg.cluster['s1-a'], keys[2]), nil)

    -- The storages serve the space and run no front-end.
    for _, name in ipairs(vc.STORAGES) do
        local server = cg.cluster[name]
        t.assert_equals(rh.status(server, vc.NAME), nil)
        t.assert_equals(vc.space_info(server).storage, 'memory')
        t.assert_equals(rh.health(server, vc.NAME), {status = 'ready'})
    end
    t.assert_equals(rh.status(cg.cluster.router, vc.NAME), 'running')
    t.assert_equals(rh.health(cg.cluster.router, vc.NAME), {status = 'ready'})
    -- The router has no data space.
    t.assert_equals(
        cg.cluster.router:exec(function(space)
            return box.space[space] == nil
        end, {vc.SPACE}),
        true
    )

    -- Metrics: the commands on the router, the space on the storages.
    local obs = rh.collect(cg.cluster.router)
    t.assert_ge(
        rh.value(
            obs,
            'tnt_memcached_hits_total',
            {name = vc.NAME, op = 'get'}
        ),
        2
    )
    t.assert_equals(rh.series(obs, 'tnt_memcached_items'), {})
    local items = 0
    for _, name in ipairs({'s1-a', 's2-a'}) do
        obs = rh.collect(cg.cluster[name])
        items = items
            + rh.value(
                obs,
                'tnt_memcached_storage_items',
                {space = vc.SPACE}
            )
        t.assert_ge(
            rh.value(
                obs,
                'tnt_memcached_storage_requests_total',
                {space = vc.SPACE}
            ),
            2
        )
    end
    t.assert_ge(items, 2)
end

-- track_access of a vshard instance goes to the storages, which run the
-- gets; the router's front-end takes it without complaint.
-- evict goes to the storages; the evictor runs on the masters only.
vg.test_evict = function(cg)
    vshard_reload(cg, {
        [vc.NAME] = router_instance(cg, {
            evict = {high = 0.9, low = 0.8, samples = 5},
        }),
    })
    for _, name in ipairs(vc.STORAGES) do
        local info = vc.space_info(cg.cluster[name])
        t.assert_equals(info.evict, {
            enabled = true,
            high = 0.9,
            low = 0.8,
            samples = 5,
            batch = 100,
        }, name
        )
        t.assert_equals(info.evictor, name ~= 's1-b', name)
    end
    -- The front-end keeps the defaults.
    t.assert_equals(
        cg.cluster.router:exec(function(name)
            return require('memcached').get(name).opts.evict.high
        end, {vc.NAME}),
        0.95
    )
    vshard_reload(cg, {
        [vc.NAME] = router_instance(cg, {
            evict = {enabled = false},
        }),
    })
    t.assert_equals(vc.space_info(cg.cluster['s1-a']).evictor, false)
    local c = client.must_connect(cg.port)
    c:send('stats settings\r\n')
    t.assert_str_contains(c:read_until('END\r\n'), 'STAT evictions off\r\n')
    c:close()
    vshard_reload(cg, {[vc.NAME] = router_instance(cg)})
    t.assert_equals(vc.space_info(cg.cluster['s1-a']).evictor, true)
end

vg.test_track_access = function(cg)
    vshard_reload(cg, {[vc.NAME] = router_instance(cg, {track_access = false})})
    local key = vc.keys_on(cg.cluster, 'rs-1', 1, 'ta')[1]
    for _, name in ipairs(vc.STORAGES) do
        t.assert_equals(vc.space_info(cg.cluster[name]).track_access, false)
    end
    t.assert_equals(rh.status(cg.cluster.router, vc.NAME), 'running')
    t.assert_equals(client.set(cg.port, key, 'v'), 'STORED\r\n')
    t.assert_equals(client.get(cg.port, key), value_reply(key, 'v'))
    t.assert_equals(vc.tuple(cg.cluster['s1-a'], key).fetched, false)

    vshard_reload(cg, {[vc.NAME] = router_instance(cg)})
    t.assert_equals(vc.space_info(cg.cluster['s1-a']).track_access, true)
    t.assert_equals(client.get(cg.port, key), value_reply(key, 'v'))
    t.assert_equals(vc.tuple(cg.cluster['s1-a'], key).fetched, true)

    -- Instances sharing a space must agree on it.
    local opts = vshard_opts(cg, {
        [vc.NAME] = router_instance(cg),
        r2 = {
            listen = '127.0.0.1:' .. cg.port2,
            backend = 'vshard',
            space_name = vc.SPACE,
            track_access = false,
        },
    })
    t.assert_str_contains(
        rh.reload_error(cg.cluster, rh.vshard_config(opts), cg.cluster.router),
        'with different track_access'
    )
end

-- commit_wait of a vshard instance goes to the storages, which run the
-- writes; the router's front-end keeps the default. The replica gets the
-- writes as usual.
vg.test_commit_wait = function(cg)
    local function frontend_mode()
        return cg.cluster.router:exec(function(name)
            return require('memcached').get(name).opts.commit_wait
        end, {vc.NAME})
    end
    vshard_reload(cg, {
        [vc.NAME] = router_instance(
            cg,
            {commit_wait = 'submit'}
        ),
    })
    for _, name in ipairs(vc.STORAGES) do
        t.assert_equals(vc.space_info(cg.cluster[name]).commit_wait, 'submit')
    end
    t.assert_equals(rh.status(cg.cluster.router, vc.NAME), 'running')
    t.assert_equals(frontend_mode(), 'complete')
    local key = vc.keys_on(cg.cluster, 'rs-1', 1, 'cw')[1]
    t.assert_equals(client.set(cg.port, key, 'v'), 'STORED\r\n')
    t.assert_equals(client.get(cg.port, key), value_reply(key, 'v'))
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals((vc.tuple(cg.cluster['s1-b'], key) or {}).value, 'v')
    end)

    vshard_reload(cg, {[vc.NAME] = router_instance(cg)})
    t.assert_equals(
        vc.space_info(cg.cluster['s1-a']).commit_wait,
        'complete'
    )

    -- Instances sharing a space must agree on it.
    local opts = vshard_opts(cg, {
        [vc.NAME] = router_instance(cg),
        r2 = {
            listen = '127.0.0.1:' .. cg.port2,
            backend = 'vshard',
            space_name = vc.SPACE,
            commit_wait = 'none',
        },
    })
    t.assert_str_contains(
        rh.reload_error(cg.cluster, rh.vshard_config(opts), cg.cluster.router),
        'with different commit_wait'
    )
end

-- A new vshard instance with its own space: the storages create it, the
-- router serves it; removed, the storages stop serving it.
vg.test_reload_instances = function(cg)
    local instances = {
        [vc.NAME] = router_instance(cg),
        r2 = {
            listen = '127.0.0.1:' .. cg.port2,
            backend = 'vshard',
            space_name = 'mcc_r2',
            storage = 'disk',
            expire = {enabled = false},
        },
    }
    vshard_reload(cg, instances)
    for _, name in ipairs({'s1-a', 's2-a'}) do
        t.assert_equals(
            vc.space_info(cg.cluster[name], 'mcc_r2').storage,
            'disk'
        )
        t.assert_equals(
            cg.cluster[name]:exec(function()
                return box.space.mcc_r2.engine
            end),
            'vinyl'
        )
    end
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(
            vc.space_info(cg.cluster['s1-b'], 'mcc_r2').ready,
            true
        )
    end)
    rh.wait_status(cg.cluster.router, 'r2', 'running')
    local keys = {
        vc.keys_on(cg.cluster, 'rs-1', 1, 'x')[1],
        vc.keys_on(cg.cluster, 'rs-2', 1, 'y')[1],
    }
    for _, key in ipairs(keys) do
        t.assert_equals(client.set(cg.port2, key, '2'), 'STORED\r\n')
        t.assert_equals(client.get(cg.port2, key), value_reply(key, '2'))
        -- Another space than the first instance's.
        t.assert_equals(client.get(cg.port, key), 'END\r\n')
    end

    vshard_reload(cg, {[vc.NAME] = router_instance(cg)})
    t.assert_equals(rh.status(cg.cluster.router, 'r2'), nil)
    t.assert_not(client.is_listening(cg.port2))
    for _, name in ipairs(vc.STORAGES) do
        t.assert_equals(vc.space_info(cg.cluster[name], 'mcc_r2'), nil)
        t.assert_equals(
            rh.check_names(cg.cluster[name]),
            {rh.CHECK .. '.' .. vc.NAME}
        )
    end
end

-- Without the role on the storages, the routers' requests fail.
vg.test_role_removed_on_storages = function(cg)
    local key = vc.keys_on(cg.cluster, 'rs-1', 1, 'z')[1]
    t.assert_equals(client.set(cg.port, key, '1'), 'STORED\r\n')
    vshard_reload(
        cg,
        {[vc.NAME] = router_instance(cg)},
        {storages = {}, routers = {ROLE}}
    )
    for _, name in ipairs(vc.STORAGES) do
        t.assert_equals(
            cg.cluster[name]:exec(function()
                local g_ = rawget(_G, 'mcc')
                return g_ ~= nil and g_.storage_exec ~= nil
            end),
            false
        )
    end
    local c = client.must_connect(cg.port)
    t.assert_str_contains(c:call('get ' .. key .. '\r\n'), 'SERVER_ERROR')
    c:close()
    -- Back: the data was kept.
    vshard_reload(cg, {[vc.NAME] = router_instance(cg)})
    t.assert_equals(client.get(cg.port, key), value_reply(key, '1'))
end
