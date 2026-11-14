-- The group of the C batch (memcached/datapath.c, "The group": the writes
-- of consecutive commands in one transaction) in child servers, where the
-- box configuration can make it fail:
-- - a commit that fails after it yielded: a synchronous transaction ahead
--   of the group in the queue times out, and its rollback takes the
--   transactions behind it. (The WAL error injections, ERRINJ_WAL_IO and
--   the like, are compiled out of the release builds the tests run on.)
-- - out of memory, without eviction: the C path and the Lua engine answer
--   byte for byte the same.

local fio = require('fio')
local t = require('luatest')
local server = require('luatest.server')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local NAME = 'grp'
local SPACE = '__mcc_' .. NAME

local function set_cmd(key, value, extra)
    return ('set %s 0 0 %d%s\r\n%s\r\n'):format(key, #value, extra or '', value)
end

-- The replies to data, up to the reply of an mn sent after it.
local function call(c, data)
    c:send(data .. 'mn\r\n')
    local reply = c:read_until('MN\r\n')
    t.assert(reply, 'no reply to ' .. data)
    return reply:sub(1, -5)
end

local function skip_unless_built()
    t.skip_if(
        not fio.path.exists(env.datapath_so),
        env.datapath_so .. ' is not built'
    )
end

local function start(alias, box_cfg)
    local srv = server:new({
        alias = alias,
        env = env.lua_env(),
        box_cfg = box_cfg,
    })
    srv:start()
    return srv
end

local function create(srv, port, datapath, opts)
    srv:exec(function(name, port_, on, opts_)
        local mcc = require('memcached')
        mcc.datapath(on)
        opts_.listen = '127.0.0.1:' .. port_
        opts_.expire = {enabled = false}
        mcc.create(name, opts_)
    end, {NAME, port, datapath, opts or {}})
end

-- The items, without their CAS and created stamp (a group whose commit
-- failed after a yield doesn't give back the values it took), exptime and
-- last access (the clock is the real one).
local function items(srv)
    return srv:exec(function(space)
        local res = {}
        for _, tuple in box.space[space]:pairs() do
            local item = tuple:totable()
            item[4], item[5], item[6], item[7] = 0, 0, 0, 0
            res[item[1]] = item
        end
        return res
    end, {SPACE})
end

--
-- A failed commit.
--

local f = t.group('group_failed_commit')

f.before_each(function(cg)
    skip_unless_built()
    cg.server = start('mcc_group_sync', {
        replication_synchro_quorum = 2,
        replication_synchro_timeout = 0.5,
    })
    cg.port = env.free_port()
    cg.server:exec(function()
        box.ctl.promote()
        local sync = box.schema.space.create('sync', {is_sync = true})
        sync:create_index('pk')
    end)
end)

f.after_each(function(cg)
    if cg.server ~= nil then
        cg.server:drop()
    end
end)

local SETUP = set_cmd('a', '1') .. set_cmd('n', '100') .. set_cmd('d', 'x')
-- Every kind of write the C batch makes, and reads of them.
local PIPELINE = set_cmd('a', '2') .. 'get a\r\nincr n 1\r\n'
    .. 'append a 0 0 1\r\nx\r\nms m 1 q\r\nv\r\nmg m v\r\ndelete d\r\n'
    .. set_cmd('b', '3', ' noreply') .. 'touch n 100\r\nget a b d n\r\n'
local REPLIES = 'STORED\r\nVALUE a 0 1\r\n2\r\nEND\r\n101\r\nSTORED\r\n'
    .. 'VA 1\r\nv\r\nDELETED\r\nTOUCHED\r\n'
    .. 'VALUE a 0 2\r\n2x\r\nVALUE b 0 1\r\n3\r\nVALUE n 0 3\r\n101\r\nEND\r\n'

-- With a synchronous transaction waiting for its quorum ahead of it, the
-- group of the pipeline waits in the queue behind it; another connection
-- reads the group's writes meanwhile, as it reads an autocommit write that
-- waits for the WAL. The synchronous transaction times out and the group
-- is rolled back with it: the Lua front-end then serves every command of
-- the group, each in its own transaction, which commits now. The replies
-- are those of the pipeline on its own, on either path.
f.test_rolled_back = function(cg)
    local results = {}
    for _, datapath in ipairs({true, false}) do
        create(cg.server, cg.port, datapath)
        local c = client.must_connect(cg.port)
        t.assert_equals(call(c, SETUP), ('STORED\r\n'):rep(3))
        cg.server:exec(function(space)
            local fiber = require('fiber')
            rawset(_G, 'mcc_writes', 0)
            box.space[space]:on_replace(function()
                rawset(_G, 'mcc_writes', rawget(_G, 'mcc_writes') + 1)
            end)
            rawset(
                _G, 'mcc_sync',
                fiber.new(function()
                    return pcall(box.space.sync.replace, box.space.sync, {1})
                end)
            )
            rawget(_G, 'mcc_sync'):set_joinable(true)
            fiber.yield()
            assert(box.info.synchro.queue.len == 1)
        end, {SPACE})
        c:send(PIPELINE .. 'mn\r\n')
        -- The dirty reads of another connection while the commit waits.
        local reader = client.must_connect(cg.port)
        local seen = reader:call('get a\r\n', 'END\r\n')
        reader:close()
        local reply = c:read_until('MN\r\n')
        t.assert(reply, 'no reply')
        local sync_ok, writes = cg.server:exec(function()
            local _, ok = rawget(_G, 'mcc_sync'):join()
            return ok, rawget(_G, 'mcc_writes')
        end)
        t.assert_equals(sync_ok, false)
        results[datapath] = {
            reply = reply:sub(1, -5),
            items = items(cg.server),
            writes = writes,
            seen = seen,
        }
        c:close()
        cg.server:exec(function(name, space)
            require('memcached').destroy(name)
            box.space[space]:drop()
        end, {NAME, SPACE})
    end
    local C = results[true]
    t.assert_equals(C.reply, REPLIES)
    -- The group's writes were visible before its commit.
    t.assert_equals(C.seen, 'VALUE a 0 2\r\n2x\r\nEND\r\n')
    -- The group's 9 writes (sets, append, incr, delete, touch, the last
    -- access of the get) were made twice: in C, then in Lua.
    t.assert_ge(C.writes, 18)
    -- The Lua engine: the first write waits and fails, the others commit.
    local L = results[false]
    t.assert_equals(
        (L.reply:gsub('SERVER_ERROR [^\r]*', 'SERVER_ERROR')),
        'SERVER_ERROR\r\nVALUE a 0 1\r\n1\r\nEND\r\n101\r\nSTORED\r\n'
            .. 'VA 1\r\nv\r\nDELETED\r\nTOUCHED\r\nVALUE a 0 2\r\n1x\r\n'
            .. 'VALUE b 0 1\r\n3\r\nVALUE n 0 3\r\n101\r\nEND\r\n'
    )
    t.assert_equals(C.items.n, L.items.n)
    t.assert_equals(C.items.m, L.items.m)
    t.assert_equals(C.items.d, nil)
    t.assert_equals(C.items.a[2], '2x')
end

--
-- Out of memory.
--

-- memtx raises anything smaller to 64 MiB.
local MEMORY = 64 * 1024 * 1024

local m = t.group('group_no_memory')

m.before_each(function(cg)
    skip_unless_built()
    cg.servers = {}
    for _, datapath in ipairs({true, false}) do
        local srv = start(
            'mcc_group_oom_' .. tostring(datapath),
            {memtx_memory = MEMORY}
        )
        local port = env.free_port()
        create(srv, port, datapath, {evict = {enabled = false}})
        cg.servers[datapath] = {server = srv, port = port}
    end
end)

m.after_each(function(cg)
    for _, s in pairs(cg.servers or {}) do
        s.server:drop()
    end
end)

-- Pipelines of new items until the memory is full and after: a write
-- that fails with an out of memory leaves the group (the commands before
-- it commit), goes to the Lua engine and answers as there, and the
-- commands after it are served in C again. (The Lua engine's own replies
-- near the limit differ from the C path's by a few items either way, the
-- groups or not: memtx fills differently. So the replies are checked
-- against the items, not against the other path.)
m.test_pipelines = function(cg)
    local value = ('v'):rep(4000)
    local NOMEM_STORE = 'SERVER_ERROR out of memory storing object\r\n'
    local NOMEM = 'SERVER_ERROR out of memory\r\n'
    for datapath, s in pairs(cg.servers) do
        local c = client.must_connect(s.port)
        t.assert_equals(call(c, set_cmd('n', '1000000')), 'STORED\r\n')
        local stored, failed, incrs, full = {}, 0, 0, 0
        for i = 1, 400 do
            local p, keys = {}, {}
            for j = 1, 50 do
                local k = 'k' .. (i * 50 + j)
                keys[j] = k
                p[#p + 1] = set_cmd(k, value)
                if j % 10 == 0 then
                    p[#p + 1] = 'incr n 1\r\ntouch ' .. k .. ' 0\r\n'
                end
            end
            c:send(table.concat(p))
            local errors = false
            for j = 1, 50 do
                local line = c:line()
                if line == 'STORED\r\n' then
                    stored[keys[j]] = true
                else
                    t.assert_equals(line, NOMEM_STORE)
                    failed = failed + 1
                    errors = true
                end
                if j % 10 == 0 then
                    line = c:line()
                    if line == NOMEM then
                        errors = true
                    else
                        t.assert_str_matches(line, '%d+\r\n')
                        incrs = incrs + 1
                    end
                    line = c:line()
                    t.assert(
                        line == NOMEM or line
                                == (stored[keys[j]] and 'TOUCHED\r\n'
                                    or 'NOT_FOUND\r\n'),
                        line
                    )
                end
            end
            if errors then
                full = full + 1
                if full == 5 then
                    break
                end
            end
        end
        c:close()
        t.assert_equals(full, 5)
        -- Every item acknowledged is there, no other one is.
        local found, n, store_no_memory = s.server:exec(function(space, name)
            local res = {}
            for _, tuple in box.space[space]:pairs() do
                res[tuple[1]] = true
            end
            local st = require('memcached').get(name):info()
            return res, box.space[space]:get('n')[2], st.store_no_memory
        end, {SPACE, NAME})
        found.n = nil
        t.assert_equals(found, stored, 'datapath ' .. tostring(datapath))
        t.assert_equals(tonumber(n), 1000000 + incrs)
        t.assert_equals(store_no_memory, failed)
    end
end
