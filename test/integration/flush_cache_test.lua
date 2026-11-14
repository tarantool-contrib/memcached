-- The cached flush time (schema.flush_time()): every change of _mcc_meta
-- is seen by the next batch, whoever makes it: a flush in another fiber, a
-- direct write, a rolled back transaction, truncate, drop, replication.

local fiber = require('fiber')
local t = require('luatest')
local server = require('luatest.server')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local g = t.group('flush_cache')

local SPACE = 'mcc_flush_cache_test'
local CTX = {space_name = SPACE}

local engine
local schema
local state

g.before_all(function()
    env.box_cfg()
    engine = require('memcached.engine')
    schema = require('memcached.schema')
    state = require('memcached.state')
end)

g.before_each(function()
    schema.ensure(SPACE, 'memory')
    box.space[SPACE]:truncate()
    box.space._mcc_meta:delete(schema.flush_key(SPACE))
end)

g.after_each(function()
    -- A test may drop the meta space.
    schema.create_meta()
end)

g.after_all(function()
    env.drop_spaces({SPACE})
    if box.space._mcc_meta ~= nil then
        box.space._mcc_meta:delete(schema.flush_key(SPACE))
    end
end)

local function one(req)
    return engine.exec(CTX, {req})[1]
end

local function set(key)
    return one({
        op = 'store',
        mode = 'set',
        key = key,
        value = 'v',
        flags = 0,
        exptime = 0,
    }).status
end

local function get(key)
    return one({op = 'get', key = key}).status
end

-- The cached flush time of the space: nil if not cached, false if there is
-- no flush.
local function cached()
    local times = state.flush_cache.times
    if times == nil then
        return nil
    end
    return times[SPACE]
end

-- A flush time after every item stored so far, and not in the future (a
-- stamp may be ahead of fiber.time() within an event loop iteration). It
-- yields: not inside a transaction.
local function flush_stamp()
    local stamp = engine.stamp()
    while fiber.time() < stamp do
        fiber.sleep(0.001)
    end
    return stamp
end

g.test_cached = function()
    t.assert_equals(set('a'), 'STORED')
    t.assert_equals(get('a'), 'HIT')
    -- No flush: cached as false.
    t.assert_equals(cached(), false)
    t.assert_equals(schema.flush_time(SPACE), nil)
    -- A flush in the batch: the batch sees it at once, the next batch
    -- reads it again.
    local resps = engine.exec(CTX, {
        {op = 'get', key = 'a'},
        {op = 'flush', delay = 0},
        {op = 'get', key = 'a'},
    })
    t.assert_equals(resps[1].status, 'HIT')
    t.assert_equals(resps[3].status, 'MISS')
    local flush = box.space._mcc_meta:get(schema.flush_key(SPACE))[2]
    t.assert_equals(schema.flush_time(SPACE), flush)
    t.assert_equals(cached(), flush)
end

g.test_other_fiber = function()
    t.assert_equals(set('a'), 'STORED')
    t.assert_equals(get('a'), 'HIT')
    t.assert_equals(cached(), false)
    -- flush_all through another context in another fiber.
    local f = fiber.new(function()
        return engine.exec({space_name = SPACE}, {{op = 'flush', delay = 0}})
    end)
    f:set_joinable(true)
    local ok, resps = f:join()
    t.assert(ok, resps)
    t.assert_equals(resps[1], {status = 'OK'})
    t.assert_equals(get('a'), 'MISS')
end

-- A write to _mcc_meta that doesn't come from the engine: what replication
-- or an application does.
g.test_direct_write = function()
    local meta = box.space._mcc_meta
    local key = schema.flush_key(SPACE)
    t.assert_equals(set('a'), 'STORED')
    t.assert_equals(get('a'), 'HIT')
    t.assert_equals(cached(), false)

    local flush = flush_stamp()
    meta:replace({key, flush})
    -- Emptied at once, by the trigger.
    t.assert_equals(cached(), nil)
    t.assert_equals(schema.flush_time(SPACE), flush)
    t.assert_equals(get('a'), 'MISS')

    -- Removed: no flush any more.
    t.assert_equals(set('b'), 'STORED')
    t.assert_equals(cached(), flush)
    meta:delete(key)
    t.assert_equals(schema.flush_time(SPACE), nil)
    t.assert_equals(get('b'), 'HIT')

    -- Back, then updated in place.
    meta:replace({key, flush_stamp()})
    t.assert_equals(get('b'), 'MISS')
    t.assert_equals(set('c'), 'STORED')
    meta:update(key, {{'=', 2, flush_stamp()}})
    t.assert_equals(get('c'), 'MISS')
end

g.test_rollback = function()
    local meta = box.space._mcc_meta
    local key = schema.flush_key(SPACE)
    t.assert_equals(set('a'), 'STORED')
    t.assert_equals(get('a'), 'HIT')
    -- The transaction sees its own flush; the cache must not keep it after
    -- the rollback.
    local flush = flush_stamp()
    box.begin()
    meta:replace({key, flush})
    t.assert_equals(schema.flush_time(SPACE), flush)
    t.assert_equals(cached(), flush)
    box.rollback()
    t.assert_equals(cached(), nil)
    t.assert_equals(schema.flush_time(SPACE), nil)
    t.assert_equals(get('a'), 'HIT')

    -- Another fiber reads while the commit waits for the WAL (without MVCC
    -- it sees the change before it is committed): what it cached is
    -- dropped when the commit completes.
    flush = flush_stamp()
    local committed = false
    local seen, during
    -- Runs at the first yield: the WAL write of the commit.
    local reader = fiber.new(function()
        during = not committed
        seen = schema.flush_time(SPACE)
    end)
    reader:set_joinable(true)
    box.begin()
    meta:replace({key, flush})
    box.commit()
    committed = true
    reader:join()
    t.assert(seen == nil or seen == flush, seen)
    if box.cfg.wal_mode ~= 'none' then
        t.assert(during)
    end
    if during then
        t.assert_equals(cached(), nil)
    end
    t.assert_equals(schema.flush_time(SPACE), flush)
    t.assert_equals(get('a'), 'MISS')
end

g.test_truncate = function()
    t.assert_equals(set('a'), 'STORED')
    box.space._mcc_meta:replace({schema.flush_key(SPACE), flush_stamp()})
    t.assert_equals(get('a'), 'MISS')
    t.assert_equals(set('b'), 'STORED')
    t.assert_not_equals(cached(), nil)
    -- truncate fires no on_replace trigger of the space.
    box.space._mcc_meta:truncate()
    t.assert_equals(schema.flush_time(SPACE), nil)
    t.assert_equals(get('b'), 'HIT')
end

g.test_drop = function()
    t.assert_equals(set('a'), 'STORED')
    box.space._mcc_meta:replace({schema.flush_key(SPACE), flush_stamp()})
    t.assert_equals(set('b'), 'STORED')
    t.assert_equals(get('a'), 'MISS')
    t.assert_equals(get('b'), 'HIT')
    t.assert_not_equals(cached(), nil)
    box.space._mcc_meta:drop()
    t.assert_equals(schema.flush_time(SPACE), nil)
    t.assert_equals(set('c'), 'STORED')
    -- Created again: the trigger is named, so it applies to the new space.
    local meta = schema.create_meta()
    t.assert_equals(get('c'), 'HIT')
    meta:replace({schema.flush_key(SPACE), flush_stamp()})
    t.assert_equals(get('c'), 'MISS')
end

-- The cache doesn't depend on the module copy: after a reload the triggers
-- of the new code empty the same cache.
g.test_reload = function()
    t.assert_equals(set('a'), 'STORED')
    t.assert_equals(get('a'), 'HIT')
    local mcc = require('memcached')
    mcc.reload()
    engine = require('memcached.engine')
    schema = require('memcached.schema')
    t.assert_equals(get('a'), 'HIT')
    box.space._mcc_meta:replace({schema.flush_key(SPACE), flush_stamp()})
    t.assert_equals(get('a'), 'MISS')
end

--
-- Replication: a flush applied by the applier is seen by the next get.
--

local r = t.group('flush_cache_replica')

r.before_all(function(cg)
    cg.master = server:new({alias = 'mcc_fc_master', env = env.lua_env()})
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
        alias = 'mcc_fc_replica',
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

local function create(srv, port)
    return srv:exec(function(port_)
        local mcc = require('memcached')
        return mcc.create('fc', {listen = '127.0.0.1:' .. port_}).status
    end, {port})
end

r.test_replicated_flush = function(cg)
    t.assert_equals(create(cg.master, cg.master_port), 'running')
    cg.replica:wait_for_vclock_of(cg.master)
    t.assert_equals(create(cg.replica, cg.replica_port), 'running')
    local m = client.must_connect(cg.master_port)
    local rc = client.must_connect(cg.replica_port)
    for round = 1, 3 do
        local key = 'k' .. round
        t.assert_equals(m:set(key, 'x'), 'STORED\r\n')
        cg.replica:wait_for_vclock_of(cg.master)
        -- The replica has cached the flush time of its space.
        t.assert_equals(rc:get(key), 'x')
        t.assert_equals(m:call('flush_all\r\n'), 'OK\r\n')
        local flush = cg.master:exec(function()
            return box.space._mcc_meta:get('flush:__mcc_fc')[2]
        end)
        -- Once the row is applied, the very next get sees it: no retries.
        cg.replica:wait_for_vclock_of(cg.master)
        t.assert_equals(
            cg.replica:exec(function()
                return require('memcached.schema').flush_time('__mcc_fc')
            end),
            flush
        )
        t.assert_equals(rc:get(key), nil)
        t.assert_equals(m:get(key), nil)
    end
    m:close()
    rc:close()
end
