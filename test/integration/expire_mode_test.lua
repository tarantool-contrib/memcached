-- Expiration modes (expire.mode, DESIGN.md "Expiration"): 'index' keeps the
-- exptime index on the data space and the sweeper walks it; 'scan' (the
-- default of memory storage) has no such index, and the sweeper scans the
-- space with a cursor. Both delete what expires and what a flush_all kills, so
-- its memory goes back; an existing space switches modes; vinyl keeps the
-- index.

local fiber = require('fiber')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local NAME = 'xm'
local SPACE = '__mcc_xm'
local OTHER = '__mcc_xm_other'

local function cleanup(cg)
    for _, name in ipairs({NAME, NAME .. '2'}) do
        if cg.mcc.get(name) ~= nil then
            cg.mcc.get(name):destroy()
        end
    end
    if box.info.ro then
        box.cfg {read_only = false}
    end
    env.drop_spaces({SPACE, OTHER})
    if box.space._mcc_meta ~= nil then
        box.space._mcc_meta:delete('flush:' .. SPACE)
        box.space._mcc_meta:delete('flush:' .. OTHER)
    end
end

-- Store n items key<i> with an exptime, pipelined.
local function fill(c, prefix, n, exptime, value)
    value = value or string.rep('v', 100)
    local buf = {}
    for i = 1, n do
        table.insert(
            buf,
            ('set %s%d 0 %d %d noreply\r\n%s\r\n'):format(
                prefix,
                i,
                exptime,
                #value,
                value
            )
        )
    end
    c:send(table.concat(buf))
    t.assert_equals(c:call('mn\r\n'), 'MN\r\n')
end

local function sweeper(name)
    return require('memcached.state').sweepers[name]
end

-- Both modes ---------------------------------------------------------------

local g = t.group('expire_mode', {{mode = 'index'}, {mode = 'scan'}})

g.before_all(function(cg)
    env.box_cfg()
    cg.mcc = require('memcached')
end)

g.before_each(function(cg)
    cleanup(cg)
    cg.port = env.free_port()
    cg.inst = cg.mcc.create(NAME, {
        listen = '127.0.0.1:' .. cg.port,
        expire = {mode = cg.params.mode},
    })
    cg.c = client.must_connect(cg.port)
end)

g.after_each(function(cg)
    cg.c:close()
    cleanup(cg)
end)

g.test_indexes = function(cg)
    local space = box.space[SPACE]
    t.assert_not_equals(space.index.primary, nil)
    t.assert_equals(space.index.exptime ~= nil, cg.params.mode == 'index')
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(sweeper(NAME).mode, cg.params.mode)
    end)
end

-- Expired items are deleted without anyone touching them, and their memory
-- goes back.
g.test_expired_reclaimed = function(cg)
    local space = box.space[SPACE]
    fill(cg.c, 'live', 200, 0)
    local live = space:bsize()
    fill(cg.c, 'ttl', 2000, 1)
    t.assert_equals(space:len(), 2200)
    t.assert_gt(space:bsize(), live * 5)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(space:len(), 200)
        t.assert_ge(cg.inst:info().expired, 2000)
    end)
    t.assert_equals(space:bsize(), live)
    t.assert_equals(cg.c:get('live1'), string.rep('v', 100))
    if cg.params.mode == 'scan' then
        t.assert_ge(cg.inst:info().checked, 2000)
    else
        t.assert_equals(cg.inst:info().checked, 0)
    end
end

-- An immediate and a delayed flush_all: the items stored before the flush
-- time are deleted once it comes; the later ones stay.
g.test_flush_reclaimed = function(cg)
    local space = box.space[SPACE]
    local c = cg.c
    fill(c, 'a', 500, 0)
    t.assert_equals(c:call('flush_all\r\n'), 'OK\r\n')
    t.assert_equals(c:set('after', 'x'), 'STORED\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(space:len(), 1)
        t.assert_ge(cg.inst:info().flushed, 500)
    end)

    fill(c, 'b', 500, 0)
    t.assert_equals(c:call('flush_all 1\r\n'), 'OK\r\n')
    -- Still there until the flush time.
    t.assert_equals(c:get('b1'), string.rep('v', 100))
    t.assert_equals(space:len(), 501)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(space:len(), 0)
        t.assert_ge(cg.inst:info().flushed, 1001)
    end)
    t.assert_equals(c:set('later', 'y'), 'STORED\r\n')
    fiber.sleep(0.3)
    t.assert_equals(c:get('later'), 'y')
end

-- A read-only instance deletes nothing; dead items are not returned.
g.test_read_only = function(cg)
    local space = box.space[SPACE]
    fill(cg.c, 'ttl', 100, 1)
    box.cfg {read_only = true}
    fiber.sleep(1.5)
    t.assert_equals(cg.c:get('ttl1'), nil)
    t.assert_equals(space:len(), 100)
    box.cfg {read_only = false}
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(space:len(), 0)
    end)
end

-- Options, defaults, switching ----------------------------------------------

local s = t.group('expire_mode_switch')

s.before_all(function(cg)
    env.box_cfg()
    cg.mcc = require('memcached')
end)

s.before_each(function(cg)
    cleanup(cg)
    cg.port = env.free_port()
    cg.listen = '127.0.0.1:' .. cg.port
end)

s.after_each(function(cg)
    if cg.c ~= nil then
        cg.c:close()
        cg.c = nil
    end
    cleanup(cg)
end)

s.test_defaults_and_validation = function(cg)
    local schema = require('memcached.schema')
    t.assert_equals(schema.expire_mode('memory'), 'scan')
    t.assert_equals(schema.expire_mode('disk'), 'index')
    t.assert_equals(schema.expire_mode('memory', 'index'), 'index')

    cg.mcc.create(NAME, {listen = cg.listen})
    t.assert_equals(box.space[SPACE].index.exptime, nil)
    cg.mcc.get(NAME):destroy()
    box.space[SPACE]:drop()

    cg.mcc.create(NAME, {listen = cg.listen, storage = 'disk'})
    t.assert_equals(box.space[SPACE].engine, 'vinyl')
    t.assert_not_equals(box.space[SPACE].index.exptime, nil)
    cg.mcc.get(NAME):destroy()
    box.space[SPACE]:drop()

    local cases = {
        {
            {expire = {mode = 'tree'}},
            "Bad value for option 'expire': 'mode' must be 'index' or 'scan'",
        },
        {
            {storage = 'disk', expire = {mode = 'scan'}},
            "Bad value for option 'expire': mode 'scan' needs storage = "
                .. "'memory': vinyl keeps the exptime index",
        },
    }
    for _, case in ipairs(cases) do
        local opts = case[1]
        opts.listen = cg.listen
        t.assert_error_msg_equals(case[2], cg.mcc.check, NAME, opts)
        t.assert_error_msg_equals(case[2], cg.mcc.create, NAME, opts)
    end
    t.assert_equals(box.space[SPACE], nil)
    -- cfg() checks it against the storage too.
    local inst = cg.mcc.create(NAME, {listen = cg.listen, storage = 'disk'})
    t.assert_error_msg_contains(
        "mode 'scan' needs storage = 'memory'",
        inst.cfg,
        inst,
        {expire = {mode = 'scan'}}
    )
    t.assert_equals(inst.opts.expire.mode, nil)

    -- The schema API.
    t.assert_error_msg_contains(
        "Space '" .. OTHER .. "': mode 'scan' needs storage = 'memory'",
        schema.create,
        OTHER,
        'disk',
        {expire = 'scan'}
    )
    t.assert_equals(box.space[OTHER], nil)
    t.assert_equals(schema.create(OTHER, 'memory').index.exptime, nil)
    box.space[OTHER]:drop()
    t.assert_not_equals(
        schema.create(OTHER, 'memory', {expire = 'index'}).index.exptime,
        nil
    )
    -- Without a mode an existing space keeps its indexes.
    t.assert_not_equals(schema.ensure(OTHER, 'memory').index.exptime, nil)
    t.assert_not_equals(schema.create(OTHER, 'memory').index.exptime, nil)
    t.assert_equals(
        schema.ensure(OTHER, 'memory', {expire = 'scan'}).index.exptime,
        nil
    )
    t.assert_equals(schema.ensure(OTHER, 'memory').index.exptime, nil)
end

-- cfg() switches the mode of a running instance: the index is dropped or
-- created, the data is kept, and the sweeper follows.
s.test_switch_running = function(cg)
    local inst = cg.mcc.create(NAME, {
        listen = cg.listen,
        expire = {mode = 'index'},
    })
    local space = box.space[SPACE]
    cg.c = client.must_connect(cg.port)
    fill(cg.c, 'live', 100, 0)
    t.assert_not_equals(space.index.exptime, nil)

    inst:cfg({expire = {mode = 'scan'}})
    t.assert_equals(space.index.exptime, nil)
    t.assert_equals(space:len(), 100)
    fill(cg.c, 'ttl', 300, 1)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(space:len(), 100)
        t.assert_equals(sweeper(NAME).mode, 'scan')
    end)
    t.assert_gt(inst:info().checked, 0)

    inst:cfg({expire = {mode = 'index'}})
    t.assert_not_equals(space.index.exptime, nil)
    t.assert_equals(space.index.exptime:count(), 100)
    fill(cg.c, 'ttl', 300, 1)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(space:len(), 100)
        t.assert_equals(sweeper(NAME).mode, 'index')
    end)
    t.assert_ge(inst:info().expired, 600)
    t.assert_equals(cg.c:get('live100'), string.rep('v', 100))

    -- The options are merged key by key: the mode stays.
    inst:cfg({expire = {enabled = true}})
    t.assert_not_equals(space.index.exptime, nil)
    -- A stopped instance aligns the space when it starts.
    inst:stop()
    inst:cfg({expire = {mode = 'scan'}})
    t.assert_not_equals(space.index.exptime, nil)
    inst:start()
    t.assert_equals(space.index.exptime, nil)
end

-- A new instance on an existing space brings its index in line.
s.test_existing_space = function(cg)
    local schema = require('memcached.schema')
    local space = schema.create(SPACE, 'memory', {expire = 'index'})
    space:insert({'k', 'v', 0, 0, 1, 0, 0, false, false, 0})
    local inst = cg.mcc.create(NAME, {listen = cg.listen})
    t.assert_equals(space.index.exptime, nil)
    t.assert_equals(space:len(), 1)
    inst:destroy()
    cg.mcc.create(NAME, {listen = cg.listen, expire = {mode = 'index'}})
    t.assert_not_equals(space.index.exptime, nil)
    t.assert_equals(space:len(), 1)
end

-- A read-only instance uses the indexes it finds and changes nothing; the
-- next start on a writable one aligns them.
s.test_read_only = function(cg)
    local schema = require('memcached.schema')
    local space = schema.create(SPACE, 'memory', {expire = 'index'})
    box.cfg {read_only = true}
    local inst = cg.mcc.create(NAME, {listen = cg.listen})
    t.assert_equals(inst.status, 'running')
    t.assert_not_equals(space.index.exptime, nil)
    inst:cfg({expire = {mode = 'index'}})
    inst:cfg({expire = {mode = 'scan'}})
    t.assert_not_equals(space.index.exptime, nil)
    box.cfg {read_only = false}
    -- The sweeper walks the index it has.
    cg.c = client.must_connect(cg.port)
    fill(cg.c, 'ttl', 50, 1)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(space:len(), 0)
        t.assert_equals(sweeper(NAME).mode, 'index')
    end)
    inst:stop()
    inst:start()
    t.assert_equals(space.index.exptime, nil)
end

-- vinyl keeps the index; without it (not replicated yet) the sweeper does
-- nothing, and a writable instance creates it.
s.test_vinyl = function(cg)
    local schema = require('memcached.schema')
    local expire = require('memcached.expire')
    local space = schema.create(SPACE, 'disk')
    t.assert_not_equals(space.index.exptime, nil)
    -- The scan would drop it: never on vinyl.
    t.assert_error_msg_contains(
        "mode 'scan' needs storage = 'memory'",
        schema.ensure,
        SPACE,
        'disk',
        {expire = 'scan'}
    )
    space.index.exptime:drop()
    space:insert({'k', 'v', 0, 1, 1, 0, 0, false, false, 0})
    local rec = {
        space_name = SPACE,
        batch = 10,
        stopped = false,
        expired = 0,
        flushed = 0,
    }
    t.assert_equals(expire.step(rec), expire.IDLE_SLEEP)
    t.assert_equals(rec.expired, 0)
    t.assert_equals(space:count(), 1)

    cg.mcc.create(NAME, {listen = cg.listen, storage = 'disk'})
    t.assert_not_equals(space.index.exptime, nil)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(space:count(), 0)
        t.assert_equals(cg.mcc.get(NAME):info().expired, 1)
    end)
end

-- The scan cycle: bounded, adaptive, and it reaches every item.
s.test_scan_cycle = function()
    local schema = require('memcached.schema')
    local expire = require('memcached.expire')
    local space = schema.create(OTHER, 'memory')
    t.assert_equals(space.index.exptime, nil)
    local function new_rec()
        return {
            space_name = OTHER,
            batch = 100,
            stopped = false,
            expired = 0,
            flushed = 0,
            checked = 0,
        }
    end
    -- Nothing to find: the wait doubles up to MAX_SLEEP.
    local rec = new_rec()
    local waits = {}
    for _ = 1, 6 do
        table.insert(waits, expire.step(rec))
    end
    t.assert_equals(waits, {0.1, 0.2, 0.4, 0.8, 1, 1})
    t.assert_equals(rec.checked, 0, 'an empty space')
    for i = 1, 100 do
        space:insert({'live' .. i, 'v', 0, 0, 1, 0, 0, false, false, 0})
    end
    expire.step(rec)
    t.assert_equals(rec.checked, 100)
    t.assert_equals(rec.scan_sleep, 1)
    -- A cycle checks enough items to finish a pass in SCAN_PASS seconds.
    local pass = expire.SCAN_PASS
    expire.SCAN_PASS = 0.5
    local ok, wait = pcall(expire.step, rec)
    expire.SCAN_PASS = pass
    t.assert(ok, wait)
    t.assert_equals(rec.checked, 300)
    t.assert_ge(rec.passes, 2)

    -- Dead items: transactions of `batch` items while most of them are
    -- dead, within the budget, then back to the full rate.
    local past = fiber.time() - 10
    box.begin()
    for i = 1, 5000 do
        space:insert({'dead' .. i, 'v', 0, past, 1, 0, 0, false, false, 0})
    end
    box.commit()
    local budget = expire.SCAN_BUDGET
    expire.SCAN_BUDGET = 0
    ok, wait = pcall(expire.step, rec)
    expire.SCAN_BUDGET = budget
    t.assert(ok, wait)
    t.assert_equals(wait, expire.SCAN_PERIOD)
    t.assert_equals(rec.checked, 400, 'one transaction of batch items')
    t.assert_ge(rec.expired, 80)
    t.assert_le(rec.expired, 100)
    t.assert_equals(space:len(), 5100 - rec.expired)
    t.helpers.retrying({timeout = 10}, function()
        expire.step(rec)
        t.assert_equals(space:len(), 100)
    end)
    t.assert_equals(rec.expired, 5000)

    -- A delayed flush: the sweeper wakes up when it comes.
    for _ = 1, 5 do
        expire.step(rec)
    end
    t.assert_equals(rec.scan_sleep, 1)
    box.space._mcc_meta:replace({schema.flush_key(OTHER), fiber.time() + 0.05})
    wait = expire.step(rec)
    t.assert_le(wait, 0.05)
    t.assert_equals(space:len(), 100)
    fiber.sleep(0.1)
    t.helpers.retrying({timeout = 10}, function()
        expire.step(rec)
        t.assert_equals(space:len(), 0)
    end)
    t.assert_equals(rec.flushed, 100)
end
