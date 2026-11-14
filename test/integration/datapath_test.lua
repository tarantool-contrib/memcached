-- The C data path (memcached/datapath.c) against the Lua engine: the same
-- requests through both give byte-identical replies and byte-identical
-- stored tuples. Skipped when the data path is not built (env.datapath_so,
-- which tt test and tt package build build).
--
-- Both runs of a script start from the same state: an empty space, the same
-- CAS counter and created stamp, and a frozen clock (fiber.time, through
-- which the engine reads it).

local ffi = require('ffi')
local fiber = require('fiber')
local msgpack = require('msgpack')
local t = require('luatest')
local server = require('luatest.server')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local g = t.group('datapath')

local NAME = 'dp'
local SPACE = '__mcc_' .. NAME

local mcc
local schema
local state
local engine

local real_time = fiber.time
-- The frozen clock of the scripts: non-integral, so that times are stored
-- as doubles; a step may move it (an integral value stores integers).
local clock = {now = 1800000000.25}

g.before_all(function(cg)
    env.box_cfg()
    mcc = require('memcached')
    schema = require('memcached.schema')
    state = require('memcached.state')
    engine = require('memcached.engine')
    -- The scripts reset these; the other test files get them back.
    cg.saved = {
        cas_counter = state.cas_counter,
        stamp_last = state.stamp_last,
        enabled = not state.datapath_off,
    }
    cg.available = mcc.datapath().available
    -- Built means loaded.
    t.assert_equals(cg.available, require('fio').path.exists(env.datapath_so))
    cg.port = env.free_port()
end)

g.before_each(function(cg)
    t.skip_if(not cg.available, env.datapath_so .. ' is not built')
end)

g.after_each(function(cg)
    fiber.time = real_time
    if box.info.ro then
        box.cfg {read_only = false}
    end
    -- As before the group: MEMCACHED_DATAPATH=lua runs the other files on
    -- the Lua engine.
    mcc.datapath(cg.saved.enabled)
    if mcc.get(NAME) ~= nil then
        mcc.destroy(NAME)
    end
    env.drop_spaces({SPACE})
    if box.space._mcc_meta ~= nil then
        box.space._mcc_meta:delete(schema.flush_key(SPACE))
    end
end)

g.after_all(function(cg)
    fiber.time = real_time
    if cg.saved ~= nil then
        mcc.datapath(cg.saved.enabled)
        -- The frozen clock is in the future: a stamp_last left there would
        -- stamp the next items and flushes in the future too.
        state.cas_counter = cg.saved.cas_counter
        state.stamp_last = cg.saved.stamp_last
    end
end)

local function start(cg, opts)
    local o = {listen = '127.0.0.1:' .. cg.port, expire = {enabled = false}}
    for k, v in pairs(opts or {}) do
        o[k] = v
    end
    mcc.create(NAME, o)
    cg.client = client.must_connect(cg.port)
end

-- The reply to data: everything up to the reply of a meta no-op sent
-- after it. The meta no-ops of data (the lines "mn") answer MN as well:
-- the reply goes on to the last MN, so that the next step doesn't read the
-- rest of this one (nor dump() run before it is done).
local function call(cg, data)
    cg.client:send(data .. 'mn\r\n')
    local n = 1
    for line in data:gmatch('([^\n]*)\n') do
        if line == 'mn' or line == 'mn\r' then
            n = n + 1
        end
    end
    local parts = {}
    for i = 1, n do
        parts[i] = cg.client:read_until('MN\r\n')
        t.assert(parts[i], 'no reply to ' .. data)
    end
    return table.concat(parts):sub(1, -5)
end

-- The stored tuples, as MsgPack, by key.
local function dump()
    local res = {}
    for _, tuple in box.space[SPACE]:pairs() do
        res[tuple[1]] = msgpack.encode(tuple)
    end
    return res
end

-- A new instance (fresh counters, these options) on an empty space.
local function reset(cg, opts)
    if mcc.get(NAME) ~= nil then
        cg.client:close()
        mcc.destroy(NAME)
    end
    start(cg, opts)
    box.space[SPACE]:truncate()
    box.space._mcc_meta:delete(schema.flush_key(SPACE))
    state.cas_counter = ffi.cast('uint64_t', 1000000)
    state.stamp_last = 0
    -- The win tokens of a read-only instance, kept in memory.
    state.tokens[SPACE] = nil
    clock.now = 1800000000.25
    fiber.time = function()
        return clock.now
    end
end

-- Count the keys and the sets served in C, by the per-request functions
-- (get, set) or the batch (get, set, and batch: the commands it served),
-- and the per-request calls left to the Lua engine (fallback).
local function count_calls(counts)
    local dp = package.loaded[engine.DATAPATH]
    local get, set, batch = dp.get, dp.set, dp.batch
    dp.get = function(...)
        local res = get(...)
        local k = res == nil and 'fallback' or 'get'
        counts[k] = counts[k] + 1
        return res
    end
    dp.set = function(...)
        local res = set(...)
        local k = res == nil and 'fallback' or 'set'
        counts[k] = counts[k] + 1
        return res
    end
    dp.batch = function(...)
        local res = {batch(...)}
        -- consumed, commands, gets, hits, stored, held, unstored, ...
        counts.get = counts.get + res[3]
        counts.set = counts.set + res[5] + (res[7] or 0)
        counts.batch = counts.batch + res[2]
        return unpack(res, 1, 19)
    end
    return function()
        dp.get, dp.set, dp.batch = get, set, batch
        -- And the commands the C connection loop served (cloop.c).
        local inst = mcc.get(NAME)
        local loop = inst ~= nil and inst.server.dp_counters
        if loop then
            local commands, gets, sets = dp.served(loop)
            counts.get = counts.get + gets
            counts.set = counts.set + sets
            counts.batch = counts.batch + commands
        end
    end
end

-- Run a script, a list of commands (strings) and actions (functions), with
-- the C data path on or off. Returns the replies, the tuples and the
-- counts of C calls.
local function run(cg, script, on, opts)
    reset(cg, opts)
    t.assert_equals(mcc.datapath(on).enabled, on)
    local counts = {get = 0, set = 0, fallback = 0, batch = 0}
    local restore = count_calls(counts)
    local replies = {}
    local ok, err = pcall(function()
        for _, step in ipairs(script) do
            if type(step) == 'function' then
                replies[#replies + 1] = step(cg) or ''
            else
                replies[#replies + 1] = call(cg, step)
            end
        end
    end)
    restore()
    t.assert(ok, err)
    if not on then
        t.assert_equals(counts, {get = 0, set = 0, fallback = 0, batch = 0})
    end
    return replies, dump(), counts
end

-- Both paths give the same replies and tuples; returns them and the counts
-- of C calls.
local function same(cg, script, opts)
    local lua_replies, lua_tuples = run(cg, script, false, opts)
    local c_replies, c_tuples, counts = run(cg, script, true, opts)
    for i = 1, #script do
        t.assert_equals(
            c_replies[i],
            lua_replies[i],
            ('step %d: %s'):format(i, tostring(script[i]))
        )
    end
    t.assert_equals(c_tuples, lua_tuples)
    return c_replies, c_tuples, counts
end

local function tick(seconds)
    return function()
        clock.now = clock.now + seconds
    end
end

local function set_cmd(key, value, flags, exptime, extra)
    return ('set %s %d %d %d%s\r\n%s\r\n'):format(
        key,
        flags or 0,
        exptime or 0,
        #value,
        extra or '',
        value
    )
end

g.test_hit_miss_multi = function(cg)
    local replies, _, counts = same(cg, {
        'get a\r\n',
        set_cmd('a', 'abc', 5),
        'get a\r\n',
        'gets a\r\n',
        'get a b a\r\n',
        set_cmd('b', '', 0),
        'gets b nope a\r\n',
        'get nope\r\n',
        'mg a v f c\r\n',
        'me a\r\n',
    })
    t.assert_equals(replies[3], 'VALUE a 5 3\r\nabc\r\nEND\r\n')
    t.assert_str_matches(replies[4], 'VALUE a 5 3 %d+\r\nabc\r\nEND\r\n')
    -- Every classic get and set on the C path, the mg and the mn after
    -- each step too; not me.
    t.assert_equals(counts, {get = 11, set = 2, fallback = 0, batch = 19})
end

g.test_stats_counters = function(cg)
    local function stats()
        local srv = mcc.get(NAME).server
        local s = require("memcached.server").sync_stats(srv)
        return ('%d %d %d %d %d'):format(
            s.cmd_get,
            s.get_hits,
            s.get_misses,
            s.cmd_set,
            s.total_items
        )
    end
    local replies = same(cg, {
        set_cmd('a', '1'),
        'get a b\r\n',
        'gets a\r\n',
        stats,
    })
    t.assert_equals(replies[4], '3 2 1 1 1')
end

g.test_value_and_key_sizes = function(cg)
    local script = {}
    -- Around the MsgPack str header limits, with \r\n and NUL inside.
    for _, n in ipairs({0, 1, 31, 32, 255, 256, 65535, 65536, 500000}) do
        local value = ('a\r\n\0'):rep(math.floor(n / 4) + 1):sub(1, n)
        local key = 'k' .. n
        table.insert(script, set_cmd(key, value, n))
        table.insert(script, 'get ' .. key .. '\r\n')
        table.insert(script, 'gets ' .. key .. '\r\n')
    end
    for _, n in ipairs({31, 32, 250}) do
        local key = ('x'):rep(n)
        table.insert(script, set_cmd(key, 'v'))
        table.insert(script, 'get ' .. key .. '\r\n')
    end
    same(cg, script)
end

g.test_flags = function(cg)
    local script = {}
    for _, flags in ipairs({
        0,
        127,
        128,
        255,
        256,
        65535,
        65536,
        4294967295,
    }) do
        table.insert(script, set_cmd('f' .. flags, 'v', flags))
        table.insert(script, 'get f' .. flags .. '\r\n')
    end
    same(cg, script)
end

g.test_exptime = function(cg)
    same(cg, {
        set_cmd('rel', 'v', 0, 100),
        set_cmd('abs', 'v', 0, 1800000100),
        set_cmd('past', 'v', 0, 1700000000),
        set_cmd('now', 'v', 0, -1),
        'get rel abs past now\r\n',
        tick(50),
        'get rel abs\r\n',
        tick(51),
        -- Dead now: deleted on access.
        'get rel abs\r\n',
        'mg rel v\r\n',
        -- An integral clock: the times are stored as integers.
        function()
            clock.now = 1800001000
        end,
        set_cmd('int', 'v', 0, 10),
        'get int\r\n',
        tick(10),
        'get int\r\n',
    })
    t.assert_equals(box.space[SPACE]:len(), 0)
end

g.test_flush = function(cg)
    same(cg, {
        set_cmd('a', '1'),
        set_cmd('b', '2'),
        'flush_all\r\n',
        'get a\r\n',
        set_cmd('c', '3'),
        'get b c\r\n',
        set_cmd('d', '4'),
        'flush_all 10\r\n',
        'get d\r\n',
        tick(11),
        'get d c\r\n',
        set_cmd('e', '5'),
        'get e\r\n',
    })
end

g.test_track_access = function(cg)
    local script = {
        set_cmd('a', 'v'),
        'get a\r\n',
        'get a\r\n',
        tick(engine.LA_UPDATE_INTERVAL - 1),
        'get a\r\n',
        tick(1),
        'get a\r\n',
        'me a\r\n',
        'mg a h l t\r\n',
    }
    local _, tuples = same(cg, script)
    t.assert_equals(msgpack.decode(tuples.a)[8], true)
    _, tuples = same(cg, script, {track_access = false})
    t.assert_equals(msgpack.decode(tuples.a)[8], false)
end

g.test_set_cas = function(cg)
    same(cg, {
        set_cmd('a', '1'),
        'gets a\r\n',
        set_cmd('a', '2'),
        'gets a\r\n',
        -- A CAS above the counter: the next one is that + 1.
        function()
            local tuple = box.space[SPACE]:get('a'):totable()
            tuple[5] = 2 ^ 60
            box.space[SPACE]:replace(tuple)
        end,
        set_cmd('a', '3'),
        'gets a\r\n',
        -- A dead item's CAS doesn't count.
        function()
            local tuple = box.space[SPACE]:get('a'):totable()
            tuple[4] = clock.now - 1
            tuple[5] = 2 ^ 61
            box.space[SPACE]:replace(tuple)
        end,
        set_cmd('a', '4'),
        'gets a\r\n',
        -- Other storage commands keep the Lua path.
        'cas a 0 0 1 1\r\nx\r\n',
        'add a 0 0 1\r\nx\r\n',
        'append a 0 0 1\r\nx\r\n',
        set_cmd('a', '5', 0, 0, ' noreply'),
        'gets a\r\n',
    })
end

-- The win token field: the C get reads tuples with and without it (format
-- v2 tuples have 10 fields) and takes no token, the C set writes it false.
g.test_token_field = function(cg)
    local replies, tuples, counts = same(cg, {
        set_cmd('s', 'v'),
        'md s I\r\n',
        -- A classic get leaves the token to the next mg.
        'get s\r\n',
        'mg s v\r\n',
        'get s\r\n',
        'gets s\r\n',
        'mg s v\r\n',
        -- A set is a new version: not stale, no token.
        set_cmd('s', 'w'),
        'mg s v\r\n',
        'mg v s N30\r\n',
        'get v\r\n',
        'mg v s\r\n',
        set_cmd('v', 'x', 0, 100),
        'mg v s\r\n',
        -- A recache win, then the item is replaced.
        'mg v s R200\r\n',
        'gets v\r\n',
        set_cmd('v', 'y'),
        -- A tuple of format v2: read, bumped and replaced on the C path.
        function()
            box.space[SPACE]:replace({
                'old',
                'v',
                3,
                0,
                5000,
                clock.now,
                clock.now,
                false,
                false,
                0,
            })
        end,
        'get old\r\n',
        'gets old\r\n',
        function()
            return tostring(#box.space[SPACE]:get('old'))
        end,
        set_cmd('old', 'n'),
        'get old\r\n',
    })
    t.assert_equals(replies[4], 'VA 1 X W\r\nv\r\n')
    t.assert_equals(replies[7], 'VA 1 Z X\r\nv\r\n')
    t.assert_equals(replies[9], 'VA 1\r\nw\r\n')
    t.assert_equals(replies[10], 'HD s0 W\r\n')
    t.assert_equals(replies[12], 'HD s0 Z\r\n')
    t.assert_equals(replies[14], 'HD s1\r\n')
    t.assert_equals(replies[15], 'HD s1 W\r\n')
    t.assert_equals(replies[19], 'VALUE old 3 1\r\nv\r\nEND\r\n')
    -- The C get's last access update keeps the 10 fields.
    t.assert_equals(replies[21], '10')
    for key, n in pairs({s = 'w', v = 'y', old = 'n'}) do
        local tuple = msgpack.decode(tuples[key])
        t.assert_equals(#tuple, schema.TOKEN, key)
        t.assert_equals({tuple[2], tuple[schema.TOKEN]}, {n, false}, key)
    end
    t.assert_equals(counts.fallback, 0)
    t.assert_equals(counts.set, 5)
end

g.test_read_only = function(cg)
    same(cg, {
        set_cmd('live', 'v'),
        set_cmd('dead', 'v', 0, 10),
        tick(20),
        function()
            box.cfg {read_only = true}
            -- The box.status watcher of the engine.
            fiber.sleep(0.01)
        end,
        -- No lazy deletion, no last access.
        'get live dead\r\n',
        set_cmd('live', 'w'),
        function()
            box.cfg {read_only = false}
            fiber.sleep(0.01)
        end,
        'get live dead\r\n',
    })
end

-- A write that fails on the C path leaves the request to the Lua engine:
-- a get still answers without its last access write, a set answers its
-- error.
g.test_failed_writes = function(cg)
    local function fail_updates(on)
        return function()
            local space = box.space[SPACE]
            if on then
                cg.trigger = function(old, new)
                    if old ~= nil and new ~= nil and old[2] == new[2] then
                        error('no update')
                    end
                end
                space:before_replace(cg.trigger)
            else
                space:before_replace(nil, cg.trigger)
            end
        end
    end
    local too_large = ('x'):rep(box.cfg.memtx_max_tuple_size + 1)
    local replies, _, counts = same(cg, {
        set_cmd('a', 'v'),
        fail_updates(true),
        'get a\r\n',
        'get a\r\n',
        fail_updates(false),
        'get a\r\n',
        set_cmd('big', too_large),
        'get big\r\n',
    }, {max_item_size = #too_large + 1024}
    )
    t.assert_equals(replies[3], 'VALUE a 0 1\r\nv\r\nEND\r\n')
    t.assert_str_matches(replies[7], 'SERVER_ERROR .*')
    -- The two gets with a failing update and the big set fell back.
    t.assert_equals(counts.fallback, 3)
end

-- The C batch (datapath.c batch()) ---------------------------------------
--
-- It serves the classic get, gets and set at the head of the input and
-- leaves the rest, from the first command it doesn't serve, to the Lua
-- front-end. The pipelines below mix both: their replies are those of the
-- Lua path. Each step is sent with one write.

g.test_batch_pipelines = function(cg)
    local long = ('k'):rep(251)
    local quiet = set_cmd('d', '5', 0, 0, ' noreply')
    quiet = quiet .. set_cmd('e', '6', 0, 0, ' noreply')
    local _, _, counts = same(cg, {
        set_cmd('a', '1') .. set_cmd('b', '22', 3) .. 'get a b c\r\ngets b\r\n',
        -- The batch stops at delete; the Lua batch runs the rest.
        'get a\r\ndelete b\r\nget b\r\n' .. set_cmd('c', '3') .. 'get c\r\n',
        'get a\r\nmg a v\r\nget a\r\n',
        set_cmd('d', '4', 0, 0, ' noreply') .. 'get d\r\n',
        quiet,
        'get d e\r\n',
        -- Spaces, a "\r" in a key, extra tokens, noreply as the last one.
        'get   a    d  \r\ngets  e\r\nget a\rb\r\n',
        set_cmd('f', 'v', 1, 0, ' x y noreply')
            .. set_cmd('g', 'w', 2, 0, ' noreply z')
            .. 'get f g\r\n',
        set_cmd('h', 'x', 0, 0, (' t'):rep(17) .. ' noreply')
            .. set_cmd('h', 'y', 0, 0, (' t'):rep(18) .. ' noreply')
            .. 'get h\r\n',
        -- Numbers in the forms the parser reads otherwise than as digits.
        'set h 007 0 1\r\nx\r\nget h\r\n',
        'set h 0 0 00000000001\r\nx\r\nget h\r\n',
        'set h 0 0 000000000001\r\nx\r\nget h\r\n',
        'set h 4294967297 0 1\r\nx\r\nget h\r\n',
        'set h +1 0 1\r\nx\r\nget h\r\n',
        'set h 0 +1 1\r\nx\r\nget h\r\n',
        'set h 0 -0 1\r\nx\r\nget h\r\n',
        -- Expired at once: relative, absolute, cast to a negative int32.
        'set h 0 -1 1\r\nx\r\nget h\r\n',
        'set h 0 1700000000 1\r\nx\r\nget h\r\n',
        set_cmd('h', 'x') .. 'set h 0 2147483648 1\r\nx\r\nget h\r\n',
        -- Errors and forms left to the parser.
        'get ' .. long .. '\r\nget a\r\n',
        'get a ' .. long .. '\r\nget a\r\n',
        'get\r\nget a\r\nget \r\nGET a\r\n get a\r\nget a\r\n',
        'set h 0 0 -1\r\nget a\r\n',
        'set h 0 0 x\r\nget a\r\n',
        'set h 0 0\r\nget a\r\n',
        'set h 0 0 1\r\nxy\r\nget a\r\n',
        'set ' .. long .. ' 0 0 1\r\nx\r\nget a\r\n',
        'set h 0 0 1 noreply\r\nx\r\nincr a 1\r\nget a h\r\n',
        'get a\r\nlru_crawler metadump all\r\n',
        'get a\r\nversion\r\n',
    })
    t.assert(counts.batch > 20, counts.batch)
end

-- The items that the gets of the C batch hold keep an arithmetic after
-- them in the same batch from working in place, as in a Lua batch (see
-- engine.lua ops.arith): the stored values tell.
g.test_batch_held = function(cg)
    same(cg, {
        set_cmd('n', '100') .. 'get n\r\ndecr n 1\r\nget n\r\n',
        -- A set drops the hold.
        set_cmd('n', '100') .. 'get n\r\n'
            .. set_cmd('n', '100') .. 'decr n 1\r\nget n\r\n',
        set_cmd('n', '100') .. 'get n\r\n'
            .. set_cmd('n', '100') .. 'get n\r\ndecr n 1\r\nget n\r\n',
        -- An mn writes the replies out.
        set_cmd('n', '100') .. 'gets n\r\nmn\r\ndecr n 1\r\nget n\r\n',
        -- A barrier is an engine call of its own.
        set_cmd('n', '100') .. 'get n\r\nstats detail on\r\ndecr n 1\r\n'
            .. 'get n\r\n',
        set_cmd('n', '100') .. 'get n m\r\ndelete m\r\nincr n 1\r\n'
            .. 'get n\r\n',
        set_cmd('n', '100') .. 'get x\r\ndecr n 1\r\nget n\r\n',
        set_cmd('n', '100') .. 'get n\r\nma n D1\r\nget n\r\n',
        set_cmd('n', '100') .. 'get n\r\nmg n v\r\ndecr n 1\r\nget n\r\n',
        set_cmd('n', '100') .. 'get n\r\nversion\r\ndecr n 1\r\nget n\r\n',
    })
end

-- Commands split across reads: the batch stops at an incomplete one, which
-- the Lua parser waits for.
local function pieces(...)
    local parts = {...}
    return function(cg)
        for _, part in ipairs(parts) do
            cg.client:send(part)
            fiber.sleep(0.02)
        end
        cg.client:send('mn\r\n')
        local reply = cg.client:read_until('MN\r\n')
        t.assert(reply, 'no reply')
        return reply:sub(1, -5)
    end
end

g.test_batch_split = function(cg)
    local value = ('v'):rep(40000)
    local _, _, counts = same(cg, {
        set_cmd('a', '1'),
        pieces('get a\r\nge', 't a\r\n'),
        pieces('get a', '\r\n'),
        pieces('get a\r', '\nget a\r\n'),
        pieces('set p 0 0 5\r\nab', 'cde\r\nget p\r\n'),
        pieces('get a\r\nset p 0 0 5', '\r\nabcde\r\nget p\r\n'),
        pieces('set p 0 0 5\r\nabcde', '\r', '\nget p\r\n'),
        pieces('set p 0 0 5\r\nabcde\r\nget', ' p\r\n'),
        pieces(
            set_cmd('big', value):sub(1, 20000),
            set_cmd('big', value):sub(20001),
            'get big\r\n'
        ),
    })
    t.assert(counts.batch > 10, counts.batch)
end

-- The item size limit, on both sides of it.
g.test_batch_item_size = function(cg)
    -- 48 + 1 + 1 + 1988 + 2 + 8 = 2048; 4 more with flags.
    same(cg, {
        set_cmd('z', ('v'):rep(1988)),
        'get z\r\n',
        set_cmd('z', ('v'):rep(1989)),
        'get z\r\n',
        set_cmd('z', ('v'):rep(1984), 1),
        set_cmd('z', ('v'):rep(1985), 1) .. 'get z\r\n',
        set_cmd('z', ('v'):rep(1984), 1, 0, ' noreply')
            .. set_cmd('z', ('v'):rep(1985), 1, 0, ' noreply')
            .. 'get z\r\n',
    }, {max_item_size = 2048}
    )
end

-- MAX_BATCH commands per batch: the C batch and the Lua one after it count
-- together, so the batches (and the items their gets hold) are those of
-- the Lua path.
g.test_batch_limit = function(cg)
    local function gets(n)
        local p = {}
        for i = 1, n do
            p[i] = 'get k' .. (i % 3) .. '\r\n'
        end
        return table.concat(p)
    end
    local _, _, counts = same(cg, {
        set_cmd('k0', '100') .. set_cmd('k1', '100'),
        gets(600) .. 'decr k1 1\r\nget k1\r\n',
        gets(510) .. 'decr k1 1\r\ndecr k0 1\r\nget k0 k1\r\n',
        gets(512) .. 'decr k1 1\r\nget k1\r\n',
    })
    t.assert(counts.batch > 1500, counts.batch)
end

-- Everything a new connection gets for data until the server closes it.
local function until_close(data)
    return function(cg)
        local c = client.must_connect(cg.port)
        c:send(data)
        local res = {}
        while true do
            local chunk = c.sock:read(1, 2)
            if chunk == nil or chunk == '' then
                break
            end
            res[#res + 1] = chunk
        end
        c:close()
        return table.concat(res)
    end
end

-- A quit, or a line too long, after commands the C batch served: their
-- replies, then the close.
g.test_batch_close = function(cg)
    local replies = same(cg, {
        until_close(set_cmd('q', 'q') .. 'get q\r\nquit\r\nget q\r\n'),
        until_close('get q\r\n' .. ('x'):rep(3000)),
        until_close('get q\r\nget q\r\n' .. ('x'):rep(3000)),
        'get q\r\n',
    })
    t.assert_equals(replies[1], 'STORED\r\nVALUE q 0 1\r\nq\r\nEND\r\n')
    t.assert_equals(replies[2], 'VALUE q 0 1\r\nq\r\nEND\r\n')
end

-- A get whose side write fails in the middle of a multi-key get: the
-- command goes to the Lua engine as a whole, and answers as there.
g.test_batch_failed_write = function(cg)
    local function fail_key(key)
        return function()
            cg.trigger = function(old, new)
                if old ~= nil and new ~= nil and old[1] == key then
                    error('no update')
                end
            end
            box.space[SPACE]:before_replace(cg.trigger)
        end
    end
    local _, _, counts = same(cg, {
        set_cmd('a', '1') .. set_cmd('b', '2') .. set_cmd('c', '3'),
        fail_key('b'),
        'get a b c\r\nget c\r\n',
        'get a b c\r\n',
        function()
            box.space[SPACE]:before_replace(nil, cg.trigger)
        end,
        'get a b c\r\n',
    })
    -- b fell back twice: in the Lua batch's per-request get, and the
    -- requests after it took that path too.
    t.assert(counts.fallback >= 2, counts.fallback)
end

-- The stats counters and the metrics of commands served in C.
g.test_batch_counters = function(cg)
    local function stats(cg_)
        local reply = cg_.client:call('stats\r\n', 'END\r\n')
        local res = {}
        for _, name in ipairs({
            'cmd_get',
            'get_hits',
            'get_misses',
            'cmd_set',
            'total_items',
            'curr_items',
            'bytes_read',
        }) do
            local value = reply:match('STAT ' .. name .. ' (%d+)')
            res[#res + 1] = name .. ' ' .. value
        end
        return table.concat(res, ' ')
    end
    local replies = same(cg, {
        set_cmd('a', '1') .. set_cmd('b', '2', 0, 0, ' noreply')
            .. 'get a b c\r\ngets a\r\nget c\r\n',
        stats,
        'get a\r\ndelete b\r\nget a b\r\n' .. set_cmd('b', '3'),
        stats,
    })
    t.assert_str_matches(
        replies[2],
        'cmd_get 5 get_hits 3 get_misses 2 cmd_set 2 total_items 2 .*'
    )
    t.assert_str_matches(
        replies[4],
        'cmd_get 8 get_hits 5 get_misses 3 cmd_set 3 total_items 3 .*'
    )
end

-- No C batch with a commit wait mode: the per-request C calls in their
-- transactions serve (engine.lua fast()).
g.test_batch_commit_wait = function(cg)
    local _, _, counts = same(cg, {
        set_cmd('a', '1'),
        'get a b\r\n',
    }, {commit_wait = 'submit'}
    )
    t.assert_equals(counts, {get = 2, set = 1, fallback = 0, batch = 0})
end

-- The sets of other connections run while a set of a batch waits for the
-- WAL: the CAS counter and the created stamp are the state's, so every
-- item gets a CAS and a stamp of its own.
g.test_batch_concurrent = function(cg)
    reset(cg)
    mcc.datapath(true)
    local clients = {}
    for i = 1, 4 do
        clients[i] = client.must_connect(cg.port)
        local p = {}
        for j = 1, 50 do
            p[j] = set_cmd(('c%d_%d'):format(i, j), 'v')
        end
        clients[i]:send(table.concat(p))
    end
    for _, c in ipairs(clients) do
        for _ = 1, 50 do
            t.assert_equals(c:line(), 'STORED\r\n')
        end
        c:close()
    end
    local cas, created = {}, {}
    local n = 0
    for _, tuple in box.space[SPACE]:pairs() do
        n = n + 1
        local c, s = tostring(tuple[5]), tuple[6]
        t.assert_equals(cas[c], nil, c)
        t.assert_equals(created[s], nil, s)
        cas[c], created[s] = true, true
    end
    t.assert_equals(n, 200)
end

-- The entry point as the application threads will call it: input as a
-- string, the replies in an ibuf.
g.test_batch_engine = function(cg)
    reset(cg)
    mcc.datapath(true)
    local buffer = require('buffer')
    local out = buffer.ibuf()
    local ctx = {space_name = SPACE}
    local tail = 'incr a 1\r\n'
    local input = set_cmd('a', 'xyz', 7) .. 'get a b\r\n'
        .. set_cmd('b', '1', 0, 0, ' noreply') .. 'gets b\r\n'
        .. tail
    t.assert_equals(
        {engine.batch(ctx, input, out, 512, 1048576)},
        {#input - #tail, 4, 3, 2, 2, {a = true, b = true}}
    )
    t.assert_equals(
        ffi.string(out.rpos, out.wpos - out.rpos),
        'STORED\r\nVALUE a 7 3\r\nxyz\r\nEND\r\n'
            .. 'VALUE b 0 1 1000001\r\n1\r\nEND\r\n'
    )
    -- The limit: no held keys, nothing is left for a Lua batch.
    out:reset()
    local res = {
        engine.batch(ctx, 'get a\r\nget a\r\nget a\r\n', out, 2, 1048576),
    }
    t.assert_equals(
        {res[1], res[2], res[3], res[4], res[5], res[6]},
        {14, 2, 2, 2, 0, nil}
    )
    -- Not served: the switch, a commit wait mode, vinyl.
    mcc.datapath(false)
    t.assert_equals(engine.batch(ctx, 'get a\r\n', out, 512, 1048576), nil)
    mcc.datapath(true)
    t.assert_equals(
        engine.batch(
            {space_name = SPACE, commit_wait = 'none'},
            'get a\r\n',
            out,
            512,
            1048576
        ),
        nil
    )
    t.assert_equals(
        engine.batch({space_name = 'nope'}, 'get a\r\n', out, 512, 1048576),
        nil
    )
end

-- The engine answers the requests marked fast with strings (hits) and the
-- shared MISS; without the mark, or with the switch off, with tables.
g.test_engine_responses = function(cg)
    reset(cg)
    mcc.datapath(true)
    local ctx = {space_name = SPACE}
    engine.exec(ctx, {
        {
            op = 'store',
            mode = 'set',
            key = 'a',
            value = 'v',
            flags = 3,
            exptime = 0,
            fast = 'set',
        },
    })
    local resps = engine.exec(ctx, {
        {op = 'get', key = 'a', fast = 'get'},
        {op = 'get', key = 'nope', fast = 'get'},
        {op = 'get', key = 'a'},
    })
    t.assert_equals(resps[1], 'VALUE a 3 1\r\nv\r\n')
    t.assert_equals(resps[2], {status = 'MISS'})
    t.assert_equals(resps[3].status, 'HIT')
    mcc.datapath(false)
    t.assert_equals(
        engine.exec(ctx, {
            {
                op = 'get',
                key = 'a',
                fast = 'get',
            },
        })[1].status,
        'HIT'
    )
    t.assert_equals(mcc.datapath(true), {available = true, enabled = true})
end

-- vinyl keeps the Lua path: its reads yield, and its gets keep their
-- transaction (engine.lua attempt()).
g.test_vinyl = function(cg)
    local _, _, counts = same(cg, {
        set_cmd('a', 'v'),
        'get a\r\n',
        'gets a b\r\n',
    }, {storage = 'disk'}
    )
    t.assert_equals(counts, {get = 0, set = 0, fallback = 0, batch = 0})
end

g.test_switch = function()
    t.assert_equals(mcc.datapath(false), {available = true, enabled = false})
    t.assert_equals(mcc.datapath(), {available = true, enabled = false})
    t.assert_equals(mcc.datapath(true), {available = true, enabled = true})
    t.assert_error_msg_contains('Usage', mcc.datapath, 'yes')
end

-- After a hot reload the module is required again and still serves. In a
-- child server: a reload in this process would hand the other test files
-- stale module tables.
g.test_reload = function()
    local srv = server:new({alias = 'mcc_dp_reload', env = env.lua_env()})
    srv:start()
    local ok, err = pcall(function()
        local port = env.free_port()
        local res = srv:exec(function(port_)
            local mcc = require('memcached')
            -- On even under MEMCACHED_DATAPATH=lua.
            mcc.datapath(true)
            mcc.create('r', {listen = '127.0.0.1:' .. port_})
            local exec = function()
                return require('memcached.engine').exec(
                    {space_name = '__mcc_r'},
                    {
                        {
                            op = 'store',
                            mode = 'set',
                            key = 'a',
                            value = 'v',
                            flags = 0,
                            exptime = 0,
                            fast = 'set',
                        },
                        {op = 'get', key = 'a', fast = 'get'},
                    }
                )[2]
            end
            local before = exec()
            local old = package.loaded['memcached.datapath']
            mcc.reload()
            local new = package.loaded['memcached.datapath']
            return {before, exec(), old ~= new, mcc.datapath()}
        end, {port})
        t.assert_equals(res[1], 'VALUE a 0 1\r\nv\r\n')
        t.assert_equals(res[2], 'VALUE a 0 1\r\nv\r\n')
        -- Required again: a new module table (over the same .so).
        t.assert_equals(res[3], true)
        t.assert_equals(res[4], {available = true, enabled = true})
        local c = client.must_connect(port)
        t.assert_equals(
            c:call('get a\r\n', 'END\r\n'),
            'VALUE a 0 1\r\nv\r\nEND\r\n'
        )
        c:close()
    end)
    srv:drop()
    t.assert(ok, err)
end

-- More commands on the C batch ------------------------------------------
--
-- The classic storage, delete, touch, gat and arithmetic commands and the
-- meta mg, ms, md, ma and mn: their replies and stored items are those of
-- the Lua path, in the forms the batch serves and in those it leaves to
-- Lua, and so are the counters (stats_step()).

local function b64(s)
    return require('memcached.proto.meta').b64encode(s)
end

local function ms_cmd(key, value, flags)
    return ('ms %s %d%s\r\n%s\r\n'):format(key, #value, flags or '', value)
end

-- A step: the command fmt with the CAS of key (plus delta) for its %s.
local function cas_step(key, fmt, delta)
    return function(cg)
        local reply = call(cg, 'mg ' .. key .. ' c\r\n')
        local cas = tonumber64(reply:match(' c(%d+)') or '0') + (delta or 0)
        local text = tostring(cas):gsub('U?LL$', '')
        return call(cg, (fmt:gsub('%%s', text)))
    end
end

-- The counters of "stats" that the commands change, as a step.
local STATS = {
    'cmd_get',
    'cmd_set',
    'cmd_touch',
    'cmd_meta',
    'get_hits',
    'get_misses',
    'delete_misses',
    'delete_hits',
    'incr_misses',
    'incr_hits',
    'decr_misses',
    'decr_hits',
    'cas_misses',
    'cas_hits',
    'cas_badval',
    'touch_hits',
    'touch_misses',
    'total_items',
    'curr_items',
}

local function stats_step(cg)
    local reply = cg.client:call('stats\r\n', 'END\r\n')
    local res = {}
    for _, name in ipairs(STATS) do
        local value = reply:match('STAT ' .. name .. ' (%d+)')
        res[#res + 1] = name .. ' ' .. tostring(value)
    end
    -- And the error counters of the metrics.
    local st = mcc.get(NAME).server.stats
    for _, name in ipairs({'protocol_errors', 'client_errors', 'server_errors'}) do
        res[#res + 1] = name .. ' ' .. st[name]
    end
    return table.concat(res, ' ')
end

-- The commands of the steps each in a step of their own and all in one,
-- so that the batch serves them one by one and pipelined.
local function both_ways(steps)
    local script = {}
    for _, step in ipairs(steps) do
        script[#script + 1] = step
    end
    script[#script + 1] = stats_step
    local all = {}
    for _, step in ipairs(steps) do
        if type(step) == 'string' then
            all[#all + 1] = step
        end
    end
    script[#script + 1] = function(cg)
        cg.client:send('flush_all\r\n')
        t.assert_equals(cg.client:line(), 'OK\r\n')
        clock.now = clock.now + 1
    end
    script[#script + 1] = table.concat(all)
    script[#script + 1] = stats_step
    return script
end

g.test_batch_storage = function(cg)
    local _, _, counts = same(
        cg,
        both_ways({
            'add a 1 0 1\r\nx\r\n',
            'add a 2 0 1\r\ny\r\n',
            'replace b 0 0 1\r\nx\r\n',
            'replace a 3 100 2\r\nyz\r\n',
            'append a 9 9 2\r\n+a\r\n',
            'prepend a 9 9 2\r\np-\r\n',
            'append nope 0 0 1\r\nx\r\n',
            'prepend nope 0 0 1 noreply\r\nx\r\n',
            'gets a\r\n',
            'cas a 0 0 1 1\r\nc\r\n',
            'cas a 0 0 1 0\r\nc\r\n',
            'cas nope 0 0 1 5\r\nc\r\n',
            cas_step('a', 'cas a 7 0 1 %s\r\nc\r\n'),
            cas_step('a', 'cas a 8 0 1 %s noreply\r\nd\r\nmg a v f\r\n'),
            cas_step('a', 'cas a 8 0 1 %s\r\nd\r\n', 1),
            'gets a\r\n',
            'add e 0 -1 1\r\nx\r\nadd e 0 0 1\r\ny\r\nget e\r\n',
            'replace a 0 -1 1\r\nx\r\nget a\r\n',
            set_cmd('a', '1') .. 'cas a 0 -1 1 1000008\r\nx\r\nget a\r\n',
            'add f 0 0 1 noreply\r\nx\r\nreplace f 0 0 2 noreply\r\nxy\r\n'
                .. 'append f 0 0 1 noreply\r\nz\r\nget f\r\n',
            -- Dead items: gone for a replace, an append, a cas.
            set_cmd('d', 'v', 0, 10),
            tick(11),
            'replace d 0 0 1\r\nx\r\nget d\r\n',
            set_cmd('d', 'v', 0, 10),
            tick(11),
            'append d 0 0 1\r\nx\r\ncas d 0 0 1 1\r\nx\r\nadd d 0 0 1\r\ny\r\n'
                .. 'get d\r\n',
            -- Forms left to Lua.
            'add g 0 0 -1\r\nget a\r\n',
            'cas g 0 0 1\r\nx\r\nget a\r\n',
            'cas g 0 0 1 noreply\r\nx\r\nget a\r\n',
            'cas g 0 0 1 x\r\nx\r\nget a\r\n',
            'append g 0 0 1\r\nxy\r\nget a\r\n',
            'prepend ' .. ('k'):rep(251) .. ' 0 0 1\r\nx\r\nget a\r\n',
        })
    )
    t.assert(counts.batch > 50, counts.batch)
end

-- The joined item of an append must fit the item size limit.
g.test_batch_append_size = function(cg)
    same(cg, {
        set_cmd('z', ('v'):rep(1000)),
        'append z 0 0 988\r\n' .. ('w'):rep(988) .. '\r\n',
        'append z 0 0 1\r\nx\r\nprepend z 0 0 1\r\nx\r\n',
        'get z\r\n',
        ms_cmd('z', 'x', ' MA') .. ms_cmd('z', 'x', ' MP s c'),
        stats_step,
    }, {max_item_size = 2048}
    )
end

g.test_batch_delete_touch = function(cg)
    local _, _, counts = same(
        cg,
        both_ways({
            set_cmd('a', '1') .. set_cmd('b', '2', 0, 100),
            'delete a\r\ndelete a\r\ndelete a noreply\r\nget a\r\n',
            'delete b x y\r\nget b\r\n',
            set_cmd('noreply', 'n') .. 'delete noreply\r\nget noreply\r\n',
            set_cmd('c', '3', 0, 100),
            'touch c 10\r\nmg c t\r\ntouch c 0\r\nmg c t\r\n',
            'touch c 1800001000\r\nmg c t\r\n',
            'touch nope 10\r\ntouch c 20 noreply\r\nmg c t\r\n',
            'touch c -1\r\nget c\r\n',
            set_cmd('c', '3') .. 'touch c 0 noreply\r\nmg c t v\r\n',
            -- The access is recorded, as by a get.
            set_cmd('h', 'v') .. 'touch h 100\r\nmg h h l\r\n',
            -- Dead items.
            set_cmd('d', 'v', 0, 10),
            tick(11),
            'delete d\r\n',
            set_cmd('d', 'v', 0, 10),
            tick(11),
            'touch d 100\r\nget d\r\n',
            -- Forms left to Lua.
            'delete\r\nget a\r\n',
            'touch c\r\nget a\r\n',
            'touch c x\r\nget a\r\n',
            'touch c 99999999999\r\nget a\r\n',
            'delete ' .. ('k'):rep(251) .. '\r\nget a\r\n',
        })
    )
    t.assert(counts.batch > 40, counts.batch)
end

g.test_batch_gat = function(cg)
    local _, _, counts = same(
        cg,
        both_ways({
            set_cmd('a', '1', 5) .. set_cmd('b', '22', 6, 100),
            'gat 10 a b c\r\nmg a t\r\nmg b t\r\n',
            'gats 0 a\r\nmg a t\r\n',
            'gat 1800002000 b noreply\r\nmg b t\r\n',
            'gats   20   a   b  \r\n',
            set_cmd('h', 'v') .. 'gat 100 h\r\nmg h h l\r\n',
            -- Into the past: left to Lua.
            'gat -1 a\r\nget a b\r\n',
            set_cmd('a', '1') .. 'gat 1700000000 a b\r\nget a b\r\n',
            -- Dead items.
            set_cmd('d', 'v', 0, 10),
            tick(11),
            'gat 100 d b\r\n',
            -- Forms left to Lua.
            'gat\r\nget b\r\n',
            'gat 10\r\nget b\r\n',
            'gat x b\r\nget b\r\n',
            'gat 10 ' .. ('k'):rep(251) .. '\r\nget b\r\n',
        })
    )
    t.assert(counts.batch > 30, counts.batch)
end

g.test_batch_incr_decr = function(cg)
    local _, _, counts = same(
        cg,
        both_ways({
            set_cmd('n', '100') .. 'incr n 1\r\ndecr n 2\r\nget n\r\n',
            'decr n 1000\r\nget n\r\nincr n 18446744073709551615\r\n',
            set_cmd('n', '18446744073709551615') .. 'incr n 2\r\nget n\r\n',
            set_cmd('n', '00012') .. 'incr n 1\r\nget n\r\n',
            set_cmd('n', '9') .. 'incr n 1\r\nget n\r\nincr n 1 noreply\r\n'
                .. 'get n\r\n',
            set_cmd('n', ' 12 ab') .. 'decr n 1\r\nget n\r\n',
            set_cmd('n', '-0') .. 'incr n 1\r\nget n\r\n',
            set_cmd('n', '+5\t') .. 'incr n 1\r\nget n\r\n',
            set_cmd('n', '12\0x') .. 'incr n 1\r\nget n\r\n',
            'incr nope 1\r\ndecr nope 1\r\ndecr nope 1 noreply\r\n',
            -- Not a number: left to Lua.
            set_cmd('x', 'ab') .. 'incr x 1\r\ndecr x 1 noreply\r\nget x\r\n',
            set_cmd('x', '') .. 'incr x 1\r\n',
            set_cmd('x', '-5') .. 'incr x 1\r\n',
            set_cmd('x', '12a') .. 'incr x 1\r\n',
            set_cmd('x', '99999999999999999999999') .. 'incr x 1\r\n',
            -- Forms left to Lua.
            'incr n\r\nget n\r\n',
            'incr n x\r\nget n\r\n',
            'incr n -1\r\nget n\r\n',
            'incr n 18446744073709551616\r\nget n\r\n',
            -- Dead items.
            set_cmd('d', '5', 0, 10),
            tick(11),
            'incr d 1\r\nget d\r\n',
            -- The last access: in place, at most once per interval.
            set_cmd('l', '100') .. 'get l\r\nmn\r\ndecr l 1\r\nmg l h l v\r\n',
            tick(engine.LA_UPDATE_INTERVAL),
            'decr l 1\r\nmg l h l v\r\n',
        })
    )
    t.assert(counts.batch > 40, counts.batch)
end

-- An arithmetic in place pads the number to the old length, unless a read
-- of the batch before it holds the item: the C batch, the Lua batch and
-- the C batch again keep track of it together.
g.test_batch_arith_held = function(cg)
    local n100 = set_cmd('n', '100')
    same(cg, {
        n100 .. 'decr n 1\r\nget n\r\n',
        n100 .. 'get n\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'mg n v\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'mg n\r\ndecr n 1\r\nmg n v\r\n',
        n100 .. 'gat 100 n\r\nincr n 1\r\nget n\r\n',
        n100 .. 'touch n 100\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nmn\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get n\r\n' .. n100 .. 'decr n 1\r\nget n\r\n',
        n100 .. 'get n\r\ndelete n\r\n' .. n100 .. 'decr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nmd n q\r\n' .. n100 .. 'decr n 1\r\nget n\r\n',
        n100 .. 'get n\r\n' .. ms_cmd('n', '100') .. 'ma n MD\r\nget n\r\n',
        n100 .. 'get n\r\ndecr n 1\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nincr n 1\r\ndecr n 1\r\nget n\r\n',
        -- Held across the Lua batch: the C batch, a command it leaves to
        -- Lua, the C batch again.
        n100 .. 'get n\r\nversion\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nmg n N30 v\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'mg n N30 v\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'me n\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nverbosity 1\r\nmn\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nstats detail on\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'stats detail on\r\nget n\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nversion\r\nmn\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nma n N30 D1\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nma n N30 D1\r\nversion\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'get x\r\nversion\r\ndecr n 1\r\nget n\r\n',
        n100 .. 'mg n v\r\nset n 0 0 3 noreply\r\n100\r\nversion\r\n'
            .. 'decr n 1\r\nget n\r\n',
        n100 .. 'get n\r\nincr n 0\r\nversion\r\ndecr n 1\r\nget n\r\n',
        -- In place with the win token kept.
        n100 .. 'md n I\r\nmg n\r\ndecr n 1\r\nmg n v c\r\nmg n v\r\n',
        n100 .. 'ma n\r\nma n D2 v\r\nma n MD D1000 v c t\r\nget n\r\n',
    })
end

g.test_batch_meta_get = function(cg)
    local key = 'k\1y'
    local _, _, counts = same(
        cg,
        both_ways({
            set_cmd('a', 'abc', 7, 100),
            'mg a\r\nmg a v\r\nmg a s v\r\nmg a k v f c s t\r\n',
            'mg a O123 k t\r\nmg a q v\r\nmg nope q v\r\nmg nope\r\n',
            'mg nope k O9 s v t\r\nmg nope q k\r\n',
            'mg a h l\r\nmg a h l u\r\nmg a v sxyz kk Oxyz\r\n',
            tick(10),
            'mg a l h t\r\n',
            -- C: no value when it matches.
            cas_step('a', 'mg a v c C%s\r\nmg a v C1\r\nmg a C%s\r\n'),
            -- The flags that change nothing here.
            'mg a v E5 F3 J1 D2 MS P L x I\r\n',
            -- Touch.
            'mg a T30 t v\r\nmg a t\r\nmg a T0 t\r\nmg a T1800003000 t\r\n',
            'mg nope T30 t\r\nmg a T-1 t v\r\nmg a v\r\n',
            -- base64 keys, the decoding of memcached's base64.c.
            set_cmd(key, 'v'),
            'mg ' .. b64(key) .. ' b v k\r\nmg ' .. b64(key) .. ' k b q\r\n',
            'mg ' .. b64(key):sub(1, 2)
                .. '*' .. b64(key):sub(3)
                .. ' b v k\r\n',
            'mg ' .. b64('zz') .. ' b k v\r\nmg ' .. b64('zz') .. ' b k q v\r\n',
            -- Dead and flushed items.
            set_cmd('d', 'v', 0, 10),
            tick(11),
            'mg d v\r\nmg d v k\r\n',
            set_cmd('f', 'v'),
            'flush_all\r\nmg f v\r\n',
            -- Forms and flags left to Lua.
            'mg\r\nmg a v\r\n',
            'mg a v v\r\nmg a v\r\n',
            'mg a v !\r\nmg a v\r\n',
            'mg a v 1\r\nmg a v\r\n',
            'mg a v w\r\nmg a v\r\n',
            'mg a T\r\nmg a v\r\n',
            'mg a Tx\r\nmg a v\r\n',
            'mg a T99999999999\r\nmg a v\r\n',
            'mg a O' .. ('o'):rep(32) .. '\r\nmg a v\r\n',
            'mg ' .. ('k'):rep(251) .. ' v\r\nmg a v\r\n',
            'mg ' .. ('k'):rep(240) .. ' b v\r\nmg a v\r\n',
            'mg a' .. (' v'):rep(19) .. '\r\nmg a v\r\n',
            'mg a v F99999999999\r\nmg a v\r\n',
            'mg a v C18446744073709551616\r\nmg a v\r\n',
            'mg a v M\r\nmg a v\r\n',
            'mg $$ b v\r\nmg a v\r\n',
            'mg\rx a v\r\nmg a v\r\n',
            'mg nope N30 v t\r\nmg nope v c\r\n',
            'mg a R30 v t\r\nmg a v\r\n',
            'mg a E1 N30 v\r\n',
        })
    )
    t.assert(counts.batch > 80, counts.batch)
end

-- mg and win tokens: a stale item, a token out, a token kept in memory on
-- a read-only instance are left to Lua; the rest is served in C.
g.test_batch_meta_tokens = function(cg)
    local _, _, counts = same(cg, {
        set_cmd('s', 'v') .. set_cmd('o', 'w', 0, 100),
        'md s I\r\nmg s v c\r\nmg s v c\r\nget s\r\nmg o v\r\n',
        set_cmd('s', 'v') .. 'mg s v\r\n',
        'mg o R200 v\r\nmg o v\r\nmg o v t\r\n',
        function()
            box.cfg {read_only = true}
            fiber.sleep(0.01)
        end,
        'mg o v\r\nmg s v h\r\nmg o R300 v\r\nmg o v\r\nmg s v\r\n',
        function()
            box.cfg {read_only = false}
            fiber.sleep(0.01)
        end,
        'mg o v\r\nmg s v\r\ndecr o 1\r\nmg o v\r\n',
        set_cmd('o', '10') .. 'mg o v\r\nincr o 1\r\nmg o v\r\n',
    })
    t.assert(counts.batch > 10, counts.batch)
end

g.test_batch_meta_set = function(cg)
    local key = 'k y'
    local _, _, counts = same(
        cg,
        both_ways({
            ms_cmd('a', 'abc') .. ms_cmd('a', 'de', ' T30 F5 c s k O1'),
            'mg a v f t c\r\n',
            ms_cmd('a', 'x', ' q') .. ms_cmd('a', 'x', ' q MS O2'),
            ms_cmd('a', 'y', ' ME c s') .. ms_cmd('b', 'y', ' ME c s k'),
            ms_cmd('c', 'y', ' MR c') .. ms_cmd('a', 'z', ' MR c q'),
            ms_cmd('a', '+', ' MA s c') .. ms_cmd('a', '-', ' MP s c q'),
            ms_cmd('nope', '+', ' MA s c') .. ms_cmd('nope', '-', ' MP k'),
            'mg a v\r\n',
            -- CAS: match, mismatch, 0, missing; with each mode.
            'mg a c\r\n',
            cas_step('a', 'ms a 2 C%s c\r\nc1\r\n'),
            cas_step('a', 'ms a 2 MR C%s c q\r\nc1\r\n'),
            cas_step('a', 'ms a 2 MA C%s c s\r\nc1\r\n'),
            cas_step('a', 'ms a 2 MA C%s c s\r\nc1\r\n', 1),
            ms_cmd('a', 'c2', ' C1 c s'),
            ms_cmd('a', 'c3', ' C0') .. ms_cmd('nope', 'c4', ' C1 c'),
            ms_cmd('a', 'c5', ' MR C1') .. ms_cmd('a', 'c6', ' ME C1 c'),
            ms_cmd('a', 'c7', ' MA C1') .. ms_cmd('a', 'c8', ' MA C0'),
            'mg a v c\r\n',
            ms_cmd('a', 'c9', ' MP C2 q'),
            -- Invalidate: an older CAS stores a stale item with the old TTL.
            ms_cmd('i', 'v', ' T100'),
            'mg i c t\r\n',
            cas_step('i', 'ms i 1 I C%s c\r\nw\r\n', -1),
            'mg i v c t\r\n',
            cas_step('i', 'ms i 1 I C%s c s\r\nw\r\n'),
            cas_step('i', 'ms i 1 I C%s c s\r\nw\r\n', 1),
            ms_cmd('i', 'x', ' I C1 c'),
            'mg i v c t\r\n',
            ms_cmd('i', 'y', ' I') .. ms_cmd('i', 'z', ' I C2000000'),
            -- TTL.
            ms_cmd('t', 'v', ' T0') .. 'mg t t\r\n'
                .. ms_cmd('t', 'v', ' T-1') .. 'mg t v\r\n',
            ms_cmd('t', 'v', ' T1800003000') .. 'mg t t\r\n',
            ms_cmd('t', 'v') .. ms_cmd('t', 'w', ' T-1 MA') .. 'mg t v t\r\n',
            -- base64 keys.
            ms_cmd(b64(key), 'v', ' b k c') .. 'mg ' .. b64(key) .. ' b v k\r\n',
            ms_cmd(b64(key), 'vv', ' b q k MA') .. 'get ' .. 'k\r\n',
            -- Dead items.
            ms_cmd('d', 'v', ' T10'),
            tick(11),
            ms_cmd('d', 'v', ' MR') .. ms_cmd('d', 'v', ' C1') .. 'mg d v\r\n',
            ms_cmd('d', 'v', ' T10'),
            tick(11),
            ms_cmd('d', 'v', ' ME c') .. 'mg d v\r\n',
            -- Forms and flags left to Lua.
            'ms a\r\nmg a v\r\n',
            'ms a x\r\nmg a v\r\n',
            'ms a -1\r\nmg a v\r\n',
            ms_cmd('a', 'x', ' MX') .. 'mg a v\r\n',
            ms_cmd('a', 'x', ' M') .. 'mg a v\r\n',
            ms_cmd('a', 'x', ' E5') .. 'mg a v c\r\n',
            ms_cmd('a', 'x', ' MA N30') .. ms_cmd('n', 'x', ' MA N30')
                .. 'mg n v\r\n',
            ms_cmd('a', 'x', ' N30') .. 'mg a v\r\n',
            ms_cmd('a', 'x', ' T') .. 'mg a v\r\n',
            ms_cmd('a', 'x', ' F-1') .. 'mg a v\r\n',
            ms_cmd('a', 'x', ' ?') .. 'mg a v\r\n',
            ms_cmd('a', 'x', ' noreply') .. 'mg a v\r\n',
            'ms a 1\r\nxy\r\nmg a v\r\n',
            'ms a 1 T1\r\nxy\r\nmg a v\r\n',
            ms_cmd('$$', 'x', ' b') .. 'mg a v\r\n',
            ms_cmd('a', 'x', ' O' .. ('o'):rep(40)) .. 'mg a v\r\n',
        })
    )
    t.assert(counts.batch > 60, counts.batch)
end

g.test_batch_meta_set_too_large = function(cg)
    same(cg, {
        ms_cmd('z', ('v'):rep(1988)),
        ms_cmd('z', ('v'):rep(1989)) .. 'mg z s\r\n',
        ms_cmd('z', ('v'):rep(1985), ' F1') .. 'mg z s\r\n',
        ms_cmd('z', ('v'):rep(1984), ' F1 q') .. 'mg z s\r\n',
        stats_step,
    }, {max_item_size = 2048}
    )
end

g.test_batch_meta_delete = function(cg)
    local key = 'k y'
    local _, _, counts = same(
        cg,
        both_ways({
            set_cmd('a', '1') .. set_cmd('b', '2') .. set_cmd('c', '3'),
            'md a\r\nmd a\r\nmd a q\r\nmd b q\r\nmd b q O1 k\r\n',
            'mg c c\r\n',
            'md c C1\r\n',
            cas_step('c', 'md c C%s O5 k\r\nmd c k\r\n'),
            -- T, F and E change nothing without I or x.
            set_cmd('e', '1') .. 'md e T30 F3 E9 k\r\nmg e v\r\n',
            set_cmd(key, '1') .. 'md ' .. b64(key) .. ' b k\r\nmd ' .. b64(key)
                .. ' b k\r\n',
            set_cmd('d', 'v', 0, 10),
            tick(11),
            'md d k\r\nmd d q\r\n',
            -- Left to Lua.
            set_cmd('i', '1') .. 'md i I\r\nmg i v c\r\nmd i x\r\nmg i v\r\n',
            'md\r\nmd i\r\n',
            'md i Q\r\nmd i\r\n',
            'md i C\r\nmd i\r\n',
        })
    )
    t.assert(counts.batch > 15, counts.batch)
end

g.test_batch_meta_arith = function(cg)
    local key = 'k y'
    local _, _, counts = same(
        cg,
        both_ways({
            set_cmd('n', '10'),
            'ma n\r\nma n v\r\nma n MD v\r\nma n M- D3 v c t\r\n',
            'ma n M+ D100 v k O1\r\nma n MI q v\r\nma n D18446744073709551615 v\r\n',
            'ma n MD D99999999999 v\r\nma n v t c\r\n',
            'ma nope\r\nma nope q k O2\r\nma nope MD v\r\n',
            'mg n c\r\n',
            'ma n C1 v\r\nma n C0 v\r\n',
            cas_step('n', 'ma n C%s v c\r\n'),
            'ma n T30 v t\r\nmg n t\r\nma n T0 t\r\nma n T-1 v t c\r\nmg n v\r\n',
            set_cmd('n', '5') .. 'ma n T1800003000 t\r\n',
            set_cmd(key, '7') .. 'ma ' .. b64(key) .. ' b k v\r\n',
            set_cmd('n', '5') .. 'ma n J5 v\r\n',
            -- Left to Lua.
            set_cmd('x', 'ab') .. 'ma x\r\nma x q\r\n',
            'ma n N30 v\r\nma new N30 J5 v\r\nma n E5 v c\r\n',
            'ma n MX\r\nma n M\r\nma n D\r\nma n Dx\r\nma n v\r\n',
            set_cmd('d', '5', 0, 10),
            tick(11),
            'ma d v\r\n',
        })
    )
    t.assert(counts.batch > 30, counts.batch)
end

-- The reply to data, up to that of an mg miss after it (not an mn: those
-- are what is tested). The parts are sent one by one.
local function upto(...)
    local parts = {...}
    return function(cg)
        for i, part in ipairs(parts) do
            if i > 1 then
                fiber.sleep(0.02)
            end
            cg.client:send(part)
        end
        cg.client:send('mg __end k\r\n')
        local reply = cg.client:read_until('EN k__end\r\n')
        t.assert(reply, 'no reply')
        return reply:sub(1, -#'EN k__end\r\n' - 1)
    end
end

g.test_batch_mn = function(cg)
    local replies, _, counts = same(cg, {
        upto('mn\r\nmn\nmn x y\r\nmn\r\n'),
        upto('mn\r\nmnx\r\nmn\r\n'),
        upto('mn\r\n mn\r\nmn\r\nmn\rx\r\nmn  \r\n'),
        upto('mn', '\r\n'),
        upto('m', 'n\r\nm', 'n', '\r\n'),
        stats_step,
    })
    t.assert_equals(
        replies[1],
        'MN\r\nCLIENT_ERROR bad command line format\r\nMN\r\nMN\r\n'
    )
    t.assert_equals(replies[4], 'MN\r\n')
    t.assert(counts.batch > 10, counts.batch)
end

-- The Lua batch stops at a command the C batch serves, and the C batch
-- goes on: the replies come in their order with one write, the batch
-- limit and the barriers are those of a Lua batch.
g.test_batch_reentry = function(cg)
    local function gets(n, from)
        local p = {}
        for i = 1, n do
            p[i] = 'get k' .. ((i + (from or 0)) % 3) .. '\r\n'
        end
        return table.concat(p)
    end
    local mixed = {}
    for i = 1, 100 do
        mixed[#mixed + 1] = i % 7 == 0 and 'version\r\n'
            or i % 5 == 0 and 'mg k1 N30 v\r\n'
            or i % 3 == 0 and 'me k0\r\n'
            or ('get k%d\r\n'):format(i % 3)
    end
    local _, _, counts = same(cg, {
        set_cmd('k0', '100') .. set_cmd('k1', '100') .. set_cmd('k2', '1'),
        'get k0\r\nversion\r\nget k1\r\nverbosity 0\r\nget k2\r\n',
        table.concat(mixed),
        'version\r\nversion\r\nget k0\r\nversion\r\nget k1\r\n',
        gets(300) .. 'version\r\n' .. gets(300) .. 'decr k1 1\r\nget k1\r\n',
        gets(200) .. 'version\r\n' .. gets(300) .. 'version\r\n'
            .. 'decr k0 1\r\ndecr k1 1\r\nget k0 k1\r\n',
        gets(511) .. 'version\r\nget k0\r\n',
        -- A barrier in the Lua batch.
        'get k0\r\nflush_all 100\r\nget k0\r\nstats detail on\r\nget k0\r\n',
        -- quit, a stream, a line too long.
        until_close('get k0\r\nversion\r\nget k0\r\nquit\r\nget k0\r\n'),
        'get k0\r\nversion\r\nget k0\r\nlru_crawler metadump all\r\n',
        'get k0\r\nversion\r\nget k0\r\n',
        until_close('get k0\r\nversion\r\nget k0\r\n' .. ('x'):rep(3000)),
        -- Fast, slow, fast, the slow one incomplete.
        pieces('get k0\r\nversion\r\nget k0\r\nmg k0 N30 v', '\r\nget k0\r\n'),
        pieces('get k0\r\nversion\r\nset k9 0 0 5\r\nab', 'cde\r\nget k9\r\n'),
        -- The same kind left to Lua twice: the rest of the batch is
        -- Lua's.
        'mg k0 N30 v\r\nmg k0 v\r\nmg k0 N30 v\r\nmg k0 v\r\nget k0\r\n',
        stats_step,
    })
    t.assert(counts.batch > 1500, counts.batch)
end

-- A Lua batch takes a part of the input at first: commands across the end
-- of that part. A kind of command that the C batch didn't serve stays with
-- the Lua batch, which then parses the big commands of that kind.
g.test_batch_reentry_chunk = function(cg)
    local script = {set_cmd('a', '1')}
    for _, n in ipairs({4060, 4075, 4080, 4085, 4090, 4096, 4100, 4110}) do
        local pad = ('version\r\n'):rep(math.floor(n / 9))
        -- set: left to Lua for its "+1".
        script[#script + 1] = 'set h +1 0 1\r\nx\r\n' .. pad
            .. set_cmd('b', ('v'):rep(50)) .. 'get b\r\n'
        script[#script + 1] = 'set h +1 0 1\r\nx\r\n'
            .. ('version\r\n'):rep(400)
            .. set_cmd('b', ('v'):rep(n))
            .. 'get b\r\n'
        script[#script + 1] = 'get ' .. ('k'):rep(251)
            .. '\r\n' .. pad
            .. 'get' .. (' a'):rep(1200)
            .. '\r\nmg a v\r\n'
        script[#script + 1] = 'mg a N30 v\r\n' .. pad
            .. 'mg a N30 v k\r\nversion\r\nget a\r\n'
        script[#script + 1] = 'mg a N30 v\r\n' .. ('version\r\n'):rep(450)
            .. ('x'):rep(n - 1000) .. '\r\nget a\r\n'
    end
    same(cg, script)
end

g.test_batch_noreply_quiet = function(cg)
    same(cg, {
        set_cmd('a', '1', 0, 0, ' noreply') .. 'add a 0 0 1 noreply\r\nx\r\n'
            .. 'replace a 0 0 1 noreply\r\ny\r\ndelete b noreply\r\n'
            .. 'incr a 1 noreply\r\ntouch a 10 noreply\r\nget a\r\n',
        ms_cmd('m', '1', ' q') .. ms_cmd('m', '2', ' q ME') .. 'mg m q v\r\n'
            .. 'mg x q v\r\nma m q\r\nma x q\r\nmd x q\r\nmd m q\r\nmn\r\n',
        stats_step,
    })
end

g.test_batch_read_only_writes = function(cg)
    local _, _, counts = same(cg, {
        set_cmd('a', '1') .. set_cmd('n', '5'),
        function()
            box.cfg {read_only = true}
            fiber.sleep(0.01)
        end,
        'mg a v h\r\nmg a T30 v\r\nget a\r\n',
        'add b 0 0 1\r\nx\r\ndelete a\r\nincr n 1\r\ntouch a 10\r\n',
        'gat 10 a\r\nms a 1\r\nx\r\nmd a\r\nma n\r\nmg a v t\r\nmn\r\n',
        stats_step,
        function()
            box.cfg {read_only = false}
            fiber.sleep(0.01)
        end,
        'mg a v h\r\n',
    })
    t.assert(counts.batch > 5, counts.batch)
end

g.test_batch_track_access_off = function(cg)
    local script = {
        set_cmd('a', '1') .. set_cmd('n', '100'),
        'mg a h l\r\nmg a h l\r\ngat 10 a\r\ntouch a 20\r\nmg a h l t\r\n',
        tick(engine.LA_UPDATE_INTERVAL),
        'decr n 1\r\nma n\r\nmg n h l v\r\n',
    }
    same(cg, script, {track_access = false})
end

-- The writes that fail in C go to Lua, which answers its error; the C
-- batch takes no CAS value or stamp for them.
g.test_batch_failed_new_writes = function(cg)
    local function failing(on)
        return function()
            local space = box.space[SPACE]
            if on then
                cg.trigger = function(old, new)
                    if old ~= nil and (new == nil or old[2] ~= new[2])
                        and old[1]:sub(1, 1) == 'f' then
                        error('no write')
                    end
                end
                space:before_replace(cg.trigger)
            else
                space:before_replace(nil, cg.trigger)
            end
        end
    end
    same(cg, {
        set_cmd('f1', '100') .. set_cmd('ok', '5'),
        failing(true),
        'delete f1\r\nincr f1 1\r\nappend f1 0 0 1\r\nx\r\nmd f1\r\n'
            .. ms_cmd('f1', 'x')
            .. 'ma f1\r\nincr ok 1\r\nmg ok v c\r\n',
        failing(false),
        'gets f1 ok\r\n' .. set_cmd('z', 'z') .. 'gets z\r\n',
        stats_step,
    })
end

-- Which forms the C batch serves itself (1) and which it leaves to Lua
-- (0), one command at a time.
g.test_batch_served_forms = function(cg)
    reset(cg)
    mcc.datapath(true)
    local buffer = require('buffer')
    local out = buffer.ibuf()
    local ctx = {space_name = SPACE}
    local function served(cmd)
        engine.batch(
            ctx,
            set_cmd('a', 'abc') .. set_cmd('n', '10'),
            out,
            512,
            1048576
        )
        out:reset()
        local res = {engine.batch(ctx, cmd .. 'x\r\n', out, 512, 1048576)}
        out:reset()
        return res[2]
    end
    local cases = {
        ['get a b\r\n'] = 1,
        ['gets a\r\n'] = 1,
        ['gat 10 a b\r\n'] = 1,
        ['gats 0 a\r\n'] = 1,
        ['gat -1 a\r\n'] = 0,
        ['touch a 10\r\n'] = 1,
        ['touch a 10 noreply\r\n'] = 1,
        ['delete a\r\n'] = 1,
        ['delete nope noreply\r\n'] = 1,
        ['incr n 5\r\n'] = 1,
        ['decr n 5 noreply\r\n'] = 1,
        ['incr a 1\r\n'] = 0,
        ['incr nope 1\r\n'] = 1,
        ['add b 0 0 1\r\nx\r\n'] = 1,
        ['replace a 0 0 1\r\nx\r\n'] = 1,
        ['append a 0 0 1\r\nx\r\n'] = 1,
        ['prepend a 0 0 1\r\nx\r\n'] = 1,
        ['cas a 0 0 1 5\r\nx\r\n'] = 1,
        ['set a 0 -1 1\r\nx\r\n'] = 0,
        ['set a 0 1700000000 1\r\nx\r\n'] = 1,
        ['mg a v k f c s t q O1 h l u\r\n'] = 1,
        ['mg a T10 v\r\n'] = 1,
        ['mg YQ== b v k\r\n'] = 1,
        ['mg a N10 v\r\n'] = 0,
        ['mg a R10 v\r\n'] = 0,
        ['ms a 1 T10 F5 q O1 k c s\r\nx\r\n'] = 1,
        ['ms a 1 MA\r\nx\r\n'] = 1,
        ['ms a 1 C5 I\r\nx\r\n'] = 1,
        ['ms a 1 E5\r\nx\r\n'] = 0,
        ['ms a 1 MA N10\r\nx\r\n'] = 0,
        ['md a q O1 k\r\n'] = 1,
        ['md a C1\r\n'] = 1,
        ['md a I\r\n'] = 0,
        ['md a x\r\n'] = 0,
        ['ma n MD D2 v c t k O1\r\n'] = 1,
        ['ma n T10 q\r\n'] = 1,
        ['ma n N10\r\n'] = 0,
        ['ma n E10\r\n'] = 0,
        ['mn\r\n'] = 1,
        ['mn\n'] = 0,
        ['me a\r\n'] = 0,
        ['version\r\n'] = 0,
    }
    local got = {}
    for cmd, _ in pairs(cases) do
        got[cmd] = served(cmd)
    end
    t.assert_equals(got, cases)
end

-- The group (datapath.c): the writes of consecutive commands of a C batch
-- in one transaction, committed before the batch returns -----------------

-- Count the transactions of the writes to the data space from here
-- (on_replace: box.txn_id() of each statement) to the step of on = false,
-- into cg.txns[<the path>][<n>] for the n-th count of the run.
local function count_txns(on)
    return function(cg)
        local space = box.space[SPACE]
        if on then
            cg.txn_ids = {}
            cg.txn_trigger = function()
                cg.txn_ids[box.txn_id()] = true
            end
            space:on_replace(cg.txn_trigger)
            return
        end
        space:on_replace(nil, cg.txn_trigger)
        local n = 0
        for _ in pairs(cg.txn_ids) do
            n = n + 1
        end
        local path = mcc.datapath().enabled and 'c' or 'lua'
        cg.txns = cg.txns or {}
        cg.txns[path] = cg.txns[path] or {}
        table.insert(cg.txns[path], n)
    end
end

-- Pipelines of writes and reads in one group: the reads see the writes,
-- the CAS values, the holds and the counters are those of the Lua path.
g.test_batch_group = function(cg)
    cg.txns = {}
    local sets = {}
    for i = 1, 16 do
        sets[i] = set_cmd('s' .. i, tostring(i))
    end
    local n100 = set_cmd('n', '100')
    same(cg, {
        set_cmd('a', '1') .. n100,
        count_txns(true),
        -- 16 sets: one transaction.
        table.concat(sets),
        count_txns(false),
        count_txns(true),
        -- Every kind of write, and reads of what the group wrote.
        set_cmd('a', '2') .. 'get a\r\ngets a\r\nappend a 0 0 1\r\nx\r\n'
            .. 'prepend a 0 0 1\r\ny\r\nget a\r\nincr n 5\r\nget n\r\n'
            .. 'add a 0 0 1\r\nz\r\nreplace b 0 0 1\r\nz\r\n'
            .. 'delete a\r\nget a\r\ntouch n 100\r\ngat 200 n\r\n'
            .. ms_cmd('m', 'v', ' c s T50') .. 'mg m v c s t\r\n'
            .. 'ma n D2 v c t\r\nmd m q\r\nmg m v\r\nmd m\r\n'
            .. 'decr n 1000 noreply\r\nget n\r\n',
        count_txns(false),
        -- Holds and the padding of an arithmetic in place, an mn inside.
        n100 .. 'get n\r\ndecr n 1\r\nget n\r\nmn\r\ndecr n 1\r\nmg n v\r\n',
        -- The CAS of a write of the group, by a later command of it.
        cas_step(
            'n',
            'cas n 0 0 1 %s\r\n7\r\n' .. set_cmd('b', '2')
                .. 'gets n b\r\ncas n 0 0 1 %s\r\n8\r\nms n 1 C%s\r\n9\r\n'
        ),
        cas_step('n', 'ms n 1 C%s c\r\n1\r\nms n 1 C%s c\r\n2\r\nmg n v c\r\n'),
        -- A command left to Lua in the middle: a group before it, one after.
        count_txns(true),
        set_cmd('c', '1') .. set_cmd('d', '2')
            .. 'version\r\n' .. set_cmd('e', '3')
            .. set_cmd('f', '4') .. 'get c d e f\r\n',
        count_txns(false),
        -- noreply and q in the group.
        set_cmd('q1', '1', 0, 0, ' noreply') .. ms_cmd('q2', '2', ' q')
            .. 'incr n 1 noreply\r\nmd q1 q\r\nma n q\r\ndelete zz noreply\r\n'
            .. 'get q1 q2 n\r\n',
        -- Dead items: the lazy deletions are writes of the group too.
        set_cmd('x1', 'v', 0, 10) .. set_cmd('x2', 'v', 0, 10)
            .. set_cmd('x3', 'v', 0, 10) .. set_cmd('x4', 'v', 0, 10),
        tick(11),
        'get x1\r\ntouch x2 10\r\nincr x3 1\r\ndelete x4\r\n'
            .. set_cmd('x1', 'w')
            .. 'get x1 x2 x3 x4\r\n',
        stats_step,
    })
    -- The Lua engine: one transaction per write; the C batch: one per
    -- group.
    t.assert_equals(cg.txns.c, {1, 1, 2})
    t.assert_equals(cg.txns.lua[1], 16)
    t.assert_gt(cg.txns.lua[2], 10)
    t.assert_equals(cg.txns.lua[3], 8)
end

-- Groups up to GROUP_MAX_WRITES writes and GROUP_MAX_BYTES of values: a
-- longer batch commits as it goes, with the same replies.
g.test_batch_group_limits = function(cg)
    cg.txns = {}
    local sets, keys = {}, {}
    for i = 1, 100 do
        sets[i] = set_cmd('k' .. i, 'v')
        keys[i] = 'k' .. i
    end
    local gat = ('gat 100 %s\r\n'):format(table.concat(keys, ' '))
    same(cg, {
        count_txns(true),
        -- 100 sets and 10 gats of their 100 keys (a touch each): 1100
        -- writes, over the limit with the last gat; then one more group.
        table.concat(sets) .. gat:rep(10) .. set_cmd('z', 'z') .. 'get z\r\n',
        count_txns(false),
        stats_step,
    })
    t.assert_equals(cg.txns.c, {2})
    t.assert_equals(cg.txns.lua, {1102})
    -- Over 1 MiB of values with the fourth set (one input: the batch over
    -- a socket would stop at the end of each read).
    reset(cg, {max_item_size = 400000})
    mcc.datapath(true)
    local out = require('buffer').ibuf()
    local input = {}
    for i = 1, 6 do
        input[i] = set_cmd('b' .. i, ('x'):rep(300000))
    end
    input = table.concat(input) .. 'get b1\r\n'
    count_txns(true)(cg)
    local res = {engine.batch({space_name = SPACE}, input, out, 512, 400000)}
    count_txns(false)(cg)
    t.assert_equals({res[1], res[2], res[5]}, {#input, 7, 6})
    t.assert_equals(cg.txns.c, {2, 2})
end

-- A write that fails in the group (a before_replace error, as a memtx out
-- of memory would): the group commits the commands before it, the command
-- goes to Lua, which answers it as before, and the C batch goes on after
-- it. (No classic set fails here: the Lua batch runs it with the
-- per-request C set first, which takes a CAS value of its own, so the CAS
-- values after it differ from the Lua path's, with or without groups.)
g.test_batch_group_failed_write = function(cg)
    local function failing(on)
        return function()
            local space = box.space[SPACE]
            if on then
                cg.trigger = function(_, new)
                    if new ~= nil and new[1]:sub(1, 1) == 'f' and new[2] ~= 'ok' then
                        error('no write')
                    end
                end
                space:before_replace(cg.trigger)
            else
                space:before_replace(nil, cg.trigger)
            end
        end
    end
    same(cg, {
        set_cmd('f1', 'ok') .. set_cmd('n', '1'),
        failing(true),
        set_cmd('a', '1') .. 'incr n 1\r\nreplace f1 0 0 1\r\nx\r\n'
            .. set_cmd('b', '2') .. 'get a b f1 n\r\n',
        set_cmd('a', '3', 0, 0, ' noreply') .. 'add f2 0 0 1 noreply\r\n'
            .. 'x\r\n' .. ms_cmd('f3', 'x', ' q')
            .. 'get f1 a\r\n' .. 'incr n 1\r\nmg n v c\r\n',
        -- The last access of a hit fails: the get answers without it.
        'get a f1 b\r\nmg f1 v h l\r\ndecr n 1\r\n',
        failing(false),
        'get a b f1 f2 f3 n\r\n',
        stats_step,
    })
end

-- A failed commit: see group_test.lua (a child server, where a timed out
-- synchronous transaction rolls back the group behind it; the WAL error
-- injections are compiled out of release builds).

-- Connections writing at once with the WAL: the groups of one connection
-- and the writes of the others interleave only at the commits, every
-- increment is counted once and every item gets a CAS of its own.
g.test_batch_group_concurrent = function(cg)
    for _, on in ipairs({true, false}) do
        reset(cg)
        mcc.datapath(on)
        fiber.time = real_time
        cg.client:call(set_cmd('shared', '0'), 'STORED\r\n')
        local clients, values = {}, {}
        local N, M = 8, 40
        for i = 1, N do
            clients[i] = client.must_connect(cg.port)
            local p = {}
            for j = 1, M do
                p[#p + 1] = set_cmd(('c%d_%d'):format(i, j), 'v')
                    .. 'incr shared 1\r\n'
                    .. ('get c%d_%d\r\n'):format(i, j)
            end
            clients[i]:send(table.concat(p))
        end
        for i, c in ipairs(clients) do
            for j = 1, M do
                t.assert_equals(c:line(), 'STORED\r\n')
                local v = tonumber(c:line():match('^(%d+)\r\n$'))
                t.assert(v ~= nil and values[v] == nil, v)
                values[v] = true
                t.assert_equals(
                    c:read_until('END\r\n'),
                    ('VALUE c%d_%d 0 1\r\nv\r\nEND\r\n'):format(i, j)
                )
            end
            c:close()
        end
        for v = 1, N * M do
            t.assert(values[v], v)
        end
        local cas = {}
        for _, tuple in box.space[SPACE]:pairs() do
            local c = tostring(tuple[5])
            t.assert_equals(cas[c], nil, c)
            cas[c] = true
        end
        t.assert_equals(
            box.space[SPACE]:get('shared')[2],
            tostring(N * M)
        )
    end
end
