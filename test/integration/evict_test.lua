-- Eviction (evict.lua): child servers with memtx's smallest memtx_memory
-- (64 MiB) are filled far beyond it, through the text protocol, with the C
-- data path on and off. Plus the options, in this process.

local fiber = require('fiber')
local t = require('luatest')
local server = require('luatest.server')

local env = require('test.helpers.env')
local client = require('test.helpers.client')
local rh = require('test.helpers.role')

-- memtx raises anything smaller to 64 MiB.
local MEMORY = 64 * 1024 * 1024
local NAME = 'ev'

local g = t.group('evict', {{datapath = true}, {datapath = false}})

g.before_each(function(cg)
    cg.server = server:new({
        alias = 'mcc_evict',
        env = env.lua_env(),
        box_cfg = {memtx_memory = MEMORY},
    })
    cg.server:start()
    cg.port = env.free_port()
    cg.server:exec(function(on)
        require('memcached').datapath(on)
    end, {cg.params.datapath})
end)

g.after_each(function(cg)
    cg.server:drop()
end)

local function create(cg, opts)
    opts.listen = '127.0.0.1:' .. cg.port
    cg.server:exec(function(name, opts_)
        require('memcached').create(name, opts_)
    end, {NAME, opts})
end

-- Store n items <prefix><from..from+n-1> of size bytes, 100 per write.
-- Returns the number of replies of each kind.
local function fill(port, prefix, n, size, from)
    local c = client.must_connect(port)
    local value = string.rep('v', size)
    local replies = {}
    from = from or 1
    local i, last = from, from + n - 1
    while i <= last do
        local parts = {}
        while #parts < 100 and i <= last do
            parts[#parts + 1] = ('set %s%d 0 0 %d\r\n%s\r\n'):format(
                prefix,
                i,
                size,
                value
            )
            i = i + 1
        end
        c:send(table.concat(parts))
        for _ = 1, #parts do
            local line = c:line(10)
            replies[line] = (replies[line] or 0) + 1
        end
    end
    c:close()
    return replies
end

local function stats(port, sub)
    local c = client.must_connect(port)
    c:send(sub and ('stats ' .. sub .. '\r\n') or 'stats\r\n')
    local res = {}
    while true do
        local line = c:line()
        if line == nil or line == 'END\r\n' then
            break
        end
        local k, v = line:match('^STAT (%S+) (.-)\r\n$')
        res[k] = tonumber(v) or v
    end
    c:close()
    return res
end

local function evict_info(cg)
    return cg.server:exec(function()
        return require('memcached.evict').info()
    end)
end

local function count_keys(cg, prefix, n)
    return cg.server:exec(function(space, prefix_, n_)
        local s = box.space[space]
        local found = 0
        for i = 1, n_ do
            if s:get(prefix_ .. i) ~= nil then
                found = found + 1
            end
        end
        return found
    end, {'__mcc_' .. NAME, prefix, n})
end

-- The evictor keeps the memory between the watermarks: every write is
-- stored, and the memory goes back below high when the writes stop.
g.test_background = function(cg)
    create(cg, {evict = {high = 0.6, low = 0.5}, metrics = true})
    local n = 40000
    local replies = fill(cg.port, 'k', n, 4000)
    t.assert_equals(replies, {['STORED\r\n'] = n})
    -- The episode ends below high.
    t.helpers.retrying({timeout = 10}, function()
        local info = evict_info(cg)
        t.assert_le(info.fill, 0.6)
        t.assert_equals(info.spaces['__mcc_' .. NAME].evicting, false)
    end)
    local st = stats(cg.port)
    t.assert_gt(st.evictions, 0)
    t.assert_lt(st.curr_items, n)
    -- Each item is in the cache or evicted.
    t.assert_equals(st.curr_items + st.evictions + st.reclaimed, n)
    t.assert_equals(stats(cg.port, 'settings').evictions, 'on')
    local info = cg.server:exec(function(name)
        return require('memcached').get(name):info()
    end, {NAME})
    t.assert_equals(info.evictions, stats(cg.port).evictions)
    t.assert_equals(
        rh.value(
            rh.collect(cg.server),
            'tnt_memcached_evictions_total',
            {name = NAME}
        ),
        info.evictions
    )
    -- The newest items are there.
    t.assert_equals(
        client.get(cg.port, 'k' .. n),
        ('VALUE k%d 0 4000\r\n%s\r\nEND\r\n'):format(
            n,
            string.rep('v', 4000)
        )
    )
end

-- Without the evictor (high = 1 is never reached: allocations fail
-- earlier), the writes that fail evict for themselves.
g.test_direct = function(cg)
    create(cg, {evict = {high = 1, low = 0.99}})
    local n = 40000
    local replies = fill(cg.port, 'k', n, 4000)
    t.assert_equals(replies, {['STORED\r\n'] = n})
    local st = stats(cg.port)
    t.assert_gt(st.evictions, 0)
    t.assert_gt(st.direct_reclaims, 0)
    t.assert_equals(st.store_no_memory, 0)
    t.assert_equals(st.curr_items + st.evictions + st.reclaimed, n)
end

-- Dead items go first, and count as reclaimed.
g.test_reclaimed = function(cg)
    create(cg, {evict = {high = 0.6, low = 0.5}, expire = {enabled = false}})
    local c = client.must_connect(cg.port)
    local value = string.rep('x', 4000)
    for i = 1, 6000 do
        c:send(('set d%d 0 1 4000\r\n%s\r\n'):format(i, value))
        t.assert_equals(c:line(), 'STORED\r\n')
    end
    c:close()
    fiber.sleep(1.1)
    fill(cg.port, 'k', 12000, 4000)
    local st = stats(cg.port)
    t.assert_gt(st.reclaimed, 0)
end

-- Approximated LRU: items fetched recently survive, untouched ones of the
-- same age don't.
g.test_lru = function(cg)
    create(cg, {evict = {high = 0.6, low = 0.5}})
    -- Every fetch records the access (instead of once per minute).
    cg.server:exec(function()
        require('memcached.engine').LA_UPDATE_INTERVAL = 0
    end)
    local N = 2000
    fill(cg.port, 'old', N, 1000)
    fill(cg.port, 'hot', N, 1000)
    local function touch_hot()
        local c = client.must_connect(cg.port)
        for i = 1, N, 100 do
            local keys = {}
            for j = i, math.min(N, i + 99) do
                keys[#keys + 1] = 'hot' .. j
            end
            c:send('get ' .. table.concat(keys, ' ') .. '\r\n')
            c:read_values()
        end
        c:close()
    end
    for round = 0, 19 do
        touch_hot()
        fill(cg.port, 'f', 3000, 1000, round * 3000 + 1)
    end
    t.assert_gt(stats(cg.port).evictions, 20000)
    local old = count_keys(cg, 'old', N) / N
    local hot = count_keys(cg, 'hot', N) / N
    t.assert_gt(hot, old + 0.5, ('hot %.2f, old %.2f'):format(hot, old))
end

-- evict.enabled = false: memcached's -M. A write that can't be stored
-- answers as memcached does, and a set drops the old value.
g.test_disabled = function(cg)
    create(cg, {evict = {enabled = false}})
    local c = client.must_connect(cg.port)
    local value = string.rep('v', 4000)
    t.assert_equals(
        c:call(('set victim 0 0 4000\r\n%s\r\n'):format(value)),
        'STORED\r\n'
    )
    local reply
    for i = 1, 40000 do
        reply = c:call(('set k%d 0 0 4000\r\n%s\r\n'):format(i, value))
        if reply ~= 'STORED\r\n' then
            break
        end
    end
    t.assert_equals(reply, 'SERVER_ERROR out of memory storing object\r\n')
    t.assert_equals(
        c:call('touch k1 100\r\n'),
        'SERVER_ERROR out of memory\r\n'
    )
    -- A replace keeps the old value.
    t.assert_equals(
        c:call(
            ('replace k1 0 0 4000\r\n%s\r\n'):format(
                value:upper()
            )
        ),
        'SERVER_ERROR out of memory storing object\r\n'
    )
    t.assert_str_contains(c:call('get k1\r\n', 'END\r\n'), 'VALUE k1 0 4000')
    -- A set drops it.
    t.assert_equals(
        c:call(('set victim 0 0 4000\r\n%s\r\n'):format(value)),
        'SERVER_ERROR out of memory storing object\r\n'
    )
    t.assert_equals(c:call('get victim\r\n'), 'END\r\n')
    c:close()
    local st = stats(cg.port)
    t.assert_equals(st.evictions, 0)
    t.assert_equals(st.store_no_memory, 3)
    t.assert_equals(stats(cg.port, 'settings').evictions, 'off')
end

-- Runtime changes: the evictor follows inst:cfg().
g.test_cfg = function(cg)
    create(cg, {evict = {enabled = false}})
    local space = '__mcc_' .. NAME
    t.assert_equals(evict_info(cg).spaces[space], nil)
    cg.server:exec(function(name)
        require('memcached').get(name):cfg({
            evict = {
                enabled = true,
                high = 0.6,
                low = 0.5,
            },
        })
    end, {NAME})
    t.assert_covers(
        evict_info(cg).spaces[space],
        {enabled = true, high = 0.6, low = 0.5}
    )
    t.assert_equals(stats(cg.port, 'settings').evictions, 'on')
    t.assert_equals(
        fill(cg.port, 'k', 30000, 4000),
        {['STORED\r\n'] = 30000}
    )
    t.assert_gt(stats(cg.port).evictions, 0)
end

--
-- Options, in this process.
--

local o = t.group('evict_options')

o.before_all(function(cg)
    env.box_cfg()
    cg.mcc = require('memcached')
    cg.evict = require('memcached.evict')
    cg.port = env.free_port()
end)

o.after_each(function(cg)
    for _, name in ipairs({'eo', 'eo2'}) do
        if cg.mcc.get(name) ~= nil then
            cg.mcc.destroy(name)
        end
    end
    env.drop_spaces({'__mcc_eo', '__mcc_eo2'})
end)

o.test_validation = function(cg)
    local listen = '127.0.0.1:' .. cg.port
    local cases = {
        {{evict = 1}, "Option 'evict' must be a table"},
        {
            {evict = {enabled = 1}},
            "Bad value for option 'evict': 'enabled' must be a boolean",
        },
        {
            {evict = {high = 0}},
            "Bad value for option 'evict': 'high' must be a number in (0, 1]",
        },
        {
            {evict = {low = 1.5}},
            "Bad value for option 'evict': 'low' must be a number in (0, 1]",
        },
        {
            {evict = {samples = 0}},
            "Bad value for option 'evict': 'samples' must be an integer in [1, 64]",
        },
        {
            {evict = {batch = 1.5}},
            "Bad value for option 'evict': 'batch' must be a positive integer",
        },
        {
            {evict = {bogus = true}},
            "Bad value for option 'evict': unknown option 'bogus'",
        },
        {
            {evict = {high = 0.5}},
            "Bad value for option 'evict': 'low' must be less than 'high'",
        },
        {
            {evict = {low = 0.95}},
            "Bad value for option 'evict': 'low' must be less than 'high'",
        },
        {
            {backend = 'vshard', evict = {enabled = false}},
            "Option 'evict' is not supported by the 'vshard' backend",
        },
    }
    for _, case in ipairs(cases) do
        case[1].listen = listen
        t.assert_error_msg_contains(case[2], cg.mcc.create, 'eo', case[1])
        t.assert_equals(cg.mcc.get('eo'), nil)
        t.assert_error_msg_contains(case[2], cg.mcc.check, 'eo', case[1])
    end

    -- cfg() merges key by key, then checks.
    local inst = cg.mcc.create('eo', {listen = listen})
    t.assert_error_msg_contains(
        "'low' must be less than 'high'",
        inst.cfg,
        inst,
        {evict = {high = 0.8}}
    )
    inst:cfg({evict = {high = 0.8, low = 0.7}})
    t.assert_equals(inst.opts.evict, {
        enabled = true,
        high = 0.8,
        low = 0.7,
        samples = 8,
        batch = 100,
    })
    t.assert_covers(
        cg.evict.info().spaces.__mcc_eo,
        {enabled = true, high = 0.8, low = 0.7}
    )
    -- The defaults of a vshard instance are accepted.
    cg.mcc.check('eo2', {
        listen = listen,
        backend = 'vshard',
        evict = {enabled = true},
    })
end

-- The evictor knows the memory spaces of the running instances only.
o.test_spaces = function(cg)
    local mcc, evict = cg.mcc, cg.evict
    local inst = mcc.create('eo', {listen = '127.0.0.1:' .. cg.port})
    local function enabled(space)
        return evict.enabled(space)
    end
    t.assert(enabled('__mcc_eo'))
    t.assert(evict.info().running)
    t.assert_covers(evict.info().spaces.__mcc_eo, {
        enabled = true,
        high = 0.95,
        low = 0.9,
        samples = 8,
        batch = 100,
        track_access = true,
        evictions = 0,
    })
    inst:cfg({track_access = false})
    t.assert_equals(evict.info().spaces.__mcc_eo.track_access, false)
    inst:stop()
    t.assert_not(enabled('__mcc_eo'))
    t.assert_not(evict.info().running)
    inst:start()
    t.assert(enabled('__mcc_eo'))
    inst:cfg({evict = {enabled = false}})
    t.assert_not(enabled('__mcc_eo'))
    inst:destroy()

    -- Disk storage: no eviction, the option is ignored.
    mcc.create('eo2', {listen = '127.0.0.1:' .. cg.port, storage = 'disk'})
    t.assert_not(enabled('__mcc_eo2'))
    local c = client.must_connect(cg.port)
    c:send('stats settings\r\n')
    t.assert_str_contains(c:read_until('END\r\n'), 'STAT evictions off\r\n')
    c:close()
end

-- The signal: used drops when an item is deleted.
o.test_memory = function(cg)
    local evict = cg.evict
    local quota, used = evict.memory()
    t.assert_equals(quota, box.slab.info().quota_size)
    t.assert_gt(used, 0)
    local s = box.schema.space.create('__mcc_eo', {if_not_exists = true})
    s:create_index('pk', {type = 'HASH', parts = {1, 'string'}})
    for i = 1, 1000 do
        s:insert({tostring(i), string.rep('x', 1000)})
    end
    local _, full = evict.memory()
    t.assert_gt(full, used + 1000 * 1000)
    s:truncate()
    collectgarbage('collect')
    local _, after = evict.memory()
    t.assert_lt(after, full - 900 * 1000)
end
