-- Metrics (memcached/metrics.lua) through the built-in metrics
-- module: values after traffic, in TX and with application threads; the
-- series follow the instances; collection doesn't yield.

local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')
local rh = require('test.helpers.role')
local th = require('test.helpers.threads')

local g = t.group('metrics')

local NTHREADS = 2
local P = 'tnt_memcached_'

g.before_all(function(cg)
    cg.server = th.start('mcc_metrics', NTHREADS)
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.before_each(function(cg)
    cg.port = env.free_port()
end)

g.after_each(function(cg)
    for _, c in ipairs(cg.clients or {}) do
        pcall(c.close, c)
    end
    cg.clients = nil
    cg.server:exec(function()
        local mcc = require('memcached')
        for _, name in ipairs({'m', 't', 'n'}) do
            if mcc.get(name) ~= nil then
                mcc.destroy(name)
            end
            if box.space['__mcc_' .. name] ~= nil then
                box.space['__mcc_' .. name]:drop()
            end
        end
        local metrics = package.loaded['memcached.metrics']
        if metrics ~= nil then
            metrics.disable()
            metrics.REFRESH_INTERVAL = 1
        end
    end)
end)

local function connect(cg)
    local c = client.must_connect(cg.port)
    cg.clients = cg.clients or {}
    table.insert(cg.clients, c)
    return c
end

local function create(cg, name, opts)
    return th.create(cg.server, name, cg.port, opts)
end

local function value(obs, name, labels)
    return rh.value(obs, P .. name, labels)
end

-- Whether the metrics callback of memcached yields (some of the
-- default metrics' callbacks do, so it is called alone).
local function callback_yields(server)
    return server:exec(function()
        local fiber = require('fiber')
        local callback = require('memcached.state').metrics.callback
        t.assert(require('metrics').registry.callbacks[callback])
        local csw = fiber.self():csw()
        callback()
        return fiber.self():csw() ~= csw
    end)
end

-- A client that counts the bytes it sends and receives.
local function counting(c)
    local stats = {sent = 0, received = 0}
    local function call(request, terminator)
        stats.sent = stats.sent + #request
        local reply = c:call(request, terminator)
        t.assert_not_equals(reply, nil, request)
        stats.received = stats.received + #reply
        return reply
    end
    return call, stats
end

g.test_tx = function(cg)
    -- The item size: 'set big' with 11 bytes is 73.
    t.assert_equals(
        create(cg, 'm', {metrics = true, max_item_size = 70}),
        'running'
    )
    local c = connect(cg)
    local call, bytes = counting(c)
    local replies = {
        {'set a 0 0 1\r\nx\r\n', 'STORED\r\n'},
        {'set b 0 0 2\r\nyy\r\n', 'STORED\r\n'},
        {'get a\r\n', 'VALUE a 0 1\r\nx\r\nEND\r\n', 'END\r\n'},
        {'get c\r\n', 'END\r\n'},
        {
            'get a b c\r\n',
            'VALUE a 0 1\r\nx\r\nVALUE b 0 2\r\nyy\r\nEND\r\n',
            'END\r\n',
        },
        {'delete a\r\n', 'DELETED\r\n'},
        {'delete zz\r\n', 'NOT_FOUND\r\n'},
        {'incr n 1\r\n', 'NOT_FOUND\r\n'},
        {'set n 0 0 1\r\n5\r\n', 'STORED\r\n'},
        {'incr n 2\r\n', '7\r\n'},
        {'decr n 1\r\n', '6\r\n'},
        {'decr zz 1\r\n', 'NOT_FOUND\r\n'},
        {'touch b 100\r\n', 'TOUCHED\r\n'},
        {'touch zz 100\r\n', 'NOT_FOUND\r\n'},
        {'cas b 0 0 1 1\r\nz\r\n', 'EXISTS\r\n'},
        {'cas zz 0 0 1 1\r\nz\r\n', 'NOT_FOUND\r\n'},
        {'foo\r\n', 'ERROR\r\n'},
        {
            'incr b 1\r\n',
            'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n',
        },
        {
            'set big 0 0 11\r\n12345678901\r\n',
            'SERVER_ERROR object too large for cache\r\n',
        },
        {'set k 0 0 abc\r\n', 'CLIENT_ERROR bad command line format\r\n'},
        {'mn\r\n', 'MN\r\n'},
        {'mg b v\r\n', 'VA 2\r\nyy\r\n', '\r\nyy\r\n'},
    }
    for _, r in ipairs(replies) do
        t.assert_equals(call(r[1], r[3]), r[2], r[1])
    end

    local obs = rh.collect(cg.server)
    local m = {name = 'm'}
    local function op(o)
        return {name = 'm', op = o}
    end
    local commands = {
        get = 6,
        set = 6,
        touch = 2,
        flush = 0,
        delete = 2,
        incr = 2,
        decr = 2,
    }
    for o, n in pairs(commands) do
        t.assert_equals(value(obs, 'commands_total', op(o)), n, o)
    end
    local hits = {get = 4, touch = 1, delete = 1, incr = 1, decr = 1, cas = 0}
    local misses = {
        get = 2,
        touch = 1,
        delete = 1,
        incr = 1,
        decr = 1,
        cas = 1,
    }
    for o, n in pairs(hits) do
        t.assert_equals(value(obs, 'hits_total', op(o)), n, o)
        t.assert_equals(value(obs, 'misses_total', op(o)), misses[o], o)
    end
    t.assert_equals(value(obs, 'cas_badval_total', m), 1)
    t.assert_equals(value(obs, 'stored_items_total', m), 3)
    t.assert_equals(value(obs, 'meta_commands_total', m), 2)
    for class, n in pairs({protocol = 1, client = 2, server = 1}) do
        t.assert_equals(
            value(obs, 'errors_total', {name = 'm', class = class}),
            n,
            class
        )
    end
    t.assert_equals(value(obs, 'connections', m), 1)
    t.assert_equals(value(obs, 'connections_total', m), 1)
    t.assert_equals(value(obs, 'rejected_connections_total', m), 0)
    t.assert_equals(value(obs, 'idle_kicks_total', m), 0)
    t.assert_equals(value(obs, 'read_bytes_total', m), bytes.sent)
    t.assert_equals(value(obs, 'written_bytes_total', m), bytes.received)
    t.assert_equals(value(obs, 'items', m), 2)
    t.assert_gt(value(obs, 'bytes', m), 0)
    t.assert_equals(value(obs, 'running', m), 1)
    t.assert_equals(value(obs, 'evictions_total', m), 0)
    t.assert_equals(value(obs, 'expired_items_total', m), 0)
    t.assert_equals(value(obs, 'flushed_items_total', m), 0)
    -- No thread label in TX.
    t.assert_equals(#rh.series(obs, P .. 'commands_total', op('get')), 1)

    -- "stats" still shows only the memcached counters.
    c:send('stats\r\n')
    local stats = c:read_until('END\r\n')
    t.assert_str_contains(stats, 'STAT cmd_get 6\r\n')
    for _, class in ipairs({'protocol', 'client', 'server'}) do
        t.assert_not_str_contains(stats, class .. '_errors')
    end

    -- The Prometheus format.
    local text = cg.server:exec(function()
        return require('metrics.plugins.prometheus').collect_http().body
    end)
    t.assert_str_contains(text, '# TYPE tnt_memcached_commands_total counter')
    t.assert_str_matches(
        text,
        '.*tnt_memcached_commands_total{[^}]*name="m"[^}]*} 6\n.*'
    )

    t.assert_not(callback_yields(cg.server))

    -- Expirations and a stop.
    t.assert_equals(call('set e 0 1 1\r\nx\r\n'), 'STORED\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(
            value(rh.collect(cg.server), 'expired_items_total', m),
            1
        )
    end)
    cg.server:exec(function()
        require('memcached').get('m'):stop()
    end)
    obs = rh.collect(cg.server)
    t.assert_equals(value(obs, 'running', m), 0)
    t.assert_equals(value(obs, 'connections', m), 0)
end

g.test_threads = function(cg)
    cg.server:exec(function()
        -- Before the instance: the poller starts with the first one.
        require('memcached.metrics').REFRESH_INTERVAL = 0.1
    end)
    t.assert_equals(
        create(cg, 't', {threads = th.GROUP, metrics = true}),
        'running'
    )
    local nconns = 8
    local conns = {}
    for i = 1, nconns do
        local c = connect(cg)
        conns[i] = c
        t.assert_equals(c:set('k' .. i, 'v' .. i), 'STORED\r\n')
        t.assert_equals(c:get('k' .. i, 'get'), 'v' .. i)
        t.assert_equals(c:call('get none\r\n'), 'END\r\n')
    end
    t.assert_not(callback_yields(cg.server))

    -- The poller reads the threads asynchronously.
    local obs
    t.helpers.retrying({timeout = 10}, function()
        obs = rh.collect(cg.server)
        local sum = 0
        for _, o in ipairs(
            rh.series(
                obs,
                P .. 'commands_total',
                {name = 't', op = 'get'}
            )
        ) do
            sum = sum + o.value
        end
        t.assert_equals(sum, 2 * nconns)
    end)
    -- With the per-thread values in place.
    t.assert_not(callback_yields(cg.server))

    -- A series per thread, with the thread label.
    local threads = cg.server:exec(function()
        local ids = {}
        local info = require('memcached').get('t'):info()
        for _, ti in ipairs(info.threads) do
            table.insert(ids, tostring(ti.thread_id))
        end
        return ids
    end)
    t.assert_equals(#threads, NTHREADS)
    local totals = {}
    for _, name in ipairs({
        'connections',
        'connections_total',
        'stored_items_total',
    }) do
        local series = rh.series(obs, P .. name, {name = 't'})
        t.assert_equals(#series, NTHREADS, name)
        totals[name] = 0
        for _, o in ipairs(series) do
            t.assert_items_include(threads, {o.label_pairs.thread})
            totals[name] = totals[name] + o.value
        end
    end
    t.assert_equals(totals, {
        connections = nconns,
        connections_total = nconns,
        stored_items_total = nconns,
    })
    local hits, misses = 0, 0
    for _, id in ipairs(threads) do
        hits = hits
            + value(
                obs,
                'hits_total',
                {name = 't', thread = id, op = 'get'}
            )
        misses = misses
            + value(
                obs,
                'misses_total',
                {name = 't', thread = id, op = 'get'}
            )
    end
    t.assert_equals({hits, misses}, {nconns, nconns})

    -- The instance-wide metrics have no thread label.
    t.assert_equals(value(obs, 'items', {name = 't'}), nconns)
    t.assert_equals(value(obs, 'running', {name = 't'}), 1)
    t.assert_equals(#rh.series(obs, P .. 'running', {name = 't'}), 1)

    -- Closed connections reach the metrics with the next poll.
    for _, c in ipairs(conns) do
        c:close()
    end
    cg.clients = nil
    t.helpers.retrying({timeout = 10}, function()
        local sum = 0
        for _, o in ipairs(
            rh.series(
                rh.collect(cg.server),
                P .. 'connections',
                {name = 't'}
            )
        ) do
            sum = sum + o.value
        end
        t.assert_equals(sum, 0)
    end)
end

-- The series follow the instances and the option.
g.test_lifecycle = function(cg)
    t.assert_equals(create(cg, 'm', {metrics = true}), 'running')
    t.assert_equals(th.create(cg.server, 'n', env.free_port(), {}), 'running')
    local obs = rh.collect(cg.server)
    t.assert_equals(value(obs, 'running', {name = 'm'}), 1)
    -- Without metrics = true an instance is not exported.
    t.assert_equals(rh.series(obs, P .. 'running', {name = 'n'}), {})

    cg.server:exec(function()
        local mcc = require('memcached')
        mcc.get('n'):cfg({metrics = true})
        mcc.get('m'):cfg({metrics = false})
    end)
    obs = rh.collect(cg.server)
    t.assert_equals(rh.series(obs, P .. 'running', {name = 'm'}), {})
    t.assert_equals(value(obs, 'running', {name = 'n'}), 1)

    cg.server:exec(function()
        require('memcached').destroy('n')
    end)
    obs = rh.collect(cg.server)
    t.assert_equals(rh.series(obs, P .. 'running'), {})
    t.assert_equals(rh.series(obs, P .. 'commands_total'), {})

    -- disable() removes the callback, the collectors and the poller.
    local info = cg.server:exec(function()
        local metrics = require('memcached.metrics')
        metrics.disable()
        local registry = require('metrics').registry
        return {
            info = metrics.info(),
            collector = registry:find('gauge', 'tnt_memcached_running') ~= nil,
        }
    end)
    t.assert_equals(info, {
        info = {
            enabled = false,
            storage = false,
            poller = false,
            threads = {},
        },
        collector = false,
    })
    -- An instance with metrics = true enables them again.
    cg.server:exec(function()
        require('memcached').get('m'):cfg({metrics = true})
    end)
    t.assert_equals(value(rh.collect(cg.server), 'running', {name = 'm'}), 1)
end

-- After mcc.reload() the callback and the poller run the new code.
g.test_reload = function(cg)
    cg.server:exec(function()
        require('memcached.metrics').REFRESH_INTERVAL = 0.1
    end)
    t.assert_equals(
        create(cg, 't', {threads = th.GROUP, metrics = true}),
        'running'
    )
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    cg.server:exec(function()
        local old = require('memcached.metrics')
        require('memcached').reload()
        local new = package.loaded['memcached.metrics']
        t.assert_not_equals(new, old)
        new.REFRESH_INTERVAL = 0.1
        local calls = {
            collect = 0,
            refresh = 0,
            orig = {collect = new.collect, refresh = new.refresh},
        }
        rawset(_G, 'mcc_test_calls', calls)
        new.collect = function()
            calls.collect = calls.collect + 1
            return calls.orig.collect()
        end
        new.refresh = function()
            calls.refresh = calls.refresh + 1
            return calls.orig.refresh()
        end
    end)
    t.assert_equals(c:set('b', '2'), 'STORED\r\n')
    t.helpers.retrying({timeout = 10}, function()
        local sum = 0
        for _, o in ipairs(
            rh.series(
                rh.collect(cg.server),
                P .. 'stored_items_total',
                {name = 't'}
            )
        ) do
            sum = sum + o.value
        end
        t.assert_equals(sum, 2)
    end)
    local calls = cg.server:exec(function()
        local calls = _G.mcc_test_calls
        rawset(_G, 'mcc_test_calls', nil)
        local mod = package.loaded['memcached.metrics']
        mod.collect, mod.refresh = calls.orig.collect, calls.orig.refresh
        return {collect = calls.collect, refresh = calls.refresh}
    end)
    t.assert_ge(calls.collect, 1)
    t.assert_ge(calls.refresh, 1)
end
