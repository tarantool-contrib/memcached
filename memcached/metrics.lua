-- Metrics of the memcached instances for the built-in metrics module. TX
-- thread only.
--
-- An instance with the option metrics = true is exported: creating or
-- reconfiguring one calls enable(), which registers a callback with
-- require('metrics'). The exporters (the Prometheus plugin,
-- metrics.http_middleware, roles.metrics-export) invoke the callbacks on
-- every collection.
--
--   enable()       register the callback, start the poller (idempotent)
--   disable()      unregister everything and remove the series
--   collect()      the callback: sets the values from the live state
--   refresh()      poll the application threads once (yields)
--   storage(on)    also export the spaces served by this vshard storage
--
-- The callback must not yield, so it reads plain Lua state only:
-- - instances in TX: the counters of inst.server;
-- - instances with threads: the counters of every thread, as the poller
--   fiber last read them (every REFRESH_INTERVAL seconds, with
--   threads.eval(), which yields). The application threads have their own
--   metrics registries, which no exporter reads, so the threads don't
--   report themselves;
-- - items and bytes: space:len() and space:bsize() of the local space;
-- - expirations: the sweeper counters; evictions: the counters of the
--   evictor (state.evict).
-- The state is in state.metrics; the callback and the poller look the code
-- up in package.loaded, so a reload takes effect.

local fiber = require('fiber')
local log = require('log')

local state = require('memcached.state')

local M = {}

local SELF = 'memcached.metrics'
local MCC = 'memcached'
local ENGINE = 'memcached.engine'
local EVICT = 'memcached.evict'
local THREADS = 'memcached.threads'
local STORAGE_SWEEPER_PREFIX = 'vshard:'

M.PREFIX = 'tnt_memcached_'
-- How often the counters of the application threads are read, seconds.
M.REFRESH_INTERVAL = 1

-- Metric name (without the prefix) -> {kind, help}. Labels: name (the
-- memcached instance) on all but the storage_* ones, which have space;
-- thread (the application thread id) on the front-end counters of an
-- instance with threads; op or class where noted.
M.DEFS = {
    -- Front-end counters.
    connections = {'gauge', 'Current client connections'},
    connections_total = {'counter', 'Accepted client connections'},
    rejected_connections_total = {
        'counter',
        'Client connections rejected by max_connections',
    },
    idle_kicks_total = {
        'counter',
        'Client connections closed by idle_timeout',
    },
    commands_total = {
        'counter',
        'Commands by op (get, set, touch, flush, delete, incr, decr), '
            .. 'counted as memcached "stats" does: per key, cas is a set',
    },
    meta_commands_total = {'counter', 'Meta protocol commands'},
    hits_total = {
        'counter',
        'Hits by op (get, touch, delete, incr, decr, cas)',
    },
    misses_total = {
        'counter',
        'Misses by op (get, touch, delete, incr, decr, cas)',
    },
    cas_badval_total = {'counter', 'cas commands with a stale CAS value'},
    stored_items_total = {'counter', 'Items stored'},
    read_bytes_total = {'counter', 'Bytes read from the clients'},
    written_bytes_total = {'counter', 'Bytes written to the clients'},
    errors_total = {
        'counter',
        'Error replies by class: protocol (ERROR), client (CLIENT_ERROR), '
            .. 'server (SERVER_ERROR)',
    },
    -- The instance (TX).
    running = {'gauge', '1 if the instance is running, 0 otherwise'},
    items = {'gauge', 'Current items in the local space'},
    bytes = {'gauge', 'Bytes of the items in the local space (bsize)'},
    expired_items_total = {
        'counter',
        'Expired items deleted by the sweeper',
    },
    flushed_items_total = {
        'counter',
        'Flushed items deleted by the sweeper',
    },
    evictions_total = {
        'counter',
        'Items evicted from the local space to free memory',
    },
    -- The spaces of a vshard storage (label space).
    storage_items = {'gauge', 'Current items in the space'},
    storage_bytes = {'gauge', 'Bytes of the items in the space (bsize)'},
    storage_expired_items_total = {
        'counter',
        'Expired items deleted by the sweeper',
    },
    storage_flushed_items_total = {
        'counter',
        'Flushed items deleted by the sweeper',
    },
    storage_evictions_total = {
        'counter',
        'Items evicted from the space to free memory',
    },
    storage_requests_total = {'counter', 'Requests from the routers'},
    storage_batches_total = {'counter', 'Batches from the routers'},
}

-- commands_total: op -> server counters to add up.
local COMMANDS = {
    {'get', {'cmd_get'}},
    {'set', {'cmd_set'}},
    {'touch', {'cmd_touch'}},
    {'flush', {'cmd_flush'}},
    {'delete', {'delete_hits', 'delete_misses'}},
    {'incr', {'incr_hits', 'incr_misses'}},
    {'decr', {'decr_hits', 'decr_misses'}},
}
-- hits_total and misses_total: <op>_hits and <op>_misses.
local HIT_OPS = {'get', 'touch', 'delete', 'incr', 'decr', 'cas'}
-- errors_total: class -> server counter.
local ERROR_CLASSES = {
    {'protocol', 'protocol_errors'},
    {'client', 'client_errors'},
    {'server', 'server_errors'},
}
-- Plain server counter -> metric.
local PLAIN = {
    {'connections_total', 'total_connections'},
    {'rejected_connections_total', 'rejected_connections'},
    {'idle_kicks_total', 'idle_kicks'},
    {'meta_commands_total', 'cmd_meta'},
    {'cas_badval_total', 'cas_badval'},
    {'stored_items_total', 'total_items'},
    {'read_bytes_total', 'bytes_read'},
    {'written_bytes_total', 'bytes_written'},
}

local function current()
    return package.loaded[SELF] or M
end

local function metrics_module()
    local mod = package.loaded.metrics
    if mod ~= nil then
        return mod
    end
    local ok, res = pcall(require, 'metrics')
    return ok and res or nil
end

local function metrics_state()
    local s = state.metrics
    if s == nil then
        s = {
            enabled = false,
            -- The function registered with metrics.register_callback().
            callback = nil,
            -- {fiber, stopped}: reads the counters of the threads.
            poller = nil,
            -- instance name -> array of {thread_id, info}, by the poller.
            threads = {},
            -- metric name -> {key -> label_pairs}: the series set by the
            -- last collect(), to remove the ones that are gone.
            series = {},
            -- Export the spaces of the vshard storage (storage()).
            storage = false,
            warned = false,
        }
        state.metrics = s
    end
    return s
end

-- Collection ---------------------------------------------------------------

local function collector(metrics, mname)
    local def = M.DEFS[mname]
    local new = def[1] == 'gauge' and metrics.gauge or metrics.counter
    return new(M.PREFIX .. mname, def[2])
end

local function series_key(labels)
    return ('%s\0%s\0%s'):format(
        labels.name or labels.space or '',
        labels.thread or '',
        labels.op or labels.class or ''
    )
end

-- Set a value. Counters are set as reset() + inc(), as the default
-- metrics of Tarantool do.
local function set(pass, mname, value, labels)
    local c = pass.collectors[mname]
    if c == nil then
        c = collector(pass.metrics, mname)
        pass.collectors[mname] = c
    end
    value = tonumber(value) or 0
    if M.DEFS[mname][1] == 'gauge' then
        c:set(value, labels)
    else
        c:reset(labels)
        if value > 0 then
            c:inc(value, labels)
        end
    end
    local key = series_key(labels)
    local seen = pass.seen[mname]
    if seen == nil then
        seen = {}
        pass.seen[mname] = seen
    end
    seen[key] = true
    local known = pass.s.series[mname]
    if known == nil then
        known = {}
        pass.s.series[mname] = known
    end
    known[key] = labels
end

local function labels_of(name, thread, k, v)
    local labels = {name = name, thread = thread}
    if k ~= nil then
        labels[k] = v
    end
    return labels
end

-- The counters of one front-end: st are the server counters, conns the
-- current connections.
local function collect_frontend(pass, name, thread, st, conns)
    set(pass, 'connections', conns, labels_of(name, thread))
    for _, p in ipairs(PLAIN) do
        set(pass, p[1], st[p[2]], labels_of(name, thread))
    end
    for _, cmd in ipairs(COMMANDS) do
        local n = 0
        for _, c in ipairs(cmd[2]) do
            n = n + (tonumber(st[c]) or 0)
        end
        set(pass, 'commands_total', n, labels_of(name, thread, 'op', cmd[1]))
    end
    for _, op in ipairs(HIT_OPS) do
        set(
            pass,
            'hits_total',
            st[op .. '_hits'],
            labels_of(name, thread, 'op', op)
        )
        set(
            pass,
            'misses_total',
            st[op .. '_misses'],
            labels_of(name, thread, 'op', op)
        )
    end
    for _, e in ipairs(ERROR_CLASSES) do
        set(
            pass,
            'errors_total',
            st[e[2]],
            labels_of(name, thread, 'class', e[1])
        )
    end
end

-- Whether the instance keeps its data in the local space (not vshard).
local function local_space(inst)
    local mcc = package.loaded[MCC]
    local modname = mcc ~= nil and mcc.backends[inst.opts.backend] or nil
    local mod = modname ~= nil and package.loaded[modname] or nil
    return not (type(mod) == 'table' and mod.REMOTE)
end

-- The evictions from a space (evict.lua), without yielding.
local function evictions(space_name)
    local evict = package.loaded[EVICT]
    if evict == nil then
        return 0
    end
    return evict.stats(space_name).evictions
end

local function space_stats(space_name)
    local engine = package.loaded[ENGINE]
    if engine == nil then
        return {curr_items = 0, bytes = 0}
    end
    return engine.stats({space_name = space_name})
end

local function collect_instance(pass, inst)
    local name = inst.name
    set(
        pass,
        'running',
        inst.status == 'running' and 1 or 0,
        labels_of(name)
    )
    if inst.fe_group ~= nil then
        for _, ti in ipairs(pass.s.threads[name] or {}) do
            local info = ti.info
            collect_frontend(
                pass,
                name,
                tostring(ti.thread_id),
                info,
                info.curr_connections
            )
        end
    elseif inst.server ~= nil then
        -- With the counters of the C connection loops (server.lua
        -- sync_stats()).
        local server = package.loaded['memcached.server']
        collect_frontend(
            pass, name, nil,
            server ~= nil and server.sync_stats ~= nil
                and server.sync_stats(inst.server) or inst.server.stats,
            inst.server.nconns
        )
    end
    if local_space(inst) then
        local st = space_stats(inst.space_name)
        set(pass, 'items', st.curr_items, labels_of(name))
        set(pass, 'bytes', st.bytes, labels_of(name))
        set(
            pass,
            'evictions_total',
            evictions(inst.space_name),
            labels_of(name)
        )
    end
    local sweeper = state.sweepers[name]
    if sweeper ~= nil then
        set(pass, 'expired_items_total', sweeper.expired, labels_of(name))
        set(pass, 'flushed_items_total', sweeper.flushed, labels_of(name))
    end
end

local function collect_storage(pass)
    local s = state.storage
    if s == nil then
        return
    end
    for space_name, conf in pairs(s.spaces) do
        local labels = {space = space_name}
        local st = space_stats(space_name)
        set(pass, 'storage_items', st.curr_items, labels)
        set(pass, 'storage_bytes', st.bytes, labels)
        local sweeper = state.sweepers[STORAGE_SWEEPER_PREFIX .. space_name]
        set(
            pass,
            'storage_expired_items_total',
            sweeper and sweeper.expired or 0,
            labels
        )
        set(
            pass,
            'storage_flushed_items_total',
            sweeper and sweeper.flushed or 0,
            labels
        )
        set(pass, 'storage_evictions_total', evictions(space_name), labels)
        set(pass, 'storage_requests_total', conf.requests, labels)
        set(pass, 'storage_batches_total', conf.batches, labels)
    end
end

-- The metrics callback: set every series from the live state and remove
-- the series of the instances that are gone. Doesn't yield.
function M.collect()
    local s = state.metrics
    local metrics = metrics_module()
    if s == nil or not s.enabled or metrics == nil then
        return
    end
    local pass = {s = s, metrics = metrics, collectors = {}, seen = {}}
    for _, inst in pairs(state.instances) do
        if inst.opts.metrics and inst.status ~= 'destroyed' then
            collect_instance(pass, inst)
        end
    end
    if s.storage then
        collect_storage(pass)
    end
    for mname, known in pairs(s.series) do
        local seen = pass.seen[mname] or {}
        for key, labels in pairs(known) do
            if not seen[key] then
                local c = pass.collectors[mname] or collector(metrics, mname)
                c:remove(labels)
                known[key] = nil
            end
        end
    end
end

-- Application threads ------------------------------------------------------

-- Read the counters of the front-ends in the application threads of the
-- instances with metrics = true. Yields.
function M.refresh()
    local s = metrics_state()
    local threads = package.loaded[THREADS]
    local polled = {}
    for name, inst in pairs(state.instances) do
        if inst.opts.metrics and inst.fe_group ~= nil and threads ~= nil then
            polled[name] = true
            local ok, list = pcall(
                threads.eval,
                inst.fe_group,
                'info',
                {name = name}
            )
            if ok then
                local res = {}
                for _, ti in ipairs(list) do
                    if type(ti) == 'table' and type(ti.info) == 'table' then
                        table.insert(res, ti)
                    end
                end
                table.sort(res, function(a, b)
                    return a.thread_id < b.thread_id
                end)
                s.threads[name] = res
            else
                log.warn(
                    "memcached: metrics of '%s': %s",
                    name,
                    tostring(list)
                )
            end
        end
    end
    for name in pairs(s.threads) do
        if not polled[name] then
            s.threads[name] = nil
        end
    end
end

local function poller_main(rec)
    while not rec.stopped do
        local ok, err = pcall(function()
            current().refresh()
        end)
        if not ok and not rec.stopped then
            log.warn('memcached: metrics: %s', tostring(err))
        end
        if rec.stopped then
            break
        end
        pcall(fiber.sleep, current().REFRESH_INTERVAL)
    end
end

local function start_poller(s)
    local rec = s.poller
    if rec ~= nil and not rec.stopped and rec.fiber:status() ~= 'dead' then
        return
    end
    rec = {stopped = false}
    rec.fiber = fiber.new(poller_main, rec)
    rec.fiber:name('mcc/metrics', {truncate = true})
    s.poller = rec
end

local function stop_poller(s)
    local rec = s.poller
    s.poller = nil
    if rec == nil then
        return
    end
    rec.stopped = true
    if rec.fiber:status() ~= 'dead' and rec.fiber:id() ~= fiber.id() then
        pcall(rec.fiber.cancel, rec.fiber)
    end
end

-- Public API ---------------------------------------------------------------

-- Export the metrics. Returns false if the metrics module is not available.
function M.enable()
    local s = metrics_state()
    local metrics = metrics_module()
    if metrics == nil then
        if not s.warned then
            log.warn(
                'memcached: the metrics module is not available, no metrics are exported'
            )
            s.warned = true
        end
        return false
    end
    if s.callback == nil then
        s.callback = function()
            local mod = package.loaded[SELF]
            if mod ~= nil then
                mod.collect()
            end
        end
    end
    -- Idempotent; registers it again after a metrics.clear().
    metrics.register_callback(s.callback)
    start_poller(s)
    s.enabled = true
    return true
end

-- Stop exporting: unregister the callback and the collectors, stop the
-- poller. The instance option metrics = true enables the export again on
-- the next create() or cfg().
function M.disable()
    local s = state.metrics
    if s == nil then
        return
    end
    s.enabled = false
    stop_poller(s)
    local metrics = metrics_module()
    if metrics ~= nil then
        if s.callback ~= nil then
            metrics.unregister_callback(s.callback)
        end
        for mname, def in pairs(M.DEFS) do
            local c = metrics.registry:find(def[1], M.PREFIX .. mname)
            if c ~= nil then
                metrics.registry:unregister(c)
            end
        end
    end
    s.callback = nil
    s.series = {}
    s.threads = {}
end

-- Export the spaces of the vshard storage (storage.lua) too, or stop.
function M.storage(on)
    local s = metrics_state()
    s.storage = on and true or false
    if s.storage then
        current().enable()
    end
end

function M.info()
    local s = state.metrics
    if s == nil then
        return {enabled = false}
    end
    local threads = {}
    for name, list in pairs(s.threads) do
        threads[name] = #list
    end
    return {
        enabled = s.enabled,
        storage = s.storage,
        poller = s.poller ~= nil and s.poller.fiber:status() ~= 'dead',
        threads = threads,
    }
end

return M
