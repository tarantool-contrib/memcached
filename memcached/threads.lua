-- Front-ends in application threads.
--
-- With the instance option threads = '<group>', the front-ends (server.lua
-- with reuseport = true on the listen URI) run in every thread of that
-- application thread group, with the backend THREAD_BACKENDS[backend]:
-- - 'local' -> 'thread': a batch is one threads.call('tx', 'mcc.exec',
--   {name, reqs}), after the raw batch: the input bytes of the classic get,
--   gets and set at its head, which TX serves with the C batch: through
--   the shared-memory channel of the thread backend object (memcached.ipc,
--   "The IPC" below), or threads.call('tx', 'mcc.batch', {name, data, ...})
--   without it (see backend/thread.lua batch()). In TX, the instance keeps
--   the backend that executes the batches (inst.backend);
-- - 'vshard' -> 'vshard': the threads call the storages through their own
--   routers (see backend/vshard.lua); TX is not in the data path. A watcher
--   fiber in TX sends the config of the TX router to the threads when it
--   changes (WATCH_INTERVAL).
-- TX controls the front-ends with threads.eval() of M.fe functions
-- (M.eval).
--
-- TX side:
--   check(opts)                    raise if the options can't be served
--   start(inst) / stop(inst) / destroy(inst)
--   rebind(inst, listen)           listen on a new URI in every thread
--   release(inst)                  stop listening in every thread, keeping
--                                  the connections, until the next rebind
--   listening(inst)                whether every thread listens
--   cfg(inst)                      push the instance options to the threads
--   info(inst)                     counters summed over the threads, and
--                                  per-thread ones in info.threads
--   export_tx()                    export the TX functions (idempotent)
--   exec(name, reqs), batch(name, data, max_commands, max_item_size),
--   ipc_open(name, id, abi), flush_all(name, delay), stats(name),
--   counters(name), conns(name), reset_stats(name)
--                                  what the TX functions call
--   ipc_info()                     the channels TX serves (for tests)
--   reload_threads()               reload the code in the threads
-- Thread side: M.fe.*, called by M.eval().
--
-- The thread-side code is loaded with TX's package.path and package.cpath,
-- so that the threads run the same code as TX.

local buffer = require('buffer')
local ffi = require('ffi')
local fiber = require('fiber')
local log = require('log')
local msgpack = require('msgpack')
local threads = require('threads')
local uri = require('uri')

local state = require('memcached.state')

local M = {}

local SELF = 'memcached.threads'
local MCC = 'memcached'
local SERVER = 'memcached.server'
local READVIEW = 'memcached.readview'
local IPC = 'memcached.ipc'

-- Backend of the instance -> backend of its front-ends in the threads.
M.THREAD_BACKENDS = {['local'] = 'thread', vshard = 'vshard'}

-- How often (seconds) TX checks whether the router config of an instance
-- has changed, for the backends with routers in the threads (vshard).
M.WATCH_INTERVAL = 0.5

-- Functions exported in TX for the threads: name -> {module, function}.
M.TX_EXPORTS = {
    ['mcc.exec'] = {SELF, 'exec'},
    ['mcc.batch'] = {SELF, 'batch'},
    ['mcc.ipc_open'] = {SELF, 'ipc_open'},
    ['mcc.flush_all'] = {SELF, 'flush_all'},
    ['mcc.stats'] = {SELF, 'stats'},
    ['mcc.counters'] = {SELF, 'counters'},
    ['mcc.conns'] = {SELF, 'conns'},
    ['mcc.reset_stats'] = {SELF, 'reset_stats'},
    ['mcc.rv_open'] = {READVIEW, 'tx_open'},
    ['mcc.rv_close'] = {READVIEW, 'tx_close'},
}

-- Runs M.fe[fn](arg) in a thread; loads the code on the first use.
M.EVAL = [[
local path, cpath, fn, arg = ...
local mod = package.loaded['memcached.threads']
if mod == nil then
    package.path = path
    package.cpath = cpath
    mod = require('memcached.threads')
end
return mod.fe[fn](arg)
]]

-- Reloads the code in a thread (see reload.lua).
M.RELOAD_EVAL = [[
local path, cpath = ...
package.path = path
package.cpath = cpath
local reload = package.loaded['memcached.reload'] or
               require('memcached.reload')
return reload.reload_thread()
]]

local function current()
    return package.loaded[SELF] or M
end

local function load_module(name)
    return package.loaded[name] or require(name)
end

local function registry(key)
    local r = state[key]
    if r == nil then
        r = {}
        state[key] = r
    end
    return r
end

-- Objects (tables with an __index metamethod, such as a vshard router)
-- can't cross threads.
local function is_object(v)
    local m = getmetatable(v)
    return m ~= nil and (type(m) ~= 'table' or m.__index ~= nil)
end

-- A deep copy without the objects.
local function copy(t)
    local res = {}
    for k, v in pairs(t) do
        if type(v) ~= 'table' then
            res[k] = v
        elseif not is_object(v) then
            res[k] = copy(v)
        end
    end
    return res
end

local function error_message(err)
    if type(err) == 'cdata' or type(err) == 'table' then
        err = err.message or tostring(err)
    end
    return tostring(err)
end

-- Run M.fe[fn](arg) in the threads of a group (all of them by default).
-- Returns an array of the results, one per thread.
function M.eval(group, fn, arg, opts)
    local res = threads.eval(
        group,
        M.EVAL,
        {package.path, package.cpath, fn, arg or {}},
        opts or {}
    )
    local out = {}
    for i, r in ipairs(res) do
        out[i] = r[1]
    end
    return out
end

-- TX functions -----------------------------------------------------------

function M.tx_call(export, ...)
    local target = M.TX_EXPORTS[export]
    if target == nil then
        error(
            ("Function '%s' is not provided by this version of memcached"):format(
                export
            ),
            0
        )
    end
    return load_module(target[1])[target[2]](...)
end

-- threads.export() can't replace a function: the exported one looks the
-- code up on every call, so a reload takes effect.
local function trampoline(export)
    return function(...)
        return current().tx_call(export, ...)
    end
end

-- Export the TX functions that are not exported yet.
function M.export_tx()
    local exported = registry('tx_exports')
    local names = {}
    for name in pairs(M.TX_EXPORTS) do
        table.insert(names, name)
    end
    table.sort(names)
    for _, name in ipairs(names) do
        if not exported[name] then
            local ok, err = pcall(threads.export, name, trampoline(name))
            if not ok
                and not (type(err) == 'cdata' and err.name == 'FUNCTION_EXISTS') then
                error(err, 0)
            end
            exported[name] = true
        end
    end
    return names
end

local function not_running(name, n)
    local message = ("Memcached instance '%s' is not running"):format(
        tostring(name)
    )
    local resps = {}
    for i = 1, n do
        resps[i] = {status = 'SERVER_ERROR', message = message}
    end
    return resps
end

local function tx_backend(name)
    local inst = state.instances[name]
    return inst ~= nil and inst.backend or nil
end

-- mcc.exec(name, reqs): execute a batch with the TX backend of an instance.
function M.exec(name, reqs)
    local backend = tx_backend(name)
    if backend == nil then
        return not_running(name, #reqs)
    end
    return backend:exec(reqs)
end

-- Output buffers of M.batch(), reused: a batch that writes yields, so
-- several may run at once.
local obufs = {}
-- A buffer bigger than this is freed after use.
local OBUF_KEEP = 65536

-- The number of results of engine.batch().
local BATCH_RESULTS = 19

-- mcc.batch(name, data, max_commands, max_item_size): the raw batch of a
-- front-end in a thread (backend/thread.lua batch()). Runs the C batch of
-- the TX backend (backend/local.lua batch(), engine.batch()) on the input
-- bytes data, a string. Returns
--   reply, consumed, commands, gets, hits, stored, held, <counters>
-- (the results of engine.batch() after held) with the replies in one
-- string, or nothing when the backend has no C batch or can't run it (no
-- data path, a commit wait mode, snapshot reads, the instance is not
-- running).
function M.batch(name, data, max_commands, max_item_size)
    local backend = tx_backend(name)
    if backend == nil or backend.batch == nil then
        return
    end
    local n = #obufs
    local obuf
    if n > 0 then
        obuf = obufs[n]
        obufs[n] = nil
    else
        obuf = buffer.ibuf()
    end
    -- ok, then the results.
    local res = {
        pcall(backend.batch, backend, data, obuf, max_commands, max_item_size),
    }
    local ok, consumed, commands = res[1], res[2], res[3]
    local reply
    if ok and consumed ~= nil then
        reply = commands > 0
            and ffi.string(obuf.rpos, tonumber(obuf.wpos - obuf.rpos))
            or ''
    end
    if tonumber(obuf.epos - obuf.buf) > OBUF_KEEP then
        obuf:recycle()
    else
        obuf.rpos = obuf.buf
        obuf.wpos = obuf.buf
    end
    table.insert(obufs, obuf)
    if not ok then
        error(consumed, 0)
    end
    if reply == nil then
        return
    end
    res[1] = reply
    return unpack(res, 1, BATCH_RESULTS + 1)
end

-- The IPC ------------------------------------------------------------------
--
-- A thread backend object sends its raw batches to TX through a channel of
-- memcached.ipc (memcached/ipc.c): slots in shared memory and two rings of
-- slot indexes with eventfd wakeups, instead of a threads.call each. The
-- thread makes the channel and asks TX to serve it (mcc.ipc_open); TX
-- serves it with a pool of fibers until the thread closes it, then
-- detaches.
--
-- The pool: one fiber leads, it waits for the requests (ch:wait(), on the
-- eventfd) and runs them one after another, each with engine.batch()
-- (through inst.backend:batch(), as M.batch()). A batch with sets yields
-- on the WAL: the requests behind it must not wait for that write. So a
-- second fiber stands by while the leader runs requests: it is woken
-- (once per wakeup of the leader, not per request) and runs when the
-- leader yields. If the leader yielded in a request, it takes the lead
-- and goes on with the next requests, with a standby of its own; the old
-- leader parks when its request is done. If the leader only went back to
-- its wait, the standby parks again. So the reads cost no fiber switch per
-- request, and the writes of a channel overlap as with a fiber per call.
-- The pool grows up to IPC_FIBERS fibers.

-- The fibers of a channel, at most.
M.IPC_FIBERS = 128
-- The leader waits this long (seconds) before it checks the channel again.
M.IPC_WAIT = 1
-- The leader yields after this many requests in a row.
M.IPC_YIELD = 64

local function ipc_module()
    local mod = package.loaded[IPC]
    if mod == nil then
        local ok, res = pcall(require, IPC)
        if not ok or type(res) ~= 'table' then
            return nil
        end
        mod = res
    end
    return mod
end

-- TX: channel id -> its record {id, name, ch, leader, busy, standby, park,
-- nfibers, nidle, active, closing, done, served}.
local function ipc_channels()
    return registry('ipc_channels')
end

local encode = msgpack.encode

-- The statuses of a reply (ipc.OK, ...), for the ABI M.IPC_ABI.
M.IPC_ABI = 1
local IPC_OK, IPC_NOTRUN, IPC_ERROR = 0, 1, 2

local function ipc_reply(ch, idx, obuf, ok, consumed, commands, ...)
    if not ok then
        ch:reply(idx, IPC_ERROR, obuf)
    elseif consumed == nil then
        ch:reply(idx, IPC_NOTRUN, obuf)
    else
        if commands == 0 then
            obuf.wpos = obuf.rpos
        end
        ch:reply(idx, IPC_OK, obuf, encode, consumed, commands, ...)
    end
end

local function shrink(buf)
    if tonumber(buf.epos - buf.buf) > OBUF_KEEP then
        buf:recycle()
    end
end

-- Run one request of a channel, as M.batch() does.
local function ipc_serve(rec, ch, idx, ibuf, obuf, max_commands, max_item_size)
    local backend = tx_backend(rec.name)
    if backend == nil or backend.batch == nil then
        ch:reply(idx, IPC_NOTRUN, obuf)
    else
        ipc_reply(
            ch, idx, obuf,
            pcall(
                backend.batch,
                backend,
                ibuf,
                obuf,
                max_commands,
                max_item_size
            )
        )
    end
    rec.served = rec.served + 1
    shrink(ibuf)
    shrink(obuf)
end

local ipc_spawn

-- The channel is done: TX lets it go, and the fibers of the pool exit.
local function ipc_finish(rec)
    if rec.done then
        return
    end
    rec.done = true
    rec.ch:detach()
    local list = ipc_channels()
    if list[rec.id] == rec then
        list[rec.id] = nil
    end
    rec.park:broadcast()
end

-- The leader: run the requests there are, then wait for more.
local function ipc_lead(rec, me, ibuf, obuf)
    local ch = rec.ch
    local n = 0
    while true do
        local idx, max_commands, max_item_size = ch:next(ibuf)
        if idx == nil then
            break
        end
        if not rec.standby then
            rec.standby = true
            if rec.nidle > 0 then
                rec.park:signal()
            elseif rec.nfibers < M.IPC_FIBERS then
                ipc_spawn(rec)
            end
        end
        rec.busy = me
        rec.active = rec.active + 1
        ipc_serve(rec, ch, idx, ibuf, obuf, max_commands, max_item_size)
        rec.active = rec.active - 1
        if rec.busy == me then
            rec.busy = nil
        end
        if rec.leader ~= me then
            -- Another fiber took the lead while the request yielded.
            return true
        end
        n = n + 1
        if n >= M.IPC_YIELD then
            fiber.yield()
            return true
        end
    end
    if rec.closing then
        -- The thread closed the channel and every request is taken: wait
        -- for the requests that other fibers run, then let it go.
        if rec.active > 0 then
            fiber.sleep(0.001)
            return true
        end
        ipc_finish(rec)
        return false
    end
    if ch:closed() then
        -- Every request sent before the close is in the ring now: take
        -- them, then finish.
        rec.closing = true
        return true
    end
    ch:wait(M.IPC_WAIT)
    return true
end

-- One step of a fiber of the pool; false: exit.
function M.ipc_step(rec, me, ibuf, obuf)
    fiber.testcancel()
    if rec.done then
        return false
    end
    local leader = rec.leader
    if leader == nil then
        rec.leader = me
    elseif leader ~= me then
        if rec.busy ~= nil and rec.busy == leader then
            -- The leader yielded in a request: lead, with a standby of
            -- its own.
            rec.leader = me
            rec.standby = false
        else
            rec.nidle = rec.nidle + 1
            rec.park:wait()
            rec.nidle = rec.nidle - 1
            rec.standby = false
        end
        return true
    end
    return ipc_lead(rec, me, ibuf, obuf)
end

-- A fiber of the pool exits: another one leads. The last one (the pool
-- was cancelled) lets the channel go: the thread falls back to
-- threads.call.
function M.ipc_exit(rec, me)
    rec.nfibers = rec.nfibers - 1
    if rec.leader == me then
        rec.leader = nil
    end
    if rec.busy == me then
        rec.busy = nil
    end
    if rec.done then
        return
    end
    if rec.nfibers == 0 then
        if rec.active == 0 then
            ipc_finish(rec)
        end
    elseif rec.leader == nil then
        rec.park:signal()
    end
end

local function ipc_main(rec)
    local me = fiber.self()
    local ibuf, obuf = buffer.ibuf(), buffer.ibuf()
    local ok, err = pcall(function()
        while current().ipc_step(rec, me, ibuf, obuf) do
        end
    end)
    ibuf:recycle()
    obuf:recycle()
    if not ok and not rec.done
        and not (type(err) == 'cdata' and err.type == 'FiberIsCancelled') then
        log.error(
            "memcached: instance '%s': a fiber of the thread channel %s: %s",
            tostring(rec.name),
            tostring(rec.id),
            error_message(err)
        )
    end
    current().ipc_exit(rec, me)
end

ipc_spawn = function(rec)
    rec.nfibers = rec.nfibers + 1
    local f = fiber.new(ipc_main, rec)
    f:name('mcc/ipc/' .. tostring(rec.name), {truncate = true})
end

-- mcc.ipc_open(name, id, abi): serve the channel id of a thread backend
-- object of the instance name (backend/thread.lua). Returns true when TX
-- serves it (also if it already did), false if it can't: no memcached.ipc
-- here, another ABI, the channel is closed.
function M.ipc_open(name, id, abi)
    local ipc = ipc_module()
    if ipc == nil or ipc.ABI ~= abi or ipc.ABI ~= M.IPC_ABI then
        return false
    end
    local list = ipc_channels()
    if list[id] ~= nil then
        return true
    end
    local ch = ipc.attach(id)
    if ch == nil then
        return false
    end
    local rec = {
        id = id,
        name = name,
        ch = ch,
        leader = nil,
        busy = nil,
        standby = false,
        park = fiber.cond(),
        nfibers = 0,
        nidle = 0,
        active = 0,
        closing = false,
        done = false,
        served = 0,
    }
    list[id] = rec
    ipc_spawn(rec)
    return true
end

-- The channels TX serves: an array of {id, name, fibers, idle, active,
-- served}, by id.
function M.ipc_info()
    local res = {}
    for id, rec in pairs(ipc_channels()) do
        table.insert(res, {
            id = id,
            name = rec.name,
            fibers = rec.nfibers,
            idle = rec.nidle,
            active = rec.active,
            served = rec.served,
        })
    end
    table.sort(res, function(a, b)
        return a.id < b.id
    end)
    return res
end

function M.flush_all(name, delay)
    local backend = tx_backend(name)
    if backend == nil then
        return not_running(name, 1)[1]
    end
    return backend:flush_all(delay)
end

-- With threads, the counters summed over the threads (counters,
-- curr_connections, threads = the number of threads): server.lua uses them
-- in "stats". Empty without threads.
function M.counters(name)
    local inst = state.instances[name]
    if inst == nil or inst.fe_group == nil then
        return {}
    end
    local agg = M.collect(inst)
    return {
        counters = agg.counters,
        curr_connections = agg.curr_connections,
        threads = agg.nthreads,
    }
end

-- The backend stats plus M.counters().
function M.stats(name)
    local inst = state.instances[name]
    local res = {}
    local backend = inst ~= nil and inst.backend or nil
    if backend ~= nil then
        local ok, b = pcall(backend.stats, backend)
        if ok and type(b) == 'table' then
            res = b
        end
    end
    for k, v in pairs(M.counters(name)) do
        res[k] = v
    end
    return res
end

function M.reset_stats(name)
    local inst = state.instances[name]
    if inst ~= nil and inst.fe_group ~= nil then
        M.eval(inst.fe_group, 'reset_stats', {name = name})
    end
    return true
end

-- With threads, the listeners and connections of every thread for "stats
-- conns" (server.conn_list() of each, in one array); nil without threads.
function M.conns(name)
    local inst = state.instances[name]
    if inst == nil or inst.fe_group == nil then
        return nil
    end
    local res = {}
    for _, list in ipairs(M.eval(inst.fe_group, 'conns', {name = name})) do
        if type(list) == 'table' then
            for _, c in ipairs(list) do
                table.insert(res, c)
            end
        end
    end
    return res
end

-- Control from TX --------------------------------------------------------

-- Raise if the threads option of these instance options can't be served.
function M.check(opts)
    local group = opts.threads
    if group == nil then
        return
    end
    local found = false
    for _, g in ipairs(threads.info().groups) do
        if g.name == group and g.name ~= 'tx' then
            found = true
            break
        end
    end
    if not found then
        error(
            ("Application thread group '%s' does not exist"):format(
                group
            ),
            0
        )
    end
    local backend = opts.backend or 'local'
    if M.THREAD_BACKENDS[backend] == nil then
        error(
            ("Backend '%s' can't run in application threads"):format(
                backend
            ),
            0
        )
    end
    local u = opts.listen ~= nil and uri.parse(tostring(opts.listen)) or nil
    if u ~= nil and (u.host == 'unix/' or tonumber(u.service) == 0) then
        error("Option 'threads' needs a TCP listen URI with a fixed port", 0)
    end
end

-- The module of the backend of the instance in TX.
local function tx_backend_module(inst)
    return load_module(load_module(MCC).backends[inst.opts.backend or 'local'])
end

-- The options of the front-ends of an instance in the threads: plain data.
-- The backend module may add what its objects in the threads need with
-- thread_cfg(cfg, opts) (vshard: the router config).
function M.thread_cfg(inst)
    local cfg = copy(inst.opts)
    cfg.name = inst.name
    cfg.space_name = inst.space_name
    local backend = M.THREAD_BACKENDS[cfg.backend or 'local']
    cfg.backend = backend
    cfg.backend_module = load_module(MCC).backends[backend]
    local mod = load_module(cfg.backend_module)
    if mod.thread_cfg ~= nil then
        cfg = mod.thread_cfg(cfg, inst.opts)
    end
    return cfg
end

-- Watchers ---------------------------------------------------------------

-- TX: instance name -> {name, ref, fiber, stopped}. ref is what the backend
-- module's thread_cfg_ref() returned when the options were last sent.
local function watchers()
    return registry('fe_watchers')
end

-- One check of a watcher: send the options to the threads again when the
-- backend module says that what it adds to them has changed. Returns
-- false when the watcher is no longer needed.
function M.watch_step(rec)
    local inst = state.instances[rec.name]
    if inst == nil or inst.fe_group == nil then
        return false
    end
    local mod = tx_backend_module(inst)
    if mod.thread_cfg_ref == nil then
        return false
    end
    local ref = mod.thread_cfg_ref(inst.opts)
    if ref == rec.ref then
        return true
    end
    rec.ref = ref
    local ok, err = pcall(current().cfg, inst)
    if not ok then
        log.error(
            "memcached: instance '%s': sending the router config to the threads: %s",
            inst.name,
            error_message(err)
        )
    end
    return true
end

local function watcher_main(rec)
    while not rec.stopped do
        local ok, res = pcall(function()
            return current().watch_step(rec)
        end)
        if not ok then
            log.error(
                "memcached: instance '%s': %s",
                rec.name,
                error_message(res)
            )
        elseif not res then
            break
        end
        if rec.stopped or not pcall(fiber.sleep, current().WATCH_INTERVAL) then
            break
        end
    end
    if watchers()[rec.name] == rec then
        watchers()[rec.name] = nil
    end
end

local function stop_watcher(name)
    local list = watchers()
    local rec = list[name]
    if rec == nil then
        return
    end
    list[name] = nil
    rec.stopped = true
    local f = rec.fiber
    if f ~= nil and f:status() ~= 'dead' and f:id() ~= fiber.id() then
        pcall(f.cancel, f)
    end
end

-- Start or stop the watcher of an instance, as its backend needs. The
-- first check sends the options again: a change during start() is not
-- lost.
local function sync_watcher(inst)
    local need = inst.fe_group ~= nil
        and tx_backend_module(inst).thread_cfg_ref ~= nil
    local rec = watchers()[inst.name]
    if not need then
        stop_watcher(inst.name)
        return
    end
    if rec ~= nil and rec.fiber:status() ~= 'dead' then
        return
    end
    rec = {name = inst.name, ref = false, stopped = false}
    rec.fiber = fiber.new(watcher_main, rec)
    rec.fiber:name('mcc/fe-watch/' .. inst.name, {truncate = true})
    watchers()[inst.name] = rec
end

-- Whether an instance has a watcher (for tests).
function M.watching(name)
    return watchers()[name] ~= nil
end

-- Control from TX (continued) -----------------------------------------------

-- Start the front-ends of an instance in its thread group. Raises if any
-- thread fails; then none of them listens.
function M.start(inst)
    M.export_tx()
    local group = inst.opts.threads
    registry('thread_groups')[group] = true
    inst.fe_group = group
    local arg = {
        name = inst.name,
        cfg = M.thread_cfg(inst),
        rv_id = load_module(READVIEW).current_id(inst.name),
    }
    local ok, err = pcall(M.eval, group, 'start', arg)
    if not ok then
        pcall(M.eval, group, 'stop', {name = inst.name})
        error(err, 0)
    end
    sync_watcher(inst)
end

-- Stop the front-ends; they keep their counters until destroy().
function M.stop(inst)
    stop_watcher(inst.name)
    local group = inst.fe_group
    if group == nil then
        return
    end
    local ok, err = pcall(M.eval, group, 'stop', {name = inst.name})
    if not ok then
        log.error(
            "memcached: instance '%s': stopping the front-ends in '%s': %s",
            inst.name,
            group,
            error_message(err)
        )
    end
end

function M.destroy(inst)
    stop_watcher(inst.name)
    local group = inst.fe_group
    if group == nil then
        return
    end
    inst.fe_group = nil
    local ok, err = pcall(M.eval, group, 'destroy', {name = inst.name})
    if not ok then
        log.error(
            "memcached: instance '%s': destroying the front-ends in '%s': %s",
            inst.name,
            group,
            error_message(err)
        )
    end
end

-- Listen on a new URI in every thread. On errors, all the threads go back
-- to the current URI (inst.opts.listen) and the error is raised.
function M.rebind(inst, listen)
    local group = inst.fe_group
    local ok, err = pcall(
        M.eval,
        group,
        'cfg',
        {name = inst.name, listen = listen}
    )
    if not ok then
        local rok, rerr = pcall(
            M.eval,
            group,
            'cfg',
            {name = inst.name, listen = inst.opts.listen}
        )
        if not rok then
            log.error(
                "memcached: instance '%s': can't listen on %s again: %s",
                inst.name,
                tostring(inst.opts.listen),
                error_message(rerr)
            )
        end
        error(err, 0)
    end
end

-- Close the listener in every thread and keep the connections, until the
-- next rebind() (see server.release()).
function M.release(inst)
    M.eval(inst.fe_group, 'release', {name = inst.name})
end

-- Whether the front-end listens in every thread: after a failed rebind
-- whose old URI couldn't be bound again either, it doesn't.
function M.listening(inst)
    local ok, list = pcall(M.eval, inst.fe_group, 'info', {name = inst.name})
    if not ok or #list == 0 then
        return false
    end
    for _, ti in ipairs(list) do
        if type(ti) ~= 'table' or not ti.info.running or ti.info.port == 0 then
            return false
        end
    end
    return true
end

-- Push the current options (limits, backend, read mode, router config) to
-- the threads.
function M.cfg(inst)
    M.eval(inst.fe_group, 'cfg', {name = inst.name, cfg = M.thread_cfg(inst)})
    sync_watcher(inst)
end

-- Per-thread info of an instance, summed up.
function M.collect(inst)
    local list = M.eval(inst.fe_group, 'info', {name = inst.name})
    local names = load_module(SERVER).COUNTERS
    local counters = {}
    for _, c in ipairs(names) do
        counters[c] = 0
    end
    local agg = {
        counters = counters,
        curr_connections = 0,
        nthreads = #list,
        port = nil,
        threads = {},
    }
    for _, ti in ipairs(list) do
        if type(ti) == 'table' then
            local info = ti.info
            for _, c in ipairs(names) do
                counters[c] = counters[c] + (tonumber(info[c]) or 0)
            end
            agg.curr_connections = agg.curr_connections
                + (info.curr_connections or 0)
            if agg.port == nil and info.port ~= 0 then
                agg.port = info.port
            end
            info.thread_id = ti.thread_id
            table.insert(agg.threads, info)
        end
    end
    table.sort(agg.threads, function(a, b)
        return a.thread_id < b.thread_id
    end)
    return agg
end

-- Fields for inst:info().
function M.info(inst)
    local agg = M.collect(inst)
    local res = {}
    for k, v in pairs(agg.counters) do
        res[k] = v
    end
    res.curr_connections = agg.curr_connections
    res.port = agg.port or 0
    res.thread_group = inst.fe_group
    res.threads = agg.threads
    return res
end

-- Reload the code in every thread group that has loaded it.
function M.reload_threads()
    local res = {}
    for group in pairs(registry('thread_groups')) do
        res[group] = threads.eval(
            group,
            M.RELOAD_EVAL,
            {package.path, package.cpath},
            {}
        )
    end
    return res
end

-- Thread side --------------------------------------------------------------

local fe = {}
M.fe = fe

-- name -> {name, srv, backend, cfg, key}
local function servers()
    return registry('thread_servers')
end

local function thread_id()
    return threads.info().thread_id
end

local function close_backend(backend)
    if backend ~= nil and backend.close ~= nil then
        local ok, err = pcall(backend.close, backend)
        if not ok then
            log.error(
                'memcached: closing a backend: %s',
                error_message(err)
            )
        end
    end
end

-- A new key means a new backend object.
local function backend_key(cfg)
    local v = cfg.vshard or {}
    return ('%s|%s|%s|%s|%s'):format(
        cfg.backend_module,
        cfg.read_mode,
        cfg.space_name,
        tostring(v.router),
        tostring(v.timeout)
    )
end

-- Before a backend is made, or when the options come again: the backend
-- module may set up what its objects need (vshard: the router).
local function prepare(cfg)
    local mod = load_module(cfg.backend_module)
    if mod.thread_prepare ~= nil then
        mod.thread_prepare(cfg)
    end
end

local function new_backend(cfg)
    local mod = load_module(cfg.backend_module)
    local bcfg = copy(cfg)
    if cfg.read_mode == 'snapshot' then
        bcfg.read_mode = 'rw'
        return load_module(READVIEW).wrap(mod.new(bcfg), cfg)
    end
    return mod.new(bcfg)
end

-- Use a read view in the front-end of an instance in this thread. Opening
-- it yields: if the front-end stopped or left the snapshot mode meanwhile,
-- the read view is dropped.
local function publish(name, id)
    local readview = load_module(READVIEW)
    local ok = readview.publish(name, id)
    local rec = servers()[name]
    if rec == nil or rec.backend == nil
        or rec.cfg == nil or rec.cfg.read_mode ~= 'snapshot' then
        readview.drop(name)
        return false
    end
    return ok
end

-- arg = {name, cfg = thread_cfg(), rv_id}
function fe.start(arg)
    local name, cfg = arg.name, arg.cfg
    local server = load_module(SERVER)
    local list = servers()
    local rec = list[name]
    if rec == nil then
        rec = {
            name = name,
            srv = server.new({
                name = name,
                listen = cfg.listen,
                reuseport = true,
                max_connections = cfg.max_connections,
                max_item_size = cfg.max_item_size,
                idle_timeout = cfg.idle_timeout,
            }),
        }
        list[name] = rec
    end
    local srv = rec.srv
    if srv.running then
        srv:stop()
    end
    prepare(cfg)
    local backend = new_backend(cfg)
    close_backend(rec.backend)
    rec.backend, rec.cfg, rec.key = backend, cfg, backend_key(cfg)
    srv.uri = cfg.listen
    srv:cfg({
        max_connections = cfg.max_connections,
        max_item_size = cfg.max_item_size,
        idle_timeout = cfg.idle_timeout,
        backend = backend,
        reuseport = true,
    })
    srv:start()
    if arg.rv_id ~= nil then
        publish(name, arg.rv_id)
    end
    return {thread_id = thread_id(), port = srv:info().port}
end

function fe.stop(arg)
    load_module(READVIEW).drop(arg.name)
    local rec = servers()[arg.name]
    if rec == nil then
        return false
    end
    rec.srv:stop()
    rec.srv.backend = nil
    close_backend(rec.backend)
    rec.backend = nil
    return true
end

function fe.destroy(arg)
    fe.stop(arg)
    servers()[arg.name] = nil
    return true
end

-- arg = {name, cfg = thread_cfg()?, listen = <new URI>?}
function fe.cfg(arg)
    local rec = servers()[arg.name]
    if rec == nil then
        error(
            ("No front-end of '%s' in thread %d"):format(
                tostring(arg.name),
                thread_id()
            ),
            0
        )
    end
    local srv, cfg = rec.srv, arg.cfg
    if cfg ~= nil then
        srv:cfg({
            max_connections = cfg.max_connections,
            max_item_size = cfg.max_item_size,
            idle_timeout = cfg.idle_timeout,
        })
        local key = backend_key(cfg)
        if srv.running and key ~= rec.key then
            local backend = new_backend(cfg)
            local prev = rec.backend
            rec.backend, rec.key = backend, key
            srv.backend = backend
            close_backend(prev)
            if cfg.read_mode ~= 'snapshot' then
                load_module(READVIEW).drop(arg.name)
            end
        end
        rec.cfg = cfg
        if srv.running then
            prepare(cfg)
        end
    end
    if arg.listen ~= nil then
        if not srv.running then
            srv.uri = arg.listen
        elseif srv.listener == nil
            or tostring(srv.listen_uri) ~= tostring(arg.listen) then
            -- A new URI, or the listener was released.
            srv:rebind(arg.listen)
        end
    end
    return true
end

function fe.release(arg)
    local rec = servers()[arg.name]
    if rec ~= nil then
        rec.srv:release()
    end
    return true
end

function fe.info(arg)
    local rec = servers()[arg.name]
    if rec == nil then
        return false
    end
    return {thread_id = thread_id(), info = rec.srv:info()}
end

function fe.conns(arg)
    local rec = servers()[arg.name]
    if rec == nil then
        return {}
    end
    return load_module(SERVER).conn_list(rec.srv)
end

function fe.reset_stats(arg)
    local rec = servers()[arg.name]
    if rec ~= nil then
        load_module(SERVER).reset_stats(rec.srv)
    end
    return true
end

function fe.rv_publish(arg)
    return publish(arg.name, arg.id)
end

-- After the code of this thread is reloaded: new accept loops.
function fe.after_reload()
    local server = load_module(SERVER)
    local n = 0
    for _, rec in pairs(servers()) do
        server.restart_accept(rec.srv)
        n = n + 1
    end
    return {thread_id = thread_id(), servers = n}
end

return M
