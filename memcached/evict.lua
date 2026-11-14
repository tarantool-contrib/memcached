-- Eviction: an approximated LRU over the memtx data spaces, so that a full
-- memtx_memory behaves as a full memcached does. TX thread only.
--
-- memtx keeps no LRU order, so the eviction samples, as Redis does. A round
-- takes `samples` random tuples of the primary HASH index and deletes:
--   1. every dead one (expired or flushed), as the sweeper would: counted as
--      reclaimed;
--   2. then the best candidate: the oldest last_access with track_access,
--      the oldest created without it: counted as an eviction.
--
-- The memory signal is the memtx quota (memtx_memory), from box.slab.info():
--   used = quota_used - arena_size + arena_used
-- is the memory the memtx allocations take (tuples, index extents, large
-- tuples). The arena never gives a slab back to the quota, so quota_used
-- alone never drops; used does, as soon as a tuple is freed. From
-- box.stat.memtx():
--   live = used - data.read_view - data.garbage - index.read_view
-- leaves out what read views (a checkpoint, a dump, read_mode = 'snapshot')
-- hold: deleting more doesn't free it, it comes back when they close.
-- An episode starts when used reaches high * quota and goes on while live
-- is above low * quota.
--
-- Two paths:
-- - the evictor fiber mcc/evict, one per process (memtx memory is shared by
--   every space): it checks the memory every INTERVAL seconds and evicts in
--   transactions of `batch` rounds, yielding between them, from the spaces
--   in an episode, each batch from a space picked with a probability
--   proportional to its size (space:bsize());
-- - direct(): a write that failed with a memtx OutOfMemory error
--   (engine.lua run()) evicts DIRECT_ITEMS items in one transaction,
--   preferring items whose size is about the size of the failed allocation
--   (the slab allocator keeps a pool per size class), and runs once more.
--
-- The evicted tuples are released at once (release()): a tuple that Lua
-- references is freed only when the Lua GC collects the reference, and the
-- memory must drop when the eviction commits. The samples are read through
-- the box C API (box_index_random, box_tuple_field), so they make no Lua
-- references at all.
--
-- The spaces: start(owner, space_name, opts) lets the evictor evict from a
-- space, stop(owner) stops it. An owner is a memcached instance (its name),
-- or a vshard storage space (storage.lua, 'vshard:<space_name>'). When
-- several owners name one space, the first owner by name gives its options.
-- Only memtx spaces are evicted from, and nothing else than these spaces.
--
-- The state is in state.evict (owners, spaces with the counters, the fiber
-- record, the direct path's back-off); the fiber looks the code up in
-- package.loaded, so a reload takes effect.

local ffi = require('ffi')
local fiber = require('fiber')
local log = require('log')
local msgpack = require('msgpack')

local schema = require('memcached.schema')
local state = require('memcached.state')

local M = {}

local SELF = 'memcached.evict'

-- The instance option evict and the storage.cfg() space option of the same
-- name.
M.DEFAULTS = {
    enabled = true,
    high = 0.95,
    low = 0.9,
    samples = 8,
    batch = 100,
}
M.MAX_SAMPLES = 64

-- How often the evictor checks the memory, seconds.
M.INTERVAL = 0.1
-- Sleep after an error or while the instance is read-only.
M.IDLE_SLEEP = 1
-- Sleep between the steps of an episode.
M.MIN_SLEEP = 0.01
-- Items a failed write evicts, and the rounds it samples for each of them
-- to find one of about its size.
M.DIRECT_ITEMS = 8
M.DIRECT_ROUNDS = 4
-- A failed write whose retry failed again keeps the writes of its size
-- class from evicting for this long, seconds: the eviction didn't help
-- (read views, a size class without items), and must not go on emptying
-- the cache for nothing.
M.DIRECT_BACKOFF = 1
-- The bytes a memtx tuple takes besides its msgpack (bsize): the tuple
-- header and the field map, as in memtx's "Failed to allocate <n> bytes"
-- (about 14 for a local space, 18 for a vshard one).
M.TUPLE_HEADER = 16
-- An evicted item is active when it was fetched within this many seconds
-- (engine.LA_UPDATE_INTERVAL: last_access is written at most this often).
M.ACTIVE_INTERVAL = 60

local KEY = schema.KEY - 1
local EXPTIME = schema.EXPTIME - 1
local CREATED = schema.CREATED - 1
local LAST_ACCESS = schema.LAST_ACCESS - 1
local FETCHED = schema.FETCHED - 1

local COUNTERS = {
    'evictions',
    'reclaimed',
    'evicted_unfetched',
    'evicted_active',
    'direct_reclaims',
}
M.COUNTERS = COUNTERS

local C = ffi.C
local decode = msgpack.decode_unchecked

local function current()
    return package.loaded[SELF] or M
end

local function evict_state()
    local s = state.evict
    if s == nil then
        s = {
            -- owner -> {space_name, high, low, samples, batch, track}.
            owners = {},
            -- space name -> {evicting, <COUNTERS>}.
            spaces = {},
            -- The evictor fiber: {fiber, cond, stopped}.
            rec = nil,
            -- size class -> time until which direct() doesn't evict.
            backoff = {},
        }
        state.evict = s
    end
    return s
end

local function space_rec(s, space_name)
    local rec = s.spaces[space_name]
    if rec == nil then
        rec = {evicting = false}
        for _, name in ipairs(COUNTERS) do
            rec[name] = 0
        end
        s.spaces[space_name] = rec
    end
    return rec
end

-- Options ---------------------------------------------------------------

local function is_integer(v)
    return type(v) == 'number' and v == math.floor(v)
        and v > -2 ^ 53 and v < 2 ^ 53
end

-- The error of a bad evict = {...} option, or nil. The missing fields are
-- the defaults; with partial = true only the given fields are checked (the
-- options are merged into other ones later, and checked again then).
function M.check(v, partial)
    if type(v) ~= 'table' then
        return 'must be a table'
    end
    for k, x in pairs(v) do
        if k == 'enabled' then
            if type(x) ~= 'boolean' then
                return "'enabled' must be a boolean"
            end
        elseif k == 'high' or k == 'low' then
            if type(x) ~= 'number' or not (x > 0 and x <= 1) then
                return ("'%s' must be a number in (0, 1]"):format(k)
            end
        elseif k == 'samples' then
            if not is_integer(x) or x < 1 or x > M.MAX_SAMPLES then
                return ("'samples' must be an integer in [1, %d]"):format(
                    M.MAX_SAMPLES
                )
            end
        elseif k == 'batch' then
            if not is_integer(x) or x < 1 then
                return "'batch' must be a positive integer"
            end
        else
            return ("unknown option '%s'"):format(tostring(k))
        end
    end
    if not partial
        and not ((v.low or M.DEFAULTS.low) < (v.high or M.DEFAULTS.high)) then
        return "'low' must be less than 'high'"
    end
    return nil
end

-- The options with the defaults filled in.
function M.merge(v)
    local res = {}
    for k, x in pairs(M.DEFAULTS) do
        res[k] = x
    end
    for k, x in pairs(v or {}) do
        res[k] = x
    end
    return res
end

-- Memory ----------------------------------------------------------------

-- The memtx quota and the memory its allocations use, bytes.
function M.memory()
    local i = box.slab.info()
    local quota = tonumber(i.quota_size)
    local used = tonumber(i.quota_used) - tonumber(i.arena_size)
        + tonumber(i.arena_used)
    return quota, used
end

-- The memory held by the read views: deleted tuples they still see, and
-- tuples they released that memtx frees on the next allocations.
function M.held()
    local m = box.stat.memtx()
    return (tonumber(m.data.read_view) or 0) + (tonumber(m.data.garbage) or 0)
        + (tonumber(m.index.read_view) or 0)
end

-- The spaces --------------------------------------------------------------

-- space name -> the options of the first owner by name.
local function configured(s)
    local res, by = {}, {}
    for owner, conf in pairs(s.owners) do
        local name = conf.space_name
        if by[name] == nil or owner < by[name] then
            by[name] = owner
            res[name] = conf
        end
    end
    return res
end

-- The spaces to evict from: name -> {space, conf, rec}. Only memtx spaces
-- that exist.
local function evictable(s)
    local res = {}
    for name, conf in pairs(configured(s)) do
        local space = schema.space(name)
        if space ~= nil and space.engine == 'memtx' then
            res[name] = {space = space, conf = conf, rec = space_rec(s, name)}
        end
    end
    return res
end

-- A space of the set, picked with a probability proportional to its size;
-- nil when they are all empty.
local function pick(set)
    local total = 0
    local sizes = {}
    for name, e in pairs(set) do
        local size = e.space:bsize()
        if size > 0 then
            sizes[name] = size
            total = total + size
        end
    end
    if total == 0 then
        return nil
    end
    local r = math.random() * total
    local last
    for name, size in pairs(sizes) do
        last = name
        r = r - size
        if r < 0 then
            return name, set[name]
        end
    end
    return last, set[last]
end

-- Rounds ------------------------------------------------------------------

local ptuple

-- A random tuple of the primary index, as a raw pointer (no Lua
-- reference), or nil when the space is empty. The pointer is valid until
-- the tuple is deleted or the fiber yields.
local function random_tuple(space_id)
    if ptuple == nil then
        ptuple = ffi.new('box_tuple_t *[1]')
    end
    if C.box_index_random(
        space_id,
        0,
        math.random(0, 0xFFFFFFFE),
        ptuple
    ) ~= 0 then
        error(box.error.last())
    end
    local tp = ptuple[0]
    if tp == nil then
        return nil
    end
    return tp
end

local function field(tp, no)
    local p = C.box_tuple_field(tp, no)
    if p == nil then
        return nil
    end
    return (decode(p))
end

-- Free a tuple Lua has a reference to now, instead of when the GC
-- collects the reference: the finalizer is dropped and run by hand. The
-- reference must not be used after that.
local function release(t)
    ffi.gc(t, nil)
    C.box_tuple_unref(t)
end

-- For the scanning sweeper (expire.lua).
M.release = release

local function delete(job, tp)
    local key = field(tp, KEY)
    local old = job.space:delete(key)
    if old ~= nil then
        release(old)
    end
end

-- Whether two allocation sizes are about the same size class: within the
-- allocator's growth factor (box.cfg.slab_alloc_factor) of each other.
local function similar(job, size)
    local need = job.need
    return math.abs(size - need) <= job.tolerance
end

-- One round: sample, delete the dead samples and the best live one.
-- With job.need, a live sample of about that size is preferred; job.strict:
-- evict none other. Returns the evicted (or reclaimed) bytes.
local function round(job)
    local space_id = job.space_id
    local now, flush, track = job.now, job.flush, job.track
    local best, best_age, best_similar
    local freed = 0
    for _ = 1, job.samples do
        local tp = random_tuple(space_id)
        if tp == nil then
            break
        end
        local exptime = field(tp, EXPTIME)
        local created = field(tp, CREATED)
        if (exptime ~= 0 and exptime <= now)
            or (flush ~= nil and flush <= now and created <= flush) then
            local size = tonumber(C.box_tuple_bsize(tp)) + M.TUPLE_HEADER
            delete(job, tp)
            job.reclaimed = job.reclaimed + 1
            freed = freed + size
        else
            local age = track and field(tp, LAST_ACCESS) or created
            local sim = false
            if job.need ~= nil then
                sim = similar(
                    job,
                    tonumber(C.box_tuple_bsize(tp)) + M.TUPLE_HEADER
                )
            end
            if best == nil or (sim and not best_similar)
                or (sim == best_similar and age < best_age) then
                best, best_age, best_similar = tp, age, sim
            end
        end
    end
    if best == nil or (job.strict and not best_similar) then
        return freed, false
    end
    local size = tonumber(C.box_tuple_bsize(best)) + M.TUPLE_HEADER
    if track then
        local fetched = field(best, FETCHED)
        if not fetched then
            job.evicted_unfetched = job.evicted_unfetched + 1
        elseif now - best_age < M.ACTIVE_INTERVAL then
            job.evicted_active = job.evicted_active + 1
        end
    end
    delete(job, best)
    job.evictions = job.evictions + 1
    return freed + size, true
end

local function new_job(name, e, now)
    return {
        space = e.space,
        space_id = e.space.id,
        space_name = name,
        rec = e.rec,
        samples = e.conf.samples,
        track = e.conf.track,
        now = now,
        flush = schema.flush_time(name),
        evictions = 0,
        reclaimed = 0,
        evicted_unfetched = 0,
        evicted_active = 0,
    }
end

-- Run fn(job) in a transaction; add the counters when it commits. Returns
-- what fn returns.
local function run(job, fn)
    local a, b = box.atomic(fn, job)
    local rec = job.rec
    rec.evictions = rec.evictions + job.evictions
    rec.reclaimed = rec.reclaimed + job.reclaimed
    rec.evicted_unfetched = rec.evicted_unfetched + job.evicted_unfetched
    rec.evicted_active = rec.evicted_active + job.evicted_active
    return a, b
end

-- One batch: `batch` rounds in a transaction. Returns the freed bytes and
-- the number of rounds that deleted something.
local function batch(job, n)
    return run(job, function()
        local freed, done = 0, 0
        for _ = 1, n do
            local bytes = round(job)
            if bytes == 0 then
                break
            end
            freed = freed + bytes
            done = done + 1
        end
        return freed, done
    end)
end

-- The evictor -------------------------------------------------------------

-- Evict about `to_free` bytes from the spaces of the set, a batch at a
-- time, yielding between the batches. Returns the bytes freed.
local function evict_bytes(rec, set, to_free)
    local total = 0
    while to_free > 0 and not rec.stopped do
        if box.info.ro then
            break
        end
        local name, e = pick(set)
        if name == nil then
            break
        end
        local job = new_job(name, e, fiber.time())
        local freed = batch(job, e.conf.batch)
        if freed == 0 then
            set[name] = nil
        end
        total = total + freed
        to_free = to_free - freed
        fiber.sleep(0)
    end
    return total
end

-- One step of the evictor. Returns the time to sleep, or nil when no space
-- is left to evict from.
function M.step(rec)
    local s = evict_state()
    if next(s.owners) == nil then
        return nil
    end
    if box.info.ro then
        return M.IDLE_SLEEP
    end
    local quota, used = M.memory()
    local fill = used / quota
    local active
    for name, e in pairs(evictable(s)) do
        if e.rec.evicting or fill >= e.conf.high then
            active = active or {}
            active[name] = e
        end
    end
    if active == nil then
        return M.INTERVAL
    end
    local live = (used - M.held()) / quota
    local to_free = 0
    for name, e in pairs(active) do
        if live <= e.conf.low then
            e.rec.evicting = false
            active[name] = nil
        else
            e.rec.evicting = true
            to_free = math.max(to_free, (live - e.conf.low) * quota)
        end
    end
    if next(active) == nil then
        return M.INTERVAL
    end
    local freed = evict_bytes(rec, active, to_free)
    if freed == 0 then
        -- Nothing left to evict: the memory is taken by other spaces.
        for _, e in pairs(active) do
            e.rec.evicting = false
        end
        return M.INTERVAL
    end
    return M.MIN_SLEEP
end

-- Whether this fiber has been cancelled (err is what a pcall caught).
-- Tarantool cancels every fiber on shutdown: that is a normal stop.
local function cancelled(err)
    if not pcall(fiber.testcancel) then
        return true
    end
    local ok, kind = pcall(function() return err.type end)
    return ok and kind == 'FiberIsCancelled'
end

local function main(rec)
    while not rec.stopped do
        -- Look the code up on every step, so that a reload takes effect.
        local mod = package.loaded[SELF] or M
        local ok, res = pcall(mod.step, rec)
        if rec.stopped or (not ok and cancelled(res)) then
            break
        end
        local timeout
        if not ok then
            log.error('memcached: evictor: %s', tostring(res))
            timeout = mod.IDLE_SLEEP
        elseif res == nil then
            break
        else
            timeout = res
        end
        rec.cond:wait(timeout)
    end
end

local function stop_fiber(s)
    local rec = s.rec
    s.rec = nil
    if rec == nil then
        return
    end
    rec.stopped = true
    if rec.fiber ~= nil and rec.fiber:status() ~= 'dead'
        and rec.fiber:id() ~= fiber.id() then
        pcall(rec.fiber.cancel, rec.fiber)
    end
end

local function start_fiber(s)
    local rec = {cond = fiber.cond(), stopped = false, fiber = nil}
    rec.fiber = fiber.new(function()
        local ok, err = pcall(main, rec)
        if not ok and not rec.stopped and not cancelled(err) then
            log.error('memcached: evictor failed: %s', tostring(err))
        end
        local st = state.evict
        if st ~= nil and st.rec == rec then
            st.rec = nil
        end
    end)
    rec.fiber:name('mcc/evict', {truncate = true})
    s.rec = rec
    return rec
end

local function ensure_fiber(s)
    local rec = s.rec
    if rec ~= nil and not rec.stopped and rec.fiber:status() ~= 'dead' then
        rec.cond:signal()
        return rec
    end
    return start_fiber(s)
end

-- The direct path ---------------------------------------------------------

-- The size of the failed allocation of a memtx OutOfMemory error, if it
-- was a tuple ("Failed to allocate <n> bytes in slab allocator for
-- memtx_tuple"; an index extent fails "in mempool for new slab").
local function failed_size(err)
    local ok, message = pcall(function() return err.message end)
    if not ok or type(message) ~= 'string'
        or not message:find('memtx_tuple', 1, true) then
        return nil
    end
    return tonumber(message:match('^Failed to allocate (%d+) bytes'))
end

-- The size class of the back-off: the allocator's classes grow by about
-- slab_alloc_factor, so these buckets are a few classes wide.
local function size_class(need)
    if need == nil then
        return 0
    end
    return math.floor(math.log(need) / math.log(1.25))
end

-- A write to the space failed with a memtx OutOfMemory error (err): evict
-- up to DIRECT_ITEMS items, preferring ones of about the failed size, from
-- the evictable spaces, picked by size as the evictor does. Only when the
-- space itself is evictable. Returns whether anything was evicted, and so
-- whether the write should run once more.
function M.direct(space_name, err)
    local s = state.evict
    if s == nil or next(s.owners) == nil or box.info.ro then
        return false
    end
    local set = evictable(s)
    local own = set[space_name]
    if own == nil then
        return false
    end
    local need = failed_size(err)
    local class = size_class(need)
    local now = fiber.time()
    local until_ = s.backoff[class]
    if until_ ~= nil then
        if now < until_ then
            return false
        end
        s.backoff[class] = nil
    end
    own.rec.direct_reclaims = own.rec.direct_reclaims + 1
    if s.rec ~= nil then
        s.rec.cond:signal()
    end
    local factor = type(box.cfg) == 'table' and box.cfg.slab_alloc_factor
        or 1.05
    local done = 0
    for _ = 1, M.DIRECT_ITEMS do
        local name, e = pick(set)
        if name == nil then
            break
        end
        local job = new_job(name, e, now)
        if need ~= nil then
            job.need = need
            job.tolerance = math.max(8, need * (factor - 1))
        end
        local ok = run(job, function()
            -- Look for an item of about the failed size first.
            if need ~= nil then
                job.strict = true
                for _ = 1, M.DIRECT_ROUNDS - 1 do
                    local _, evicted = round(job)
                    if evicted then
                        return true
                    end
                end
                job.strict = false
            end
            local bytes = round(job)
            return bytes > 0
        end)
        if ok then
            done = done + 1
        else
            set[name] = nil
        end
    end
    return done > 0
end

-- A write evicted for (direct()) and failed again: no direct eviction for
-- its size class for DIRECT_BACKOFF seconds.
function M.futile(err)
    local s = state.evict
    if s == nil then
        return
    end
    s.backoff[size_class(failed_size(err))] = fiber.time() + M.DIRECT_BACKOFF
end

-- Public API --------------------------------------------------------------

-- Evict from the space for owner. opts = {high, low, samples, batch,
-- track_access}; the defaults fill the missing ones.
function M.start(owner, space_name, opts)
    local s = evict_state()
    local conf = M.merge(opts)
    s.owners[owner] = {
        space_name = space_name,
        high = conf.high,
        low = conf.low,
        samples = conf.samples,
        batch = conf.batch,
        track = opts == nil or opts.track_access ~= false,
    }
    space_rec(s, space_name)
    ensure_fiber(s)
end

-- Stop evicting for owner. The counters of the space are kept.
function M.stop(owner)
    local s = state.evict
    if s == nil or s.owners[owner] == nil then
        return
    end
    s.owners[owner] = nil
    if next(s.owners) == nil then
        stop_fiber(s)
    end
end

-- Whether the evictor evicts from the space.
function M.enabled(space_name)
    local s = state.evict
    if s == nil then
        return false
    end
    for _, conf in pairs(s.owners) do
        if conf.space_name == space_name then
            return true
        end
    end
    return false
end

-- The counters of a space: evictions, reclaimed, evicted_unfetched,
-- evicted_active, direct_reclaims.
function M.stats(space_name)
    local s = state.evict
    local rec = s ~= nil and s.spaces[space_name] or nil
    local res = {}
    for _, name in ipairs(COUNTERS) do
        res[name] = rec ~= nil and rec[name] or 0
    end
    return res
end

-- Restart the fiber with the current code (mcc.reload()); the owners and
-- the counters are kept. Returns whether it runs.
function M.restart()
    local s = evict_state()
    stop_fiber(s)
    if next(s.owners) == nil then
        return false
    end
    start_fiber(s)
    return true
end

-- The memory, the fiber and the spaces, for inst:info() and debugging.
-- Yields never.
function M.info()
    local s = evict_state()
    local quota, used = M.memory()
    local held = M.held()
    local res = {
        running = s.rec ~= nil and s.rec.fiber:status() ~= 'dead',
        quota = quota,
        used = used,
        held = held,
        fill = used / quota,
        live = (used - held) / quota,
        spaces = {},
    }
    local conf = configured(s)
    for name, rec in pairs(s.spaces) do
        local c = conf[name]
        local info = {
            enabled = c ~= nil,
            evicting = rec.evicting,
        }
        if c ~= nil then
            info.high, info.low = c.high, c.low
            info.samples, info.batch = c.samples, c.batch
            info.track_access = c.track
        end
        for _, k in ipairs(COUNTERS) do
            info[k] = rec[k]
        end
        res.spaces[name] = info
    end
    return res
end

-- For tests: one step now, as the fiber would run it.
function M.run_once()
    local s = evict_state()
    return current().step(s.rec or {stopped = false})
end

return M
