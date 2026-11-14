-- The expiration sweeper. TX thread only.
--
-- A fiber per instance (or vshard storage space) deletes the dead items
-- (expired or flushed) that nobody reads, to give their memory back early.
-- Correctness doesn't depend on it: dead items are never returned, a read
-- deletes them, and the evictor deletes the dead ones it samples. It works
-- with what the space has, so a space whose indexes change (expire.mode,
-- schema.ensure()) or arrive by replication needs no restart:
-- - with the exptime index (expire.mode = 'index', and vinyl): it walks the
--   index upwards from exptime > 0 while exptime <= now and deletes the
--   items in small transactions, yielding between them. After a flush_all
--   it also scans the whole space once and deletes the items stored before
--   the flush time;
-- - without it (expire.mode = 'scan', memtx): it scans the primary HASH
--   index with a cursor that it keeps between the transactions, and deletes
--   the dead items, expired or flushed, adaptively as Redis' active
--   expiration does (a scan of its expires dict since Redis 6): a cycle
--   checks `batch` items per transaction while more than SCAN_DEAD_RATIO of
--   them were dead, for at most SCAN_BUDGET seconds, and the next one starts
--   SCAN_PERIOD later; a cycle that finds nothing doubles the wait, up to
--   MAX_SLEEP. A cycle checks at least enough items to finish a pass over
--   the space in SCAN_PASS seconds;
-- - a vinyl space without the index (it has not arrived by replication
--   yet): nothing, the items go when they are read.
--
-- Random samples (box_index_random(), as the evictor takes them) would not
-- do for the scan: memtx picks the first tuple after a random slot of the
-- hash table, which never shrinks, so after many deletes the tuples that
-- follow another one in the table are seldom picked and the last dead ones
-- stay for a long time.

local clock = require('clock')
local ffi = require('ffi')
local fiber = require('fiber')
local log = require('log')
local msgpack = require('msgpack')

local schema = require('memcached.schema')
local state = require('memcached.state')
local evict = require('memcached.evict')

local M = {}

local SELF = 'memcached.expire'

local KEY = schema.KEY
local EXPTIME = schema.EXPTIME
local CREATED = schema.CREATED

-- Sleep bounds between rounds, seconds.
M.MAX_SLEEP = 1
M.MIN_SLEEP = 0.01
-- Sleep after an error or while the instance is read-only.
M.IDLE_SLEEP = 1

M.DEFAULT_BATCH = 100

-- Scanning (no exptime index): the shortest wait between two cycles, the
-- time a cycle may run, seconds, and the share of dead items in a
-- transaction above which the cycle goes on (Redis: 10 Hz, 25% of the CPU,
-- 10%); the longest pass over the space when there is nothing to delete,
-- seconds.
M.SCAN_PERIOD = 0.1
M.SCAN_BUDGET = 0.025
M.SCAN_DEAD_RATIO = 0.1
M.SCAN_PASS = 600

-- Delete the keys whose tuples still satisfy dead(t), in one transaction.
-- The win tokens go with the tuples.
local function delete_keys(space, keys, dead)
    local n = 0
    box.atomic(function()
        for _, key in ipairs(keys) do
            local t = space:get(key)
            if t ~= nil and dead(t) then
                space:delete(key)
                n = n + 1
            end
        end
    end)
    return n
end

-- Whether index is still the exptime index of the sweeper's space: it
-- may be dropped while the sweeper yields (expire.mode = 'scan').
local function same_index(rec, index)
    local space = box.space[rec.space_name]
    return index ~= nil and space ~= nil and space.index.exptime == index
end

-- Delete expired items. Returns the time to sleep before the next round.
local function sweep_expired(rec, space)
    local index = space.index.exptime
    while not rec.stopped do
        if box.info.ro then
            return M.IDLE_SLEEP
        end
        if not same_index(rec, index) then
            return M.MIN_SLEEP
        end
        local now = fiber.time()
        local tuples = index:select({0}, {iterator = 'GT', limit = rec.batch})
        local keys = {}
        local next_exptime
        for _, t in ipairs(tuples) do
            if t[EXPTIME] <= now then
                table.insert(keys, t[KEY])
            else
                next_exptime = t[EXPTIME]
                break
            end
        end
        if #keys > 0 then
            rec.expired = rec.expired
                + delete_keys(space, keys, function(t)
                    return t[EXPTIME] ~= 0 and t[EXPTIME] <= now
                end)
        end
        if next_exptime ~= nil then
            return next_exptime - now
        end
        if #tuples < rec.batch then
            return M.MAX_SLEEP
        end
        fiber.sleep(0)
    end
    return M.MAX_SLEEP
end

-- Delete the items stored before the flush time; walks the exptime index
-- (it has all the tuples and, unlike HASH, supports pagination).
-- Returns true when the scan is complete.
local function sweep_flushed(rec, space, flush)
    local index = space.index.exptime
    local dead = function(t)
        return t[CREATED] <= flush
    end
    local pos
    while true do
        if rec.stopped or box.info.ro or not same_index(rec, index)
            or schema.flush_time(rec.space_name) ~= flush then
            return false
        end
        local tuples
        tuples, pos = index:select({}, {
            iterator = 'GE',
            limit = rec.batch,
            after = pos,
            fetch_pos = true,
        })
        if #tuples == 0 then
            break
        end
        local keys = {}
        for _, t in ipairs(tuples) do
            if dead(t) then
                table.insert(keys, t[KEY])
            end
        end
        if #keys > 0 then
            rec.flushed = rec.flushed + delete_keys(space, keys, dead)
        end
        if #tuples < rec.batch then
            break
        end
        fiber.sleep(0)
    end
    rec.scanned_flush = flush
    return true
end

-- Scanning ----------------------------------------------------------------

pcall(
    ffi.cdef,
    [[
box_iterator_t *
box_index_iterator(uint32_t space_id, uint32_t index_id, int type,
                   const char *key, const char *key_end);
]]
)

local C = ffi.C
local decode = msgpack.decode_unchecked
local release = evict.release
local ITER_ALL = box.index.ALL
local EMPTY_KEY = msgpack.encode({})
local ptuple = ffi.new('box_tuple_t *[1]')

local function field(tp, no)
    local p = C.box_tuple_field(tp, no - 1)
    if p == nil then
        return nil
    end
    return (decode(p))
end

-- Close the scan cursor of a sweeper.
local function close_cursor(rec)
    local it = rec.cursor
    rec.cursor = nil
    if it ~= nil then
        ffi.gc(it, nil)
        C.box_iterator_free(it)
    end
end

-- The next tuple of the scan of the primary index, as a raw pointer (no Lua
-- reference, valid until the tuple is deleted or the fiber yields), or nil
-- at the end of a pass. The cursor is a memtx iterator kept between the
-- transactions: a HASH iterator is a position in the hash table, so the
-- writes in between make it skip or repeat a few tuples at most, and a
-- truncate or an index change ends the pass.
local function next_tuple(rec, space_id)
    local it = rec.cursor
    if it ~= nil and rec.cursor_space ~= space_id then
        close_cursor(rec)
        it = nil
    end
    if it == nil then
        local key = ffi.cast('const char *', EMPTY_KEY)
        it = C.box_index_iterator(space_id, 0, ITER_ALL, key, key + #EMPTY_KEY)
        if it == nil then
            error(box.error.last())
        end
        rec.cursor = ffi.gc(it, C.box_iterator_free)
        rec.cursor_space = space_id
    end
    if C.box_iterator_next(it, ptuple) ~= 0 then
        close_cursor(rec)
        error(box.error.last())
    end
    local tp = ptuple[0]
    if tp == nil then
        close_cursor(rec)
        rec.passes = (rec.passes or 0) + 1
    end
    return tp
end

-- One transaction of the scan: the next job.batch items (fewer in a smaller
-- space: none twice), then the dead ones are deleted, and released at once
-- so that their memory goes when the transaction commits. The transaction
-- doesn't yield before the commit, so what is dead stays dead. Counts into
-- job.
local function scan_batch(job)
    local rec, space, space_id = job.rec, job.space, job.space_id
    local now, flush = job.now, job.flush
    local n = math.min(job.batch, space:len())
    local keys, kinds = {}, {}
    local wrapped = false
    while job.checked < n do
        local tp = next_tuple(rec, space_id)
        if tp == nil then
            if wrapped then
                break
            end
            wrapped = true
        else
            job.checked = job.checked + 1
            local exptime = field(tp, EXPTIME)
            local kind
            if exptime ~= 0 and exptime <= now then
                kind = 'expired'
            elseif flush ~= nil and flush <= now and field(tp, CREATED) <= flush then
                kind = 'flushed'
            end
            if kind ~= nil then
                table.insert(keys, field(tp, KEY))
                table.insert(kinds, kind)
            end
        end
    end
    for i, key in ipairs(keys) do
        local old = space:delete(key)
        if old ~= nil then
            release(old)
            local kind = kinds[i]
            job[kind] = job[kind] + 1
        end
    end
end

-- One cycle of the scan. Returns the time to sleep.
local function sweep_scan(rec, space)
    local name = rec.space_name
    local flush = schema.flush_time(name)
    if flush ~= rec.scan_flush then
        -- A new flush time: start over at the full rate.
        rec.scan_flush = flush
        rec.scan_sleep = nil
    end
    -- Enough items to finish a pass in SCAN_PASS seconds at this rate.
    local quota = math.max(
        rec.batch,
        math.ceil(space:len() * (rec.scan_sleep or M.SCAN_PERIOD) / M.SCAN_PASS)
    )
    local started = clock.monotonic()
    local checked, found = 0, 0
    while not rec.stopped do
        if box.info.ro then
            return M.IDLE_SLEEP
        end
        local job = {
            rec = rec,
            space = space,
            space_id = space.id,
            batch = rec.batch,
            now = fiber.time(),
            flush = flush,
            checked = 0,
            expired = 0,
            flushed = 0,
        }
        box.atomic(scan_batch, job)
        rec.checked = (rec.checked or 0) + job.checked
        rec.expired = rec.expired + job.expired
        rec.flushed = rec.flushed + job.flushed
        local dead = job.expired + job.flushed
        checked = checked + job.checked
        found = found + dead
        if job.checked == 0 or (checked >= quota
            and dead <= job.checked * M.SCAN_DEAD_RATIO)
            or clock.monotonic() - started >= M.SCAN_BUDGET then
            break
        end
        fiber.sleep(0)
        -- The space may be gone or have changed while the fiber yielded.
        space = schema.space(name)
        if space == nil or space.index.exptime ~= nil then
            close_cursor(rec)
            return M.MIN_SLEEP
        end
        flush = schema.flush_time(name)
    end
    local timeout
    if found > 0 or rec.scan_sleep == nil then
        timeout = M.SCAN_PERIOD
    else
        timeout = math.min(rec.scan_sleep * 2, M.MAX_SLEEP)
    end
    rec.scan_sleep = timeout
    if flush ~= nil then
        local now = fiber.time()
        if flush > now then
            -- Wake up when the delayed flush comes.
            timeout = math.min(timeout, flush - now)
        end
    end
    return timeout
end

-- One round of the sweeper. Returns the time to sleep.
function M.step(rec)
    local space = schema.space(rec.space_name)
    if space == nil or box.info.ro then
        return M.IDLE_SLEEP
    end
    if space.index.exptime == nil then
        if space.engine ~= 'memtx' then
            return M.IDLE_SLEEP
        end
        rec.mode = 'scan'
        return sweep_scan(rec, space)
    end
    close_cursor(rec)
    rec.mode = 'index'
    local timeout = sweep_expired(rec, space)
    local flush = schema.flush_time(rec.space_name)
    if flush ~= nil and flush ~= rec.scanned_flush then
        local now = fiber.time()
        if flush <= now then
            sweep_flushed(rec, space, flush)
        else
            timeout = math.min(timeout, flush - now)
        end
    end
    return timeout
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
        -- Look the code up on every round, so that a reload takes effect.
        local mod = package.loaded[SELF] or M
        local ok, res = pcall(mod.step, rec)
        if rec.stopped or (not ok and cancelled(res)) then
            break
        end
        local timeout
        if ok then
            timeout = res
        else
            log.error(
                "memcached: sweeper of '%s': %s",
                rec.name,
                tostring(res)
            )
            timeout = mod.IDLE_SLEEP
        end
        timeout = math.max(mod.MIN_SLEEP, math.min(timeout, mod.MAX_SLEEP))
        rec.cond:wait(timeout)
    end
end

-- Start the sweeper of an instance. opts = {batch = <number>}. A sweeper
-- that replaces one of the same space (new options) keeps its counters.
function M.start(name, space_name, opts)
    local old = state.sweepers[name]
    M.stop(name)
    opts = opts or {}
    local rec = {
        name = name,
        space_name = space_name,
        batch = opts.batch or M.DEFAULT_BATCH,
        cond = fiber.cond(),
        stopped = false,
        expired = 0,
        flushed = 0,
        -- The scan (no exptime index): items checked, passes done.
        checked = 0,
        passes = 0,
        scanned_flush = nil,
        -- How the last round swept: 'index' or 'scan' (nil before it).
        mode = nil,
        -- The scan: its cursor (a box iterator) and the space it is of, the
        -- flush time of the last cycle, the last wait.
        cursor = nil,
        cursor_space = nil,
        scan_flush = nil,
        scan_sleep = nil,
        fiber = nil,
    }
    rec.fiber = fiber.new(function()
        local ok, err = pcall(main, rec)
        if not ok and not rec.stopped and not cancelled(err) then
            log.error(
                "memcached: sweeper of '%s' failed: %s",
                name,
                tostring(err)
            )
        end
        if state.sweepers[name] == rec then
            state.sweepers[name] = nil
        end
    end)
    if old ~= nil and old.space_name == space_name then
        for _, k in ipairs({'expired', 'flushed', 'checked', 'passes'}) do
            rec[k] = old[k] or 0
        end
    end
    rec.fiber:name('mcc/expire/' .. name, {truncate = true})
    state.sweepers[name] = rec
    return rec
end

-- Stop the sweeper of an instance, if any.
function M.stop(name)
    local rec = state.sweepers[name]
    if rec == nil then
        return
    end
    state.sweepers[name] = nil
    rec.stopped = true
    close_cursor(rec)
    if rec.fiber ~= nil and rec.fiber:status() ~= 'dead'
        and rec.fiber:id() ~= fiber.id() then
        pcall(rec.fiber.cancel, rec.fiber)
    end
end

-- Wake up the sweepers of a space (e.g. after a flush).
function M.wakeup(space_name)
    for _, rec in pairs(state.sweepers) do
        if rec.space_name == space_name then
            rec.cond:signal()
        end
    end
end

function M.info(name)
    local rec = state.sweepers[name]
    if rec == nil then
        return nil
    end
    return {
        expired = rec.expired,
        flushed = rec.flushed,
        checked = rec.checked or 0,
        passes = rec.passes or 0,
        mode = rec.mode,
    }
end

return M
