-- Command semantics over the data space. TX thread only.
--
-- engine.exec(ctx, reqs) -> resps, see DESIGN.md "Engine contract"; and
-- engine.batch(), the C batch, see DESIGN.md "The C data path".

local clock = require('clock')
local ffi = require('ffi')
local fiber = require('fiber')
local log = require('log')
local table_new = require('table.new')

local schema = require('memcached.schema')
local state = require('memcached.state')

local M = {}

local KEY = schema.KEY
local VALUE = schema.VALUE
local FLAGS = schema.FLAGS
local EXPTIME = schema.EXPTIME
local CAS = schema.CAS
local CREATED = schema.CREATED
local LAST_ACCESS = schema.LAST_ACCESS
local FETCHED = schema.FETCHED
local STALE = schema.STALE
local TOKEN = schema.TOKEN

-- Relative expiration times are at most 30 days.
M.MAX_RELATIVE_TIME = 2592000

-- A fetch updates last_access at most this often, like memcached's
-- ITEM_UPDATE_INTERVAL: a write per get would double the WAL traffic.
-- The first fetch of an item is always written (meta 'h').
M.LA_UPDATE_INTERVAL = 60

M.NON_NUMERIC = 'cannot increment or decrement non-numeric value'

local UINT64_MAX_STR = '18446744073709551615'

local uint64_t = ffi.typeof('uint64_t')

-- A uint64_t cdata from a Lua number or any 64-bit integer cdata.
local function u64(v)
    return ffi.cast(uint64_t, v)
end
M.u64 = u64

-- Decimal representation of a number or a 64-bit cdata, without 'ULL'.
function M.u64str(v)
    if type(v) == 'number' then
        return string.format('%d', v)
    end
    local s = tostring(u64(v))
    return s:sub(1, #s - 3)
end

-- A decimal uint64 string -> uint64_t cdata, or nil. Strict: digits only.
function M.parse_u64(s)
    if type(s) ~= 'string' or s == '' or s:find('[^0-9]') then
        return nil
    end
    s = s:gsub('^0+', '')
    if s == '' then
        return u64(0)
    end
    if #s > 20 or (#s == 20 and s > UINT64_MAX_STR) then
        return nil
    end
    return u64(tonumber64(s))
end

local INT64_MIN = u64(0x8000000000000000ULL)

-- The value of an item as a uint64, read the way memcached's
-- safe_strtoull() reads it (util.c; the data ends in "\r\n" there):
-- strtoull() skips leading whitespace and takes an optional sign, the digits
-- must be followed by whitespace, a NUL or the end of the value, and
-- anything after that is ignored ("12 ab" is 12). A '-' wraps the number,
-- and the result is refused when it is negative as an int64 ("-0" is 0).
local function value_u64(s)
    local sign, digits, after = s:match('^%s*([%+%-]?)(%d+)()')
    if digits == nil then
        return nil
    end
    if after <= #s and not s:find('^[%z%s]', after) then
        return nil
    end
    local v = M.parse_u64(digits)
    if v ~= nil and sign == '-' then
        v = u64(0) - v
        if v >= INT64_MIN then
            return nil
        end
    end
    return v
end

-- The per-process CAS counter: starts at the current time in microseconds.
local function counter_start()
    local c = state.cas_counter
    if not c then
        c = u64(clock.time64() / 1000)
        state.cas_counter = c
    end
    return c
end

local function counter()
    local c = counter_start()
    state.cas_counter = c + 1
    return c
end

-- A new CAS value for an item whose current CAS is old (nil if none).
function M.next_cas(old)
    local c = counter()
    if old ~= nil then
        old = u64(old) + 1
        if old > c then
            return old
        end
    end
    return c
end
local next_cas = M.next_cas

-- A strictly increasing timestamp for 'created' and flush times, so that an
-- item stored right after an immediate flush_all survives it even within one
-- event loop iteration (fiber.time() is cached per iteration).
function M.stamp(now)
    now = now or fiber.time()
    local last = state.stamp_last
    if now <= last then
        now = last + 1e-6
    end
    state.stamp_last = now
    return now
end
local stamp = M.stamp

-- A raw memcached exptime -> absolute time; 0 = never, -1 = already expired.
function M.abs_time(raw, now)
    if raw == nil or raw == 0 then
        return 0
    end
    raw = tonumber(raw)
    if raw < 0 then
        return -1
    end
    if raw <= M.MAX_RELATIVE_TIME then
        return (now or fiber.time()) + raw
    end
    return raw
end
local abs_time = M.abs_time

-- The size of an item as memcached counts it against its item size limit;
-- the same formula as proto/text.lua item_size() (see there).
function M.item_size(nkey, flags, vlen)
    return 48 + nkey + 1 + (flags ~= 0 and 4 or 0) + vlen + 2 + 8
end

local function expired(exptime, now)
    return exptime ~= 0 and exptime <= now
end

-- Whether a tuple is dead: expired or flushed.
function M.is_dead(t, now, flush)
    if expired(t[EXPTIME], now) then
        return true
    end
    return flush ~= nil and flush <= now and t[CREATED] <= flush
end
local is_dead = M.is_dead

local function item_of(t)
    return {
        key = t[KEY],
        value = t[VALUE],
        flags = t[FLAGS],
        exptime = t[EXPTIME],
        cas = u64(t[CAS]),
        created = t[CREATED],
        last_access = t[LAST_ACCESS],
        fetched = t[FETCHED],
        stale = t[STALE],
    }
end

-- Execution environment of one request.
--   space, space_name, bucket_id, flush (flush time or nil), ro, now,
--   mem (state.tokens[space_name] or nil), no_wins (see run())

-- Win tokens (DESIGN.md "Win tokens"). The item's token field is true when
-- the token was handed out for its current version: a win sets it in the
-- transaction of the get, and every write of a new version writes it false
-- (new_tuple(), the updates of arith and delete), but an arithmetic in
-- place, which keeps it as memcached does (see ops.arith). A read-only
-- instance can't write it: there a win goes to
-- state.tokens[space_name][key] = {cas, created}, the version it was handed
-- out for (env.mem). Every new version changes the CAS or the created
-- stamp, so such an entry needs no write to drop it; one that no longer
-- matches is dropped when it is seen.

-- Whether the win token of the live tuple t is out.
local function token_sent(env, key, t)
    if t[TOKEN] == true then
        return true
    end
    local mem = env.mem
    local m = mem ~= nil and mem[key] or nil
    if m == nil then
        return false
    end
    local sent
    if type(m) == 'table' then
        sent = m[1] == u64(t[CAS]) and m[2] == t[CREATED]
    else
        -- A CAS: a token of the code before the token field, kept by a
        -- hot reload.
        sent = m == u64(t[CAS])
    end
    if not sent then
        mem[key] = nil
    end
    return sent
end

-- A win on a read-only instance, kept in memory.
local function remember_win(env, key, t)
    local mem = env.mem
    if mem == nil then
        mem = {}
        state.tokens[env.space_name] = mem
        env.mem = mem
    end
    mem[key] = {u64(t[CAS]), t[CREATED]}
    -- The C loops look these up once per call (M.loop()).
    M.inputs_changed()
end

-- No live item: drop a token kept in memory for the key, if any.
local function forget_win(env, key)
    local mem = env.mem
    if mem ~= nil then
        mem[key] = nil
    end
end

-- Look up a live tuple; a dead one is deleted lazily (unless read-only or
-- peek is set).
local function lookup(env, key, peek)
    local t = env.space:get(key)
    if t ~= nil and is_dead(t, env.now, env.flush) then
        if not env.ro and not peek then
            env.space:delete(key)
        end
        t = nil
    end
    if t == nil then
        forget_win(env, key)
    end
    return t
end

local function new_tuple(env, key, value, flags, exptime, cas, stale, token)
    return {
        key,
        value,
        flags,
        exptime,
        cas,
        stamp(env.now),
        env.now,
        false,
        stale or false,
        env.bucket_id,
        token or false,
    }
end

-- Drop the reference Lua holds to a tuple now rather than when the GC
-- collects it, as evict.lua release() does: memtx frees a replaced or
-- deleted tuple only when no reference is left, and the memory it counts
-- drives the eviction. With little garbage the GC runs seldom, and such
-- references would keep the memory of the old values for long. The
-- reference must not be used after that.
local function release(t)
    if t ~= nil then
        ffi.gc(t, nil)
        ffi.C.box_tuple_unref(t)
    end
end

local ops = {}

-- The responses with nothing but a status: shared, never modified (the
-- responses are read-only, see DESIGN.md "Engine contract").
local MISS = {status = 'MISS'}
local NOT_FOUND = {status = 'NOT_FOUND'}
local NOT_STORED = {status = 'NOT_STORED'}
local EXISTS = {status = 'EXISTS'}
local DELETED = {status = 'DELETED'}

-- The CAS of a modified item: the client's one (meta E) or a new one.
local function modified_cas(req, old)
    if req.new_cas ~= nil then
        return u64(req.new_cas)
    end
    return next_cas(old)
end

-- Whether a get bumps last_access/fetched: meta u turns it off, and so does
-- the instance option track_access = false (env.track) for all the gets.
local function bumps(env, req)
    return env.track and req.bump_la ~= false and not req.no_lru_bump
end

-- get: see DESIGN.md. With peek (meta 'me') the lookup has no side
-- effects at all: no touch, no vivify, no last access bump, no win token.
-- Only a get with win_token (meta mg) takes the win token of a stale item
-- or a recache: memcached's classic reads (process_get_cmd, touch) never
-- touch ITEM_TOKEN_SENT, so the next mg still wins.
function ops.get(env, req)
    local key = req.key
    local now = env.now
    if req.peek then
        local t = lookup(env, key, true)
        if t == nil then
            return MISS
        end
        local item = item_of(t)
        return {
            status = 'HIT',
            item = item,
            stale = item.stale or nil,
            already_won = token_sent(env, key, t) or nil,
        }
    end
    local t = lookup(env, key)
    if t == nil then
        if req.vivify_ttl == nil then
            return MISS
        end
        local exptime = abs_time(req.vivify_ttl, now)
        local cas = modified_cas(req, nil)
        -- The caller wins: the stub has the token.
        t = new_tuple(env, key, '', 0, exptime, cas, false, true)
        -- The item as before this access: not fetched yet.
        local item = item_of(t)
        if expired(exptime, now) then
            -- As memcached (mg N-1): the stub is created, expires at once
            -- and the caller still wins.
            return {status = 'HIT', item = item, won = true, vivified = true}
        end
        -- The vivifying fetch counts as the first one: memcached bumps the
        -- new item (process_mget_cmd -> do_item_bump sets ITEM_FETCHED).
        if bumps(env, req) then
            t[FETCHED] = true
        end
        env.space:replace(t)
        return {status = 'HIT', item = item, won = true, vivified = true}
    end
    local item = item_of(t)
    local resp = {status = 'HIT', item = item}
    if item.stale then
        resp.stale = true
    end
    local update
    local won = false
    if req.win_token then
        if token_sent(env, key, t) then
            resp.already_won = true
        elseif item.stale then
            won = true
        elseif req.recache_ttl ~= nil and item.exptime ~= 0
            and item.exptime < abs_time(req.recache_ttl, now) then
            won = true
        end
        if env.no_wins then
            -- The token can't be written (see run()): no win.
            won = false
        end
        if won then
            resp.won = true
            if not env.ro then
                update = {{'=', TOKEN, true}}
            end
        end
    end
    if req.touch_ttl ~= nil then
        local exptime = abs_time(req.touch_ttl, now)
        item.exptime = exptime
        if expired(exptime, now) then
            env.space:delete(key)
            return resp
        end
        update = update or {}
        table.insert(update, {'=', EXPTIME, exptime})
    end
    if bumps(env, req) and not env.ro
        and (not t[FETCHED] or now - t[LAST_ACCESS] >= M.LA_UPDATE_INTERVAL) then
        update = update or {}
        table.insert(update, {'=', LAST_ACCESS, now})
        table.insert(update, {'=', FETCHED, true})
    end
    if update ~= nil then
        env.space:update(key, update)
    end
    if won and env.ro then
        -- Last: nothing after it can fail.
        remember_win(env, key, t)
    end
    return resp
end

function ops.store(env, req)
    local key = req.key
    local now = env.now
    local mode = req.mode or 'set'
    local cas_in = req.cas ~= nil and u64(req.cas) or nil
    local t = lookup(env, key)
    local value, flags, exptime
    local stale = false
    local keep_token = false
    local old_cas
    if t == nil then
        if cas_in ~= nil and (mode == 'set' or mode == 'replace') then
            return NOT_FOUND
        end
        if mode == 'replace' then
            return NOT_STORED
        elseif mode == 'append' or mode == 'prepend' then
            if req.vivify_ttl == nil then
                return NOT_STORED
            end
            exptime = abs_time(req.vivify_ttl, now)
        else
            exptime = abs_time(req.exptime, now)
        end
        value = req.value
        flags = req.flags or 0
    else
        old_cas = u64(t[CAS])
        if mode == 'add' then
            return NOT_STORED
        end
        -- As memcached's do_store_item, a CAS of 0 is no CAS (CAS_NONE):
        -- append/prepend skip the check, the cas command (set with a CAS)
        -- fails with it, invalidate or not.
        if mode == 'append' or mode == 'prepend' then
            if cas_in ~= nil and cas_in ~= 0 and cas_in ~= old_cas then
                return EXISTS
            end
            flags = t[FLAGS]
            -- As memcached's do_store_item: the joined item must fit the
            -- item size limit (req.max_size, the front-end's max_item_size,
            -- counted as proto/text.lua item_size() does), or nothing is
            -- stored and the old item stays.
            local max_size = req.max_size
            if max_size ~= nil
                and M.item_size(#key, flags, #t[VALUE] + #req.value) > max_size then
                return NOT_STORED
            end
            if mode == 'append' then
                value = t[VALUE] .. req.value
            else
                value = req.value .. t[VALUE]
            end
            exptime = t[EXPTIME]
        else
            if cas_in ~= nil and (cas_in == 0 or cas_in ~= old_cas) then
                if cas_in == 0 or not (req.invalidate and cas_in < old_cas) then
                    return EXISTS
                end
                -- An older CAS with invalidate: store, but mark stale and
                -- keep the TTL and the win token.
                stale = true
                keep_token = true
                exptime = t[EXPTIME]
            else
                exptime = abs_time(req.exptime, now)
            end
            value = req.value
            flags = req.flags or 0
        end
    end
    local cas = modified_cas(req, old_cas)
    if expired(exptime, now) then
        -- Stored and expired at once.
        if t ~= nil then
            release(env.space:delete(key))
            release(t)
        end
        return {status = 'STORED', cas = cas, size = #value}
    end
    release(
        env.space:replace(
            new_tuple(
                env,
                key,
                value,
                flags,
                exptime,
                cas,
                stale,
                keep_token and token_sent(env, key, t)
            )
        )
    )
    release(t)
    return {status = 'STORED', cas = cas, size = #value}
end

-- delete: removes the item; with invalidate (meta I) marks it stale and
-- bumps the CAS instead; with clear_value (meta x) replaces the item with
-- an empty one with the given flags. ttl (meta T) applies to the last two.
--
-- As memcached's process_mdelete_cmd, clear_value stores a new item: it is
-- not fetched, its last access is now and it is not stale unless this
-- delete also invalidates. It keeps the old CAS (DESIGN.md "CAS": the one
-- exception to the new-CAS rule); invalidate gives a new one, new_cas (E)
-- sets it in both cases.
function ops.delete(env, req)
    local key = req.key
    local now = env.now
    local t = lookup(env, key)
    if t == nil then
        return NOT_FOUND
    end
    local old_cas = u64(t[CAS])
    if req.cas ~= nil and u64(req.cas) ~= old_cas then
        return EXISTS
    end
    if not req.invalidate and not req.clear_value then
        env.space:delete(key)
        return DELETED
    end
    local cas
    if req.invalidate then
        cas = modified_cas(req, old_cas)
    elseif req.new_cas ~= nil then
        cas = u64(req.new_cas)
    else
        cas = old_cas
    end
    local exptime = t[EXPTIME]
    if req.ttl ~= nil then
        exptime = abs_time(req.ttl, now)
        if expired(exptime, now) then
            env.space:delete(key)
            return {status = 'DELETED', cas = cas}
        end
    end
    -- A new version, whose win token is not out: memcached clears
    -- ITEM_TOKEN_SENT on md I, and md x stores a new item.
    local update = {
        {'=', CAS, cas},
        {'=', EXPTIME, exptime},
        {'=', TOKEN, false},
    }
    if req.invalidate then
        table.insert(update, {'=', STALE, true})
    end
    if req.clear_value then
        table.insert(update, {'=', VALUE, ''})
        table.insert(update, {'=', FLAGS, req.flags or 0})
        table.insert(update, {'=', CREATED, stamp(now)})
        table.insert(update, {'=', LAST_ACCESS, now})
        table.insert(update, {'=', FETCHED, false})
        if not req.invalidate then
            table.insert(update, {'=', STALE, false})
        end
    end
    env.space:update(key, update)
    return {status = 'DELETED', cas = cas}
end

function ops.arith(env, req)
    local key = req.key
    local now = env.now
    local t = lookup(env, key)
    if t == nil then
        if req.vivify_ttl == nil then
            return NOT_FOUND
        end
        local value = u64(req.initial or 0)
        local exptime = abs_time(req.vivify_ttl, now)
        local cas = modified_cas(req, nil)
        if not expired(exptime, now) then
            env.space:replace(
                new_tuple(
                    env,
                    key,
                    M.u64str(value),
                    0,
                    exptime,
                    cas
                )
            )
        end
        return {
            status = 'STORED',
            value = value,
            cas = cas,
            exptime = exptime,
        }
    end
    -- The order of memcached's do_add_delta: an empty value is refused
    -- first, then a non-zero CAS is checked (0 is no CAS), then the number
    -- is read.
    if t[VALUE] == '' then
        return {status = 'CLIENT_ERROR', message = M.NON_NUMERIC}
    end
    local old_cas = u64(t[CAS])
    if req.cas ~= nil and u64(req.cas) ~= 0 and u64(req.cas) ~= old_cas then
        return EXISTS
    end
    local value = value_u64(t[VALUE])
    if value == nil then
        return {status = 'CLIENT_ERROR', message = M.NON_NUMERIC}
    end
    local delta = u64(req.delta or 1)
    if req.incr ~= false then
        value = value + delta
    elseif delta > value then
        value = u64(0)
    else
        value = value - delta
    end
    local cas = modified_cas(req, old_cas)
    local exptime = t[EXPTIME]
    -- As memcached's do_add_delta: a result that fits in the old value is
    -- written in place (res + 2 <= it->nbytes), padded with spaces to the
    -- old length, unless an earlier read of the batch holds the item
    -- (refcount > 2, see M.exec()). The item keeps its fetched flag, its
    -- stale marker and its win token, and its last access is bumped as
    -- do_item_update() does (with track_access). Otherwise the number is a
    -- new item of its own length, which no read holds: not fetched,
    -- accessed now, not stale, its win token not out.
    local text = M.u64str(value)
    local old_len = #t[VALUE]
    local held = env.held
    local in_place = #text <= old_len and not (held ~= nil and held[key])
    if held ~= nil then
        held[key] = nil
    end
    if in_place and #text < old_len then
        text = text .. string.rep(' ', old_len - #text)
    end
    local update = {
        {'=', VALUE, text},
        {'=', CAS, cas},
        {'=', CREATED, stamp(now)},
    }
    if in_place then
        -- The token field takes a token a read-only instance kept in memory
        -- too (env.mem): that entry is for the old CAS.
        table.insert(update, {'=', TOKEN, token_sent(env, key, t)})
        if env.track and now - t[LAST_ACCESS] >= M.LA_UPDATE_INTERVAL then
            table.insert(update, {'=', LAST_ACCESS, now})
        end
    else
        table.insert(update, {'=', TOKEN, false})
        table.insert(update, {'=', LAST_ACCESS, now})
        table.insert(update, {'=', FETCHED, false})
        table.insert(update, {'=', STALE, false})
    end
    if req.ttl ~= nil then
        exptime = abs_time(req.ttl, now)
        if expired(exptime, now) then
            env.space:delete(key)
            return {
                status = 'STORED',
                value = value,
                cas = cas,
                exptime = exptime,
            }
        end
        table.insert(update, {'=', EXPTIME, exptime})
    end
    env.space:update(key, update)
    return {status = 'STORED', value = value, cas = cas, exptime = exptime}
end

-- Sweepers to wake up after a flush (expire.lua registers them).
local function wakeup_sweepers(space_name)
    for _, rec in pairs(state.sweepers) do
        if rec.space_name == space_name and rec.cond ~= nil then
            rec.cond:signal()
        end
    end
end

-- The flush time for a raw flush_all delay.
function M.flush_time(delay, now)
    now = now or fiber.time()
    delay = tonumber(delay) or 0
    if delay <= 0 then
        return stamp(now)
    end
    return abs_time(delay, now)
end

-- Persist a flush: returns the new flush time. Raises on errors.
local function flush(space_name, delay)
    local meta = box.space[schema.META_SPACE]
    if meta == nil then
        meta = schema.create_meta()
    end
    local time = M.flush_time(delay)
    box.atomic(function()
        meta:replace({schema.flush_key(space_name), time})
    end)
    return time
end

local function error_message(err)
    if type(err) == 'cdata' or type(err) == 'table' then
        err = err.message or tostring(err)
    end
    return (tostring(err):gsub('[\r\n]+', ' '))
end
M.error_message = error_message

local function server_error(err)
    return {status = 'SERVER_ERROR', message = error_message(err)}
end

-- Out of memory ----------------------------------------------------------
--
-- memtx fails an allocation with an OutOfMemory error ("Failed to allocate
-- <n> bytes in slab allocator for memtx_tuple"). The request evicts items
-- (evict.lua direct(), when its space is evicted from) and runs once more;
-- otherwise, or when it fails again, it answers as memcached does: a store
-- "SERVER_ERROR out of memory storing object" (no_memory = true, for the
-- store_no_memory counter), anything else "SERVER_ERROR out of memory".

M.NO_MEMORY_STORE = 'out of memory storing object'
M.NO_MEMORY = 'out of memory'

local function out_of_memory(err)
    if type(err) ~= 'cdata' then
        return false
    end
    local ok, kind, code = pcall(function()
        return err.type, err.code
    end)
    return ok and (kind == 'OutOfMemory' or code == box.error.MEMORY_ISSUE)
end

local function evict_module()
    return package.loaded['memcached.evict'] or require('memcached.evict')
end

-- The answer to a request that can't get the memory. As memcached
-- (process_update_cmd_start), a set that can't be stored drops the old
-- value, so that a stale one doesn't stay in the cache.
local function no_memory(env, req)
    if req.op ~= 'store' then
        return {status = 'SERVER_ERROR', message = M.NO_MEMORY}
    end
    if (req.mode == nil or req.mode == 'set') and req.cas == nil then
        pcall(env.space.delete, env.space, req.key)
    end
    return {
        status = 'SERVER_ERROR',
        message = M.NO_MEMORY_STORE,
        no_memory = true,
    }
end

-- A get only writes optionally (last access, lazy deletion) unless it
-- touches or vivifies.
local function optional_writes(req)
    return req.op == 'get' and req.touch_ttl == nil and req.vivify_ttl == nil
end

-- The instance option commit_wait (DESIGN.md "Commit wait"): the
-- box.commit() options of the requests' transactions. 'complete', the
-- default, has none: box.atomic and autocommit statements.
local COMMIT_WAIT = {
    submit = {wait = 'submit'},
    none = {wait = 'none'},
}

-- The box.begin() options with a commit wait mode on vinyl and under memtx
-- MVCC: the default best-effort isolation shows a transaction that hasn't
-- written yet only the changes whose WAL write has finished, so a request
-- would not see the instance's own submitted writes.
local READ_COMMITTED = {txn_isolation = 'read-committed'}

-- Run a request: in its own transaction, except a plain get on memtx
-- (env.autocommit_reads). Such a get writes at most once (the lazy deletion
-- or the last access), with no yield between its read and its write, so
-- the write is a single autocommit statement, and box.atomic would only
-- add its cost.
--
-- Under memtx MVCC (env.mvcc) an autocommit read doesn't see a prepared
-- change, so two gets in the WAL write of a win could both win. There a get
-- that may take a win token keeps its transaction: the second one is
-- aborted by the conflict, and run() reads again without a win.
--
-- With a commit wait mode (env.wait) every request, plain gets and wins
-- included, is a transaction committed with box.commit(env.wait), so that
-- no write waits for the WAL. Under MVCC it reads the prepared changes
-- (env.txn_opts), a prepared win too. exec() makes sure that no
-- transaction is active.
local function attempt(fn, env, req)
    local wait = env.wait
    if wait ~= nil then
        box.begin(env.txn_opts)
        local ok, res = pcall(fn, env, req)
        if ok then
            local committed, err = pcall(box.commit, wait)
            if not committed then
                ok, res = false, err
            end
        end
        if not ok then
            -- A failed commit may leave the transaction open.
            box.rollback()
        end
        return ok, res
    end
    if env.autocommit_reads and optional_writes(req)
        and not (env.mvcc and req.win_token) then
        return pcall(fn, env, req)
    end
    return pcall(box.atomic, fn, env, req)
end

-- A failed attempt is rolled back as a whole, the win token field with it.
local function run(fn, env, req)
    if type(req.key) ~= 'string' then
        return {status = 'SERVER_ERROR', message = 'bad request: no key'}
    end
    local ok, res = attempt(fn, env, req)
    if ok then
        return res
    end
    if not env.ro and box.info.ro then
        -- The batch took the instance for writable (see read_only()), and
        -- it is not: run the request, and the rest of the batch, as on a
        -- read-only instance.
        env.ro = true
        ok, res = attempt(fn, env, req)
        if ok then
            return res
        end
        return server_error(res)
    end
    if optional_writes(req) and not env.ro then
        -- A read must not fail because of its optional writes: read
        -- without them. A win is a write too: this read takes none.
        env.ro = true
        env.no_wins = true
        ok, res = attempt(fn, env, req)
        env.ro = false
        env.no_wins = false
        if ok then
            return res
        end
    end
    if out_of_memory(res) then
        -- Evict and run once more (see "Out of memory" above).
        local evict = evict_module()
        if not env.ro and evict.direct(env.space_name, res) then
            ok, res = attempt(fn, env, req)
            if ok then
                return res
            end
            if out_of_memory(res) then
                evict.futile(res)
            end
        end
        if out_of_memory(res) then
            return no_memory(env, req)
        end
    end
    return server_error(res)
end

-- The C data path --------------------------------------------------------
--
-- memcached/datapath.c serves the requests the text parser marks with
-- `fast` ('get', 'gets' or 'set': classic, plain) on a memtx space; see
-- DESIGN.md "The C data path". It runs the same semantics as ops.get and
-- ops.store, with what needs the Lua state passed in: the clock, the CAS
-- counter, the created stamp, the flush time and the read-only flag. A set
-- writes the token field false, as new_tuple() does; a classic get takes
-- no win token. Anything it can't finish (a failed write, a tuple it
-- doesn't expect) goes through run() as if it had never been tried.
--
-- Its batch() serves these commands and most others (the classic storage,
-- delete, touch, gat and arithmetic, the meta mg, ms, md, ma and mn)
-- straight from a connection's input, before the front-end parses it
-- (M.batch()).

M.DATAPATH = 'memcached.datapath'
-- The ABI of datapath.c this code is written for (its DATAPATH_ABI).
M.DATAPATH_ABI = 4

-- The module; false when it can't be used; nil before the first look. A
-- reload of this module looks again (and requires the module again: the
-- .so stays loaded, a new one needs a restart).
local datapath_mod

local function load_datapath()
    local ok, mod = pcall(require, M.DATAPATH)
    if not ok then
        if tostring(mod):find(
            "module '" .. M.DATAPATH .. "' not found",
            1,
            true
        ) then
            log.warn(
                'memcached: the C data path (%s) is not built, the '
                    .. 'Lua engine serves every request',
                M.DATAPATH
            )
        else
            log.error(
                'memcached: failed to load %s, the Lua engine serves every request: %s',
                M.DATAPATH,
                tostring(mod)
            )
        end
        return false
    end
    if type(mod) ~= 'table' or mod.ABI ~= M.DATAPATH_ABI then
        log.error(
            'memcached: %s has ABI %s, this code needs %d: not used '
                .. '(restart Tarantool after an upgrade)',
            M.DATAPATH,
            tostring(type(mod) == 'table' and mod.ABI or nil),
            M.DATAPATH_ABI
        )
        return false
    end
    return mod
end

-- Whether the C data path is built and loads.
function M.datapath_available()
    if datapath_mod == nil then
        datapath_mod = load_datapath()
    end
    return datapath_mod ~= false
end

-- The C data path, or nil when it is not available or switched off
-- (state.datapath_off: memcached.datapath(false), MEMCACHED_DATAPATH=lua).
local function datapath()
    if state.datapath_off then
        return nil
    end
    if datapath_mod == nil then
        datapath_mod = load_datapath()
    end
    return datapath_mod or nil
end
M.datapath = datapath

-- With a commit wait mode, a C call runs in a transaction that fast()
-- begins (datapath.c's box calls work in one) and this commits: res is the
-- call's result, returned when the commit succeeds. When the call did
-- nothing (nil) or the commit fails, the transaction is rolled back and nil
-- is returned: the request goes through run(), as when the C code fails.
local function dp_commit(env, res)
    if res ~= nil and pcall(box.commit, env.wait) then
        return res
    end
    box.rollback()
    return nil
end

-- A request marked fast: on the C data path when it can serve it,
-- otherwise run(fn, env, req). env.dp is the module (nil: not for this
-- batch).
--
-- A get ('get', 'gets') answers a hit with the encoded VALUE block, a
-- string, which only the text encoder reads (see proto/text.lua); a miss
-- with MISS. A set ('set') answers as ops.store.
--
-- A set keeps the Lua path when the instance is read-only (it fails
-- there, with the Lua engine's error and counters), when the item would
-- expire at once (ops.store deletes the old one then), and under memtx MVCC
-- (env.dp_store: ops.store reads and writes in one transaction, which MVCC
-- can abort on a conflict; the C set reads and writes as two autocommit
-- statements).
--
-- With a commit wait mode (env.wait), each C call runs in its own
-- transaction, see dp_commit().
local function fast(env, req, mode, fn)
    local key = req.key
    if type(key) ~= 'string' then
        return run(fn, env, req)
    end
    local dp = env.dp
    local wait = env.wait
    if mode == 'set' then
        if env.dp_store and not env.ro and req.op == 'store' and req.cas == nil
            and (req.mode == nil or req.mode == 'set') then
            local now = env.now
            local exptime = abs_time(req.exptime, now)
            if not expired(exptime, now) then
                local value = req.value
                if wait ~= nil then
                    box.begin(env.txn_opts)
                end
                local cas = dp.set(
                    env.space_id,
                    key,
                    value,
                    req.flags or 0,
                    exptime,
                    counter(),
                    stamp(now),
                    now,
                    env.flush,
                    env.bucket_id
                )
                if wait ~= nil then
                    cas = dp_commit(env, cas)
                end
                if cas ~= nil then
                    return {
                        status = 'STORED',
                        cas = u64(cas),
                        size = #value,
                    }
                end
            end
        end
    elseif req.op == 'get' and req.touch_ttl == nil then
        if wait ~= nil then
            box.begin(env.txn_opts)
        end
        local res = dp.get(
            env.space_id,
            key,
            env.now,
            env.flush,
            env.ro,
            bumps(env, req),
            mode == 'gets',
            M.LA_UPDATE_INTERVAL
        )
        if wait ~= nil then
            res = dp_commit(env, res)
        end
        if res == 0 or res == 1 then
            -- No live item (1: a dead one was deleted), as in lookup().
            forget_win(env, key)
            return MISS
        elseif res ~= nil then
            return res
        end
    end
    return run(fn, env, req)
end

-- Whether the instance is read-only, for a batch.
--
-- A box.status watcher keeps state.ro_cache.is_ro, so that a writable
-- instance doesn't call box.info.ro per batch. Only "writable" is taken from
-- the cache: the watcher runs asynchronously and may be an event loop
-- iteration late. A late "writable" is harmless: env.ro only allows the
-- optional writes, and run() checks box.info.ro when a request fails and
-- runs it again as on a read-only instance. Anything else asks box.info.ro.
--
-- The watcher callback is a module-level function: a closure defined in
-- read_only() would make every return from it a UCLO, which the JIT can't
-- compile, and abort every trace through engine.exec().
local function on_box_status(_, value)
    local cache = state.ro_cache
    if type(value) == 'table' and type(value.is_ro) == 'boolean' then
        cache.is_ro = value.is_ro
    else
        cache.is_ro = nil
    end
end

local function read_only()
    local cache = state.ro_cache
    if cache.is_ro == false then
        return false
    end
    if cache.watcher == nil then
        local ok, watcher = pcall(box.watch, 'box.status', on_box_status)
        cache.watcher = ok and watcher or false
    end
    return box.info.ro
end

-- Whether memtx MVCC is on (see attempt() and fast()).
local function mvcc()
    local cfg = box.cfg
    return type(cfg) == 'table' and cfg.memtx_use_mvcc_engine == true
end

-- The C batch: datapath.c batch() serves the commands it can at the head
-- of a connection's input (an ibuf or a string) and appends their replies
-- to output (an ibuf), up to max_commands commands; see DESIGN.md "The C
-- data path". The rest of the input is the front-end's as usual. Returns
-- nil when the C code can't serve this ctx (no data path, not memtx, a
-- commit wait mode), otherwise
--   consumed, commands, gets, hits, stored, held, unstored, touches,
--   touch_hits, delete_hits, delete_misses, incr_hits, incr_misses,
--   decr_hits, decr_misses, cas_hits, cas_misses, cas_badval, meta
-- (see datapath.c; nothing after held when those counters are all 0): the
-- bytes and commands served, the counters, and nil
-- or the keys the served reads hold for the requests after them
-- (track_held(), the `held` field of a request). held is nil or the keys
-- that the reads before this call hold (as exec() returns them), which
-- the call updates.
--
-- The per-call state is that of exec(): one clock reading (datapath.c
-- reads it again after a write or a commit that yielded), the flush time
-- and the read-only flag. The writes take the CAS counter and the created
-- stamp from the state table themselves, and are left to Lua on a
-- read-only instance and with memtx MVCC, as in fast(). The writes of
-- consecutive commands go into one transaction, committed before the call
-- returns (datapath.c "The group"): the replies may go out after it.
function M.batch(ctx, input, output, max_commands, max_item_size, held)
    if COMMIT_WAIT[ctx.commit_wait] ~= nil then
        return nil
    end
    local dp = datapath()
    if dp == nil then
        return nil
    end
    local space_name = ctx.space_name
    local space = box.space[space_name]
    if space == nil or space.engine ~= 'memtx' then
        return nil
    end
    counter_start()
    local ro = read_only()
    local mem = state.tokens[space_name]
    if mem ~= nil and next(mem) == nil then
        mem = nil
    end
    return dp.batch(
        space.id,
        input,
        output,
        max_commands,
        max_item_size,
        fiber.time(),
        schema.flush_time(space_name),
        ro,
        ctx.track_access ~= false,
        M.LA_UPDATE_INTERVAL,
        not ro and not mvcc(),
        ctx.bucket_id or 0,
        state,
        mem,
        fiber.time,
        held
    )
end

-- The inputs of the C connection loop (M.loop()) taken at its call have
-- changed: the flush time, the win tokens kept in memory, the options of an
-- instance, the switches. The loop compares state.dp_gen before each batch
-- and returns, and the server calls it again with the new inputs.
function M.inputs_changed()
    state.dp_gen = (state.dp_gen or 0) + 1
end

-- The C connection loop (datapath.c loop(), memcached/cloop.c): reads the
-- connection's socket, runs the C batch on its input and writes the
-- replies, for as long as the batch serves the whole input; see DESIGN.md
-- "The C connection loop". Returns nil when it can't run here (as
-- M.batch(), or the loop switched off: state.cloop_off), otherwise the
-- reason it returned for and, for LOOP_STOP, the commands of the batch
-- served and the items their reads hold. srv.dp_counters (made here) gets
-- the counters of the commands it serves, see server.sync_stats().
--
-- The arguments of the batch are those of M.batch(), taken once per call
-- (but the read-only flag and the clock, per batch); the loop returns when
-- they may have changed (M.inputs_changed(), a schema change).
function M.loop(ctx, srv, conn, ibuf, obuf, idle, new_cmd, limits)
    if state.cloop_off or COMMIT_WAIT[ctx.commit_wait] ~= nil then
        return nil
    end
    local dp = datapath()
    if dp == nil or dp.loop == nil then
        return nil
    end
    local space_name = ctx.space_name
    local space = box.space[space_name]
    if space == nil or space.engine ~= 'memtx' then
        return nil
    end
    local counters = srv.dp_counters
    if counters == nil then
        counters = dp.counters()
        srv.dp_counters = counters
    end
    local flush = schema.flush_time(space_name)
    if not schema.flush_tracked() then
        return nil
    end
    counter_start()
    local mem = state.tokens[space_name]
    if mem ~= nil and next(mem) == nil then
        mem = nil
    end
    return dp.loop(
        conn,
        conn.fd,
        ibuf,
        obuf,
        counters,
        space.id,
        srv.parse_opts.max_item_size,
        flush,
        ctx.track_access ~= false,
        M.LA_UPDATE_INTERVAL,
        mvcc(),
        ctx.bucket_id or 0,
        state,
        mem,
        idle,
        conn.replied,
        new_cmd,
        conn.last_cmd,
        limits.MAX_BATCH,
        limits.READ_SIZE,
        limits.BUFFER_KEEP
    )
end

-- The items an earlier read of the batch holds, as memcached's responses
-- hold a reference to the items they return until they are written (a
-- classic get/gets/gat/gats or an mg hit: req.hold), so that an arithmetic
-- on them can't update the value in place (see ops.arith). A store or a
-- delete replaces the item; memcached writes the responses out at an mn
-- (req.release, set by the server on the next request).
local function track_held(env, req, resp)
    local held = env.held
    local key = req.key
    if req.op == 'get' then
        if req.hold and (type(resp) == 'string' or resp.status == 'HIT') then
            if held == nil then
                held = {}
                env.held = held
            end
            held[key] = true
        end
    elseif held ~= nil and held[key] and type(resp) == 'table' then
        local status = resp.status
        if (req.op == 'store' and status == 'STORED')
            or (req.op == 'delete' and status == 'DELETED'
                and (req.clear_value or not req.invalidate)) then
            held[key] = nil
        end
    end
end

-- The execution environments: a batch takes one from the pool and puts it
-- back when it is done, instead of a new table per batch. exec() runs in
-- several fibers at once (a batch yields on its writes), each with an env
-- of its own; nothing keeps a reference to an env after its batch. A batch
-- that raises drops its env. The pool is per module instance (a reload
-- starts a new one) and per thread.
local ENV_POOL_MAX = 64
local env_pool = {}
local env_pool_n = 0

-- A new env with every field exec() sets, false for now: a field that is
-- set to nil keeps its slot then. Setting nil to a field the table doesn't
-- have adds a slot, and a full table, rehashed, drops the slots of the nil
-- fields again: an env reused with such fields would be reallocated on
-- every batch.
local function new_env()
    return {
        space = false,
        space_name = false,
        bucket_id = false,
        flush = false,
        ro = false,
        mem = false,
        no_wins = false,
        now = false,
        track = false,
        autocommit_reads = false,
        mvcc = false,
        dp = false,
        dp_store = false,
        space_id = false,
        wait = false,
        txn_opts = false,
        held = false,
    }
end

local function take_env()
    local n = env_pool_n
    if n == 0 then
        return new_env()
    end
    local env = env_pool[n]
    env_pool[n] = nil
    env_pool_n = n - 1
    return env
end

local function put_env(env)
    local n = env_pool_n
    if n >= ENV_POOL_MAX then
        return
    end
    -- No reference to the space, the tokens or the held keys from the pool.
    env.space = nil
    env.mem = nil
    env.held = nil
    env.dp = nil
    n = n + 1
    env_pool[n] = env
    env_pool_n = n
end

-- Execute a batch of requests; each one in its own transaction (but see
-- attempt() and fast()). Returns the responses and, when the batch tracks
-- them, the items its reads hold at its end (track_held(), nil for none):
-- the server passes them on to the C batch after it.
function M.exec(ctx, reqs)
    local space_name = ctx.space_name
    -- Sized at once: no reallocation as it fills.
    local resps = table_new(#reqs, 0)
    local space = box.space[space_name]
    if space == nil then
        local err = ("Space '%s' does not exist"):format(space_name)
        for i = 1, #reqs do
            resps[i] = server_error(err)
        end
        return resps
    end
    local memtx = space.engine == 'memtx'
    local dp = memtx and datapath() or nil
    local mvcc_on = memtx and mvcc()
    local wait = COMMIT_WAIT[ctx.commit_wait]
    if wait ~= nil and box.is_in_txn() then
        -- Called in the caller's transaction (mcc.exec() in box.begin()),
        -- where no transaction of its own can begin: as with 'complete'.
        wait = nil
    end
    local env = take_env()
    env.space = space
    env.space_name = space_name
    env.bucket_id = ctx.bucket_id or 0
    -- Cached, see schema.flush_time().
    env.flush = schema.flush_time(space_name)
    env.ro = read_only()
    -- The win tokens taken while read-only (see token_sent()), and
    -- whether a win can't be written (see run()).
    env.mem = state.tokens[space_name]
    env.no_wins = false
    env.now = nil
    -- The instance option track_access: whether gets record the last
    -- access and the fetched flag.
    env.track = ctx.track_access ~= false
    -- vinyl reads yield: its gets keep their transaction, so that a
    -- write between the read and the lazy deletion is a conflict.
    env.autocommit_reads = memtx
    -- memtx MVCC, see attempt() and fast().
    env.mvcc = mvcc_on
    -- The C data path for the requests marked fast (memtx only), see
    -- fast().
    env.dp = dp
    env.dp_store = dp ~= nil and not mvcc_on
    env.space_id = space.id
    -- The instance option commit_wait: the box.commit() options of the
    -- requests' transactions, nil for 'complete' (see attempt()), and
    -- the box.begin() options with them (see READ_COMMITTED).
    env.wait = wait
    env.txn_opts = wait ~= nil and (not memtx or mvcc_on) and READ_COMMITTED
        or nil
    -- The items the reads of the batch hold (track_held()).
    env.held = nil
    -- Only a batch with an arithmetic needs to know which items its reads
    -- hold (track_held()): no table for the others. Nor does its caller,
    -- unless it passes held items in (a C batch's, or a table to fill):
    -- then exec() returns them.
    local track = false
    for i = 1, #reqs do
        local req = reqs[i]
        if req.op == 'arith' or req.held ~= nil then
            track = true
            break
        end
    end
    for i, req in ipairs(reqs) do
        local op = req.op
        if req.held ~= nil then
            -- The items the reads of the C batch before this one hold
            -- (M.batch()).
            env.held = req.held
        end
        if req.release then
            env.held = nil
        end
        if op == 'flush' then
            local ok, res = pcall(flush, space_name, req.delay)
            if ok then
                env.flush = res
                resps[i] = {status = 'OK'}
                wakeup_sweepers(space_name)
            else
                resps[i] = server_error(res)
            end
        else
            local fn = ops[op]
            if fn == nil then
                resps[i] = {
                    status = 'SERVER_ERROR',
                    message = ("unknown operation '%s'"):format(
                        tostring(op)
                    ),
                }
            else
                env.now = fiber.time()
                local mode = req.fast
                if mode ~= nil and dp ~= nil then
                    resps[i] = fast(env, req, mode, fn)
                else
                    resps[i] = run(fn, env, req)
                end
                if track then
                    track_held(env, req, resps[i])
                end
            end
        end
    end
    local held = env.held
    put_env(env)
    return resps, held
end

-- Item count and size of a data space.
function M.stats(ctx)
    local space = box.space[ctx.space_name]
    if space == nil then
        return {curr_items = 0, bytes = 0}
    end
    local ok_len, len = pcall(space.len, space)
    local ok_size, size = pcall(space.bsize, space)
    return {
        curr_items = ok_len and len or 0,
        bytes = ok_size and size or 0,
    }
end

return M
