-- The vshard storage side: the sharded data spaces and the functions the
-- routers call. TX thread only.
--
--   local storage = require('memcached.storage')
--   storage.cfg({spaces = {
--       [<space_name>] = {storage = 'memory' | 'disk',
--                         expire = {enabled = true, batch = 100,
--                                   mode = 'index' | 'scan'},
--                         evict = {enabled = true, high = 0.95, low = 0.9,
--                                  samples = 8, batch = 100},
--                         track_access = true,
--                         commit_wait = 'complete' | 'submit' | 'none'},
--   }})
--   storage.info() -> {ro, spaces = {[name] = {storage, track_access,
--                                              commit_wait, expire_mode,
--                                              ready, sweeper, expired,
--                                              flushed, checked, evict,
--                                              evictor, evictions, reclaimed,
--                                              evicted_unfetched,
--                                              evicted_active,
--                                              direct_reclaims, batches,
--                                              requests}}}
--   storage.stop()
--
-- cfg() sets the spaces this storage serves; each call replaces the whole
-- set. A space left out is no longer served, and its data is kept. The
-- spaces are created in format v3 with the bucket_id index, which makes them
-- sharded for vshard: at once if the instance is writable, and then whenever
-- it becomes writable. A box.status watcher reacts to is_ro == false, as the
-- vshard config applier does when it deploys the vshard functions. The
-- expiration sweepers and the evictor (evict.lua) run only while the
-- instance is writable; the evictor evicts from the memory spaces with
-- evict.enabled.
--
-- expire.mode is the instance option of the same name: 'index' keeps the
-- exptime index on the space, 'scan' has none (memory only); the default
-- is 'scan' for memory and 'index' for disk. apply() creates or drops the
-- index of an existing space to match on a writable instance (schema.ensure()).
--
-- track_access (default true) is the instance option of the same name for
-- the gets of that space: the gets run here, so this is where it is set
-- (see engine.exec()). false: gets never write the item. commit_wait
-- (default 'complete') is the instance option of the same name for the
-- writes on that space, which run here too.
--
-- The routers call these Lua globals (backend/vshard.lua):
--   mcc.storage_exec(space_name, bucket_id, reqs) -> resps
--       through vshard.router.callrw/callro/callbro, so it runs inside
--       vshard.storage.call() under a ref of the bucket: the bucket can't
--       move while the requests run;
--   mcc.storage_flush(space_name, delay) -> resp
--   mcc.storage_stats(space_name) -> {curr_items, bytes, limit_maxbytes,
--       evict, <the eviction counters>}
--       through vshard.router.map_callrw, under a storage ref; without the
--       tuples of the buckets this storage doesn't own;
--   mcc.storage_dump(space_name, bucket_id, sub, after, limit) -> {lines,
--       after}: a page of "lru_crawler metadump|mgdump" of one bucket,
--       through callrw/callro/callbro.
-- vshard 1.0.0 on Tarantool 3 runs the function of vshard.storage.call()
-- and of the map stage with net.box.self:call(). It looks the name up in
-- box.func first and then as a path of Lua globals ('mcc.storage_exec' is
-- _G.mcc.storage_exec). The vshard functions are setuid, so the globals need
-- no _func entries and no grants. They also work on read-only replicas,
-- where nothing could be created.
--
-- The state is in state.storage, so it survives a reload; the globals,
-- the watcher and the sweepers look the code up in package.loaded.

local fiber = require('fiber')
local log = require('log')

local state = require('memcached.state')
local schema = require('memcached.schema')
local engine = require('memcached.engine')
local expire = require('memcached.expire')
local evict = require('memcached.evict')

local M = {}

local SELF = 'memcached.storage'

-- Sweepers of the storage spaces are named SWEEPER_PREFIX .. space_name,
-- and so are their owners in the evictor.
M.SWEEPER_PREFIX = 'vshard:'

-- Global name -> function of this module.
M.EXPORTS = {
    storage_exec = 'exec',
    storage_flush = 'flush',
    storage_stats = 'stats',
    storage_dump = 'dump',
}

-- The global table the functions are exported in.
M.GLOBAL = 'mcc'

local DEFAULT_EXPIRE = {enabled = true, batch = 100}

local function current()
    return package.loaded[SELF] or M
end

local function storage_state()
    local s = state.storage
    if s == nil then
        s = {
            -- space name -> {storage, expire = {enabled, batch}, evict =
            --                {enabled, high, low, samples, batch},
            --                track_access, ready, batches, requests}
            spaces = {},
            -- The box.status watcher.
            watcher = nil,
            -- apply() is running; again = run it once more.
            applying = false,
            again = false,
            cond = fiber.cond(),
            last_error = nil,
        }
        state.storage = s
    end
    return s
end

-- Options ---------------------------------------------------------------

local function is_integer(v)
    return type(v) == 'number' and v == math.floor(v)
        and v > -2 ^ 53 and v < 2 ^ 53
end

local function check_space(name, conf)
    if type(name) ~= 'string' or name == '' then
        error(
            'memcached.storage: a space name must be a non-empty string',
            0
        )
    end
    local prefix = ("memcached.storage: space '%s': "):format(name)
    if conf == nil then
        conf = {}
    elseif type(conf) ~= 'table' then
        error(prefix .. 'options must be a table', 0)
    end
    local res = {
        storage = 'memory',
        expire = {},
        track_access = true,
        commit_wait = 'complete',
        batches = 0,
        requests = 0,
    }
    for k, v in pairs(conf) do
        if k == 'storage' then
            if v ~= 'memory' and v ~= 'disk' then
                error(prefix .. "bad value for option 'storage'", 0)
            end
            res.storage = v
        elseif k == 'track_access' then
            if type(v) ~= 'boolean' then
                error(prefix .. "bad value for option 'track_access'", 0)
            end
            res.track_access = v
        elseif k == 'commit_wait' then
            if v ~= 'complete' and v ~= 'submit' and v ~= 'none' then
                error(prefix .. "bad value for option 'commit_wait'", 0)
            end
            res.commit_wait = v
        elseif k == 'expire' then
            if type(v) ~= 'table' then
                error(prefix .. "option 'expire' must be a table", 0)
            end
            for ek, ev in pairs(v) do
                if ek == 'enabled' and type(ev) == 'boolean' then
                    res.expire.enabled = ev
                elseif ek == 'batch' and is_integer(ev) and ev >= 1 then
                    res.expire.batch = ev
                elseif ek == 'mode' and schema.EXPIRE_MODES[ev] then
                    res.expire.mode = ev
                else
                    error(prefix .. "bad value for option 'expire'", 0)
                end
            end
        elseif k == 'evict' then
            local err = evict.check(v)
            if err ~= nil then
                error(prefix .. "bad value for option 'evict': " .. err, 0)
            end
            res.evict = evict.merge(v)
        else
            error(prefix .. ("unknown option '%s'"):format(tostring(k)), 0)
        end
    end
    for k, v in pairs(DEFAULT_EXPIRE) do
        if res.expire[k] == nil then
            res.expire[k] = v
        end
    end
    res.expire.mode = schema.expire_mode(res.storage, res.expire.mode)
    local err = schema.check_expire_mode(res.storage, res.expire.mode)
    if err ~= nil then
        error(prefix .. "bad value for option 'expire': " .. err, 0)
    end
    res.evict = res.evict or evict.merge()
    return res
end

local function check_cfg(cfg)
    if type(cfg) ~= 'table' then
        error('memcached.storage: cfg must be a table', 0)
    end
    for k in pairs(cfg) do
        if k ~= 'spaces' then
            error(
                ("memcached.storage: unknown option '%s'"):format(
                    tostring(k)
                ),
                0
            )
        end
    end
    local spaces = cfg.spaces or {}
    if type(spaces) ~= 'table' then
        error("memcached.storage: option 'spaces' must be a table", 0)
    end
    local res = {}
    for name, conf in pairs(spaces) do
        res[name] = check_space(name, conf)
    end
    return res
end

-- Exported functions ----------------------------------------------------

local function export()
    local g = rawget(_G, M.GLOBAL)
    if g == nil then
        g = {}
        rawset(_G, M.GLOBAL, g)
    elseif type(g) ~= 'table' then
        error(
            ("memcached.storage: global '%s' is not a table"):format(
                M.GLOBAL
            ),
            0
        )
    end
    for name, fn in pairs(M.EXPORTS) do
        g[name] = function(...)
            return current()[fn](...)
        end
    end
end

local function unexport()
    local g = rawget(_G, M.GLOBAL)
    if type(g) ~= 'table' then
        return
    end
    for name in pairs(M.EXPORTS) do
        g[name] = nil
    end
    if next(g) == nil then
        rawset(_G, M.GLOBAL, nil)
    end
end

local function served(space_name)
    local s = state.storage
    return s ~= nil and s.spaces[space_name] or nil
end

local function not_served(space_name)
    return ("space '%s' is not served by this storage"):format(
        tostring(space_name)
    )
end

-- Execute a batch of requests of one bucket. See engine.exec().
function M.exec(space_name, bucket_id, reqs)
    if type(reqs) ~= 'table' or type(bucket_id) ~= 'number' then
        error('Usage: mcc.storage_exec(space_name, bucket_id, reqs)', 0)
    end
    local conf = served(space_name)
    if conf == nil then
        local message = not_served(space_name)
        local resps = {}
        for i = 1, #reqs do
            resps[i] = {status = 'SERVER_ERROR', message = message}
        end
        return resps
    end
    conf.batches = conf.batches + 1
    conf.requests = conf.requests + #reqs
    return engine.exec({
        space_name = space_name,
        bucket_id = bucket_id,
        track_access = conf.track_access,
        commit_wait = conf.commit_wait,
    }, reqs)
end

-- flush_all on this storage: the flush time goes to its _mcc_meta.
function M.flush(space_name, delay)
    if served(space_name) == nil then
        return {status = 'SERVER_ERROR', message = not_served(space_name)}
    end
    return engine.exec(
        {space_name = space_name},
        {{op = 'flush', delay = delay}}
    )[1]
end

-- The statuses (vshard.consts.BUCKET) of the buckets whose tuples this
-- storage doesn't own: a transfer to it in progress, or sent away and not
-- garbage collected yet. A sending (or readonly) bucket is still its own.
M.FOREIGN_BUCKETS = {'receiving', 'sent', 'garbage'}

-- The number of tuples of the space in the buckets this storage doesn't
-- own: an index count per such bucket (the few in transfer or waiting for
-- the garbage collector), not a scan. memtx counts in log time; vinyl
-- reads the range of each bucket.
function M.foreign_count(space)
    local index = space.index.bucket_id
    local _bucket = box.space._bucket
    local status = _bucket ~= nil and _bucket.index.status or nil
    if index == nil or status == nil then
        return 0
    end
    -- The ids first: a vinyl count may yield.
    local ids = {}
    for _, name in ipairs(M.FOREIGN_BUCKETS) do
        for _, bucket in status:pairs({name}) do
            table.insert(ids, bucket[1])
        end
    end
    local n = 0
    for _, id in ipairs(ids) do
        n = n + index:count({id})
    end
    return n
end

-- Item count and size of the space on this storage, without the tuples of
-- the buckets it doesn't own (foreign_count()), so a bucket in transfer is
-- counted once in the sum over the storages. The bytes of those tuples are
-- taken out at the average size of the items: approximate.
function M.stats(space_name)
    local conf = served(space_name)
    if conf == nil then
        error(not_served(space_name), 0)
    end
    local res = engine.stats({space_name = space_name})
    local space = schema.space(space_name)
    local total = res.curr_items
    if space ~= nil and total > 0 then
        local foreign = current().foreign_count(space)
        if foreign > 0 then
            local own = math.max(total - foreign, 0)
            res.bytes = math.floor(res.bytes * own / total)
            res.curr_items = own
        end
    end
    local key = conf.storage == 'disk' and 'vinyl_memory' or 'memtx_memory'
    res.limit_maxbytes = box.cfg[key]
    for k, v in pairs(evict.stats(space_name)) do
        res[k] = v
    end
    res.evict = evict.enabled(space_name)
    return res
end

-- A page of "lru_crawler metadump|mgdump" (sub) of the items of a bucket,
-- for backend:dump() on the routers: the lines of the live items among the
-- next `limit` tuples of the bucket after the position `after` in the
-- bucket_id index, and the position to go on from (nil at the end).
function M.dump(space_name, bucket_id, sub, after, limit)
    if served(space_name) == nil then
        error(not_served(space_name), 0)
    end
    local space = schema.space(space_name)
    local index = space ~= nil and space.index.bucket_id or nil
    if index == nil then
        error(("space '%s' is not ready"):format(space_name), 0)
    end
    local readview = package.loaded['memcached.readview']
        or require('memcached.readview')
    local line = sub == 'mgdump' and readview.mgdump_line
        or readview.metadump_line
    local now = fiber.time()
    local flush = schema.flush_time(space_name)
    local is_dead = engine.is_dead
    local buf, n, scanned, last = {}, 0, 0, nil
    -- box.NULL is not a position.
    after = after ~= nil and after or nil
    for _, t in index:pairs({bucket_id}, {iterator = 'EQ', after = after}) do
        scanned = scanned + 1
        last = t
        if not is_dead(t, now, flush) then
            n = n + 1
            buf[n] = line(t)
        end
        if scanned >= limit then
            break
        end
    end
    return {
        lines = table.concat(buf, '', 1, n),
        after = scanned >= limit and index:tuple_pos(last) or nil,
    }
end

-- Applying the config ---------------------------------------------------

local function sweeper_name(space_name)
    return M.SWEEPER_PREFIX .. space_name
end

local function is_storage_sweeper(name)
    return name:sub(1, #M.SWEEPER_PREFIX) == M.SWEEPER_PREFIX
end

local function update_sweeper(space_name, conf, ro)
    local name = sweeper_name(space_name)
    local rec = state.sweepers[name]
    if conf.ready and conf.expire.enabled and not ro then
        if rec == nil or rec.batch ~= conf.expire.batch then
            expire.start(name, space_name, {batch = conf.expire.batch})
        end
    elseif rec ~= nil then
        expire.stop(name)
    end
end

-- The evictor evicts from a ready memory space with evict.enabled while
-- the instance is writable, as the sweepers run.
local function update_evictor(space_name, conf, ro)
    local owner = sweeper_name(space_name)
    local e = conf.evict
    if conf.ready and conf.storage == 'memory' and e.enabled and not ro then
        evict.start(owner, space_name, {
            high = e.high,
            low = e.low,
            samples = e.samples,
            batch = e.batch,
            track_access = conf.track_access,
        })
    else
        evict.stop(owner)
    end
end

-- The evictor owners of this storage's spaces: a set.
local function storage_owners()
    local res = {}
    local s = state.evict
    for owner in pairs(s ~= nil and s.owners or {}) do
        if is_storage_sweeper(owner) then
            res[owner] = true
        end
    end
    return res
end

local function apply_once(s)
    local errors = {}
    for name, conf in pairs(s.spaces) do
        -- Creates the space and its bucket_id index on a writable
        -- instance; on a read-only one only checks what has arrived.
        local ok, space = pcall(
            schema.ensure,
            name,
            conf.storage,
            {vshard = true, expire = conf.expire.mode}
        )
        if ok then
            conf.ready = space ~= nil
        else
            conf.ready = false
            table.insert(errors, tostring(space))
        end
        -- The role may change while ensure() yields: look again.
        local ro = box.info.ro
        update_sweeper(name, conf, ro)
        update_evictor(name, conf, ro)
    end
    for name, rec in pairs(state.sweepers) do
        if is_storage_sweeper(name) and s.spaces[rec.space_name] == nil then
            expire.stop(name)
        end
    end
    for owner in pairs(storage_owners()) do
        if s.spaces[owner:sub(#M.SWEEPER_PREFIX + 1)] == nil then
            evict.stop(owner)
        end
    end
    if #errors > 0 then
        error(table.concat(errors, '; '), 0)
    end
end

-- Bring the spaces and the sweepers in line with the config and the
-- instance's role. One at a time: a call made while another one runs makes
-- that one run once more and waits for it. Returns true or false and the
-- error.
function M.apply()
    local s = storage_state()
    if s.applying then
        s.again = true
        while s.applying do
            s.cond:wait()
        end
        return s.last_error == nil, s.last_error
    end
    s.applying = true
    local err
    repeat
        s.again = false
        local ok, res = pcall(apply_once, s)
        err = not ok and res or nil
    until not s.again
    s.last_error = err
    s.applying = false
    s.cond:broadcast()
    if err ~= nil then
        log.error('memcached.storage: %s', tostring(err))
    end
    return err == nil, err
end

local function on_box_status()
    local ok, err = pcall(function()
        current().apply()
    end)
    if not ok then
        log.error('memcached.storage: %s', tostring(err))
    end
end

-- Public API ------------------------------------------------------------

function M.cfg(cfg)
    if type(box.cfg) == 'function' then
        error('Please call box.cfg{} first', 0)
    end
    local spaces = check_cfg(cfg)
    local s = storage_state()
    -- Keep the readiness and the counters of the spaces that stay.
    for name, conf in pairs(spaces) do
        local old = s.spaces[name]
        if old ~= nil then
            conf.ready = old.ready
            conf.batches = old.batches or 0
            conf.requests = old.requests or 0
        else
            conf.ready = false
        end
    end
    s.spaces = spaces
    export()
    if s.watcher == nil then
        -- The callback also runs once right after the registration.
        s.watcher = box.watch('box.status', on_box_status)
    end
    local ok, err = current().apply()
    if not ok then
        error(err, 0)
    end
end

function M.info()
    local res = {ro = box.info.ro, spaces = {}}
    local s = state.storage
    if s == nil then
        return res
    end
    local owners = storage_owners()
    for name, conf in pairs(s.spaces) do
        local rec = state.sweepers[sweeper_name(name)]
        local info = {
            storage = conf.storage,
            track_access = conf.track_access,
            commit_wait = conf.commit_wait,
            expire_mode = conf.expire.mode,
            ready = schema.space(name) ~= nil,
            sweeper = rec ~= nil,
            expired = rec and rec.expired or 0,
            flushed = rec and rec.flushed or 0,
            checked = rec and rec.checked or 0,
            evict = conf.evict,
            evictor = owners[sweeper_name(name)] == true,
            batches = conf.batches,
            requests = conf.requests,
        }
        for k, v in pairs(evict.stats(name)) do
            info[k] = v
        end
        res.spaces[name] = info
    end
    return res
end

-- Stop serving: drop the globals, the watcher and the sweepers. The data is
-- kept.
function M.stop()
    local s = state.storage
    if s == nil then
        return
    end
    if s.watcher ~= nil then
        pcall(s.watcher.unregister, s.watcher)
        s.watcher = nil
    end
    s.spaces = {}
    if s.applying then
        s.again = true
    end
    for name in pairs(state.sweepers) do
        if is_storage_sweeper(name) then
            expire.stop(name)
        end
    end
    for owner in pairs(storage_owners()) do
        evict.stop(owner)
    end
    unexport()
end

return M
