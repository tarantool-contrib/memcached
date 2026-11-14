-- Read views: the stale-read mode (read_mode = 'snapshot') and the dumps
-- (lru_crawler metadump / mgdump).
--
-- Snapshot mode. TX keeps a read view per instance and refreshes it every
-- snapshot_refresh seconds (M.start, M.refresh); each new one is published
-- by id to the application threads that run the front-ends of the instance
-- (M.publish). A backend wrapped by M.wrap serves the plain gets (no touch,
-- vivify or recache) from the read view of its own thread, without side
-- effects: no last access or fetched bump, no lazy deletion. Everything else
-- goes to the wrapped backend, and so do the hits on stale items whose win
-- token is not out (taking it is a write). The reads are stale: a write is
-- visible to them after the next refresh, so read-your-writes is not
-- guaranteed. A flush_all through the wrapper drops the read view of its
-- thread at once and makes TX refresh; the thread then ignores the read
-- views TX had opened before the flush (rv_floor, see M.invalidate). Other
-- threads see the flush at the next publication.
--
-- Dumps. M.dump serves "lru_crawler metadump|mgdump" from a fresh read view:
-- TX only opens it, the iteration runs in the thread of the connection.
--
-- Only box.read_view here: the module runs in application threads too, and
-- box.read_view.open({name = ...}) is called in TX only.
--
-- Records (state.readviews, per Lua state, so per thread):
--   name -> {name, rv, id, cache}; in TX also opened, interval, cond, fiber,
--   stopped. In a thread, rv = nil with an id is a floor after a flush.

local ffi = require('ffi')
local fiber = require('fiber')
local log = require('log')
local threads = require('threads')

local schema = require('memcached.schema')
local state = require('memcached.state')
local text = require('memcached.proto.text')
local meta = require('memcached.proto.meta')

local M = {}

local SELF = 'memcached.readview'
local THREADS = 'memcached.threads'

-- Read view names: <prefix><instance name>.
M.PREFIX = 'mcc/'
M.DUMP_PREFIX = 'mcc/dump/'
-- TX closes a dump read view that a thread didn't release in this time.
M.DUMP_TIMEOUT = 60
-- Items per write (and per yield) of a dump.
M.DUMP_BATCH = 512
-- Sleep of a refresher after an error.
M.ERROR_SLEEP = 1

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

local concat = table.concat
local find = string.find
local floor = math.floor
local format = string.format
local gsub = string.gsub

local uint64_t = ffi.typeof('uint64_t')
local u64str = text.u64str

local function current()
    return package.loaded[SELF] or M
end

local function registry(key)
    local r = state[key]
    if r == nil then
        r = {}
        state[key] = r
    end
    return r
end

local function views()
    return registry('readviews')
end

local function error_message(err)
    if type(err) == 'cdata' or type(err) == 'table' then
        err = err.message or tostring(err)
    end
    return (tostring(err):gsub('[\r\n]+', ' '))
end

local function close_rv(rv)
    if rv ~= nil then
        pcall(rv.close, rv)
    end
end

-- Items ------------------------------------------------------------------

-- Same as engine.is_dead (engine.lua is TX-only).
local function is_dead(t, now, flush)
    local exptime = t[EXPTIME]
    if exptime ~= 0 and exptime <= now then
        return true
    end
    return flush ~= nil and flush <= now and t[CREATED] <= flush
end

local function item_of(t)
    return {
        key = t[KEY],
        value = t[VALUE],
        flags = t[FLAGS],
        exptime = t[EXPTIME],
        cas = ffi.cast(uint64_t, t[CAS]),
        created = t[CREATED],
        last_access = t[LAST_ACCESS],
        fetched = t[FETCHED],
        stale = t[STALE],
    }
end

-- The flush time of a space as of the read view.
local function rv_flush_time(rv, space_name)
    local space = rv.space[schema.META_SPACE]
    local index = space ~= nil and space.index.primary or nil
    if index == nil then
        return nil
    end
    local t = index:get(schema.flush_key(space_name))
    return t ~= nil and t[2] or nil
end

-- The read view of a space in this thread: {index, flush}, or nil.
function M.view(name, space_name)
    local rec = views()[name]
    local rv = rec ~= nil and rec.rv or nil
    if rv == nil then
        return nil
    end
    local cache = rec.cache
    if cache == nil or cache.space_name ~= space_name then
        local space = rv.space[space_name]
        local index = space ~= nil and space.index.primary or nil
        if index == nil then
            return nil
        end
        cache = {
            space_name = space_name,
            index = index,
            flush = rv_flush_time(rv, space_name),
        }
        rec.cache = cache
    end
    return cache
end

-- Whether a request can be served from a read view: a get without side
-- effects other than the last access bump (which is skipped).
function M.plain_get(req)
    return req.op == 'get' and req.touch_ttl == nil
        and req.vivify_ttl == nil and req.recache_ttl == nil
end

-- The response to a plain get from a read view; nil if TX must serve it.
-- win_token: the request's (meta mg), which reports a win token that is
-- out, as of the read view.
function M.lookup(view, key, now, win_token)
    local t = view.index:get(key)
    if t == nil or is_dead(t, now, view.flush) then
        return {status = 'MISS'}
    end
    local sent = t[TOKEN] == true
    if t[STALE] and not sent then
        -- A hit on a stale item takes its win token, a write: TX serves it.
        return nil
    end
    return {
        status = 'HIT',
        item = item_of(t),
        already_won = (win_token and sent) or nil,
    }
end

-- Publishing ---------------------------------------------------------------

-- The id of the read view of an instance in this thread, or nil.
function M.current_id(name)
    local rec = views()[name]
    if rec == nil or rec.rv == nil then
        return nil
    end
    return rec.id
end

-- Use the read view with this id in this thread (an application thread).
-- Ids grow: an id not above the current one (or the floor set by a flush,
-- see M.invalidate) is ignored, also when it was being opened while a newer
-- one came. Opening yields, so the instance may stop meanwhile: the caller
-- checks that (threads.lua).
function M.publish(name, id)
    local recs = views()
    local rec = recs[name]
    if rec ~= nil and rec.id ~= nil and rec.id >= id then
        return rec.rv ~= nil and rec.id == id
    end
    local ok, rv = pcall(box.read_view.open, {id = id})
    if not ok then
        log.warn(
            "memcached: instance '%s': can't open read view %s: %s",
            name,
            tostring(id),
            error_message(rv)
        )
        return false
    end
    rec = recs[name]
    if rec ~= nil and rec.id ~= nil and rec.id >= id then
        close_rv(rv)
        return false
    end
    local old = rec ~= nil and rec.rv or nil
    if rec ~= nil then
        rec.rv, rec.id, rec.cache = rv, id, nil
    else
        recs[name] = {name = name, rv = rv, id = id}
    end
    close_rv(old)
    return true
end

-- Stop using the current read view of an instance in this thread until a
-- newer one is published (after a flush); in TX, refresh now.
-- floor (in a thread, after a flush done by TX) is the id of TX's read view
-- at the time of the flush: the read views up to it may predate the flush,
-- so they are not used; a newer current one is kept.
function M.invalidate(name, floor)
    local recs = views()
    local rec = recs[name]
    if floor ~= nil then
        if rec == nil then
            recs[name] = {name = name, id = floor}
            return
        end
        if rec.rv ~= nil and rec.id ~= nil and rec.id > floor then
            return
        end
        if rec.id == nil or rec.id < floor then
            rec.id = floor
        end
    elseif rec == nil then
        return
    end
    local rv = rec.rv
    rec.rv, rec.cache = nil, nil
    close_rv(rv)
    if rec.cond ~= nil then
        rec.cond:signal()
    end
end

-- Forget the read view of an instance in this thread; in TX, also stop the
-- refresher.
function M.stop(name)
    local recs = views()
    local rec = recs[name]
    if rec == nil then
        return
    end
    recs[name] = nil
    rec.stopped = true
    local f = rec.fiber
    if f ~= nil and f:status() ~= 'dead' and f:id() ~= fiber.id() then
        pcall(f.cancel, f)
    end
    local rv = rec.rv
    rec.rv, rec.cache = nil, nil
    close_rv(rv)
end
M.drop = M.stop

-- Refreshers (TX) ----------------------------------------------------------

local function threads_module()
    return package.loaded[THREADS] or require(THREADS)
end

-- Open a new read view for an instance, publish it, close the old one.
function M.refresh(rec)
    local rv = box.read_view.open({name = M.PREFIX .. rec.name})
    if rec.stopped then
        close_rv(rv)
        return
    end
    local old = rec.rv
    rec.rv, rec.id, rec.cache, rec.opened = rv, rv.id, nil, fiber.time()
    -- The threads have their own references to the old one.
    close_rv(old)
    local inst = state.instances[rec.name]
    local group = inst ~= nil and inst.fe_group or nil
    if group ~= nil then
        local ok, err = pcall(
            threads_module().eval,
            group,
            'rv_publish',
            {name = rec.name, id = rv.id}
        )
        if not ok and not rec.stopped then
            log.warn(
                "memcached: instance '%s': publishing read view %s: %s",
                rec.name,
                tostring(rv.id),
                error_message(err)
            )
        end
    end
end

-- Refresh when the read view is snapshot_refresh old (at once when there
-- is none), but not sooner than ERROR_SLEEP after a failure. A read view
-- kept by a restart (hot reload) is refreshed on time.
local function refresher_main(rec)
    local retry_at = 0
    while not rec.stopped do
        local mod = current()
        local due = rec.rv ~= nil and (rec.opened or 0) + rec.interval or 0
        if due < retry_at then
            due = retry_at
        end
        local timeout = due - fiber.time()
        if timeout <= 0 then
            local ok, err = pcall(mod.refresh, rec)
            if rec.stopped then
                break
            end
            if ok then
                retry_at = 0
                timeout = rec.interval
            else
                log.error(
                    "memcached: instance '%s': read view refresh: %s",
                    rec.name,
                    error_message(err)
                )
                timeout = mod.ERROR_SLEEP
                retry_at = fiber.time() + timeout
            end
        end
        rec.cond:wait(timeout)
    end
end

local function spawn(rec)
    rec.fiber = fiber.new(function()
        pcall(refresher_main, rec)
    end)
    rec.fiber:name('mcc/rv/' .. rec.name, {truncate = true})
end

-- A refresher record; the read view of an old one is kept.
local function new_refresher(name, interval, old)
    return {
        name = name,
        rv = old ~= nil and old.rv or nil,
        id = old ~= nil and old.id or nil,
        opened = old ~= nil and old.opened or nil,
        cache = nil,
        interval = interval,
        cond = fiber.cond(),
        stopped = false,
        fiber = nil,
    }
end

-- Start the refresher of an instance in snapshot mode (TX), or update its
-- interval. The first read view is opened in the background: until then,
-- the reads go to the engine.
function M.start(inst)
    local recs = views()
    local name = inst.name
    local interval = inst.opts.snapshot_refresh
    local rec = recs[name]
    if rec ~= nil and rec.fiber ~= nil
        and not rec.stopped and rec.fiber:status() ~= 'dead' then
        if rec.interval ~= interval then
            rec.interval = interval
            rec.cond:signal()
        end
        return rec
    end
    rec = new_refresher(name, interval, rec)
    recs[name] = rec
    spawn(rec)
    return rec
end

-- Restart the refreshers with the current code, keeping their read views
-- (hot reload).
function M.restart_all()
    local recs = views()
    local list = {}
    for name, rec in pairs(recs) do
        if rec.fiber ~= nil then
            list[name] = rec
        end
    end
    local n = 0
    for name, old in pairs(list) do
        old.stopped = true
        local f = old.fiber
        if f:status() ~= 'dead' and f:id() ~= fiber.id() then
            pcall(f.cancel, f)
        end
        local rec = new_refresher(name, old.interval, old)
        recs[name] = rec
        spawn(rec)
        n = n + 1
    end
    return n
end

-- Options ---------------------------------------------------------------

-- Raise if the snapshot mode can't be used with these options.
function M.validate(cfg)
    if cfg.read_mode ~= 'snapshot' then
        return
    end
    if cfg.backend ~= nil and cfg.backend ~= 'local' then
        error(
            ("read_mode 'snapshot' is not supported by the %s backend"):format(
                cfg.backend
            ),
            0
        )
    end
    if cfg.storage == 'disk' then
        error(
            "read_mode 'snapshot' needs storage 'memory': read views are memtx-only",
            0
        )
    end
end

-- The wrapper backend ----------------------------------------------------

local W = {}
M.wrapper = W

local wrapper_mt = {
    __index = function(_, k)
        return current().wrapper[k]
    end,
}

-- A backend that serves the plain gets from the read view of the instance
-- in this thread and passes everything else to inner.
function M.wrap(inner, cfg)
    return setmetatable({
        inner = inner,
        name = cfg.name,
        space_name = cfg.space_name,
        -- exec() passes reqs to inner, and keeps no reference to it itself
        -- (server.lua "The per-batch tables").
        borrows_reqs = inner.borrows_reqs == true,
    }, wrapper_mt)
end

function W.exec(self, reqs)
    local mod = current()
    local view = mod.view(self.name, self.space_name)
    if view == nil then
        return self.inner:exec(reqs)
    end
    local plain_get, lookup = mod.plain_get, mod.lookup
    local now = fiber.time()
    local resps = {}
    local rest, where
    for i = 1, #reqs do
        local req = reqs[i]
        local resp
        if plain_get(req) then
            resp = lookup(view, req.key, now, req.win_token)
        end
        if resp ~= nil then
            resps[i] = resp
        else
            if rest == nil then
                rest, where = {}, {}
            end
            rest[#rest + 1] = req
            where[#where + 1] = i
        end
    end
    if rest ~= nil then
        local res = self.inner:exec(rest)
        for k = 1, #rest do
            resps[where[k]] = res[k]
        end
    end
    return resps
end

local function in_tx()
    return threads.info().group_name == 'tx'
end

-- In TX, the response carries rv_floor, the id of the read view that was
-- current when the flush was done: the wrappers in the threads don't use it
-- or older ones (see M.invalidate).
function W.flush_all(self, delay)
    local mod = current()
    local resp = self.inner:flush_all(delay)
    if in_tx() then
        local rec = views()[self.name]
        local floor = rec ~= nil and rec.id or nil
        mod.invalidate(self.name)
        if floor ~= nil and type(resp) == 'table' then
            resp.rv_floor = floor
        end
    else
        mod.invalidate(
            self.name,
            type(resp) == 'table' and resp.rv_floor or nil
        )
    end
    return resp
end

function W.stats(self)
    return self.inner:stats()
end

function W.conns(self)
    local inner = self.inner
    if inner.conns ~= nil then
        return inner:conns()
    end
    return nil
end

function W.reset_stats(self)
    local inner = self.inner
    if inner.reset_stats ~= nil then
        inner:reset_stats()
    end
end

function W.close(self)
    local inner = self.inner
    if inner.close ~= nil then
        inner:close()
    end
end

-- Dumps ------------------------------------------------------------------

-- Dump read views opened by TX for the threads: id -> {rv, fiber}.
local function dumps()
    return registry('dump_readviews')
end

-- TX: open a read view for a dump in an application thread; the thread
-- releases it with tx_close() once it has its own reference.
function M.tx_open(name)
    local rv = box.read_view.open({name = M.DUMP_PREFIX .. tostring(name)})
    local id = rv.id
    local rec = {rv = rv}
    local timeout = current().DUMP_TIMEOUT
    rec.fiber = fiber.new(function()
        if pcall(fiber.sleep, timeout) then
            rec.fiber = nil
            current().tx_close(id)
        end
    end)
    rec.fiber:name('mcc/rv-dump/' .. tostring(id), {truncate = true})
    dumps()[id] = rec
    return id
end

function M.tx_close(id)
    local list = dumps()
    local rec = list[id]
    if rec == nil then
        return false
    end
    list[id] = nil
    local f = rec.fiber
    rec.fiber = nil
    if f ~= nil and f:status() ~= 'dead' and f:id() ~= fiber.id() then
        pcall(f.cancel, f)
    end
    close_rv(rec.rv)
    return true
end

-- A fresh read view in this thread.
local function open_fresh(name)
    if in_tx() then
        return box.read_view.open({name = M.DUMP_PREFIX .. tostring(name)})
    end
    local id = threads.call('tx', 'mcc.rv_open', {name})[1][1]
    local ok, rv = pcall(box.read_view.open, {id = id})
    pcall(threads.call, 'tx', 'mcc.rv_close', {id})
    if not ok then
        error(rv, 0)
    end
    return rv
end

-- memcached's uriencode(): all but [0-9A-Za-z._~-] as %XX.
local function uriencode(s)
    return (gsub(s, '[^0-9A-Za-z_~%.%-]', function(c)
        return format('%%%02X', c:byte())
    end))
end
M.uriencode = uriencode

local function abs_time(t)
    return format('%d', floor(tonumber(t) or 0))
end

-- memcached 1.6 crawler_metadump_eval(), including the trailing space.
function M.metadump_line(t)
    local key, exptime = t[KEY], t[EXPTIME]
    return 'key=' .. uriencode(key) .. ' exp='
        .. (exptime == 0 and '-1' or abs_time(exptime)) .. ' la='
        .. abs_time(t[LAST_ACCESS]) .. ' cas=' .. u64str(t[CAS]) .. ' fetch='
        .. (t[FETCHED] and 'yes' or 'no') .. ' cls=1 size='
        .. (#key + #t[VALUE]) .. ' flags=' .. u64str(t[FLAGS]) .. ' \n'
end

-- memcached 1.6 crawler_mgdump_eval(): keys that the text protocol can't
-- carry (set with meta 'b') are in base64.
function M.mgdump_line(t)
    local key = t[KEY]
    if find(key, '[%z\1-\32\127]') then
        return 'mg ' .. meta.b64encode(key) .. ' b\r\n'
    end
    return 'mg ' .. key .. '\r\n'
end

-- Write the dump of a space from a read view. Returns false when a write
-- fails.
function M.dump_view(rv, space_name, req, write)
    local space = rv.space[space_name]
    local index = space ~= nil and space.index.primary or nil
    if index == nil then
        return write(
            ("SERVER_ERROR no read view of space '%s': dumps "
                .. "need storage 'memory'\r\n"):format(space_name)
        )
    end
    if not write('OK\r\n') then
        return false
    end
    local mgdump = req.sub == 'mgdump'
    if req.items ~= false then
        local mod = current()
        local line = mgdump and mod.mgdump_line or mod.metadump_line
        local batch = mod.DUMP_BATCH
        local now = fiber.time()
        local flush = rv_flush_time(rv, space_name)
        local buf, n = {}, 0
        for _, t in index:pairs() do
            if not is_dead(t, now, flush) then
                n = n + 1
                buf[n] = line(t)
                if n >= batch then
                    if not write(concat(buf, '', 1, n)) then
                        return false
                    end
                    n = 0
                    fiber.yield()
                end
            end
        end
        if n > 0 and not write(concat(buf, '', 1, n)) then
            return false
        end
    end
    return write(mgdump and 'EN\r\n' or 'END\r\n')
end

-- The data space of a backend object, if it has local data (a REMOTE
-- backend, vshard, keeps it on the storages).
local function backend_space(backend)
    if type(backend) ~= 'table' or backend.REMOTE then
        return nil
    end
    local name = backend.space_name
    if name == nil and type(backend.ctx) == 'table' then
        name = backend.ctx.space_name
    end
    return name
end

-- Serve "lru_crawler metadump|mgdump" on a server: write(data) sends data
-- to the client and returns false on errors. A backend may provide its own
-- backend:dump(req, write).
function M.dump(srv, req, write)
    local backend = srv.backend
    if type(backend) == 'table' and backend.dump ~= nil then
        return backend:dump(req, write)
    end
    local space_name = backend_space(backend)
    if space_name == nil then
        return write(
            'SERVER_ERROR dumps are not supported by this backend\r\n'
        )
    end
    local ok, rv = pcall(open_fresh, srv.name)
    if not ok then
        return write('SERVER_ERROR ' .. error_message(rv) .. '\r\n')
    end
    local res
    ok, res = pcall(current().dump_view, rv, space_name, req, write)
    close_rv(rv)
    if not ok then
        error(res, 0)
    end
    return res
end

return M
