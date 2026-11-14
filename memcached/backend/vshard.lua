-- The front-end on a vshard router: sends the engine requests to the vshard
-- storages, where memcached/storage.lua serves them.
--
-- No box.space, box.schema or other TX-only box APIs here: the module may run
-- in an application thread, given a vshard router that works there.
--
-- Backend contract (see DESIGN.md):
--   backend:exec(reqs) -> resps
--   backend:flush_all(delay) -> resp
--   backend:stats() -> table
--   backend:dump(req, write)  lru_crawler metadump|mgdump, bucket by bucket
--   backend:reset_stats()     in an application thread: every thread
--   backend:conns()           in an application thread: the connections of
--                             every thread ("stats conns"); nil in TX
-- plus backend:close(), called when the instance stops using the backend.
-- For threads.lua: thread_cfg(cfg, opts) and thread_cfg_ref(opts) in TX,
-- thread_prepare(cfg) and thread_router(name, cfg) in the threads.
--
-- Options (from the instance options):
--   space_name  the sharded space on the storages;
--   read_mode   'rw': everything goes to the masters (callrw);
--               'ro' / 'bro': gets without side effects (no touch, vivify
--               or recache) go through callro / callbro;
--   vshard = {
--       router  = a vshard router object, or the name of a router made by
--                 vshard.router.new(); default vshard.router.static;
--       timeout = seconds per batch, default DEFAULT_TIMEOUT,
--   }
--
-- exec() groups a batch by bucket. Each group is one call of
-- mcc.storage_exec(space_name, bucket_id, reqs) through
-- vshard.router.callrw/callro/callbro, so it runs on the storage under a ref
-- of its bucket. The groups go out at once with is_async, and the responses
-- are put back in the request order. Requests on one key are in one group,
-- in their order. A group with a write goes to the master as a whole.
--
-- An async call doesn't follow a moved bucket. So a group whose function
-- did not run - the request was not sent, or the storage refused it before
-- running it (bucket moved, locked or in transfer, not a master) - is sent
-- again synchronously: vshard then follows the bucket until the deadline.
-- A refusal comes back from the future as a "result" (see unwrap()). A
-- timeout or any other error becomes SERVER_ERROR for every request of the
-- group: its writes may or may not have been applied.
--
-- In application threads (the instance option threads, see threads.lua)
-- each thread has its own router, which mirrors the router of the instance
-- in TX: the static router or a router with the same name. TX sends the
-- router's config as data (thread_cfg()); the thread makes the router or
-- reconfigures it (thread_prepare()). The front-ends in the threads then
-- call the storages directly; TX is not in the data path. Such a backend
-- (fe_thread) gets the counters summed over the threads from TX for
-- "stats".

local ffi = require('ffi')
local fiber = require('fiber')
local log = require('log')
local threads = require('threads')

local state = require('memcached.state')

local M = {}

local SELF = 'memcached.backend.vshard'

-- The name of vshard's static router (vshard.consts.STATIC_ROUTER_NAME).
M.STATIC_ROUTER = '_static_router'

-- The data lives on the storages: the instance neither creates nor waits
-- for a local space and runs no sweeper (see init.lua).
M.REMOTE = true

M.DEFAULT_TIMEOUT = 1

M.EXEC_FUNC = 'mcc.storage_exec'
M.FLUSH_FUNC = 'mcc.storage_flush'
M.STATS_FUNC = 'mcc.storage_stats'
M.DUMP_FUNC = 'mcc.storage_dump'

-- Dumps: buckets dumped at once, and tuples per call.
M.DUMP_WORKERS = 8
M.DUMP_PAGE = 512

-- read_mode -> the router call for reads.
local READ_CALLS = {rw = 'callrw', ro = 'callro', bro = 'callbro'}

-- Errors after which the function has not run: the group can be resent.
local RESEND = {
    WRONG_BUCKET = true,
    NON_MASTER = true,
    MISSING_MASTER = true,
    TRANSFER_IS_IN_PROGRESS = true,
    BUCKET_IS_LOCKED = true,
}

-- Request fields only the protocol layer needs: not sent to the storages.
local PROTO_ONLY = {
    meta = true,
    text = true,
    cmd = true,
    noreply = true,
    with_cas = true,
    touch_only = true,
    too_large = true,
}

local TIMEOUT = 'Timeout exceeded'

local uint64_t = ffi.typeof('uint64_t')

-- Methods are looked up in the current module on every call, so existing
-- backend objects use the new code after a reload.
local mt = {
    __index = function(_, k)
        return (package.loaded[SELF] or M)[k]
    end,
}

local function error_message(err)
    if type(err) == 'cdata' or type(err) == 'table' then
        err = err.message or tostring(err)
    end
    return (tostring(err):gsub('[\r\n]+', ' '))
end

local function server_error(err)
    return {status = 'SERVER_ERROR', message = error_message(err)}
end

local function in_app_thread()
    return threads.info().group_name ~= 'tx'
end

-- vshard.util asserts when it is loaded that it can read the schema
-- version: box.info.schema_version, or box.internal.schema_version on old
-- Tarantool versions. The box.info of an application thread is a stub
-- without it. Only vshard.storage uses the schema version, so a router in a
-- thread works with a stub.
local function thread_shim()
    if in_app_thread() and box.info.schema_version == nil
        and box.internal ~= nil and box.internal.schema_version == nil then
        box.internal.schema_version = function() return 0 end
    end
end

-- The vshard versions the backend supports: 1.0.0 and newer 1.x. It
-- relies on vshard 1.0.0 internals (router.current_cfg,
-- vshard.router.internal.routers, the load-time check of vshard.util the
-- thread shim answers, the async future that unwrap() works around; see
-- DESIGN.md "vshard"). Nothing is known to break outside the range, so
-- another version only gets a warning; a missing API fails with its own
-- error.
M.VSHARD_MIN = {1, 0, 0}
M.VSHARD_BELOW = {2, 0, 0}
M.VSHARD_RANGE = '>= 1.0.0, < 2.0.0'

local function version_less(a, b)
    for i = 1, 3 do
        if a[i] ~= b[i] then
            return a[i] < b[i]
        end
    end
    return false
end

-- The warning for a vshard version (a string like '1.0.0', or nil when
-- vshard doesn't say), or nil when it is supported.
function M.version_warning(version, name)
    name = name or 'vshard'
    local major, minor, patch = tostring(version or ''):match(
        '^(%d+)%.(%d+)%.(%d+)'
    )
    local v = {tonumber(major), tonumber(minor), tonumber(patch)}
    if v[1] ~= nil and not version_less(v, M.VSHARD_MIN)
        and version_less(v, M.VSHARD_BELOW) then
        return nil
    end
    return ('memcached: %s %s is not a supported version (%s, tested '
        .. 'with 1.0.0): the vshard backend relies on vshard internals '
        .. 'and may not work'):format(
            name,
            version ~= nil and tostring(version) or '(unknown version)',
            M.VSHARD_RANGE
        )
end

-- Warn once (per thread) when the loaded vshard is outside the supported
-- range. prefix is 'vshard' or 'vshard-ee'; the version is in its consts
-- module (VERSION), or in the package (_VERSION).
local function check_version(prefix)
    if state.vshard_version_checked then
        return
    end
    state.vshard_version_checked = true
    local ok, consts = pcall(require, prefix .. '.consts')
    local version = ok and type(consts) == 'table' and consts.VERSION or nil
    if version == nil then
        local pkg = package.loaded[prefix]
        version = type(pkg) == 'table' and pkg._VERSION or nil
    end
    local warning = M.version_warning(version, prefix)
    if warning ~= nil then
        log.warn(warning)
    end
end

-- The vshard router module, or nil and an error.
local function router_module()
    for _, prefix in ipairs({'vshard', 'vshard-ee'}) do
        local mod = package.loaded[prefix .. '.router']
        if mod ~= nil then
            check_version(prefix)
            return mod
        end
    end
    thread_shim()
    local ok, res = pcall(require, 'vshard.router')
    if ok then
        check_version('vshard')
        return res
    end
    local ok_ee, res_ee = pcall(require, 'vshard-ee.router')
    if ok_ee then
        check_version('vshard-ee')
        return res_ee
    end
    return nil, res
end

-- Raise if the options can't be served by this backend.
function M.validate(cfg)
    local mode = cfg.read_mode or 'rw'
    if READ_CALLS[mode] == nil then
        error(
            ("read_mode '%s' is not supported by the vshard backend"):format(
                tostring(mode)
            ),
            0
        )
    end
    local mod, err = router_module()
    if mod == nil then
        error(('vshard is not available: %s'):format(error_message(err)), 0)
    end
    local router = (cfg.vshard or {}).router
    if router ~= nil and type(router) ~= 'string'
        and (type(router) ~= 'table' or type(router.callrw) ~= 'function') then
        error(
            "Option 'vshard.router' must be a vshard router or the name of one",
            0
        )
    end
end

-- cfg: the instance options plus name.
function M.new(cfg)
    M.validate(cfg)
    local opts = cfg.vshard or {}
    return setmetatable({
        name = cfg.name,
        space_name = cfg.space_name,
        read_call = READ_CALLS[cfg.read_mode or 'rw'],
        -- A router object, a router name or nil (the static router). Names
        -- are resolved on every batch: the router may appear later.
        router_ref = opts.router,
        timeout = opts.timeout or M.DEFAULT_TIMEOUT,
        -- Groups sent again after a moved bucket or a missing connection.
        resends = 0,
        -- Serves a front-end in an application thread (threads.lua).
        fe_thread = cfg.fe_thread == true,
    }, mt)
end

-- Thread side: name (STATIC_ROUTER for the static router) -> {router, cfg =
-- <the config applied last>, busy, cond}, the routers made by
-- thread_router() in this thread. Empty in TX.
local function thread_routers()
    local list = state.vshard_thread_routers
    if list == nil then
        list = {}
        state.vshard_thread_routers = list
    end
    return list
end

-- The router by a reference: a router object, a name, or nil for the
-- static router. Returns the router or nil and an error. A name is looked
-- up among the routers made in this thread, then in vshard's internal
-- table of routers (vshard has no public lookup by name).
local function resolve(ref)
    if type(ref) == 'table' then
        return ref
    end
    local mod, err = router_module()
    if mod == nil then
        return nil, err
    end
    if ref == nil or ref == M.STATIC_ROUTER then
        local router = mod.static
        if router == nil then
            return nil, 'vshard router is not configured'
        end
        return router
    end
    local rec = thread_routers()[ref]
    local router = rec ~= nil and rec.router or nil
    if router == nil and mod.internal ~= nil and mod.internal.routers ~= nil then
        router = mod.internal.routers[ref]
    end
    if router == nil then
        return nil, ("vshard router '%s' does not exist"):format(ref)
    end
    return router
end

-- The router to use, or nil and an error.
function M.router(self)
    return resolve(self.router_ref)
end

-- Routers in application threads --------------------------------------------

-- TX: the name of the router of the options for the threads: nil for the
-- static router.
local function router_name(ref)
    if type(ref) == 'table' then
        ref = ref.name
    end
    if ref == M.STATIC_ROUTER then
        return nil
    end
    return ref
end

-- TX: the config of the router of the instance options as data (a deep
-- copy made by vshard, with the defaults), or nil if there is no such
-- router yet. A new table after every reconfiguration of the router.
function M.thread_cfg_ref(opts)
    local router = resolve((opts.vshard or {}).router)
    return router ~= nil and router.current_cfg or nil
end

-- TX: the options of the front-ends in the threads (threads.thread_cfg())
-- from cfg, a plain copy of the instance options, and opts, the instance
-- options. A router object can't cross threads: the threads get its name
-- and its config (router_cfg).
function M.thread_cfg(cfg, opts)
    local vopts = opts.vshard or {}
    cfg.vshard = {router = router_name(vopts.router), timeout = vopts.timeout}
    cfg.router_cfg = M.thread_cfg_ref(opts)
    cfg.fe_thread = true
    return cfg
end

-- Deep equality of two configs (box.NULL is nil).
local function same(a, b)
    if type(a) ~= 'table' or type(b) ~= 'table' then
        return a == b
    end
    for k, v in pairs(a) do
        if not same(v, b[k]) then
            return false
        end
    end
    for k in pairs(b) do
        if type(a[k]) == 'nil' then
            return false
        end
    end
    return true
end
M.same_cfg = same

local function deep_copy(t)
    local res = {}
    for k, v in pairs(t) do
        res[k] = type(v) == 'table' and deep_copy(v) or v
    end
    return res
end

local function apply_router_cfg(mod, name, router, cfg)
    cfg = deep_copy(cfg)
    -- A thread has no box.cfg.
    cfg.box_cfg_mode = 'manual'
    if router ~= nil then
        router:cfg(cfg)
        return router
    end
    if name == nil then
        local _, err = mod.cfg(cfg)
        if err ~= nil then
            error(err, 0)
        end
        return mod.static
    end
    local res, err = mod.new(name, cfg)
    if res == nil then
        error(err, 0)
    end
    return res
end

-- Thread side: make the router of this thread with the name (nil: the
-- static router) and the config, or reconfigure it if the config differs
-- from the one applied last. One configuration of a router at a time.
-- Returns the router; raises on errors.
function M.thread_router(name, cfg)
    local mod, err = router_module()
    if mod == nil then
        error(('vshard is not available: %s'):format(error_message(err)), 0)
    end
    local list = thread_routers()
    local key = name or M.STATIC_ROUTER
    local rec = list[key]
    if rec == nil then
        rec = {router = nil, cfg = nil, busy = false, cond = fiber.cond()}
        list[key] = rec
    end
    while rec.busy do
        rec.cond:wait()
    end
    local router = rec.router or resolve(name)
    if router ~= nil and rec.cfg ~= nil and same(rec.cfg, cfg) then
        return router
    end
    rec.busy = true
    local ok, res = pcall(apply_router_cfg, mod, name, router, cfg)
    rec.busy = false
    rec.cond:broadcast()
    if not ok then
        error(res, 0)
    end
    rec.router, rec.cfg = res, cfg
    return res
end

-- Thread side, before a backend is made from a thread_cfg() or the config
-- of its router may have changed.
function M.thread_prepare(cfg)
    if cfg.router_cfg ~= nil then
        M.thread_router((cfg.vshard or {}).router, cfg.router_cfg)
    end
end

-- The routers made in this thread: name -> the config applied last.
function M.thread_routers()
    local res = {}
    for name, rec in pairs(thread_routers()) do
        res[name] = rec.cfg
    end
    return res
end

-- A get that changes nothing but what a replica can skip (last access,
-- lazy deletion) may go to a replica.
local function is_plain_read(req)
    return req.op == 'get' and req.touch_ttl == nil
        and req.vivify_ttl == nil and req.recache_ttl == nil
end

-- The engine part of a request.
local function wire(req)
    local res = {}
    for k, v in pairs(req) do
        if not PROTO_ONLY[k] then
            res[k] = v
        end
    end
    return res
end

-- msgpack gives numbers below 2^53 back as Lua numbers: CAS values and
-- counters are uint64_t cdata in the engine contract.
local function u64(v)
    if type(v) == 'number' then
        return ffi.cast(uint64_t, v)
    end
    return v
end

local function fix_response(resp)
    if resp.cas ~= nil then
        resp.cas = u64(resp.cas)
    end
    if resp.value ~= nil then
        resp.value = u64(resp.value)
    end
    local item = resp.item
    if type(item) == 'table' and item.cas ~= nil then
        item.cas = u64(item.cas)
    end
    return resp
end

local function resendable(err)
    return type(err) == 'table' and err.type == 'ShardingError'
        and RESEND[err.name] == true
end

-- The responses in the result of an async call, or nil and the error.
--
-- vshard (1.0.0 at least) wraps an async result wrongly when the storage
-- refuses the call before running the function (bucket moved, locked or in
-- transfer, not a master): vshard.storage.call() returns nil, err, the nil
-- arrives as box.NULL, which is true, and wait_result() "succeeds" with
-- {err}. mcc.storage_exec always returns an array with a response per
-- request, so anything else is the error. A response is a table, or a
-- string: a get hit on the storage's C data path (engine.lua fast()).
local function unwrap(res)
    local resps = res[1]
    if type(resps) == 'table' then
        local first = type(resps[1])
        if first == 'table' or first == 'string' then
            return resps
        end
    end
    return nil, resps or 'no response'
end

-- Send a group synchronously: vshard follows a moved bucket and waits for
-- a master until the deadline. Never raises.
local function resend(router, group, func, deadline)
    local timeout = deadline - fiber.clock()
    if timeout <= 0 then
        group.err = TIMEOUT
        return
    end
    local ok, res, err = pcall(
        router[group.call],
        router,
        group.bid,
        func,
        group.args,
        {timeout = timeout}
    )
    if not ok then
        group.err = res
    elseif res == nil then
        group.err = err or 'no response'
    else
        group.res = res
    end
end

-- Resend the groups, in parallel when there are several.
local function resend_all(router, groups, func, deadline)
    if #groups == 1 then
        resend(router, groups[1], func, deadline)
        return
    end
    local left = #groups
    local done = fiber.cond()
    for _, group in ipairs(groups) do
        local f = fiber.new(function()
            resend(router, group, func, deadline)
            left = left - 1
            if left == 0 then
                done:signal()
            end
        end)
        f:name('mcc/vshard/resend', {truncate = true})
    end
    while left > 0 do
        done:wait()
    end
end

-- exec() keeps no reference to the reqs array after it returns (it sends
-- copies, see wire()): the server reuses it (server.lua "The per-batch
-- tables").
M.borrows_reqs = true

function M.exec(self, reqs)
    local n = #reqs
    local resps = {}
    local router, rerr = self:router()
    if router == nil then
        local resp = server_error(rerr)
        for i = 1, n do
            resps[i] = {status = resp.status, message = resp.message}
        end
        return resps
    end

    -- Group by bucket, in the order of the first request of each bucket.
    local groups, list = {}, {}
    local read_call = self.read_call
    for i = 1, n do
        local req = reqs[i]
        local key = req.key
        if type(key) ~= 'string' then
            resps[i] = {
                status = 'SERVER_ERROR',
                message = 'bad request: no key',
            }
        else
            local bid = router:bucket_id_strcrc32(key)
            local group = groups[bid]
            if group == nil then
                group = {bid = bid, reqs = {}, idx = {}, call = read_call}
                groups[bid] = group
                list[#list + 1] = group
            end
            local k = #group.reqs + 1
            group.reqs[k] = wire(req)
            group.idx[k] = i
            if group.call ~= 'callrw' and not is_plain_read(req) then
                group.call = 'callrw'
            end
        end
    end

    local func = M.EXEC_FUNC
    local space_name = self.space_name
    local timeout = self.timeout
    local deadline = fiber.clock() + timeout
    local async = {is_async = true, timeout = timeout}

    -- Send.
    for _, group in ipairs(list) do
        group.args = {space_name, group.bid, group.reqs}
        local ok, res, err = pcall(
            router[group.call],
            router,
            group.bid,
            func,
            group.args,
            async
        )
        if not ok then
            group.err = res
        elseif res == nil then
            -- Not sent: safe to send again.
            group.err = err
            group.resend = true
        else
            group.future = res
        end
    end

    -- Collect.
    local again = {}
    for _, group in ipairs(list) do
        local future = group.future
        if future ~= nil then
            local wait = deadline - fiber.clock()
            local res, err = future:wait_result(wait > 0 and wait or 0)
            if res ~= nil then
                res, err = unwrap(res)
            end
            if res ~= nil then
                group.res = res
            elseif resendable(err) then
                again[#again + 1] = group
            else
                group.err = err
                pcall(future.discard, future)
            end
        elseif group.resend then
            again[#again + 1] = group
        end
    end
    if #again > 0 then
        self.resends = self.resends + #again
        resend_all(router, again, func, deadline)
    end

    -- Put the responses back in the request order.
    for _, group in ipairs(list) do
        local res = group.res
        if type(res) == 'table' then
            for k, i in ipairs(group.idx) do
                local resp = res[k]
                if type(resp) == 'table' then
                    resps[i] = fix_response(resp)
                elseif type(resp) == 'string' then
                    -- A get hit on the storage's C data path.
                    resps[i] = resp
                else
                    resps[i] = {
                        status = 'SERVER_ERROR',
                        message = 'no response',
                    }
                end
            end
        else
            local message = error_message(group.err or 'no response')
            for _, i in ipairs(group.idx) do
                resps[i] = {status = 'SERVER_ERROR', message = message}
            end
        end
    end
    return resps
end

-- Call a function on every master through map_callrw. Returns the map
-- {replicaset id -> the function's result} or nil and an error message.
local function map_call(self, func, args)
    local router, err = self:router()
    if router == nil then
        return nil, error_message(err)
    end
    local ok, res, merr, rs_id = pcall(
        router.map_callrw,
        router,
        func,
        args,
        {timeout = self.timeout}
    )
    if not ok then
        return nil, error_message(res)
    end
    if res == nil then
        local message = error_message(merr or 'no response')
        if rs_id ~= nil then
            message = ('%s: %s'):format(rs_id, message)
        end
        return nil, message
    end
    local results = {}
    for id, r in pairs(res) do
        results[id] = type(r) == 'table' and r[1] or nil
    end
    return results
end

-- flush_all on every storage. Each one stores the flush time in its own
-- _mcc_meta and uses its own clock.
function M.flush_all(self, delay)
    local results, err = map_call(
        self,
        M.FLUSH_FUNC,
        {self.space_name, delay or 0}
    )
    if results == nil then
        return server_error(err)
    end
    for id, resp in pairs(results) do
        if type(resp) ~= 'table' then
            return server_error(('%s: no response'):format(id))
        end
        if resp.status ~= 'OK' then
            return resp
        end
    end
    return {status = 'OK'}
end

-- The counters of the storages that are added up besides the items.
local SUMMED = {
    'curr_items',
    'bytes',
    'evictions',
    'reclaimed',
    'evicted_unfetched',
    'evicted_active',
    'direct_reclaims',
}

-- The item counts, sizes and eviction counters of all the storages added
-- up, or nil and an error. evict: whether any storage evicts from the
-- space.
local function item_stats(self)
    local results, err = map_call(self, M.STATS_FUNC, {self.space_name})
    if results == nil then
        return nil, err
    end
    local stats = {curr_items = 0, bytes = 0, evict = false}
    local maxbytes = 0
    for _, s in pairs(results) do
        if type(s) == 'table' then
            for _, k in ipairs(SUMMED) do
                stats[k] = (stats[k] or 0) + (tonumber(s[k]) or 0)
            end
            maxbytes = maxbytes + (tonumber(s.limit_maxbytes) or 0)
            stats.evict = stats.evict or s.evict == true
        end
    end
    if maxbytes > 0 then
        stats.limit_maxbytes = maxbytes
    end
    return stats
end

-- Call a function exported in TX by threads.lua; returns its first result.
local function tx_call(func, args)
    return threads.call('tx', func, args)[1][1]
end

-- The item counts and sizes of all the storages added up. Raises on errors.
-- In an application thread, also the counters summed over the threads
-- (counters, curr_connections, threads), and zero items when the storages
-- can't be reached.
function M.stats(self)
    local stats, err = item_stats(self)
    if not self.fe_thread then
        if stats == nil then
            error(err, 0)
        end
        return stats
    end
    stats = stats or {curr_items = 0, bytes = 0}
    local ok, agg = pcall(tx_call, 'mcc.counters', {self.name})
    if ok and type(agg) == 'table' then
        stats.counters = agg.counters
        stats.curr_connections = agg.curr_connections
        stats.threads = agg.threads
    end
    return stats
end

-- Dumps --------------------------------------------------------------------

-- A synchronous call through the router, which follows a moved bucket.
-- Returns the result, or nil and an error.
local function sync_call(router, call, bid, func, args, timeout)
    local ok, res, err = pcall(
        router[call],
        router,
        bid,
        func,
        args,
        {timeout = timeout}
    )
    if not ok then
        return nil, res
    end
    if res == nil then
        return nil, err or 'no response'
    end
    return res
end

local DONE = {}

-- One dump worker: takes the next bucket until there are none, and puts
-- the pages of its items into the channel.
local function dump_worker(self, router, ctx)
    while not ctx.stop do
        ctx.next = ctx.next + 1
        local bid = ctx.next
        if bid > ctx.count then
            break
        end
        local after
        repeat
            local res, err = sync_call(
                router,
                self.read_call,
                bid,
                M.DUMP_FUNC,
                {
                    self.space_name,
                    bid,
                    ctx.sub,
                    after,
                    M.DUMP_PAGE,
                },
                self.timeout
            )
            if type(res) ~= 'table' then
                ctx.err = ctx.err or err or 'no response'
                ctx.stop = true
                break
            end
            local lines = res.lines
            if type(lines) == 'string' and lines ~= '' and not ctx.ch:put(lines) then
                break
            end
            after = res.after
        until after == nil or ctx.stop
    end
end

-- Write the items of every bucket. Returns true, false if a write failed,
-- or false and an error.
local function dump_buckets(self, router, sub, write)
    local ctx = {
        count = router:bucket_count(),
        next = 0,
        sub = sub,
        ch = fiber.channel(M.DUMP_WORKERS),
        stop = false,
        err = nil,
    }
    local n = math.min(M.DUMP_WORKERS, ctx.count)
    local left = n
    for _ = 1, n do
        local f = fiber.new(function()
            local ok, err = pcall(dump_worker, self, router, ctx)
            if not ok then
                ctx.err = ctx.err or err
                ctx.stop = true
            end
            left = left - 1
            if left == 0 then
                ctx.ch:put(DONE)
            end
        end)
        f:name('mcc/vshard/dump', {truncate = true})
    end
    local ok, res = pcall(function()
        while n > 0 do
            local data = ctx.ch:get()
            if data == DONE or data == nil then
                return true
            end
            if not write(data) then
                return false
            end
        end
        return true
    end)
    ctx.stop = true
    -- The workers waiting in put() give up.
    ctx.ch:close()
    if not ok then
        error(res, 0)
    end
    if not res then
        return false
    end
    if ctx.err ~= nil then
        return false, ctx.err
    end
    return true
end

-- "lru_crawler metadump|mgdump" (see readview.dump()): the items of every
-- bucket, one bucket at a time through the router, so that a bucket being
-- moved is dumped once, from where it is. DUMP_WORKERS buckets go at once;
-- each call returns up to DUMP_PAGE items. An error after "OK" is written
-- as SERVER_ERROR and closes the connection (returns false), so that a
-- client can't take a partial dump for a complete one.
function M.dump(self, req, write)
    local router, err = self:router()
    if router == nil then
        return write(('SERVER_ERROR %s\r\n'):format(error_message(err)))
    end
    if not write('OK\r\n') then
        return false
    end
    if req.items ~= false then
        local ok, derr = dump_buckets(self, router, req.sub, write)
        if not ok then
            if derr ~= nil then
                write(('SERVER_ERROR %s\r\n'):format(error_message(derr)))
            end
            return false
        end
    end
    return write(req.sub == 'mgdump' and 'EN\r\n' or 'END\r\n')
end

-- "stats reset" in an application thread resets the counters of every
-- thread.
function M.reset_stats(self)
    if self.fe_thread then
        tx_call('mcc.reset_stats', {self.name})
    end
end

-- "stats conns" in an application thread lists the connections of every
-- thread; in TX the server lists its own.
function M.conns(self)
    if self.fe_thread then
        return tx_call('mcc.conns', {self.name})
    end
    return nil
end

function M.close(self)
end

return M
