-- memcached: a memcached server for Tarantool 3.
--
--   local mcc = require('memcached')
--   local inst = mcc.create(name, opts)
--   inst:cfg(opts) / inst:start() / inst:stop() / inst:destroy() / inst:info()
--   mcc.get(name), mcc.destroy(name)
--   mcc.exec(name, reqs)   a batch of engine requests, in TX
--   mcc.reload()           hot reload of the Lua code (reload.lua)

local fiber = require('fiber')
local log = require('log')

local state = require('memcached.state')
local schema = require('memcached.schema')
local expire = require('memcached.expire')
local evict = require('memcached.evict')
local server = require('memcached.server')

local M = {}

local SELF = 'memcached'

M.VERSION = server.VERSION

M.RUNNING = 'running'
M.STOPPED = 'stopped'
M.WAITING = 'waiting'
M.FAILED = 'failed'
M.DESTROYED = 'destroyed'

-- Backends by name: module name. A backend module has
--   new(cfg) -> object with exec/flush_all/stats/close methods,
--   validate(cfg) (optional), raises if cfg can't be served.
-- cfg is a copy of the instance options plus name.
M.backends = {
    ['local'] = 'memcached.backend.local',
    thread = 'memcached.backend.thread',
    vshard = 'memcached.backend.vshard',
}

-- Values of the 'backend' option.
M.BACKEND_OPTIONS = {['local'] = true, vshard = true}

-- How often a waiting instance checks whether its space has appeared.
M.SPACE_POLL_INTERVAL = 0.1

local function current()
    return package.loaded[SELF] or M
end

-- Options ---------------------------------------------------------------

-- The inputs of the C connection loops changed (engine.inputs_changed()):
-- they return and take them again.
local function inputs_changed()
    state.dp_gen = (state.dp_gen or 0) + 1
end

local function is_integer(v)
    return type(v) == 'number' and v == math.floor(v)
        and v > -2 ^ 53 and v < 2 ^ 53
end

local function check_expire(v)
    for k, x in pairs(v) do
        if k == 'enabled' then
            if type(x) ~= 'boolean' then
                return false
            end
        elseif k == 'batch' then
            if not is_integer(x) or x < 1 then
                return false
            end
        elseif k == 'mode' then
            if not schema.EXPIRE_MODES[x] then
                return false, "'mode' must be 'index' or 'scan'"
            end
        else
            return false
        end
    end
    return true
end

-- evict = {enabled, high, low, samples, batch}, see evict.lua. Only the
-- given fields here: low < high is checked once the options are merged
-- (check_merged()).
local function check_evict(v)
    local err = evict.check(v, true)
    return err == nil, err
end

-- vshard = {router = <router object or name>, timeout = <seconds>}, see
-- backend/vshard.lua.
local function check_vshard(v)
    for k, x in pairs(v) do
        if k == 'router' then
            if type(x) ~= 'string' and type(x) ~= 'table' then
                return false
            end
        elseif k == 'timeout' then
            if type(x) ~= 'number' or not (x > 0) then
                return false
            end
        else
            return false
        end
    end
    return true
end

local OPTIONS = {
    listen = {
        type = {string = true, number = true},
        check = function(v) return server.check_listen(v) end,
    },
    space_name = {
        type = {string = true},
        check = function(v) return #v > 0 end,
    },
    storage = {
        type = {string = true},
        check = function(v) return v == 'memory' or v == 'disk' end,
    },
    backend = {
        type = {string = true},
        check = function(v) return M.BACKEND_OPTIONS[v] ~= nil end,
    },
    -- The application thread group of the front-ends. false or box.NULL:
    -- in TX, which is how cfg() moves them back (normalize()).
    threads = {
        type = {string = true, boolean = true, cdata = true},
        expected = 'string or false',
        check = function(v)
            if type(v) == 'string' then
                return true
            end
            -- false, or box.NULL (a NULL pointer equals nil).
            return v == false or (type(v) == 'cdata' and v == nil)
        end,
    },
    read_mode = {
        type = {string = true},
        check = function(v)
            return v == 'rw' or v == 'ro' or v == 'bro' or v == 'snapshot'
        end,
    },
    snapshot_refresh = {
        type = {number = true},
        check = function(v) return v >= 0.1 end,
    },
    max_item_size = {
        type = {number = true},
        check = function(v)
            return is_integer(v) and v >= 1 and v <= 1024 * 1024 * 1024
        end,
    },
    max_connections = {
        type = {number = true},
        check = function(v) return is_integer(v) and v >= 1 end,
    },
    -- Seconds a connection may wait for a command before it is closed; 0 =
    -- never. memcached's -o idle_timeout, see server.lua.
    idle_timeout = {
        type = {number = true},
        check = function(v) return v >= 0 and v < math.huge end,
    },
    -- The expiration sweeper (expire.lua): {enabled, batch, mode}. mode
    -- 'index' keeps the exptime index on the space, 'scan' has none and
    -- scans the space (memory storage only); the default is 'scan' for
    -- memory and 'index' for disk (schema.expire_mode()).
    expire = {
        type = {table = true},
        check = check_expire,
    },
    -- Eviction when memtx memory is near full (evict.lua): the items of
    -- this space only, memory storage only.
    evict = {
        type = {table = true},
        check = check_evict,
    },
    vshard = {
        type = {table = true},
        check = check_vshard,
    },
    -- Export the instance's metrics through the built-in metrics module
    -- (metrics.lua).
    metrics = {
        type = {boolean = true},
    },
    -- Whether gets record the last access time and the fetched flag of the
    -- items (meta 'l' and 'h' flags, 'me'). The first get after a store
    -- and then at most one get per engine.LA_UPDATE_INTERVAL write the item
    -- for it. false: gets never write the item (only the lazy deletion of
    -- a dead one); see engine.lua.
    track_access = {
        type = {boolean = true},
    },
    -- When the commit of a write returns: Tarantool's box.commit({wait =
    -- <mode>}). 'complete': after the WAL write; 'submit': when handed to
    -- the WAL queue (it waits only for room there); 'none': the same, and a
    -- full queue fails the write. See engine.lua and DESIGN.md "Commit
    -- wait".
    commit_wait = {
        type = {string = true},
        check = function(v)
            return v == 'complete' or v == 'submit' or v == 'none'
        end,
    },
}

local DEFAULTS = {
    storage = 'memory',
    backend = 'local',
    read_mode = 'rw',
    snapshot_refresh = 1,
    max_item_size = 1048576,
    max_connections = 4096,
    idle_timeout = 0,
    expire = {enabled = true, batch = 100},
    evict = evict.merge(),
    metrics = false,
    track_access = true,
    commit_wait = 'complete',
}
-- Read-only: the role passes every option it controls.
M.DEFAULTS = DEFAULTS

-- Options that can't be changed after the instance is created.
local IMMUTABLE = {'space_name', 'storage'}

local function check_options(opts)
    if type(opts) ~= 'table' then
        error('memcached: options must be a table', 0)
    end
    for k, v in pairs(opts) do
        local def = OPTIONS[k]
        if def == nil then
            error(("Unknown option '%s'"):format(tostring(k)), 0)
        end
        if not def.type[type(v)] then
            local expected = {}
            for t in pairs(def.type) do
                table.insert(expected, t)
            end
            table.sort(expected)
            error(
                ("Option '%s' must be a %s, got %s"):format(
                    k,
                    def.expected or table.concat(expected, ' or '),
                    type(v)
                ),
                0
            )
        end
        if def.check ~= nil then
            local ok, reason = def.check(v)
            if not ok then
                error(
                    ("Bad value for option '%s'%s"):format(
                        k,
                        reason and (': ' .. reason) or ''
                    ),
                    0
                )
            end
        end
    end
end

-- The merged options as the instance keeps them: threads = false or
-- box.NULL (the front-ends in TX) becomes nil.
local function normalize(opts)
    local v = opts.threads
    if v == false or type(v) == 'cdata' then
        opts.threads = nil
    end
    return opts
end

-- Raise if the merged options don't go together.
local function check_merged(opts)
    local err = evict.check(opts.evict)
    if err ~= nil then
        error(("Bad value for option 'evict': %s"):format(err), 0)
    end
    err = schema.check_expire_mode(opts.storage, opts.expire.mode)
    if err ~= nil then
        error(("Bad value for option 'expire': %s"):format(err), 0)
    end
end

-- The expiration mode of the instance's space, the default filled in.
local function expire_mode(opts)
    return schema.expire_mode(opts.storage, opts.expire.mode)
end

-- The schema.ensure() options of the instance's space.
local function ensure_opts(opts)
    return {expire = expire_mode(opts)}
end

-- Objects (tables with an __index metamethod, such as a vshard router in
-- the vshard options) are copied and compared by reference.
local function is_object(v)
    local m = getmetatable(v)
    return m ~= nil and (type(m) ~= 'table' or m.__index ~= nil)
end

local function copy(t)
    local res = {}
    for k, v in pairs(t) do
        res[k] = (type(v) == 'table' and not is_object(v)) and copy(v) or v
    end
    return res
end

-- Options merged key by key.
local NESTED = {expire = true, evict = true}

-- base + opts; 'expire' and 'evict' are merged key by key.
local function merge(base, opts)
    local res = copy(base)
    for k, v in pairs(opts) do
        if NESTED[k] then
            res[k] = res[k] or {}
            for ek, ev in pairs(v) do
                res[k][ek] = ev
            end
        else
            res[k] = type(v) == 'table' and copy(v) or v
        end
    end
    return res
end

local function same(a, b)
    if type(a) == 'table' and type(b) == 'table'
        and not is_object(a) and not is_object(b) then
        for k, v in pairs(a) do
            if not same(v, b[k]) then
                return false
            end
        end
        for k in pairs(b) do
            if a[k] == nil then
                return false
            end
        end
        return true
    end
    return a == b
end

local function same_listen(a, b)
    return tostring(a) == tostring(b)
end

-- Tarantool limits the size of a tuple; warn if max_item_size doesn't fit.
local TUPLE_OVERHEAD = 1024

local function check_tuple_size(name, opts)
    local key = opts.storage == 'disk' and 'vinyl_max_tuple_size'
        or 'memtx_max_tuple_size'
    local limit = type(box.cfg) == 'table' and box.cfg[key]
    if limit and limit < opts.max_item_size + TUPLE_OVERHEAD then
        log.warn(
            "memcached: instance '%s': max_item_size %d "
                .. "doesn't fit into box.cfg.%s %d: bigger values fail with "
                .. "SERVER_ERROR",
            name,
            opts.max_item_size,
            key,
            limit
        )
    end
end

-- Backends --------------------------------------------------------------

local function backend_module(name)
    local modname = M.backends[name]
    if modname == nil then
        error(("Unknown backend '%s'"):format(tostring(name)), 0)
    end
    local ok, mod = pcall(require, modname)
    if not ok then
        error(
            ("Backend '%s' is not available: %s"):format(
                name,
                tostring(mod)
            ),
            0
        )
    end
    return mod
end

local function backend_cfg(name, opts)
    local cfg = copy(opts)
    cfg.name = name
    return cfg
end

-- Front-ends in application threads and read views (looked up on use, so
-- that a reload takes effect).
local function threads_module()
    return package.loaded['memcached.threads'] or require('memcached.threads')
end

local function readview_module()
    return package.loaded['memcached.readview'] or require('memcached.readview')
end

-- With the option metrics = true: export the metrics (idempotent).
local function enable_metrics(opts)
    if opts.metrics then
        local mod = package.loaded['memcached.metrics']
            or require('memcached.metrics')
        mod.enable()
    end
end

-- read_mode = 'snapshot': the backend is wrapped by readview.wrap(), which
-- serves the plain gets from a read view; the backend itself gets 'rw'.
local function validate_backend(name, opts)
    local mod = backend_module(opts.backend)
    if mod.REMOTE and opts.track_access == false then
        -- The storages run the gets: the option is set there, per space
        -- (storage.cfg(), and the role passes it from the instance config).
        error(
            ("Option 'track_access' = false is not supported by the "
                .. "'%s' backend: set it on the storages, "
                .. "storage.cfg({spaces = {[<space>] = {track_access = false}}})"):format(
                    opts.backend
                ),
            0
        )
    end
    if mod.REMOTE and opts.commit_wait ~= nil and opts.commit_wait ~= 'complete' then
        -- The writes run on the storages too.
        error(
            ("Option 'commit_wait' = '%s' is not supported by the "
                .. "'%s' backend: set it on the storages, "
                .. "storage.cfg({spaces = {[<space>] = {commit_wait = '%s'}}})"):format(
                    opts.commit_wait,
                    opts.backend,
                    opts.commit_wait
                ),
            0
        )
    end
    if mod.REMOTE and not same(opts.evict, DEFAULTS.evict) then
        -- The data, and so the eviction, is on the storages.
        error(
            ("Option 'evict' is not supported by the '%s' backend: set "
                .. "it on the storages, storage.cfg({spaces = {[<space>] = "
                .. "{evict = {...}}}})"):format(opts.backend),
            0
        )
    end
    local cfg = backend_cfg(name, opts)
    if cfg.read_mode == 'snapshot' then
        readview_module().validate(cfg)
        cfg.read_mode = 'rw'
    end
    if mod.validate ~= nil then
        mod.validate(cfg)
    end
end

local function new_backend(self)
    local mod = backend_module(self.opts.backend)
    local cfg = backend_cfg(self.name, self.opts)
    if cfg.read_mode == 'snapshot' then
        cfg.read_mode = 'rw'
        return readview_module().wrap(
            mod.new(cfg),
            backend_cfg(self.name, self.opts)
        )
    end
    return mod.new(cfg)
end

local function close_backend(backend)
    if backend ~= nil and backend.close ~= nil then
        local ok, err = pcall(backend.close, backend)
        if not ok then
            log.error(
                'memcached: closing a backend: %s',
                tostring(err)
            )
        end
    end
end

-- Whether the backend works on the local data space. A backend module that
-- sets REMOTE (vshard) keeps the data elsewhere: the instance then neither
-- creates nor waits for the space and runs no sweeper.
local function local_space(opts)
    local ok, mod = pcall(backend_module, opts.backend)
    return not (ok and mod.REMOTE)
end

-- Whether the evictor evicts from the instance's space: a local memtx
-- space with evict.enabled. vinyl's limit is the disk: no eviction there.
local function evicts(opts)
    return opts.evict.enabled and opts.storage == 'memory' and local_space(opts)
end

-- Start or stop evicting from the instance's space (evict.lua).
local function update_evictor(self)
    if evicts(self.opts) then
        local e = self.opts.evict
        evict.start(self.name, self.space_name, {
            high = e.high,
            low = e.low,
            samples = e.samples,
            batch = e.batch,
            track_access = self.opts.track_access,
        })
    else
        evict.stop(self.name)
    end
end

-- Instances -------------------------------------------------------------

local methods = {}
M.instance_methods = methods

-- Methods are looked up in the current module, so instances use the new
-- code after a reload.
M.instance_mt = {
    __index = function(_, k)
        return current().instance_methods[k]
    end,
}

local function check_alive(self)
    if self.status == M.DESTROYED then
        error(("Memcached instance '%s' is destroyed"):format(self.name), 0)
    end
end

local function check_threads(opts)
    if opts.threads ~= nil then
        threads_module().check(opts)
    end
end

local function server_opts(self)
    return {
        name = self.name,
        listen = self.opts.listen,
        max_connections = self.opts.max_connections,
        max_item_size = self.opts.max_item_size,
        idle_timeout = self.opts.idle_timeout,
    }
end

-- Start serving. The space must exist.
local function do_start(self)
    close_backend(self.backend)
    self.backend = nil
    local backend = new_backend(self)
    local srv = self.server
    if srv == nil then
        srv = server.new(server_opts(self))
        self.server = srv
    end
    srv.uri = self.opts.listen
    srv:cfg({
        max_connections = self.opts.max_connections,
        max_item_size = self.opts.max_item_size,
        idle_timeout = self.opts.idle_timeout,
        backend = backend,
    })
    local ok, err
    if self.opts.threads ~= nil then
        -- The front-ends run in the threads and call this backend.
        self.backend = backend
        ok, err = pcall(threads_module().start, self)
    else
        ok, err = pcall(srv.start, srv)
    end
    if not ok then
        srv.backend = nil
        self.backend = nil
        close_backend(backend)
        self.status = M.FAILED
        error(err, 0)
    end
    self.backend = backend
    if self.opts.expire.enabled and local_space(self.opts) then
        expire.start(
            self.name,
            self.space_name,
            {batch = self.opts.expire.batch}
        )
    end
    update_evictor(self)
    if self.opts.read_mode == 'snapshot' then
        readview_module().start(self)
    end
    self.status = M.RUNNING
    log.info(
        "memcached: instance '%s' is listening on %s",
        self.name,
        tostring(self.opts.listen)
    )
end

local function stop_waiter(self)
    local f = self.waiter
    self.waiter = nil
    if f ~= nil and f:status() ~= 'dead' and f:id() ~= fiber.id() then
        pcall(f.cancel, f)
    end
end

-- Wait until the space exists; on a writable instance, create it.
local function waiter_main(self)
    log.info(
        "memcached: instance '%s': space '%s' does not exist "
            .. "and the instance is read-only, waiting for the space",
        self.name,
        self.space_name
    )
    while self.status == M.WAITING do
        local ok, space = pcall(
            schema.ensure,
            self.space_name,
            self.opts.storage,
            ensure_opts(self.opts)
        )
        if not ok then
            log.error(
                "memcached: instance '%s': %s",
                self.name,
                tostring(space)
            )
        elseif space ~= nil then
            if self.status ~= M.WAITING then
                return
            end
            self.waiter = nil
            local started, err = pcall(current().internal.do_start, self)
            if not started then
                self.status = M.FAILED
                log.error(
                    "memcached: instance '%s': %s",
                    self.name,
                    tostring(err)
                )
            end
            return
        end
        fiber.sleep(current().SPACE_POLL_INTERVAL)
    end
end

local function start_waiter(self)
    stop_waiter(self)
    local f = fiber.new(waiter_main, self)
    f:name('mcc/wait/' .. self.name, {truncate = true})
    self.waiter = f
end

local function do_stop(self)
    self.released = nil
    stop_waiter(self)
    expire.stop(self.name)
    evict.stop(self.name)
    readview_module().stop(self.name)
    if self.fe_group ~= nil then
        threads_module().stop(self)
    end
    if self.server ~= nil then
        self.server:stop()
        self.server.backend = nil
    end
    close_backend(self.backend)
    self.backend = nil
    self.status = M.STOPPED
end

-- Listen on a URI, keeping the connections; also after
-- release_listener(). If it can't be bound, the old URI is kept, and if
-- even that can't be bound again the instance is failed; the error is
-- raised.
local function rebind(self, listen)
    self.released = nil
    local srv = self.server
    if self.fe_group ~= nil then
        local threads = threads_module()
        local ok, err = pcall(threads.rebind, self, listen)
        if not ok then
            if not threads.listening(self) then
                self.status = M.FAILED
            end
            error(err, 0)
        end
        srv.uri = listen
        return
    end
    local ok, err = pcall(srv.rebind, srv, listen)
    if not ok then
        if not srv.running then
            self.status = M.FAILED
        end
        error(err, 0)
    end
end

-- For the role: stop listening and keep the connections (server.release(),
-- in every thread with threads), until the next cfg() binds the new URI or
-- restore_listener() the current one. The role releases first the
-- addresses that move to another instance in one config, so that swaps and
-- chains of listen URIs apply in one reload.
local function release_listener(self)
    if self.status ~= M.RUNNING then
        return
    end
    if self.fe_group ~= nil then
        threads_module().release(self)
    else
        self.server:release()
    end
    self.released = true
end

-- Listen on the current URI again after release_listener(), when no cfg()
-- has done it (it failed before the rebind). Raises as rebind().
local function restore_listener(self)
    if self.released and self.status == M.RUNNING then
        rebind(self, self.opts.listen)
    end
    self.released = nil
end

M.internal = {
    do_start = do_start,
    do_stop = do_stop,
    start_waiter = start_waiter,
    stop_waiter = stop_waiter,
    check_options = check_options,
    release_listener = release_listener,
    restore_listener = restore_listener,
}

function methods.start(self)
    check_alive(self)
    if self.status == M.RUNNING or self.status == M.WAITING then
        error(
            ("Memcached instance '%s' is already started"):format(
                self.name
            ),
            0
        )
    end
    check_threads(self.opts)
    -- A remote backend needs no local space.
    local space = not local_space(self.opts)
        or schema.ensure(
            self.space_name,
            self.opts.storage,
            ensure_opts(self.opts)
        )
    if space == nil then
        self.status = M.WAITING
        start_waiter(self)
        return self
    end
    do_start(self)
    return self
end

function methods.stop(self)
    check_alive(self)
    if self.status == M.STOPPED then
        error(
            ("Memcached instance '%s' is already stopped"):format(
                self.name
            ),
            0
        )
    end
    do_stop(self)
    return self
end

function methods.destroy(self)
    check_alive(self)
    if self.status ~= M.STOPPED then
        do_stop(self)
    end
    if self.fe_group ~= nil then
        threads_module().destroy(self)
    end
    self.status = M.DESTROYED
    if state.instances[self.name] == self then
        state.instances[self.name] = nil
    end
end

function methods.cfg(self, opts)
    check_alive(self)
    check_options(opts)
    for _, k in ipairs(IMMUTABLE) do
        if opts[k] ~= nil and opts[k] ~= self.opts[k] then
            error(
                ("Option '%s' can't be changed after the instance is "
                    .. "created"):format(k),
                0
            )
        end
    end
    local new = normalize(merge(self.opts, opts))
    check_merged(new)
    check_threads(new)
    enable_metrics(new)
    -- The backend takes these options when it is created: a new one is
    -- made when they change.
    local backend_changed = new.backend ~= self.opts.backend
        or new.read_mode ~= self.opts.read_mode
        or new.snapshot_refresh ~= self.opts.snapshot_refresh
        or new.track_access ~= self.opts.track_access
        or new.commit_wait ~= self.opts.commit_wait
        or not same(new.vshard, self.opts.vshard)
    if backend_changed or not same(new.evict, self.opts.evict) then
        validate_backend(self.name, new)
    end
    local srv = self.server
    local running = self.status == M.RUNNING and srv ~= nil

    if new.threads ~= self.opts.threads then
        -- The front-ends move between TX and the threads, or between thread
        -- groups (threads = false: back to TX): restart them (the
        -- connections are closed).
        if running then
            do_stop(self)
        end
        if self.fe_group ~= nil then
            threads_module().destroy(self)
        end
        self.opts = new
        if running then
            do_start(self)
        end
        return self
    end

    -- Rebind first: it is the only step that may fail at runtime. A
    -- released listener (release_listener()) is bound again.
    if self.released or not same_listen(new.listen, self.opts.listen) then
        if running then
            rebind(self, new.listen)
        elseif srv ~= nil then
            srv.uri = new.listen
        end
    end

    local old = self.opts
    self.opts = new
    if new.max_item_size ~= old.max_item_size then
        check_tuple_size(self.name, new)
    end
    if srv ~= nil then
        srv:cfg({
            max_connections = new.max_connections,
            max_item_size = new.max_item_size,
            idle_timeout = new.idle_timeout,
        })
    end
    if running and backend_changed then
        if local_space(new) and not local_space(old) then
            pcall(schema.ensure, self.space_name, new.storage, ensure_opts(new))
        end
        local backend = new_backend(self)
        local prev = self.backend
        self.backend = backend
        srv.backend = backend
        -- The C connection loops took the old one's inputs.
        inputs_changed()
        close_backend(prev)
        if new.read_mode == 'snapshot' then
            readview_module().start(self)
        else
            readview_module().stop(self.name)
        end
    end
    if running and self.fe_group ~= nil then
        threads_module().cfg(self)
    end
    local sweeps = local_space(new)
    if running and sweeps
        and local_space(old) and expire_mode(new) ~= expire_mode(old) then
        -- Create or drop the exptime index (a writable instance; a stopped
        -- one does it when it starts, a read-only one leaves it). The
        -- sweeper follows the indexes the space has, so a failure here
        -- costs only speed or memory: it is logged, the option is kept.
        local ok, err = pcall(
            schema.ensure,
            self.space_name,
            new.storage,
            ensure_opts(new)
        )
        if not ok then
            log.error(
                "memcached: instance '%s': expire.mode = '%s': %s",
                self.name,
                expire_mode(new),
                tostring(err)
            )
        end
    end
    if running
        and (not same(new.expire, old.expire) or sweeps ~= local_space(old)) then
        if new.expire.enabled and sweeps then
            expire.start(
                self.name,
                self.space_name,
                {batch = new.expire.batch}
            )
        else
            expire.stop(self.name)
        end
    end
    if running
        and (not same(new.evict, old.evict)
            or new.track_access ~= old.track_access
            or sweeps ~= local_space(old)) then
        update_evictor(self)
    end
    return self
end

function methods.info(self)
    check_alive(self)
    local info = {
        name = self.name,
        status = self.status,
        space_name = self.space_name,
        listen = self.opts.listen,
        curr_connections = 0,
    }
    if self.fe_group ~= nil then
        -- Summed over the threads (yields); per thread in info.threads.
        for k, v in pairs(threads_module().info(self)) do
            info[k] = v
        end
    elseif self.server ~= nil then
        for k, v in pairs(self.server:info()) do
            if k ~= 'listen' and k ~= 'running' then
                info[k] = v
            end
        end
    end
    local sweeper = expire.info(self.name)
    if sweeper ~= nil then
        info.expired = sweeper.expired
        info.flushed = sweeper.flushed
        info.checked = sweeper.checked
    end
    if local_space(self.opts) then
        -- The counters of the space (evict.lua), shared by the instances
        -- that use it.
        for k, v in pairs(evict.stats(self.space_name)) do
            info[k] = v
        end
    end
    return info
end

-- Module API ------------------------------------------------------------

function M.create(name, opts)
    if type(name) ~= 'string' or name == '' then
        error(
            'memcached: instance name must be a non-empty string',
            0
        )
    end
    if state.instances[name] ~= nil then
        error(("Memcached instance '%s' already exists"):format(name), 0)
    end
    if type(box.cfg) == 'function' then
        error('Please call box.cfg{} first', 0)
    end
    opts = opts or {}
    check_options(opts)
    if opts.listen == nil then
        error("Option 'listen' is required", 0)
    end
    local conf = normalize(merge(DEFAULTS, opts))
    conf.space_name = conf.space_name or ('__mcc_' .. name)
    check_merged(conf)
    check_threads(conf)
    validate_backend(name, conf)
    check_tuple_size(name, conf)
    -- Creates the space, unless it exists or the instance is read-only.
    if local_space(conf) then
        schema.ensure(conf.space_name, conf.storage, ensure_opts(conf))
    end
    local inst = setmetatable({
        name = name,
        opts = conf,
        space_name = conf.space_name,
        status = M.STOPPED,
        server = nil,
        backend = nil,
        waiter = nil,
    }, M.instance_mt)
    inst.server = server.new(server_opts(inst))
    state.instances[name] = inst
    local ok, err = pcall(methods.start, inst)
    if not ok then
        pcall(methods.destroy, inst)
        error(err, 0)
    end
    enable_metrics(conf)
    return inst
end

-- Raise if mcc.create(name, opts) would reject the options; no side
-- effects. The role validates its config with it.
function M.check(name, opts)
    if type(name) ~= 'string' or name == '' then
        error(
            'memcached: instance name must be a non-empty string',
            0
        )
    end
    opts = opts or {}
    check_options(opts)
    if opts.listen == nil then
        error("Option 'listen' is required", 0)
    end
    local conf = normalize(merge(DEFAULTS, opts))
    conf.space_name = conf.space_name or ('__mcc_' .. name)
    check_merged(conf)
    check_threads(conf)
    validate_backend(name, conf)
end

function M.get(name)
    return state.instances[name]
end

function M.destroy(name)
    local inst = state.instances[name]
    if inst == nil then
        error(
            ("Memcached instance '%s' does not exist"):format(
                tostring(name)
            ),
            0
        )
    end
    inst:destroy()
end

-- Execute a batch of engine requests with the backend of an instance (TX).
-- The application threads call it as the exported 'mcc.exec'.
function M.exec(name, reqs)
    return threads_module().exec(name, reqs)
end

-- GC tuning -------------------------------------------------------------

-- The Lua garbage collector is one per Lua state (the TX thread, and each
-- application thread has its own) and is shared by every module and
-- application in it. memcached never changes it by itself:
-- mcc.tune_gc() is the explicit knob, for an application that is mostly a
-- memcached server.
--
--   mcc.tune_gc({pause = 400, stepmul = 200}) -> previous {pause, stepmul}
--
-- Both fields are optional; only the given ones are set, and the previous
-- values of those are returned, so the caller can restore them.
--   pause    collectgarbage('setpause'): a new GC cycle starts when the
--            heap reaches pause% of its size after the last collection.
--            LuaJIT's default is 200; 400 means fewer cycles for a bigger
--            heap. 100..1000.
--   stepmul  collectgarbage('setstepmul'): the speed of the incremental
--            collector relative to allocation. LuaJIT's default is 200.
--            100..10000.
-- The benchmark (README.md, Performance) measured pause = 400 on top of
-- the Lua fixes: up to +10% at pipeline 16, nothing at pipeline 1.
M.GC_LIMITS = {
    pause = {100, 1000},
    stepmul = {100, 10000},
}

function M.tune_gc(opts)
    if type(opts) ~= 'table' then
        error('Usage: mcc.tune_gc({pause = <number>, stepmul = <number>})', 0)
    end
    for k, v in pairs(opts) do
        local limits = M.GC_LIMITS[k]
        if limits == nil then
            error(("Unknown GC parameter '%s'"):format(tostring(k)), 0)
        end
        if not is_integer(v) or v < limits[1] or v > limits[2] then
            error(
                ("GC parameter '%s' must be an integer in %d..%d"):format(
                    k,
                    limits[1],
                    limits[2]
                ),
                0
            )
        end
    end
    local prev = {}
    if opts.pause ~= nil then
        prev.pause = collectgarbage('setpause', opts.pause)
    end
    if opts.stepmul ~= nil then
        prev.stepmul = collectgarbage('setstepmul', opts.stepmul)
    end
    log.info(
        'memcached: Lua GC of this thread tuned: pause %s '
            .. '(was %s), stepmul %s (was %s)',
        tostring(opts.pause or '-'),
        tostring(prev.pause or '-'),
        tostring(opts.stepmul or '-'),
        tostring(prev.stepmul or '-')
    )
    return prev
end

-- The C data path (memcached/datapath.c, see engine.lua): the classic
-- get/gets/set on memtx spaces in C. On by default when the module is
-- built; MEMCACHED_DATAPATH=lua in the environment switches it off from the
-- start.
--
--   mcc.datapath()        -> {available = <bool>, enabled = <bool>}
--   mcc.datapath(false)   the Lua engine serves every request
--   mcc.datapath(true)    the C data path again, if it is available
--
-- TX: the engine runs there. It survives a reload. The answers are the
-- same either way; the switch is for comparisons and debugging.
function M.datapath(enabled)
    if enabled ~= nil and type(enabled) ~= 'boolean' then
        error('Usage: mcc.datapath([enabled])', 0)
    end
    local engine = package.loaded['memcached.engine']
        or require('memcached.engine')
    if enabled ~= nil then
        state.datapath_off = not enabled
        inputs_changed()
    end
    local available = engine.datapath_available()
    return {
        available = available,
        enabled = available and not state.datapath_off,
    }
end

-- The C connection loop (memcached/cloop.c, see server.lua conn_step()):
-- the connections of a TX server with the local backend run their reads,
-- the C batch and their writes in C while the C batch serves their
-- commands. On by default with the C data path; MEMCACHED_DATAPATH=batch in
-- the environment switches it off from the start (the C batch serving the
-- commands from the Lua loop).
--
--   mcc.datapath_loop()       -> {available = <bool>, enabled = <bool>}
--   mcc.datapath_loop(false)  the Lua connection loop for every connection
--   mcc.datapath_loop(true)   the C loop again, if it is available
--
-- It applies to the connections at their next batch, survives a reload, and
-- the answers are the same either way.
function M.datapath_loop(enabled)
    if enabled ~= nil and type(enabled) ~= 'boolean' then
        error('Usage: mcc.datapath_loop([enabled])', 0)
    end
    local engine = package.loaded['memcached.engine']
        or require('memcached.engine')
    if enabled ~= nil then
        state.cloop_off = not enabled
        inputs_changed()
    end
    local dp = engine.datapath_available() and package.loaded[engine.DATAPATH]
        or nil
    local available = dp ~= nil and dp.loop ~= nil
    return {
        available = available,
        enabled = available and not state.datapath_off and not state.cloop_off,
    }
end

-- Hot reload of the Lua code, see reload.lua.
function M.reload()
    local reload = package.loaded['memcached.reload']
        or require('memcached.reload')
    return reload.reload()
end

return M
