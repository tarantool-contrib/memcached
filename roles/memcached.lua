-- The roles.memcached role: memcached instances from the Tarantool 3
-- declarative configuration.
--
-- roles: [roles.memcached]
-- roles_cfg:
--   roles.memcached:
--     cache:                      # the memcached instance name
--       listen: 0.0.0.0:11211     # required
--       space_name: __mcc_cache   # default __mcc_<name>
--       storage: memory           # or disk
--       backend: local            # or vshard
--       threads: fe               # an application thread group (threads.groups)
--       read_mode: rw             # ro, bro (vshard), snapshot
--       snapshot_refresh: 1
--       max_item_size: 1048576
--       max_connections: 4096
--       idle_timeout: 0           # seconds; close idle connections, 0 = never
--       expire: {enabled: true, batch: 100, mode: scan}  # mode: index or
--                                 # scan; default scan (memory), index (disk)
--       evict: {enabled: true, high: 0.95, low: 0.9, samples: 8, batch: 100}
--       vshard: {router: <name>, timeout: 1}
--       metrics: false            # export the instance's metrics
--       track_access: true        # whether gets record the last access
--       commit_wait: complete     # writes reply after the WAL write, or
--                                 # before it: submit, none
--
-- Every key of the role config is a memcached instance; the options are the
-- ones of memcached.create(). The role creates new instances,
-- reconfigures changed ones and destroys the removed ones. An option removed
-- from the config falls back to its default (threads: TX). An instance is
-- recreated when its space_name or storage changes; a change of threads
-- moves its front-ends, which closes their connections. A reload also
-- starts the instances that were stopped or failed.
--
-- vshard (sharding.roles of this Tarantool instance):
-- - on a router, an instance with backend = 'vshard' is a front-end of the
--   static router (or of the router named by vshard.router);
-- - on a storage, the spaces of the instances with backend = 'vshard' are
--   served to the routers (memcached.storage), and no front-end is
--   started for them unless the instance is a router too.
-- So one roles_cfg can be shared by the routers and the storages. The
-- requests of a vshard instance run on the storages and its data is there,
-- so its track_access, commit_wait and evict go there.
--
-- On a read-only Tarantool instance the memcached instances listen as well:
-- reads are served and writes answer SERVER_ERROR. If the space doesn't
-- exist yet, the instance waits for it to arrive by replication.
--
-- The memcached modules are looked up in package.loaded on every
-- call, so that memcached.reload() takes effect. The role state is
-- in memcached.state, so it survives a reload as well.

local uri = require('uri')
local log = require('log')

local ok_schema, schema = pcall(require, 'experimental.config.utils.schema')
if not ok_schema then
    error('roles.memcached needs Tarantool 3.9 or newer', 0)
end

local ROLE = 'roles.memcached'
local MCC = 'memcached'
local STATE = 'memcached.state'
local STORAGE = 'memcached.storage'
local SCHEMA = 'memcached.schema'
local METRICS = 'memcached.metrics'

local M = {}

local function load(name)
    return package.loaded[name] or require(name)
end

local function mcc()
    return load(MCC)
end

local function role_state()
    local state = load(STATE)
    local s = state.role
    if s == nil then
        s = {
            -- name -> {inst, opts}: the instances created by the role.
            instances = {},
            -- name -> error of the last apply.
            errors = {},
            -- Whether the role configured memcached.storage.
            storage = false,
            -- name -> space_name of the vshard instances whose spaces
            -- this storage serves.
            storage_spaces = {},
            -- Whether the storage metrics are exported.
            metrics_storage = false,
        }
        state.role = s
    end
    return s
end

local function sorted_keys(t)
    local keys = {}
    for k in pairs(t) do
        table.insert(keys, k)
    end
    table.sort(keys)
    return keys
end

local function error_message(err)
    if type(err) == 'cdata' or type(err) == 'table' then
        err = err.message or tostring(err)
    end
    return tostring(err)
end

-- Options -----------------------------------------------------------------

local function validate_listen(data, w)
    if type(data) ~= 'string' and type(data) ~= 'number' then
        w.error('Expected a string or a number, got %s', type(data))
    end
    local parsed = uri.parse(tostring(data))
    if parsed == nil or parsed.service == nil then
        w.error('Unable to parse the URI %q', tostring(data))
    end
end

local function non_empty(data, w)
    if data == '' then
        w.error('Must not be empty')
    end
end

-- A fraction of memtx_memory: (0, 1].
local function fraction()
    return schema.scalar({
        type = 'number',
        validate = function(data, w)
            if not (data > 0 and data <= 1) then
                w.error('Must be in range (0, 1], got %s', data)
            end
        end,
    })
end

local function integer_in(min, max)
    return schema.scalar({
        type = 'integer',
        validate = function(data, w)
            if data < min or (max ~= nil and data > max) then
                if max ~= nil then
                    w.error(
                        'Must be in range [%d, %d], got %d',
                        min,
                        max,
                        data
                    )
                end
                w.error('Must be at least %d, got %d', min, data)
            end
        end,
    })
end

local instance_schema = schema.record({
    listen = schema.scalar({
        -- 'any': a string or a number.
        type = 'any',
        validate = validate_listen,
    }),
    space_name = schema.scalar({type = 'string', validate = non_empty}),
    storage = schema.enum({'memory', 'disk'}),
    backend = schema.scalar({
        type = 'string',
        validate = function(data, w)
            if mcc().BACKEND_OPTIONS[data] == nil then
                w.error('Unknown backend %q', data)
            end
        end,
    }),
    threads = schema.scalar({type = 'string', validate = non_empty}),
    read_mode = schema.enum({'rw', 'ro', 'bro', 'snapshot'}),
    snapshot_refresh = schema.scalar({
        type = 'number',
        validate = function(data, w)
            if not (data >= 0.1) then
                w.error('Must be at least 0.1 seconds, got %s', data)
            end
        end,
    }),
    max_item_size = integer_in(1, 1024 * 1024 * 1024),
    max_connections = integer_in(1),
    idle_timeout = schema.scalar({
        type = 'number',
        validate = function(data, w)
            if not (data >= 0 and data < math.huge) then
                w.error('Must be a number of seconds >= 0, got %s', data)
            end
        end,
    }),
    expire = schema.record({
        enabled = schema.scalar({type = 'boolean'}),
        batch = integer_in(1),
        mode = schema.enum({'index', 'scan'}),
    }),
    evict = schema.record({
        enabled = schema.scalar({type = 'boolean'}),
        high = fraction(),
        low = fraction(),
        samples = integer_in(1, 64),
        batch = integer_in(1),
    }),
    vshard = schema.record({
        router = schema.scalar({type = 'string', validate = non_empty}),
        timeout = schema.scalar({
            type = 'number',
            validate = function(data, w)
                if not (data > 0) then
                    w.error('Must be positive, got %s', data)
                end
            end,
        }),
    }),
    metrics = schema.scalar({type = 'boolean'}),
    track_access = schema.scalar({type = 'boolean'}),
    commit_wait = schema.enum({'complete', 'submit', 'none'}),
},
    {
        validate = function(data, w)
            if data.listen == nil then
                w.error("Option 'listen' is required")
            end
        end,
    })

local cfg_schema = schema.new(
    ROLE,
    schema.map({
        key = schema.scalar({type = 'string', validate = non_empty}),
        value = instance_schema,
    })
)

local function copy(v)
    if type(v) ~= 'table' then
        return v
    end
    local res = {}
    for k, x in pairs(v) do
        if x ~= nil then
            res[k] = copy(x)
        end
    end
    return res
end

-- Options merged key by key with their defaults.
local NESTED = {expire = true, evict = true}

-- The memcached.create() options of an instance. Every option the
-- role controls is set, so that an option removed from the config falls
-- back to its default. box.NULL values count as missing.
local function module_opts(name, icfg)
    local opts = copy(mcc().DEFAULTS or {})
    for k, v in pairs(icfg) do
        if v ~= nil then
            if NESTED[k] then
                opts[k] = opts[k] or {}
                for ek, ev in pairs(v) do
                    if ev ~= nil then
                        opts[k][ek] = ev
                    end
                end
            else
                opts[k] = copy(v)
            end
        end
    end
    opts.space_name = opts.space_name or ('__mcc_' .. name)
    opts.vshard = opts.vshard or {}
    -- No thread group: the front-ends run in TX, and cfg() moves them back
    -- there when the option is removed.
    opts.threads = opts.threads or false
    return opts
end

-- The options of the front-end of an instance. With backend = 'vshard' the
-- requests run on the storages and the data is there, so track_access,
-- commit_wait and evict go to the storage side (apply_storage()) and the
-- front-end keeps the defaults.
local function frontend_opts(name, icfg)
    local opts = module_opts(name, icfg)
    if opts.backend == 'vshard' then
        local defaults = mcc().DEFAULTS or {}
        opts.track_access = defaults.track_access
        opts.commit_wait = defaults.commit_wait
        opts.evict = copy(defaults.evict)
    end
    return opts
end

-- sharding.roles of this Tarantool instance: a set.
local function sharding_roles()
    local res = {}
    local ok, roles = pcall(function()
        return require('config'):get('sharding.roles')
    end)
    if ok and type(roles) == 'table' then
        for _, r in ipairs(roles) do
            res[r] = true
        end
    end
    return res
end

local function is_vshard(icfg)
    return icfg.backend == 'vshard'
end

-- Whether this Tarantool instance runs the front-end of the instance: not
-- for a vshard one on a storage that is not a router (unless the instance
-- names its own router).
local function is_frontend(icfg, sharding)
    if not is_vshard(icfg) then
        return true
    end
    return sharding.router or (icfg.vshard ~= nil and icfg.vshard.router ~= nil)
end

-- Validation --------------------------------------------------------------

local function fail(name, fmt, ...)
    error(('[%s] %s: %s'):format(ROLE, name, fmt:format(...)), 0)
end

local WILDCARD = {['0.0.0.0'] = true, ['::'] = true, ['*'] = true}

-- host ('*' for any), port of a listen URI.
local function listen_addr(listen)
    local u = uri.parse(tostring(listen))
    if u.host == 'unix/' then
        return 'unix/', u.service
    end
    local host = u.host
    if host == nil or host == '' or WILDCARD[host] then
        host = '*'
    end
    return host, tostring(u.service)
end

-- Whether two addresses (listen_addr()) overlap: the same port, and the
-- same host or a wildcard one. Port 0 never does.
local function addr_overlap(host_a, port_a, host_b, port_b)
    return port_a ~= '0' and port_a == port_b
        and (host_a == host_b
            or (host_a ~= 'unix/' and host_b ~= 'unix/'
                and (host_a == '*' or host_b == '*')))
end

local function listen_overlap(a, b)
    local host_a, port_a = listen_addr(a)
    local host_b, port_b = listen_addr(b)
    return addr_overlap(host_a, port_a, host_b, port_b)
end

-- Two front-ends can't listen on the same address. With threads both would
-- bind it with SO_REUSEPORT and share the connections.
local function check_listen(cfg, sharding)
    local seen = {}
    for _, name in ipairs(sorted_keys(cfg)) do
        local icfg = cfg[name]
        if is_frontend(icfg, sharding) then
            local host, port = listen_addr(icfg.listen)
            if port ~= '0' then
                for _, other in ipairs(seen) do
                    if addr_overlap(other.host, other.port, host, port) then
                        local a, b = tostring(cfg[other.name].listen),
                            tostring(icfg.listen)
                        if a == b then
                            error(
                                ("[%s] instances '%s' and '%s' have the "
                                    .. "same listen URI '%s'"):format(
                                        ROLE,
                                        other.name,
                                        name,
                                        a
                                    ),
                                0
                            )
                        end
                        error(
                            ("[%s] instances '%s' and '%s' have "
                                .. "overlapping listen URIs '%s' and '%s'"):format(
                                    ROLE,
                                    other.name,
                                    name,
                                    a,
                                    b
                                ),
                            0
                        )
                    end
                end
                table.insert(seen, {name = name, host = host, port = port})
            end
        end
    end
end

-- Instances that share a space must agree on how it is stored.
local function check_spaces(cfg)
    local owner = {}
    for _, name in ipairs(sorted_keys(cfg)) do
        local opts = module_opts(name, cfg[name])
        local prev = owner[opts.space_name]
        if prev == nil then
            owner[opts.space_name] = {name = name, opts = opts}
        else
            local p = prev.opts
            local where = ("[%s] instances '%s' and '%s' use the space '%s'"):format(
                ROLE,
                prev.name,
                name,
                opts.space_name
            )
            if (p.backend == 'vshard') ~= (opts.backend == 'vshard') then
                error(where .. ' with backends vshard and local', 0)
            end
            if p.storage ~= opts.storage then
                error(where .. ' with different storage', 0)
            end
            if opts.backend == 'vshard'
                and (p.expire.enabled ~= opts.expire.enabled
                    or p.expire.batch ~= opts.expire.batch) then
                error(where .. ' with different expire options', 0)
            end
            -- The exptime index is per space.
            local mschema = load(SCHEMA)
            if mschema.expire_mode(p.storage, p.expire.mode)
                ~= mschema.expire_mode(opts.storage, opts.expire.mode) then
                error(where .. ' with different expire.mode', 0)
            end
            if opts.backend == 'vshard' and p.track_access ~= opts.track_access then
                error(where .. ' with different track_access', 0)
            end
            if opts.backend == 'vshard' and p.commit_wait ~= opts.commit_wait then
                error(where .. ' with different commit_wait', 0)
            end
            -- The evictor evicts per space.
            for _, k in ipairs({
                'enabled',
                'high',
                'low',
                'samples',
                'batch',
            }) do
                if p.evict[k] ~= opts.evict[k] then
                    error(where .. ' with different evict options', 0)
                end
            end
        end
    end
end

function M.validate(cfg)
    if cfg == nil then
        return
    end
    cfg_schema:validate(cfg)
    local sharding = sharding_roles()
    check_listen(cfg, sharding)
    check_spaces(cfg)
    for _, name in ipairs(sorted_keys(cfg)) do
        -- With the defaults filled in: the storages check it only when
        -- they apply it.
        local opts = module_opts(name, cfg[name])
        local err = load(SCHEMA).check_expire_mode(
            opts.storage,
            opts.expire.mode
        )
        if err ~= nil then
            fail(name, 'expire: %s', err)
        end
        local e = opts.evict
        if not (e.low < e.high) then
            fail(
                name,
                'evict.low (%s) must be less than evict.high (%s)',
                e.low,
                e.high
            )
        end
    end
    for _, name in ipairs(sorted_keys(cfg)) do
        local icfg = cfg[name]
        if is_vshard(icfg) and not is_frontend(icfg, sharding)
            and not sharding.storage then
            fail(
                name,
                "backend 'vshard' needs a vshard router: add "
                    .. "'router' to sharding.roles of this instance or set "
                    .. "vshard.router"
            )
        end
        if is_frontend(icfg, sharding) then
            -- What memcached.create() would reject.
            local ok, err = pcall(
                mcc().check,
                name,
                frontend_opts(name, icfg)
            )
            if not ok then
                fail(name, '%s', error_message(err))
            end
        end
    end
end

-- Applying the config -----------------------------------------------------

local function instance_alive(name, entry)
    local inst = entry.inst
    return inst.status ~= mcc().DESTROYED and mcc().get(name) == inst
end

local function must_recreate(entry, opts)
    local inst = entry.inst
    return opts.space_name ~= inst.space_name
        or opts.storage ~= inst.opts.storage
end

local function destroy(s, name)
    local entry = s.instances[name]
    s.instances[name] = nil
    if entry ~= nil and instance_alive(name, entry) then
        local ok, err = pcall(entry.inst.destroy, entry.inst)
        if not ok then
            log.error(
                "%s: destroying memcached instance '%s': %s",
                ROLE,
                name,
                error_message(err)
            )
        end
    end
end

local function apply_instance(s, name, opts)
    local entry = s.instances[name]
    if entry == nil then
        if mcc().get(name) ~= nil then
            error(
                ("memcached instance '%s' already exists and is not "
                    .. "managed by %s"):format(name, ROLE),
                0
            )
        end
        local inst = mcc().create(name, opts)
        s.instances[name] = {inst = inst, opts = opts}
        return
    end
    local inst = entry.inst
    inst:cfg(opts)
    entry.opts = opts
    -- Stopped by hand or failed to start or rebind: bring it back.
    if inst.status == mcc().STOPPED or inst.status == mcc().FAILED then
        inst:start()
    end
end

-- Listen URIs that move between instances in one config (a swap, a chain):
-- each rebind would find its new address still taken by another instance,
-- which only gives it up in its own rebind. So first release the listeners
-- of the running instances whose URI changes and that another instance of
-- the config wants, keeping their connections; their cfg() then binds the
-- new URI. Returns the names released.
local function release_moving(s, wanted)
    local moving = {}
    for name, entry in pairs(s.instances) do
        local opts = wanted[name]
        local inst = entry.inst
        if opts ~= nil and inst.status == mcc().RUNNING
            and tostring(opts.listen) ~= tostring(inst.opts.listen) then
            moving[name] = inst.opts.listen
        end
    end
    local released = {}
    for _, name in ipairs(sorted_keys(moving)) do
        for other, opts in pairs(wanted) do
            if other ~= name and listen_overlap(opts.listen, moving[name]) then
                local inst = s.instances[name].inst
                local ok, err = pcall(mcc().internal.release_listener, inst)
                if ok then
                    table.insert(released, name)
                else
                    log.error(
                        "%s: releasing the listener of memcached instance '%s': %s",
                        ROLE,
                        name,
                        error_message(err)
                    )
                end
                break
            end
        end
    end
    return released
end

-- The vshard storage side: serve the spaces of the vshard instances.
local function apply_storage(s, cfg, sharding)
    local spaces, names, with_metrics = {}, {}, false
    if sharding.storage then
        for _, name in ipairs(sorted_keys(cfg)) do
            if is_vshard(cfg[name]) then
                local opts = module_opts(name, cfg[name])
                spaces[opts.space_name] = {
                    storage = opts.storage,
                    track_access = opts.track_access,
                    commit_wait = opts.commit_wait,
                    expire = {
                        enabled = opts.expire.enabled,
                        batch = opts.expire.batch,
                        mode = opts.expire.mode,
                    },
                    evict = copy(opts.evict),
                }
                names[name] = opts.space_name
                with_metrics = with_metrics or opts.metrics == true
            end
        end
    end
    s.storage_spaces = names
    if with_metrics or s.metrics_storage then
        load(METRICS).storage(with_metrics)
        s.metrics_storage = with_metrics
    end
    if next(spaces) ~= nil then
        s.storage = true
        load(STORAGE).cfg({spaces = spaces})
    elseif s.storage then
        s.storage = false
        load(STORAGE).stop()
    end
end

function M.apply(cfg)
    cfg = cfg or {}
    local s = role_state()
    local sharding = sharding_roles()
    local errors = {}
    s.errors = {}

    local ok, err = pcall(apply_storage, s, cfg, sharding)
    if not ok then
        table.insert(errors, 'vshard storage: ' .. error_message(err))
    end

    local wanted = {}
    for name, icfg in pairs(cfg) do
        if is_frontend(icfg, sharding) then
            wanted[name] = frontend_opts(name, icfg)
        end
    end

    -- Destroy the instances removed from the config, the ones destroyed by
    -- hand and the ones to recreate, so that their ports are free.
    for _, name in ipairs(sorted_keys(s.instances)) do
        local entry = s.instances[name]
        local opts = wanted[name]
        if opts == nil or not instance_alive(name, entry)
            or must_recreate(entry, opts) then
            destroy(s, name)
        end
    end

    local function failed(name, ierr)
        ierr = error_message(ierr)
        s.errors[name] = s.errors[name] ~= nil
            and (s.errors[name] .. '; ' .. ierr)
            or ierr
        table.insert(
            errors,
            ("memcached instance '%s': %s"):format(
                name,
                ierr
            )
        )
    end

    local released = release_moving(s, wanted)
    for _, name in ipairs(sorted_keys(wanted)) do
        local iok, ierr = pcall(apply_instance, s, name, wanted[name])
        if not iok then
            failed(name, ierr)
        end
    end
    -- A released instance whose cfg() failed before its rebind listens on
    -- its URI again.
    for _, name in ipairs(released) do
        local entry = s.instances[name]
        if entry ~= nil and instance_alive(name, entry) then
            local rok, rerr = pcall(
                mcc().internal.restore_listener,
                entry.inst
            )
            if not rok then
                failed(name, rerr)
            end
        end
    end

    if #errors > 0 then
        error(table.concat(errors, '; '), 0)
    end
end

function M.stop()
    local s = role_state()
    for _, name in ipairs(sorted_keys(s.instances)) do
        destroy(s, name)
    end
    s.errors = {}
    s.storage_spaces = {}
    if s.metrics_storage then
        s.metrics_storage = false
        pcall(function() load(METRICS).storage(false) end)
    end
    if s.storage then
        s.storage = false
        load(STORAGE).stop()
    end
end

-- Health check ------------------------------------------------------------

local function tarantool_version()
    local major, minor = require('tarantool').version:match('^(%d+)%.(%d+)')
    return tonumber(major) * 100 + tonumber(minor)
end

-- Since 3.9 a role health check may return a result per subcheck
-- (multi_result).
local HAS_MULTI_RESULT = tarantool_version() >= 309

-- For tests: nil = by the Tarantool version, true or false to force.
M.internal = {multi_result = nil}

local READY, NOT_READY, DEGRADED = 'ready', 'not_ready', 'degraded'

local function frontend_health(name, entry, err)
    local inst = entry.inst
    local status = inst.status
    if mcc().get(name) ~= inst then
        status = mcc().DESTROYED
    end
    if status == mcc().RUNNING then
        if err ~= nil then
            return DEGRADED, 'the last config was not applied: ' .. err
        end
        return READY
    end
    local reason
    if status == mcc().WAITING then
        reason = ("waiting for the space '%s' to be replicated"):format(
            inst.space_name
        )
    elseif status == mcc().FAILED then
        reason = 'failed to start'
    else
        reason = status
    end
    if err ~= nil then
        reason = reason .. ': ' .. err
    end
    return NOT_READY, reason
end

local function storage_health(space_name)
    local space = box.space[space_name]
    if space ~= nil and space.index.bucket_id ~= nil then
        return READY
    end
    if box.info.ro then
        return NOT_READY,
            ("waiting for the space '%s' to be replicated to this storage"):format(
                space_name
            )
    end
    local s = package.loaded[STATE].storage
    local err = s ~= nil and s.last_error or nil
    return NOT_READY,
        ("the space '%s' is not created on this storage%s"):format(
            space_name,
            err and (': ' .. tostring(err)) or ''
        )
end

local function worse(a, b)
    local rank = {[READY] = 0, [DEGRADED] = 1, [NOT_READY] = 2}
    return rank[b] > rank[a] and b or a
end

-- name -> {status, reason?}. Doesn't yield: only reads the state.
local function results()
    local state = package.loaded[STATE]
    local s = state ~= nil and state.role or nil
    local res = {}
    if s == nil then
        return res
    end
    local function add(name, status, reason)
        local r = res[name]
        if r == nil then
            res[name] = {status = status, reason = reason}
            return
        end
        r.status = worse(r.status, status)
        if reason ~= nil then
            r.reason = r.reason ~= nil and (r.reason .. '; ' .. reason)
                or reason
        end
    end
    for name, entry in pairs(s.instances) do
        add(name, frontend_health(name, entry, s.errors[name]))
    end
    for name, err in pairs(s.errors) do
        if s.instances[name] == nil then
            add(name, NOT_READY, err)
        end
    end
    for name, space_name in pairs(s.storage_spaces) do
        add(name, storage_health(space_name))
    end
    return res
end

-- Called by the config module (3.8+); must not yield. On 3.9+: a result
-- per memcached instance, as subchecks role.roles.memcached.<name>.
-- Before: true, or false and the reasons.
function M.health_check()
    local res = results()
    local multi = M.internal.multi_result
    if multi == nil then
        multi = HAS_MULTI_RESULT
    end
    if multi then
        return res
    end
    local failed = {}
    for _, name in ipairs(sorted_keys(res)) do
        local r = res[name]
        if r.status ~= READY then
            table.insert(failed, ('%s: %s'):format(name, r.reason or r.status))
        end
    end
    if #failed == 0 then
        return true
    end
    return false,
        'memcached instances are not ready: ' .. table.concat(failed, '; ')
end

return M
