-- Space format v3, the meta space and their creation. TX thread only.

local log = require('log')

local state = require('memcached.state')

local M = {}

M.META_SPACE = '_mcc_meta'

-- Field numbers of the data space.
M.KEY = 1
M.VALUE = 2
M.FLAGS = 3
M.EXPTIME = 4
M.CAS = 5
M.CREATED = 6
M.LAST_ACCESS = 7
M.FETCHED = 8
M.STALE = 9
M.BUCKET_ID = 10
-- Added in v3 (nullable: the tuples written before have 10 fields; a missing
-- token is false).
M.TOKEN = 11

M.FORMAT = {
    {name = 'key', type = 'string'},
    {name = 'value', type = 'string'},
    {name = 'flags', type = 'unsigned'},
    {name = 'exptime', type = 'number'},
    {name = 'cas', type = 'unsigned'},
    {name = 'created', type = 'number'},
    {name = 'last_access', type = 'number'},
    {name = 'fetched', type = 'boolean'},
    {name = 'stale', type = 'boolean'},
    {name = 'bucket_id', type = 'unsigned'},
    {name = 'token', type = 'boolean', is_nullable = true},
}

-- The fields of format v2: a space with only these is upgraded to v3.
local V2_FIELDS = 10

M.META_FORMAT = {
    {name = 'key', type = 'string'},
    {name = 'value', type = 'any'},
}

-- 'memory' -> memtx, 'disk' -> vinyl.
function M.engine(storage)
    return storage == 'disk' and 'vinyl' or 'memtx'
end

function M.flush_key(space_name)
    return 'flush:' .. space_name
end

-- Raise if an existing space can't be used as a data space: its format
-- must start with the fields of v2 (v3 adds 'token' after them). Returns
-- true when the space is in format v2 and needs the upgrade.
local function check_format(space)
    local format = space:format()
    if #format == 0 then
        return false
    end
    for i, f in ipairs(M.FORMAT) do
        local have = format[i]
        if have == nil and i > V2_FIELDS then
            return true
        end
        if have == nil or have.name ~= f.name then
            error(
                ("Space '%s' has an incompatible format: field %d "
                    .. "must be '%s'"):format(space.name, i, f.name),
                0
            )
        end
    end
    return false
end

-- Format v2 -> v3: add the nullable token field. The instance must be
-- writable. Tarantool checks the existing tuples once (they have 10
-- fields, so they all pass).
local function upgrade(space)
    local format = space:format()
    for i = V2_FIELDS + 1, #M.FORMAT do
        format[i] = M.FORMAT[i]
    end
    log.info(
        "memcached: space '%s': adding the field '%s' (format v3)",
        space.name,
        M.FORMAT[M.TOKEN].name
    )
    space:format(format)
end

-- Expiration modes (the instance option expire.mode, see expire.lua):
-- 'index': the data space has the exptime TREE index, which the sweeper
-- walks; 'scan': it has none (every write of a tuple then updates one
-- index less) and the sweeper scans the primary index with a cursor. Only
-- memtx scans (the cursor is a HASH iterator kept across yields), so vinyl
-- is always 'index'.
M.EXPIRE_MODES = {index = true, scan = true}

-- The expiration mode of an option value (nil: the default of the storage).
function M.expire_mode(storage, mode)
    if mode ~= nil then
        return mode
    end
    return storage == 'disk' and 'index' or 'scan'
end

-- The error of an expiration mode that the storage can't have, or nil.
function M.check_expire_mode(storage, mode)
    if mode == 'scan' and storage == 'disk' then
        return "mode 'scan' needs storage = 'memory': vinyl keeps the "
            .. "exptime index"
    end
    return nil
end

-- The data space if it exists and has its primary index, otherwise nil.
-- The exptime index is optional (expire.mode = 'scan').
function M.space(space_name)
    local space = box.space[space_name]
    if space == nil or space.index.primary == nil then
        return nil
    end
    return space
end

-- Bring the exptime index of a data space in line with the expiration
-- mode: create it for 'index', drop it for 'scan' (memtx only; a vinyl
-- space keeps it). The instance must be writable. A memtx index is built
-- with yields; a vinyl one reads the whole space.
local function align_exptime(space, mode)
    local index = space.index.exptime
    if mode == 'index' and index == nil then
        log.info(
            "memcached: space '%s': creating the exptime index "
                .. "(expire.mode = 'index')",
            space.name
        )
        space:create_index('exptime', {
            type = 'TREE',
            unique = false,
            parts = {{M.EXPTIME, 'number'}},
            if_not_exists = true,
        })
    elseif mode == 'scan' and index ~= nil and space.engine == 'memtx' then
        log.info(
            "memcached: space '%s': dropping the exptime index " .. "(expire.mode = 'scan')",
            space.name
        )
        index:drop()
    end
end

-- Create the meta space. The instance must be writable.
function M.create_meta()
    local space = box.schema.space.create(M.META_SPACE, {
        engine = 'memtx',
        format = M.META_FORMAT,
        if_not_exists = true,
    })
    space:create_index('primary', {
        type = 'TREE',
        parts = {{1, 'string'}},
        if_not_exists = true,
    })
    return space
end

-- Create the data space with its indexes and the meta space.
-- The instance must be writable. opts.vshard adds the bucket_id index;
-- opts.expire is the expiration mode: the exptime index is created for
-- 'index' and, on an existing space, dropped for 'scan'. nil: an
-- existing space keeps its exptime index, a new one gets the default mode
-- of the storage (expire_mode()).
function M.create(space_name, storage, opts)
    opts = opts or {}
    local err = M.check_expire_mode(storage, opts.expire)
    if err ~= nil then
        error(("Space '%s': %s"):format(space_name, err), 0)
    end
    local engine = M.engine(storage)
    local space = box.schema.space.create(space_name, {
        engine = engine,
        format = M.FORMAT,
        if_not_exists = true,
    })
    if check_format(space) then
        upgrade(space)
    end
    space:create_index('primary', {
        type = engine == 'memtx' and 'HASH' or 'TREE',
        parts = {{M.KEY, 'string'}},
        if_not_exists = true,
    })
    local mode = opts.expire
    if mode == nil then
        mode = space.index.exptime ~= nil and 'index' or M.expire_mode(storage)
    end
    align_exptime(space, mode)
    if opts.vshard then
        space:create_index('bucket_id', {
            type = 'TREE',
            unique = false,
            parts = {{M.BUCKET_ID, 'unsigned'}},
            if_not_exists = true,
        })
    end
    M.create_meta()
    return space
end

-- The data space, created when it is missing and the instance is writable.
-- Returns nil when the space is missing and the instance is read-only: it is
-- to arrive by replication. A space in format v2 is upgraded to v3 on a
-- writable instance; a read-only one uses it as it is (the engine reads a
-- missing token as false) and gets the upgrade by replication.
-- opts = {vshard = <bool>, expire = 'index' | 'scan' | nil}: with an
-- expiration mode, a writable instance creates or drops the exptime index
-- of an existing space to match it (align_exptime()); nil leaves an
-- existing space as it is and creates a new one in the default mode of the
-- storage. A read-only instance uses the indexes it has.
function M.ensure(space_name, storage, opts)
    local space = M.space(space_name)
    if box.info.ro then
        if space ~= nil then
            check_format(space)
        end
        return space
    end
    if space ~= nil then
        if check_format(space) then
            upgrade(space)
        end
        if box.space[M.META_SPACE] == nil then
            M.create_meta()
        end
        if opts ~= nil and opts.vshard and space.index.bucket_id == nil then
            M.create(space_name, storage, opts)
        elseif opts ~= nil and opts.expire ~= nil then
            local err = M.check_expire_mode(storage, opts.expire)
            if err ~= nil then
                error(("Space '%s': %s"):format(space_name, err), 0)
            end
            align_exptime(space, opts.expire)
        end
        return space
    end
    log.info("memcached: creating space '%s'", space_name)
    return M.create(space_name, storage, opts)
end

-- Flush times -----------------------------------------------------------
--
-- The engine needs the flush time of its space for every batch. Reading it
-- from _mcc_meta costs a key concat, an index get and a tuple (a cdata with
-- a finalizer) per batch, so it is cached in state.flush_cache:
--   {meta = <the _mcc_meta space object>, times = {[space_name] = time|false}}
-- The cache is emptied
-- - by an on_replace trigger of _mcc_meta, at once and again when the
--   transaction commits or rolls back: any change of the space, local, by
--   another fiber or applied by replication, is seen by the next lookup,
--   and a value read while the change was in progress (a dirty read
--   without MVCC) doesn't outlive it;
-- - by an on_replace trigger of _truncate (truncate fires no on_replace);
-- - when box.space._mcc_meta is another object (dropped, created again).
-- The triggers are named (the trigger module, Tarantool 3): they don't
-- depend on the space object, exist before the space does, and a reload
-- replaces them instead of adding more.

M.FLUSH_TRIGGER = 'memcached.flush_cache'

local trigger_ok, trigger = pcall(require, 'trigger')

-- Whether this copy of the module has set the triggers (a reload sets them
-- again, with its own functions).
local triggers_set = false

-- The C connection loops take the flush time once per call: they return
-- and take it again (engine.inputs_changed(), which this module can't
-- require).
local function invalidate()
    state.flush_cache.times = {}
    state.dp_gen = (state.dp_gen or 0) + 1
end

-- Never raises: an error here would fail the write (a flush, a replication
-- row, the recovery).
local function on_change()
    invalidate()
    pcall(box.on_commit, invalidate)
    pcall(box.on_rollback, invalidate)
end

-- Without both triggers there is no cache: flush_time() reads the space.
local function set_triggers()
    local ok, err = pcall(
        trigger.set,
        'box.space.' .. M.META_SPACE .. '.on_replace',
        M.FLUSH_TRIGGER,
        on_change
    )
    if ok then
        ok, err = pcall(
            trigger.set,
            'box.space._truncate.on_replace',
            M.FLUSH_TRIGGER,
            on_change
        )
    end
    if not ok then
        log.warn('memcached: no flush time cache: %s', tostring(err))
        trigger_ok = false
        return
    end
    triggers_set = true
end

-- Whether flush_time() sees every change at once (its cache is kept by the
-- triggers): the C connection loops take it once per call and rely on the
-- triggers to return (engine.loop()).
function M.flush_tracked()
    return trigger_ok and triggers_set
end

-- The flush time as stored, nil if none.
local function read_flush_time(meta, space_name)
    local t = meta:get(M.flush_key(space_name))
    return t ~= nil and t[2] or nil
end

-- The flush time of a data space, nil if none.
function M.flush_time(space_name)
    local meta = box.space[M.META_SPACE]
    if meta == nil then
        return nil
    end
    if not trigger_ok then
        return read_flush_time(meta, space_name)
    end
    if not triggers_set then
        set_triggers()
        if not trigger_ok then
            return read_flush_time(meta, space_name)
        end
    end
    local cache = state.flush_cache
    if cache.meta ~= meta then
        cache.meta = meta
        cache.times = {}
    end
    local times = cache.times
    local time = times[space_name]
    if time == nil then
        -- No yield between the read and the store (memtx).
        time = read_flush_time(meta, space_name) or false
        times[space_name] = time
    end
    return time or nil
end

return M
