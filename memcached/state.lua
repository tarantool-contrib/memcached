-- State that survives hot reload.
--
-- This module is stored in package.loaded['memcached.state'] and is
-- never dropped by mcc.reload(): every other module keeps its mutable state
-- here and only constants and functions in its own upvalues.
--
-- Every field is created by ensure() so that a newer version of the code can
-- add fields to a state created by an older one: mcc.reload() runs the
-- ensure() of the new state.lua on the live state (reload.lua).
--
-- Each thread has its own state: TX, and every application thread that
-- runs front-ends.

local M = {}

local DEFAULTS = {
    -- name -> instance object (init.lua).
    instances = {},
    -- name -> sweeper record (expire.lua).
    sweepers = {},
    -- Next value of the per-process CAS counter, uint64_t cdata or nil
    -- before the first use (engine.lua).
    cas_counter = false,
    -- The last timestamp given to a store or a flush (engine.lua), see
    -- engine.stamp().
    stamp_last = 0,
    -- space name -> {key -> {cas, created}}: the meta "win" tokens handed
    -- out while the instance was read-only, which can't be written to the
    -- item's token field (engine.lua). Only valid while the item keeps
    -- that CAS and created stamp.
    tokens = {},
    -- TX: name -> true for the functions exported to the application
    -- threads (threads.lua); threads.export() can't replace them.
    tx_exports = {},
    -- TX: application thread group name -> true for the groups that run
    -- front-ends, to reload (threads.lua).
    thread_groups = {},
    -- TX: memcached.ipc channel id -> the record of the fibers that serve
    -- it, for a thread backend object (threads.lua "The IPC").
    ipc_channels = {},
    -- An application thread: instance name -> front-end record {name, srv,
    -- backend, cfg, key} (threads.lua).
    thread_servers = {},
    -- instance name -> read view record (readview.lua): in TX, the
    -- refresher {name, rv, id, opened, cache, interval, cond, fiber,
    -- stopped}; in a thread, the published read view {name, rv, id, cache}.
    readviews = {},
    -- TX: read view id -> {rv, fiber}, the read views opened for the dumps
    -- in the application threads until they take them (readview.lua).
    dump_readviews = {},
    -- Whether mcc.reload() is running (reload.lua).
    reload_in_progress = false,
    -- TX: the cached flush times {meta = <_mcc_meta space object>, times =
    -- {space name -> flush time or false}} (schema.lua).
    flush_cache = {},
    -- TX: the read-only state as last reported by the box.status watcher
    -- {watcher = <handle>, is_ro = <bool or nil>} (engine.lua).
    ro_cache = {},
    -- TX: whether the C data path is switched off (engine.lua): by
    -- memcached.datapath(false), or from the start by the environment
    -- variable MEMCACHED_DATAPATH=lua.
    datapath_off = os.getenv('MEMCACHED_DATAPATH') == 'lua',
    -- TX: whether the C connection loop is switched off (engine.loop()),
    -- the C batch serving the commands from the Lua loop: by
    -- memcached.datapath_loop(false), or MEMCACHED_DATAPATH=batch.
    cloop_off = os.getenv('MEMCACHED_DATAPATH') == 'batch',
    -- TX: the generation of the inputs of the C connection loops
    -- (engine.inputs_changed()).
    dp_gen = 0,
    -- Whether the vshard backend has checked the version of the vshard it
    -- loaded (backend/vshard.lua), so that it warns once per thread.
    vshard_version_checked = false,
}

-- Add the missing fields to target (this state by default).
function M.ensure(target)
    target = target or M
    for k, v in pairs(DEFAULTS) do
        if target[k] == nil then
            if type(v) == 'table' then
                target[k] = {}
            else
                target[k] = v
            end
        end
    end
    return target
end

M.ensure()

return M
