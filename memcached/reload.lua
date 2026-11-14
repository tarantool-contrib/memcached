-- Hot reload of the Lua code: mcc.reload().
--
-- In TX, reload():
-- 1. runs the ensure() of the new state.lua on the live state (the state
--    module itself is never reloaded), so that the new code finds the
--    fields it adds; then drops every loaded memcached.* module
--    except the state from package.loaded and requires them again; if any
--    of them fails to load, the old modules are put back and the error is
--    raised;
-- 2. re-points the instances to the new methods (this and the next steps
--    run with the new code: M.restart);
-- 3. re-exports the TX functions for the application threads;
-- 4. restarts the accept loops of the TX front-ends;
-- 5. restarts the expiration sweepers (keeping their counters) and the
--    evictor (its spaces and counters are in the state);
-- 6. reloads the code in the application threads that run front-ends
--    (threads.eval), which restart their accept loops too;
-- 7. restarts the read view refreshers (keeping their read views).
-- Listeners, connections, counters and read views live in the state, so
-- they survive. Connection fibers look the code up once per batch: their
-- next batch runs the new code.
--
-- The new code is found as require() finds it in TX at the moment of the
-- reload: the threads get TX's package.path and package.cpath (the search
-- root, package.searchroot(), is the same in every thread).

local fiber = require('fiber')

local state = require('memcached.state')

local M = {}

local STATE = 'memcached.state'

M.PREFIX = 'memcached'
-- Modules that are never reloaded.
M.KEEP = {[STATE] = true}

local function error_message(err)
    if type(err) == 'cdata' or type(err) == 'table' then
        err = err.message or tostring(err)
    end
    return tostring(err)
end

local function failed(err)
    error(
        ('memcached: reload failed, the old code is kept: %s'):format(
            error_message(err)
        ),
        0
    )
end

-- The loader of a module, as require() finds it. Raises if there is none or
-- the file doesn't compile.
local function find_loader(name)
    local errors = {}
    for _, searcher in ipairs(package.loaders) do
        local loader = searcher(name)
        if type(loader) == 'function' then
            return loader
        elseif type(loader) == 'string' then
            table.insert(errors, loader)
        end
    end
    error(("module '%s' not found:%s"):format(name, table.concat(errors)), 0)
end

-- Add the fields that the new state.lua knows to the live state, with the
-- ensure() of the new state.lua.
function M.upgrade_state()
    local fresh = find_loader(STATE)(STATE)
    if type(fresh) == 'table' and type(fresh.ensure) == 'function' then
        fresh.ensure(state)
    end
end

-- Names of the loaded modules to reload, sorted.
function M.loaded_modules()
    local names = {}
    local prefix = M.PREFIX .. '.'
    for name in pairs(package.loaded) do
        if type(name) == 'string' and not M.KEEP[name]
            and (name == M.PREFIX or name:sub(1, #prefix) == prefix) then
            table.insert(names, name)
        end
    end
    table.sort(names)
    return names
end

-- Drop the modules and require them again. On errors, restore the old ones
-- and raise. Returns the names.
function M.reload_modules()
    local ok, err = pcall(M.upgrade_state)
    if not ok then
        failed(err)
    end
    local names = M.loaded_modules()
    local old = {}
    for _, name in ipairs(names) do
        old[name] = package.loaded[name]
        package.loaded[name] = nil
    end
    for _, name in ipairs(names) do
        if package.loaded[name] == nil then
            ok, err = pcall(require, name)
            if not ok then
                -- Modules loaded so far are dropped as well.
                for _, n in ipairs(M.loaded_modules()) do
                    package.loaded[n] = nil
                end
                for n, mod in pairs(old) do
                    package.loaded[n] = mod
                end
                failed(err)
            end
        end
    end
    return names
end

-- Restart the sweepers with the new code, keeping their counters.
local function restart_sweepers()
    local expire = require('memcached.expire')
    local list = {}
    for name, rec in pairs(state.sweepers) do
        list[name] = rec
    end
    local n = 0
    for name, rec in pairs(list) do
        local new = expire.start(name, rec.space_name, {batch = rec.batch})
        new.expired = rec.expired or 0
        new.flushed = rec.flushed or 0
        new.checked = rec.checked or 0
        new.passes = rec.passes or 0
        new.scanned_flush = rec.scanned_flush
        n = n + 1
    end
    return n
end

-- Steps 2 to 7 of the reload in TX, with the new code. Returns a summary.
function M.restart(modules)
    local summary = {modules = modules}
    local mcc = require('memcached')
    local server = require('memcached.server')
    local threads = require('memcached.threads')
    local readview = require('memcached.readview')
    local n = 0
    for _, inst in pairs(state.instances) do
        setmetatable(inst, mcc.instance_mt)
        n = n + 1
    end
    summary.instances = n
    threads.export_tx()
    for _, inst in pairs(state.instances) do
        if inst.server ~= nil then
            server.restart_accept(inst.server)
        end
    end
    -- The connections in the C loop (server.lua conn_step()) return to
    -- the new code at their next batch.
    require('memcached.engine').inputs_changed()
    summary.sweepers = restart_sweepers()
    summary.evictor = require('memcached.evict').restart()
    summary.threads = threads.reload_threads()
    summary.refreshers = readview.restart_all()
    return summary
end

-- The reload in TX. Returns a summary.
function M.reload()
    if state.reload_in_progress then
        error('memcached: a reload is already in progress', 0)
    end
    state.reload_in_progress = true
    local ok, res = pcall(function()
        local modules = M.reload_modules()
        -- The rest runs with the new code (if it has it).
        local new = require('memcached.reload')
        return (new.restart or M.restart)(modules)
    end)
    state.reload_in_progress = false
    if not ok then
        error(res, 0)
    end
    return res
end

-- The reload in an application thread (threads.RELOAD_EVAL).
function M.reload_thread()
    local names = M.reload_modules()
    local res = require('memcached.threads').fe.after_reload()
    res.modules = #names
    return res
end

-- Number of the fibers named mcc/* in this thread (for tests).
function M.fiber_count()
    local n = 0
    for _, f in pairs(fiber.info()) do
        if tostring(f.name):find('^mcc/') then
            n = n + 1
        end
    end
    return n
end

return M
