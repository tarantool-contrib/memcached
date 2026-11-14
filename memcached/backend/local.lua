-- The front-end in the TX thread: calls engine.exec() directly.
--
-- Backend contract (see DESIGN.md):
--   backend:exec(reqs) -> resps
--   backend:flush_all(delay) -> resp
--   backend:stats() -> table
-- plus backend:close(), called when the instance stops using the backend,
-- and backend:batch(), the C batch (backend/thread.lua has one too), and
-- backend:loop(), the C connection loop (this backend only).

local SELF = 'memcached.backend.local'
local ENGINE = 'memcached.engine'
local EVICT = 'memcached.evict'

local engine = require(ENGINE)

local M = {}

-- Methods are looked up in the current module on every call, so existing
-- backend objects use the new code after a reload.
local mt = {
    __index = function(_, k)
        return (package.loaded[SELF] or M)[k]
    end,
}

-- Raise if the options can't be served by this backend.
function M.validate(cfg)
    if cfg.read_mode ~= nil and cfg.read_mode ~= 'rw' then
        error(
            ("read_mode '%s' is not supported by the local backend"):format(
                cfg.read_mode
            ),
            0
        )
    end
end

-- cfg: the instance options plus name.
function M.new(cfg)
    M.validate(cfg)
    return setmetatable({
        name = cfg.name,
        -- 'memory' or 'disk': which box.cfg memory limit is limit_maxbytes.
        storage = cfg.storage,
        -- See engine.exec(): track_access = false, gets don't write;
        -- commit_wait, the commit wait mode of the writes.
        ctx = {
            space_name = cfg.space_name,
            track_access = cfg.track_access ~= false,
            commit_wait = cfg.commit_wait,
        },
    }, mt)
end

local function current_engine()
    return package.loaded[ENGINE] or engine
end

-- exec() keeps no reference to the reqs array after it returns: the
-- server reuses it (server.lua "The per-batch tables").
M.borrows_reqs = true

-- The responses, and the items the reads hold at the end (engine.exec()).
function M.exec(self, reqs)
    return current_engine().exec(self.ctx, reqs)
end

-- The C batch of the connection loop (server.lua conn_step()), see
-- engine.batch(); held: the items the reads before it hold.
function M.batch(self, input, output, max_commands, max_item_size, held)
    return current_engine().batch(
        self.ctx,
        input,
        output,
        max_commands,
        max_item_size,
        held
    )
end

-- The C connection loop of the server's connection fibers (server.lua
-- conn_step()), see engine.loop(). nil when it can't run here.
function M.loop(self, srv, conn, ibuf, obuf, idle, new_cmd, limits)
    return current_engine().loop(
        self.ctx,
        srv,
        conn,
        ibuf,
        obuf,
        idle,
        new_cmd,
        limits
    )
end

-- The C batch costs no more than a function call here: the server goes
-- back to it after the commands it leaves to Lua (server.lua conn_step()).
M.batch_reentry = true

function M.flush_all(self, delay)
    return current_engine().exec(self.ctx, {{op = 'flush', delay = delay}})[1]
end

function M.stats(self)
    local stats = current_engine().stats(self.ctx)
    -- As storage.stats() on the vshard storages.
    local key = self.storage == 'disk' and 'vinyl_memory' or 'memtx_memory'
    local memory = type(box.cfg) == 'table' and box.cfg[key]
    if memory then
        stats.limit_maxbytes = memory
    end
    -- The eviction counters of the space and whether it is evicted from.
    local evict = package.loaded[EVICT] or require(EVICT)
    for k, v in pairs(evict.stats(self.ctx.space_name)) do
        stats[k] = v
    end
    stats.evict = evict.enabled(self.ctx.space_name)
    return stats
end

function M.close(self)
end

return M
