-- A test-only Tarantool 3 role for the vshard tests; the real role is
-- roles/memcached.lua.
--
--   roles_cfg['test.helpers.vshard_role'] = {
--       -- On the storages: memcached.storage.cfg().
--       storage = {spaces = {[<space_name>] = {storage = 'memory'}}},
--       -- Memcached instances: created on the first apply, then cfg().
--       instances = {[<name>] = <instance options>},
--   }

local M = {}

local function instances(cfg)
    return cfg.instances or {}
end

function M.validate(cfg)
    if type(cfg) ~= 'table' then
        error('test.helpers.vshard_role: the config must be a table', 0)
    end
end

function M.apply(cfg)
    if cfg.storage ~= nil then
        require('memcached.storage').cfg(cfg.storage)
    end
    local mcc = require('memcached')
    for name, opts in pairs(instances(cfg)) do
        local inst = mcc.get(name)
        if inst == nil then
            mcc.create(name, opts)
        else
            inst:cfg(opts)
        end
    end
end

function M.stop()
    local mcc = require('memcached')
    local state = require('memcached.state')
    for name in pairs(state.instances) do
        mcc.destroy(name)
    end
    require('memcached.storage').stop()
end

return M
