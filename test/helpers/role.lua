-- Helpers for the roles.memcached tests: clusters started by
-- luatest.cluster, config reloads, instance status, health checks, and the
-- vshard cluster of test/helpers/vshard_cluster.lua configured through the
-- role only.

local t = require('luatest')
local cluster = require('luatest.cluster')

local env = require('test.helpers.env')
local vc = require('test.helpers.vshard_cluster')

local M = {}

M.ROLE = 'roles.memcached'
-- The health check of the role; the instances are its subchecks.
M.CHECK = 'role.' .. M.ROLE

-- Start a cluster; the tests of a group share it.
function M.start(config)
    local c = cluster:new(
        config,
        {env = env.lua_env()},
        {auto_cleanup = false}
    )
    c:start()
    return c
end

-- Write the config (if given) and reload it on the servers, in order.
function M.reload(c, config, servers)
    if config ~= nil then
        c:sync(config)
    end
    for _, server in ipairs(servers) do
        server:exec(function()
            require('config'):reload()
        end)
    end
end

-- Write the config and reload it on a server; returns the error or nil.
function M.reload_error(c, config, server)
    c:sync(config)
    return server:exec(function()
        local ok, err = pcall(require('config').reload, require('config'))
        return not ok and tostring(err) or nil
    end)
end

-- The status of a memcached instance, or nil if there is none.
function M.status(server, name)
    return server:exec(function(name_)
        local inst = require('memcached').get(name_)
        return inst and inst.status or nil
    end, {name})
end

function M.wait_status(server, name, status)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(M.status(server, name), status)
    end)
end

-- The readiness checks of a server: check name -> {status, reason}.
function M.checks(server)
    return server:exec(function()
        return require('internal.healthcheck').info().readiness.checks
    end)
end

-- The readiness of a memcached instance (a subcheck of the role's check),
-- or of the role's check itself if name is nil.
function M.health(server, name)
    local checks = M.checks(server)
    return checks[name and (M.CHECK .. '.' .. name) or M.CHECK]
end

-- The check names of the role on a server, sorted.
function M.check_names(server)
    local names = {}
    for name in pairs(M.checks(server)) do
        if name == M.CHECK or name:sub(1, #M.CHECK + 1) == M.CHECK .. '.' then
            table.insert(names, name)
        end
    end
    table.sort(names)
    return names
end

-- vshard -------------------------------------------------------------------

-- The config of test/helpers/vshard_cluster.lua with the role instead of
-- the test-only one: both groups get the same roles_cfg, opts.instances
-- (default: the router's instance vc.NAME with backend = 'vshard').
function M.vshard_config(opts)
    local config = vc.config(opts)
    local instances = opts.instances
        or {
            [vc.NAME] = {
                listen = '127.0.0.1:' .. opts.port,
                backend = 'vshard',
                vshard = {timeout = opts.timeout or 2},
            },
        }
    for _, group in ipairs({'storages', 'routers'}) do
        local g = config.groups[group]
        g.roles = opts.roles and opts.roles[group] or {M.ROLE}
        g.roles_cfg = {[M.ROLE] = instances}
    end
    return config
end

-- Start the cluster, bootstrap the buckets, wait for the space on the
-- storages and the instance on the router (as vc.start does).
function M.vshard_start(opts)
    local c = M.start(M.vshard_config(opts))
    c.router:exec(function()
        local vshard = require('vshard')
        t.helpers.retrying({timeout = 60}, function()
            local ok, err = vshard.router.bootstrap({
                timeout = 10,
                if_not_bootstrapped = true,
            })
            t.assert(ok, tostring(err))
        end)
        t.helpers.retrying({timeout = 60}, function()
            local info = vshard.router.info()
            t.assert_equals(info.bucket.unknown, 0)
            t.assert_equals(
                info.bucket.available_rw,
                vshard.router.bucket_count()
            )
        end)
    end)
    for _, name in ipairs(vc.STORAGES) do
        vc.wait_space(c[name])
    end
    vc.wait_instance(c)
    return c
end

-- Metrics ------------------------------------------------------------------

-- metrics.collect() of a server, with the callbacks: an array of
-- {metric_name, label_pairs, value}. The global labels (Tarantool 3 config
-- sets alias) are left out.
function M.collect(server)
    return server:exec(function()
        local global = {alias = true}
        local res = {}
        for _, obs in ipairs(
            require('metrics').collect({
                invoke_callbacks = true,
            })
        ) do
            local labels = {}
            for k, v in pairs(obs.label_pairs) do
                if not global[k] then
                    labels[k] = v
                end
            end
            table.insert(res, {
                metric_name = obs.metric_name,
                label_pairs = labels,
                value = obs.value,
            })
        end
        return res
    end)
end

-- The value of a series: labels must match exactly.
function M.value(observations, metric_name, labels)
    for _, obs in ipairs(observations) do
        if obs.metric_name == metric_name then
            local same = true
            for k, v in pairs(labels) do
                if obs.label_pairs[k] ~= v then
                    same = false
                end
            end
            for k in pairs(obs.label_pairs) do
                if labels[k] == nil then
                    same = false
                end
            end
            if same then
                return obs.value
            end
        end
    end
    return nil
end

-- The series of a metric whose labels include these ones.
function M.series(observations, metric_name, labels)
    local res = {}
    for _, obs in ipairs(observations) do
        if obs.metric_name == metric_name then
            local match = true
            for k, v in pairs(labels or {}) do
                if obs.label_pairs[k] ~= v then
                    match = false
                end
            end
            if match then
                table.insert(res, obs)
            end
        end
    end
    return res
end

return M
