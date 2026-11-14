-- A vshard cluster for the tests: Tarantool 3 config with a sharding
-- section, started by luatest.cluster.
--
--   storages: rs-1 = {s1-a (master), s1-b (replica)}, rs-2 = {s2-a}
--   router:   router, running memcached instance 'r' with backend = 'vshard'
--
-- The storages and the router run test/helpers/vshard_role.lua.

local t = require('luatest')
local cbuilder = require('luatest.cbuilder')
local cluster = require('luatest.cluster')

local env = require('test.helpers.env')

local M = {}

M.ROLE = 'test.helpers.vshard_role'
M.NAME = 'r'
M.SPACE = '__mcc_r'
M.STORAGES = {'s1-a', 's1-b', 's2-a'}
M.MASTERS = {['rs-1'] = 's1-a', ['rs-2'] = 's2-a'}

-- opts:
--   port              memcached port on the router (required);
--   bucket_count      default 30;
--   timeout           vshard.timeout of the instance;
--   rebalancer_mode   default 'off';
--   rebalancer_max_sending  default: vshard's (1);
--   weights           {[replicaset] = weight};
--   leaders           {[replicaset] = instance}, default MASTERS;
--   spaces            more storage spaces: {[space_name] = options}.
function M.config(opts)
    local spaces = {[M.SPACE] = {storage = 'memory'}}
    for name, conf in pairs(opts.spaces or {}) do
        spaces[name] = conf
    end
    local storage_cfg = {storage = {spaces = spaces}}
    local router_cfg = {
        instances = {
            [M.NAME] = {
                listen = '127.0.0.1:' .. opts.port,
                backend = 'vshard',
                vshard = {timeout = opts.timeout or 2},
            },
        },
    }
    local weights = opts.weights or {}
    local leaders = opts.leaders or {}
    local builder = cbuilder:new()
        :set_global_option('credentials.users.storage.roles', {'sharding'})
        :set_global_option('credentials.users.storage.password', 'secret')
        :set_global_option('iproto.advertise.sharding.login', 'storage')
        :set_global_option('replication.failover', 'manual')
        :set_global_option('sharding.bucket_count', opts.bucket_count or 30)
        :set_global_option(
            'sharding.rebalancer_mode',
            opts.rebalancer_mode or 'off'
        )
    if opts.rebalancer_max_sending ~= nil then
        builder:set_global_option(
            'sharding.rebalancer_max_sending',
            opts.rebalancer_max_sending
        )
    end
    builder
        :use_group('storages')
        :set_group_option('sharding.roles', {'storage'})
        :set_group_option('roles', {M.ROLE})
        :set_group_option('roles_cfg', {[M.ROLE] = storage_cfg})
        :use_replicaset('rs-1')
        :set_replicaset_option('leader', leaders['rs-1'] or 's1-a')
        :set_replicaset_option('sharding.weight', weights['rs-1'] or 1)
        :add_instance('s1-a', {})
        :add_instance('s1-b', {})
        :use_replicaset('rs-2')
        :set_replicaset_option('leader', leaders['rs-2'] or 's2-a')
        :set_replicaset_option('sharding.weight', weights['rs-2'] or 1)
        :add_instance('s2-a', {})
        :use_group('routers')
        :set_group_option('sharding.roles', {'router'})
        :set_group_option('roles', {M.ROLE})
        :set_group_option('roles_cfg', {[M.ROLE] = router_cfg})
        :use_replicaset('router')
        :set_replicaset_option('leader', 'router')
        :add_instance('router', {})
    return builder:config()
end

-- Start the cluster, bootstrap the buckets and wait until the router knows
-- all of them and the storages have the space.
function M.start(opts)
    local c = cluster:new(
        M.config(opts),
        {env = env.lua_env()},
        {auto_cleanup = false}
    )
    c:start()
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
    for _, name in ipairs(M.STORAGES) do
        M.wait_space(c[name])
    end
    M.wait_instance(c)
    return c
end

-- Wait until the data space with its bucket_id index is on the instance.
function M.wait_space(server)
    server:exec(function(space_name)
        t.helpers.retrying({timeout = 30}, function()
            local space = box.space[space_name]
            t.assert_not_equals(space, nil)
            t.assert_not_equals(space.index.bucket_id, nil)
        end)
    end, {M.SPACE})
end

function M.wait_instance(c)
    c.router:exec(function(name)
        t.helpers.retrying({timeout = 30}, function()
            local inst = require('memcached').get(name)
            t.assert_not_equals(inst, nil)
            t.assert_equals(inst.status, 'running')
        end)
    end, {M.NAME})
end

-- The bucket of a key and the replicaset it is on, by the router.
function M.route(c, key)
    return c.router:exec(function(key_)
        local vshard = require('vshard')
        local bid = vshard.router.bucket_id_strcrc32(key_)
        local rs = vshard.router.route(bid)
        return bid, rs and rs.id
    end, {key})
end

-- n keys "<prefix><i>" stored on the replicaset.
function M.keys_on(c, rs_id, n, prefix)
    return c.router:exec(function(rs_id_, n_, prefix_)
        local vshard = require('vshard')
        local keys = {}
        local i = 0
        while #keys < n_ do
            i = i + 1
            local key = prefix_ .. i
            local rs = vshard.router.route(
                vshard.router.bucket_id_strcrc32(key)
            )
            if rs.id == rs_id_ then
                table.insert(keys, key)
            end
        end
        return keys
    end, {rs_id, n, prefix or 'k'})
end

-- The tuple of a key on an instance as a map, or nil.
function M.tuple(server, key)
    return server:exec(function(space_name, key_)
        local tuple = box.space[space_name]:get(key_)
        return tuple and tuple:tomap({names_only = true}) or nil
    end, {M.SPACE, key})
end

-- storage.info().spaces[space_name or SPACE] of an instance.
function M.space_info(server, space_name)
    return server:exec(function(space_name_)
        return require('memcached.storage').info().spaces[space_name_]
    end, {space_name or M.SPACE})
end

-- vshard.storage.info().bucket of a storage: {active, sending, receiving,
-- garbage, pinned, total}.
function M.buckets(server)
    return server:exec(function()
        return require('vshard').storage.info().bucket
    end)
end

return M
