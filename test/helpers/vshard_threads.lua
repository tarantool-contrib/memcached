-- A vshard cluster whose router runs the memcached front-ends in
-- application threads: test/helpers/vshard_cluster.lua plus an application
-- thread group on the router, and instance 'r' with threads = GROUP.
--
--   storages: rs-1 = {s1-a, s1-b}, rs-2 = {s2-a}, rs-3 = {s3-a} (opts.rs3)
--   router:   router, with the thread group GROUP of opts.nthreads threads

local t = require('luatest')
local cluster = require('luatest.cluster')

local env = require('test.helpers.env')
local vc = require('test.helpers.vshard_cluster')

local M = {}

M.GROUP = 'fe'
M.NTHREADS = 3

-- opts: those of vshard_cluster.config(), plus
--   nthreads    the size of the thread group, default NTHREADS;
--   in_threads  instance 'r' runs in the threads, default true;
--   rs3         add replicaset rs-3 = {s3-a}, with weight opts.rs3_weight
--               (default 1).
function M.config(opts)
    local config = vc.config(opts)
    local routers = config.groups.routers
    routers.threads = {
        groups = {{name = M.GROUP, size = opts.nthreads or M.NTHREADS}},
    }
    if opts.in_threads ~= false then
        routers.roles_cfg[vc.ROLE].instances[vc.NAME].threads = M.GROUP
    end
    if opts.rs3 then
        config.groups.storages.replicasets['rs-3'] = {
            leader = 's3-a',
            sharding = {weight = opts.rs3_weight or 1},
            instances = {['s3-a'] = {}},
        }
    end
    return config
end

-- Start the cluster as vshard_cluster.start() does, with M.config(opts).
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
    for _, name in ipairs(vc.STORAGES) do
        vc.wait_space(c[name])
    end
    vc.wait_instance(c)
    return c
end

-- Evaluate a Lua expression in every thread of the group on the router;
-- returns an array of the first results, by thread.
function M.thread_eval(c, expr, args)
    return c.router:exec(function(group, expr_, args_)
        local res = require('threads').eval(group, expr_, args_)
        local out = {}
        for i, r in ipairs(res) do
            out[i] = r[1]
        end
        return out
    end, {M.GROUP, expr, args or {}})
end

-- Client port -> thread id, for the connections of an instance in the
-- threads of the router.
function M.conn_threads(c, name)
    local list = M.thread_eval(
        c,
        [[
        local rec = require('memcached.state').thread_servers[...]
        local ports = {}
        if rec ~= nil then
            for _, conn in pairs(rec.srv.conns) do
                local peer = conn.sock:peer()
                if peer ~= nil then
                    table.insert(ports, peer.port)
                end
            end
        end
        return {thread_id = require('threads').info().thread_id,
                ports = ports}
    ]],
        {name or vc.NAME}
    )
    local res = {}
    for _, r in ipairs(list) do
        for _, port in ipairs(r.ports) do
            t.assert_equals(res[port], nil, 'a connection is in one thread')
            res[port] = r.thread_id
        end
    end
    return res
end

-- The vshard router of the instance's front-end in each thread: whether
-- it exists, its buckets and replicasets, and a marker field (set with
-- mark_routers) that tells whether it is the same object.
function M.thread_routers(c, name)
    return M.thread_eval(
        c,
        [[
        local rec = require('memcached.state').thread_servers[...]
        local backend = rec and rec.backend
        local router = backend and backend:router()
        if router == nil then
            return {exists = false}
        end
        local info = router:info()
        local rs = {}
        for id in pairs(router.replicasets) do
            table.insert(rs, id)
        end
        table.sort(rs)
        return {
            exists = true,
            name = router.name,
            available_rw = info.bucket.available_rw,
            unknown = info.bucket.unknown,
            replicasets = rs,
            mark = rawget(router, 'mcc_test_mark'),
            sync_timeout = router.current_cfg.sync_timeout,
            resends = backend.resends,
            thread_id = require('threads').info().thread_id,
        }
    ]],
        {name or vc.NAME}
    )
end

-- Set a marker field on the router of each thread.
function M.mark_routers(c, mark, name)
    M.thread_eval(
        c,
        [[
        local name, mark = ...
        local rec = require('memcached.state').thread_servers[name]
        rawset(rec.backend:router(), 'mcc_test_mark', mark)
    ]],
        {name or vc.NAME, mark}
    )
end

-- The total of the batches the storage masters have served.
function M.storage_batches(c, masters)
    local n = 0
    for _, name in ipairs(masters or {'s1-a', 's2-a'}) do
        n = n + vc.space_info(c[name]).batches
    end
    return n
end

return M
