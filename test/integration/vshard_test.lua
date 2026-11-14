-- vshard: the memcached front-end on a router (backend = 'vshard') with the
-- data on two storage replicasets (memcached/storage.lua).
--
-- The cluster is in test/helpers/vshard_cluster.lua: rs-1 = {s1-a, s1-b},
-- rs-2 = {s2-a} and a router running memcached instance 'r'.

local fiber = require('fiber')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')
local vc = require('test.helpers.vshard_cluster')

local SPACE = vc.SPACE
local BIG_CAS = '1000000000000000000'

-- Send a request and expect exactly this reply.
local function check(c, request, reply)
    c:send(request)
    local got = #reply > 0 and c:read(#reply) or ''
    t.assert_equals(got, reply, request)
end

-- "stats" as a map.
local function read_stats(c, args)
    c:send(args and ('stats ' .. args .. '\r\n') or 'stats\r\n')
    local stats = {}
    while true do
        local line = c:line()
        t.assert_not_equals(line, nil)
        if line == 'END\r\n' then
            break
        end
        local name, value = line:match('^STAT (%S+) (.-)\r\n$')
        t.assert_not_equals(name, nil, line)
        stats[name] = value
    end
    return stats
end

local function reset_space(cluster)
    for _, name in ipairs({'s1-a', 's2-a'}) do
        cluster[name]:exec(function(space)
            box.space[space]:truncate()
            box.space._mcc_meta:delete('flush:' .. space)
        end, {SPACE})
    end
end

local function create_instance(cluster, name, opts)
    cluster.router:exec(function(name_, opts_)
        require('memcached').create(name_, opts_)
    end, {name, opts})
end

local function destroy_instances(cluster, names)
    cluster.router:exec(function(names_)
        local mcc = require('memcached')
        for _, name in ipairs(names_) do
            if mcc.get(name) ~= nil then
                mcc.destroy(name)
            end
        end
    end, {names})
end

local function batches(server)
    return vc.space_info(server).batches
end

-- A key "<prefix><i>" in the bucket.
local function key_in_bucket(cluster, bid, prefix)
    return cluster.router:exec(function(bid_, prefix_)
        local vshard = require('vshard')
        local i = 0
        while true do
            i = i + 1
            local key = prefix_ .. i
            if vshard.router.bucket_id_strcrc32(key) == bid_ then
                return key
            end
        end
    end, {bid, prefix})
end

local function bucket_send(server, bid, dst)
    server:exec(function(bid_, dst_)
        t.helpers.retrying({timeout = 30}, function()
            local ok, err = require('vshard').storage.bucket_send(
                bid_,
                dst_,
                {timeout = 10}
            )
            t.assert(ok, tostring(err))
        end)
    end, {bid, dst})
end

--
-- Commands through the router.
--

local g = t.group('vshard')

-- Instances a test may create on the router.
local EXTRA = {'ro', 'bro', 'bad', 'nort'}

g.before_all(function(cg)
    cg.port = env.free_port()
    cg.cluster = vc.start({port = cg.port})
    -- Keys on each replicaset.
    cg.a = vc.keys_on(cg.cluster, 'rs-1', 20, 'a')
    cg.b = vc.keys_on(cg.cluster, 'rs-2', 20, 'b')
end)

g.after_all(function(cg)
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)

g.before_each(function(cg)
    reset_space(cg.cluster)
    cg.c = client.must_connect(cg.port)
end)

g.after_each(function(cg)
    cg.c:close()
    for _, c in ipairs(cg.extra or {}) do
        c:close()
    end
    cg.extra = nil
    destroy_instances(cg.cluster, EXTRA)
end)

local function connect(cg, port)
    local c = client.must_connect(port)
    cg.extra = cg.extra or {}
    table.insert(cg.extra, c)
    return c
end

g.test_router_side = function(cg)
    -- No local space and no sweeper on the router.
    local info = cg.cluster.router:exec(function(name, space)
        local inst = require('memcached').get(name)
        local sweeper = false
        for _, f in pairs(require('fiber').info({backtrace = false})) do
            if f.name:find('mcc/expire/', 1, true) then
                sweeper = true
            end
        end
        return {
            status = inst.status,
            space = box.space[space] ~= nil,
            sweeper = sweeper,
            expired = inst:info().expired,
        }
    end, {vc.NAME, SPACE})
    t.assert_equals(info, {
        status = 'running',
        space = false,
        sweeper = false,
    })
end

g.test_storage_side = function(cg)
    for _, name in ipairs(vc.STORAGES) do
        local res = cg.cluster[name]:exec(function(space)
            local mcc = rawget(_G, 'mcc')
            return {
                info = require('memcached.storage').info(),
                exported = type(mcc.storage_exec) == 'function'
                    and type(mcc.storage_flush) == 'function'
                    and type(mcc.storage_stats) == 'function',
                -- Plain Lua globals, no _func entries.
                func = box.func['mcc.storage_exec'] ~= nil,
                bucket_index = box.space
                    [space]
                    .index
                    .bucket_id
                    .parts
                    [1]
                    .fieldno,
                -- expire.mode = 'scan' by default: no exptime index, on
                -- the replica too (replicated).
                exptime_index = box.space[space].index.exptime ~= nil,
            }
        end, {SPACE})
        local replica = name == 's1-b'
        t.assert_equals(res.info.ro, replica, name)
        t.assert_equals(res.info.spaces[SPACE].ready, true, name)
        -- The sweeper runs on the masters only.
        t.assert_equals(res.info.spaces[SPACE].sweeper, not replica, name)
        t.assert(res.exported, name)
        t.assert_equals(res.func, false, name)
        t.assert_equals(res.bucket_index, 10, name)
        t.assert_equals(res.exptime_index, false, name)
        t.assert_equals(res.info.spaces[SPACE].expire_mode, 'scan', name)
    end

    -- Expiration on the storages.
    local c = cg.c
    t.assert_equals(c:set(cg.a[1], 'x', {exptime = 1}), 'STORED\r\n')
    t.assert_equals(c:set(cg.b[1], 'y', {exptime = 1}), 'STORED\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(vc.tuple(cg.cluster['s1-a'], cg.a[1]), nil)
        t.assert_equals(vc.tuple(cg.cluster['s2-a'], cg.b[1]), nil)
    end)
    t.assert_ge(vc.space_info(cg.cluster['s1-a']).expired, 1)
    t.assert_ge(vc.space_info(cg.cluster['s2-a']).expired, 1)
end

-- The storages run the classic get and set on the C data path when it is
-- built (env.datapath_so): a hit comes back to the router as the encoded VALUE block.
g.test_datapath = function(cg)
    local dp = cg.cluster['s1-a']:exec(function()
        return require('memcached').datapath()
    end)
    t.assert_equals(dp.available, require('fio').path.exists(env.datapath_so))
    local key = cg.a[1]
    local res = cg.cluster.router:exec(function(name, key_)
        local backend = require('memcached').get(name).backend
        local resps = backend:exec({
            {
                op = 'store',
                mode = 'set',
                key = key_,
                value = 'v',
                flags = 3,
                exptime = 0,
                fast = 'set',
            },
            {op = 'get', key = key_, fast = 'get'},
            {op = 'get', key = key_ .. '-nope', fast = 'get'},
        })
        return {resps[1].status, resps[2], resps[3].status}
    end, {vc.NAME, key})
    t.assert_equals(res[1], 'STORED')
    -- Off with MEMCACHED_DATAPATH=lua.
    if dp.enabled then
        t.assert_equals(res[2], 'VALUE ' .. key .. ' 3 1\r\nv\r\n')
    else
        t.assert_equals(res[2].status, 'HIT')
    end
    t.assert_equals(res[3], 'MISS')
    check(
        cg.c,
        'get ' .. key .. ' ' .. cg.b[1] .. '\r\n',
        'VALUE ' .. key .. ' 3 1\r\nv\r\nEND\r\n'
    )
end

g.test_classic_commands = function(cg)
    local c = cg.c
    for _, rs in ipairs({'rs-1', 'rs-2'}) do
        local keys = rs == 'rs-1' and cg.a or cg.b
        local k, n, m = keys[1], keys[2], keys[3]
        local master = cg.cluster[vc.MASTERS[rs]]
        local other = cg.cluster[rs == 'rs-1' and 's2-a' or 's1-a']

        t.assert_equals(c:set(k, 'v1', {flags = 3}), 'STORED\r\n')
        local value, flags, cas = c:get(k)
        t.assert_equals({value, flags}, {'v1', 3})
        -- The tuple is on the replicaset of its bucket, with the bucket id.
        local bid = vc.route(cg.cluster, k)
        local tuple = vc.tuple(master, k)
        t.assert_equals(tuple.value, 'v1')
        t.assert_equals(tuple.bucket_id, bid)
        t.assert_equals(vc.tuple(other, k), nil)

        -- cas
        check(c, ('cas %s 0 0 2 %s\r\nv2\r\n'):format(k, cas), 'STORED\r\n')
        check(c, ('cas %s 0 0 2 %s\r\nv3\r\n'):format(k, cas), 'EXISTS\r\n')
        check(c, ('cas %s 0 0 2 1\r\nv3\r\n'):format(n), 'NOT_FOUND\r\n')
        local _, _, cas2 = c:get(k)
        t.assert_gt(tonumber64(cas2), tonumber64(cas))

        -- add, replace, append, prepend
        t.assert_equals(c:set(k, 'x', {cmd = 'add'}), 'NOT_STORED\r\n')
        t.assert_equals(c:set(n, 'x', {cmd = 'replace'}), 'NOT_STORED\r\n')
        t.assert_equals(c:set(n, 'x', {cmd = 'add'}), 'STORED\r\n')
        t.assert_equals(c:set(n, 'yz', {cmd = 'append'}), 'STORED\r\n')
        t.assert_equals(c:set(n, 'w', {cmd = 'prepend'}), 'STORED\r\n')
        t.assert_equals(c:get(n, 'get'), 'wxyz')

        -- incr, decr
        check(c, ('incr %s 1\r\n'):format(m), 'NOT_FOUND\r\n')
        t.assert_equals(c:set(m, '10'), 'STORED\r\n')
        check(c, ('incr %s 5\r\n'):format(m), '15\r\n')
        check(c, ('incr %s 18446744073709551615\r\n'):format(m), '14\r\n')
        check(c, ('decr %s 100\r\n'):format(m), '0\r\n')
        check(
            c,
            ('incr %s 1\r\n'):format(n),
            'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n'
        )

        -- touch, gat
        check(c, ('touch %s 100\r\n'):format(k), 'TOUCHED\r\n')
        t.assert_almost_equals(
            vc.tuple(master, k).exptime,
            fiber.time() + 100,
            10
        )
        check(
            c,
            ('gat 0 %s\r\n'):format(k),
            ('VALUE %s 0 2\r\nv2\r\nEND\r\n'):format(k)
        )
        t.assert_equals(vc.tuple(master, k).exptime, 0)

        -- delete
        check(c, ('delete %s\r\n'):format(k), 'DELETED\r\n')
        check(c, ('delete %s\r\n'):format(k), 'NOT_FOUND\r\n')
        t.assert_equals(c:get(k), nil)

        -- noreply
        c:send(('set %s 0 0 1 noreply\r\nq\r\n'):format(k))
        t.assert_equals(c:get(k, 'get'), 'q')
    end
end

g.test_meta_commands = function(cg)
    local c = cg.c
    for _, keys in ipairs({cg.a, cg.b}) do
        local k, n, w = keys[4], keys[5], keys[6]
        check(c, ('ms %s 2 F5 T0\r\nhi\r\n'):format(k), 'HD\r\n')
        check(
            c,
            ('mg %s v f t s k\r\n'):format(k),
            ('VA 2 f5 t-1 s2 k%s\r\nhi\r\n'):format(k)
        )
        c:send(('mg %s c\r\n'):format(k))
        local cas = c:line():match('^HD c(%d+)\r\n$')
        t.assert_not_equals(cas, nil)
        check(c, ('ms %s 1 C1\r\nx\r\n'):format(k), 'EX\r\n')
        check(c, ('ms %s 1 C%s\r\nx\r\n'):format(k, cas), 'HD\r\n')
        check(c, ('mg %s\r\n'):format(n), 'EN\r\n')

        -- Arithmetic with auto-create.
        check(c, ('ma %s\r\n'):format(n), 'NF\r\n')
        check(c, ('ma %s N0 J10 v\r\n'):format(n), 'VA 2\r\n10\r\n')
        check(c, ('ma %s D5 v\r\n'):format(n), 'VA 2\r\n15\r\n')
        check(c, ('ma %s MD D20 v\r\n'):format(n), 'VA 1\r\n0\r\n')

        -- me: no side effects.
        c:send(('me %s\r\n'):format(k))
        t.assert_str_matches(c:line(), 'ME ' .. k .. ' .*\r\n')

        -- Vivify: the first client wins, the next one sees Z.
        check(c, ('mg %s N30 s\r\n'):format(w), 'HD s0 W\r\n')
        check(c, ('mg %s s\r\n'):format(w), 'HD s0 Z\r\n')

        -- Quiet mode and mn.
        check(c, ('md %s q\r\nmg %s v q\r\nmn\r\n'):format(k, k), 'MN\r\n')
        check(c, ('mg %s v\r\n'):format(k), 'EN\r\n')
    end
end

g.test_multi_get = function(cg)
    local c = cg.c
    local keys = {}
    for i = 1, 8 do
        table.insert(keys, cg.a[i])
        table.insert(keys, cg.b[i])
    end
    local expected = {}
    local stored = {}
    for i, k in ipairs(keys) do
        -- Every third key is missing.
        if i % 3 ~= 0 then
            local value = 'v' .. i
            t.assert_equals(c:set(k, value, {flags = i}), 'STORED\r\n')
            table.insert(
                expected,
                ('VALUE %s %d %d\r\n%s\r\n'):format(
                    k,
                    i,
                    #value,
                    value
                )
            )
            table.insert(stored, k)
        end
    end
    local line = table.concat(keys, ' ')
    t.assert_equals(
        c:call('get ' .. line .. '\r\n', 'END\r\n'),
        table.concat(expected) .. 'END\r\n'
    )

    -- gets: the CAS values are those of the single-key gets.
    local cas = {}
    for _, k in ipairs(stored) do
        cas[k] = select(3, c:get(k))
    end
    c:send('gets ' .. line .. '\r\n')
    local _, items = c:read_values()
    t.assert_equals(#items, #stored)
    for i, item in ipairs(items) do
        t.assert_equals(item.key, stored[i])
        t.assert_equals(item.cas, cas[item.key])
    end
end

g.test_pipeline_order = function(cg)
    local a, b = cg.a, cg.b
    local steps = {}
    local function step(req, reply)
        table.insert(steps, {req, reply})
    end
    local keys = {}
    for i = 1, 12 do
        local k = i % 2 == 1 and a[i] or b[i]
        keys[i] = k
        local v = tostring(i)
        step(('set %s 0 0 %d\r\n%s\r\n'):format(k, #v, v), 'STORED\r\n')
        step(('incr %s 10\r\n'):format(k), ('%d\r\n'):format(i + 10))
        local v2 = tostring(i + 10)
        step(
            ('get %s\r\n'):format(k),
            ('VALUE %s 0 %d\r\n%s\r\nEND\r\n'):format(k, #v2, v2)
        )
    end
    -- One get over all the keys, in reverse order, with misses.
    local line, reply = {}, {}
    for i = 12, 1, -1 do
        table.insert(line, keys[i])
        table.insert(line, 'missing' .. i)
        local v = tostring(i + 10)
        table.insert(
            reply,
            ('VALUE %s 0 %d\r\n%s\r\n'):format(
                keys[i],
                #v,
                v
            )
        )
    end
    step(
        'get ' .. table.concat(line, ' ') .. '\r\n',
        table.concat(reply) .. 'END\r\n'
    )
    -- Meta commands in the same batch.
    local m = b[13]
    step(('ms %s 1 T0\r\nm\r\n'):format(m), 'HD\r\n')
    step(('mg %s v\r\n'):format(m), 'VA 1\r\nm\r\n')
    step(('md %s q\r\n'):format(m), '')
    step(('mg %s v q\r\n'):format(m), '')
    step(('append %s 0 0 1\r\nx\r\n'):format(m), 'NOT_STORED\r\n')
    step('mn\r\n', 'MN\r\n')
    -- Deletes in reverse order, each followed by a get.
    for i = 12, 1, -1 do
        step(('delete %s\r\n'):format(keys[i]), 'DELETED\r\n')
        step(('get %s\r\n'):format(keys[i]), 'END\r\n')
    end
    step(('set %s 0 0 1 noreply\r\nz\r\n'):format(a[13]), '')
    step(
        ('get %s\r\n'):format(a[13]),
        ('VALUE %s 0 1\r\nz\r\nEND\r\n'):format(a[13])
    )

    local reqs, replies = {}, {}
    for _, s in ipairs(steps) do
        table.insert(reqs, s[1])
        table.insert(replies, s[2])
    end
    check(cg.c, table.concat(reqs), table.concat(replies))
end

g.test_flush_all = function(cg)
    local c = cg.c
    local a, b = cg.a[1], cg.b[1]
    t.assert_equals(c:set(a, 'x'), 'STORED\r\n')
    t.assert_equals(c:set(b, 'y'), 'STORED\r\n')
    check(c, 'flush_all\r\n', 'OK\r\n')
    check(c, ('get %s %s\r\n'):format(a, b), 'END\r\n')
    -- Each storage has its own flush time; the replica gets it too.
    for _, name in ipairs(vc.STORAGES) do
        cg.cluster[name]:exec(function(space)
            t.helpers.retrying({timeout = 10}, function()
                t.assert_not_equals(
                    box.space._mcc_meta:get('flush:' .. space),
                    nil
                )
            end)
        end, {SPACE})
    end
    -- What is stored after the flush survives it.
    t.assert_equals(c:set(a, 'x2'), 'STORED\r\n')
    t.assert_equals(c:set(b, 'y2'), 'STORED\r\n')
    check(
        c, ('get %s %s\r\n'):format(a, b),
        ('VALUE %s 0 2\r\nx2\r\nVALUE %s 0 2\r\ny2\r\nEND\r\n'):format(
            a,
            b
        )
    )
    -- A delayed flush.
    check(c, 'flush_all 1\r\n', 'OK\r\n')
    t.assert_equals(c:get(a, 'get'), 'x2')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(c:get(a, 'get'), nil)
        t.assert_equals(c:get(b, 'get'), nil)
    end)
    -- noreply.
    t.assert_equals(c:set(a, 'x3'), 'STORED\r\n')
    c:send('flush_all noreply\r\n')
    check(c, ('get %s\r\n'):format(a), 'END\r\n')
end

g.test_stats = function(cg)
    local c = cg.c
    for i = 1, 5 do
        t.assert_equals(c:set(cg.a[i], 'aaaa'), 'STORED\r\n')
    end
    for i = 1, 7 do
        t.assert_equals(c:set(cg.b[i], 'bb'), 'STORED\r\n')
    end
    local bytes, maxbytes = 0, 0
    for _, name in ipairs({'s1-a', 's2-a'}) do
        local bsize, memory = cg.cluster[name]:exec(function(space)
            return box.space[space]:bsize(), box.cfg.memtx_memory
        end, {SPACE})
        bytes = bytes + bsize
        maxbytes = maxbytes + memory
    end
    local stats = read_stats(c)
    t.assert_equals(tonumber(stats.curr_items), 12)
    t.assert_equals(tonumber(stats.bytes), bytes)
    t.assert_equals(tonumber(stats.limit_maxbytes), maxbytes)
    t.assert_equals(tonumber(read_stats(c, 'settings').maxbytes), maxbytes)
end

g.test_read_mode_ro = function(cg)
    local cluster = cg.cluster
    local port, bport = env.free_port(), env.free_port()
    cluster.router:exec(function(port_, bport_, space)
        local vshard = require('vshard')
        local routers = vshard.router.internal.routers
        if routers.mcc_ro == nil then
            -- The router and s1-b are in zone 1, the masters in zone 2:
            -- callro prefers s1-b on rs-1.
            local cfg = table.deepcopy( -- luacheck: ignore 143
                vshard.router.static.current_cfg
            )
            cfg.zone = 1
            cfg.weights = {
                [1] = {[1] = 0, [2] = 10},
                [2] = {[1] = 10, [2] = 0},
            }
            for _, rs in pairs(cfg.sharding) do
                for name, replica in pairs(rs.replicas) do
                    replica.zone = name == 's1-b' and 1 or 2
                end
            end
            vshard.router.new('mcc_ro', cfg)
        end
        t.helpers.retrying({timeout = 30}, function()
            local rs = routers.mcc_ro.replicasets['rs-1']
            t.assert_not_equals(rs.replica, nil)
            t.assert_equals(rs.replica.name, 's1-b')
            t.assert(rs.replica:is_connected())
        end)
        local mcc = require('memcached')
        mcc.create('ro', {
            listen = '127.0.0.1:' .. port_,
            backend = 'vshard',
            space_name = space,
            read_mode = 'ro',
            vshard = {router = 'mcc_ro', timeout = 2},
        })
        mcc.create('bro', {
            listen = '127.0.0.1:' .. bport_,
            backend = 'vshard',
            space_name = space,
            read_mode = 'bro',
        })
    end, {port, bport, SPACE})

    local master, replica = cluster['s1-a'], cluster['s1-b']
    local k, missing = cg.a[1], cg.a[2]
    t.assert_equals(cg.c:set(k, 'v'), 'STORED\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_not_equals(vc.tuple(replica, k), nil)
    end)
    local ro = connect(cg, port)

    -- Expect the batch to go to the master (true) or the replica (false).
    local function expect(to_master, request, reply)
        local m0, r0 = batches(master), batches(replica)
        check(ro, request, reply)
        if to_master then
            t.assert_equals(
                {batches(master), batches(replica)},
                {m0 + 1, r0},
                request
            )
        else
            t.assert_equals(
                {batches(master), batches(replica)},
                {m0, r0 + 1},
                request
            )
        end
    end

    -- Plain reads go to the replica.
    expect(
        false,
        ('get %s\r\n'):format(k),
        ('VALUE %s 0 1\r\nv\r\nEND\r\n'):format(k)
    )
    expect(false, ('mg %s v\r\n'):format(k), 'VA 1\r\nv\r\n')
    expect(false, ('mg %s\r\n'):format(missing), 'EN\r\n')
    -- The replica doesn't write: the item is not marked as fetched.
    t.assert_equals(vc.tuple(master, k).fetched, false)
    -- Reads with side effects and writes go to the master.
    expect(true, ('touch %s 100\r\n'):format(k), 'TOUCHED\r\n')
    expect(true, ('mg %s v T0\r\n'):format(k), 'VA 1\r\nv\r\n')
    expect(true, ('mg %s s N30\r\n'):format(missing), 'HD s0 W\r\n')
    expect(true, ('set %s 0 0 2\r\nv2\r\n'):format(k), 'STORED\r\n')
    -- A batch with a write goes to the master as a whole: it reads what it
    -- has written.
    expect(
        true,
        ('set %s 0 0 2\r\nv3\r\nget %s\r\n'):format(k, k),
        ('STORED\r\nVALUE %s 0 2\r\nv3\r\nEND\r\n'):format(k)
    )

    -- Win tokens. A plain mg of a stale item goes to the replica. A token
    -- the master has handed out is in the item, so the replica sees it.
    local s, s2 = cg.a[3], cg.a[4]
    for _, key in ipairs({s, s2}) do
        t.assert_equals(cg.c:set(key, 'v'), 'STORED\r\n')
        check(cg.c, ('md %s I\r\n'):format(key), 'HD\r\n')
    end
    check(cg.c, ('mg %s\r\n'):format(s), 'HD X W\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(vc.tuple(replica, s).token, true)
        t.assert_equals(vc.tuple(replica, s2).stale, true)
    end)
    expect(false, ('mg %s\r\n'):format(s), 'HD Z X\r\n')
    -- The replica can't write the item: it keeps its own tokens in memory,
    -- per replica.
    expect(false, ('mg %s\r\n'):format(s2), 'HD X W\r\n')
    expect(false, ('mg %s\r\n'):format(s2), 'HD Z X\r\n')
    t.assert_equals(vc.tuple(replica, s2).token, false)
    check(cg.c, ('mg %s\r\n'):format(s2), 'HD X W\r\n')

    -- bro balances the reads over the master and the replica.
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(vc.tuple(replica, k).value, 'v3')
    end)
    local bro = connect(cg, bport)
    local m0, r0 = batches(master), batches(replica)
    t.helpers.retrying({timeout = 10}, function()
        check(
            bro,
            ('get %s\r\n'):format(k),
            ('VALUE %s 0 2\r\nv3\r\nEND\r\n'):format(k)
        )
        t.assert_gt(batches(master), m0)
        t.assert_gt(batches(replica), r0)
    end)
end

g.test_bucket_move = function(cg)
    local cluster = cg.cluster
    local c = cg.c
    local k = cg.a[7]
    local bid = vc.route(cluster, k)
    local w = key_in_bucket(cluster, bid, 'w')
    -- Without discovery the router learns about the move only from the
    -- storage, so the move is certain to go through the resend path.
    cluster.router:exec(function()
        require('vshard').router.discovery_set('off')
    end)
    local function resends()
        return cluster.router:exec(function(name)
            return require('memcached').get(name).backend.resends
        end, {vc.NAME})
    end

    check(c, ('ms %s 1 E%s\r\nv\r\n'):format(k, BIG_CAS), 'HD\r\n')
    check(c, ('mg %s c\r\n'):format(k), ('HD c%s\r\n'):format(BIG_CAS))
    check(c, ('mg %s N30 s\r\n'):format(w), 'HD s0 W\r\n')
    check(c, ('mg %s s\r\n'):format(w), 'HD s0 Z\r\n')

    local before = resends()
    bucket_send(cluster['s1-a'], bid, 'rs-2')
    -- The item has moved with its CAS.
    check(
        c,
        ('mg %s v c\r\n'):format(k),
        ('VA 1 c%s\r\nv\r\n'):format(BIG_CAS)
    )
    t.assert_gt(resends(), before)
    local tuple = vc.tuple(cluster['s2-a'], k)
    t.assert_equals(tuple.bucket_id, bid)
    t.assert_equals(tostring(tuple.cas), BIG_CAS .. 'ULL')
    -- CAS keeps growing on the new storage, whose counter is far behind.
    check(c, ('ms %s 1\r\nx\r\n'):format(k), 'HD\r\n')
    check(c, ('mg %s c\r\n'):format(k), 'HD c1000000000000000001\r\n')
    -- The win token has moved with the item, in its tuple.
    t.assert_equals(vc.tuple(cluster['s2-a'], w).token, true)
    check(c, ('mg %s s\r\n'):format(w), 'HD s0 Z\r\n')

    -- And back.
    cluster['s1-a']:exec(function(bid_)
        t.helpers.retrying({timeout = 30}, function()
            t.assert_equals(box.space._bucket:get(bid_), nil)
        end)
    end, {bid})
    bucket_send(cluster['s2-a'], bid, 'rs-1')
    check(c, ('mg %s s\r\n'):format(w), 'HD s0 Z\r\n')
    check(c, ('ms %s 1\r\ny\r\n'):format(k), 'HD\r\n')
    check(
        c,
        ('mg %s v c\r\n'):format(k),
        'VA 1 c1000000000000000002\r\ny\r\n'
    )
    t.assert_equals(vc.tuple(cluster['s1-a'], k).value, 'y')
    cluster.router:exec(function()
        require('vshard').router.discovery_set('on')
    end)
end

-- The items of a bucket in transfer count once in the sum of the storage
-- stats: the destination leaves out a receiving bucket, the source a sent
-- or garbage one.
g.test_stats_bucket_in_transfer = function(cg)
    local cluster = cg.cluster
    local s1, s2 = cluster['s1-a'], cluster['s2-a']
    local c = cg.c
    local bid = vc.route(cluster, cg.a[1])
    local K = 6
    local keys = {}
    local i = 0
    while #keys < K do
        i = i + 1
        local key = key_in_bucket(cluster, bid, 'm' .. i .. '_')
        table.insert(keys, key)
        t.assert_equals(c:set(key, 'value'), 'STORED\r\n')
    end
    for j = 1, 4 do
        t.assert_equals(c:set(cg.a[10 + j], 'aaaa'), 'STORED\r\n')
        t.assert_equals(c:set(cg.b[j], 'bb'), 'STORED\r\n')
    end
    local TOTAL = K + 8
    local function storage(server)
        return server:exec(function(space, bid_)
            local st = _G.mcc.storage_stats(space)
            return {
                items = st.curr_items,
                bytes = st.bytes,
                len = box.space[space]:len(),
                bsize = box.space[space]:bsize(),
                status = (box.space._bucket:get(bid_) or {})[2],
            }
        end, {SPACE, bid})
    end
    local function errinj(server, name, value)
        server:exec(function(name_, value_)
            require('vshard').storage.internal.errinj[name_] = value_
        end, {name, value})
    end
    local before = {storage(s1), storage(s2)}
    t.assert_equals(before[1].items + before[2].items, TOTAL)
    t.assert_equals(before[1].items, before[1].len)

    -- The data has reached rs-2, the bucket is not active there yet.
    errinj(s1, 'ERRINJ_LAST_SEND_DELAY', true)
    local sender = fiber.new(bucket_send, s1, bid, 'rs-2')
    sender:set_joinable(true)
    local held = {}
    t.helpers.retrying({timeout = 10}, function()
        held = {storage(s1), storage(s2)}
        t.assert_equals(held[1].status, 'sending')
        t.assert_equals(held[2].status, 'receiving')
        t.assert_equals(held[2].len, before[2].len + K)
    end)
    t.assert_equals(held[1].items, before[1].items)
    t.assert_equals(held[2].items, before[2].items)
    t.assert_lt(held[2].bytes, held[2].bsize)
    -- The router's stats wait for the transfer (the storage ref of
    -- map_callrw) up to the batch timeout, then show no items: never the
    -- items twice.
    c:send('stats\r\n')
    local stats = c:read_until('END\r\n', 10)
    t.assert_str_contains(stats, 'STAT curr_items 0\r\n')

    -- Sent, and not garbage collected on rs-1.
    errinj(s1, 'ERRINJ_BUCKET_GC_PAUSE', true)
    errinj(s1, 'ERRINJ_LAST_SEND_DELAY', false)
    t.assert(sender:join())
    local sent = {storage(s1), storage(s2)}
    t.assert_items_include({'sent', 'garbage'}, {sent[1].status})
    t.assert_equals(sent[1].len, before[1].len)
    t.assert_equals(sent[2].status, 'active')
    t.assert_equals(sent[1].items, before[1].items - K)
    t.assert_equals(sent[2].items, before[2].items + K)
    t.assert_lt(sent[1].bytes, sent[1].bsize)

    -- Garbage collected: back to the raw counts. And the bucket goes back.
    errinj(s1, 'ERRINJ_BUCKET_GC_PAUSE', false)
    s1:exec(function(bid_)
        t.helpers.retrying({timeout = 30}, function()
            t.assert_equals(box.space._bucket:get(bid_), nil)
        end)
    end, {bid})
    local after = storage(s1)
    t.assert_equals({after.items, after.bytes}, {after.len, after.bsize})
    t.assert_equals(tonumber(read_stats(c).curr_items), TOTAL)
    bucket_send(s2, bid, 'rs-1')
    s2:exec(function(bid_)
        t.helpers.retrying({timeout = 30}, function()
            t.assert_equals(box.space._bucket:get(bid_), nil)
        end)
    end, {bid})
    t.assert_equals(tonumber(read_stats(c).curr_items), TOTAL)
end

g.test_errors = function(cg)
    local port, nport = env.free_port(), env.free_port()
    create_instance(cg.cluster, 'bad', {
        listen = '127.0.0.1:' .. port,
        backend = 'vshard',
        space_name = 'nope',
    })
    create_instance(cg.cluster, 'nort', {
        listen = '127.0.0.1:' .. nport,
        backend = 'vshard',
        space_name = SPACE,
        vshard = {router = 'missing'},
    })
    local NOT_SERVED = "SERVER_ERROR space 'nope' is not served by this "
        .. "storage\r\n"
    local c = connect(cg, port)
    t.assert_equals(c:set(cg.a[1], 'x'), NOT_SERVED)
    check(c, ('get %s %s\r\n'):format(cg.a[1], cg.b[1]), NOT_SERVED)
    check(c, ('mg %s v\r\n'):format(cg.b[1]), NOT_SERVED)
    check(c, 'flush_all\r\n', NOT_SERVED)
    t.assert_equals(tonumber(read_stats(c).curr_items), 0)

    c = connect(cg, nport)
    local NO_ROUTER = "SERVER_ERROR vshard router 'missing' does not exist\r\n"
    check(c, ('get %s\r\n'):format(cg.a[1]), NO_ROUTER)
    t.assert_equals(c:set(cg.b[1], 'x'), NO_ROUTER)
    check(c, 'flush_all\r\n', NO_ROUTER)
end

--
-- Rebalancing under load.
--

-- A client that keeps storing and reading keys through the router. Each
-- round goes over all the keys in pipelined chunks: "ms <key> <len> c" and
-- "mg <key> v c" per key. It checks read-your-writes and strictly growing
-- CAS values, and records any error instead of raising in its fiber.
local LOAD_CHUNK = 20
local LOAD_TIMEOUT = 15

local function load_new(port, keys)
    return {
        port = port,
        keys = keys,
        -- Rounds finished.
        rounds = 0,
        errors = {},
        -- key -> the last value stored and its CAS (a string).
        values = {},
        cas = {},
        stop = false,
        done = false,
    }
end

local function load_error(load, key, what)
    table.insert(load.errors, ('%s: %s'):format(key, what))
end

-- One pipelined chunk of a round. Returns false when the connection is out
-- of sync.
local function load_chunk(load, c, from, to, round)
    local reqs = {}
    for i = from, to do
        local key = load.keys[i]
        local value = ('%s:%d'):format(key, round)
        table.insert(
            reqs,
            ('ms %s %d c\r\n%s\r\nmg %s v c\r\n'):format(
                key,
                #value,
                value,
                key
            )
        )
    end
    c:send(table.concat(reqs))
    for i = from, to do
        local key = load.keys[i]
        local value = ('%s:%d'):format(key, round)
        local line = c:line(LOAD_TIMEOUT)
        if line == nil then
            load_error(load, key, 'ms: no reply')
            return false
        end
        local cas = line:match('^HD c(%d+)\r\n$')
        if cas == nil then
            load_error(load, key, 'ms: ' .. line)
        else
            local last = load.cas[key]
            if last ~= nil and tonumber64(cas) <= tonumber64(last) then
                load_error(load, key, ('CAS %s after %s'):format(cas, last))
            end
            load.cas[key] = cas
            load.values[key] = value
        end
        line = c:line(LOAD_TIMEOUT)
        if line == nil then
            load_error(load, key, 'mg: no reply')
            return false
        end
        local len, got_cas = line:match('^VA (%d+) c(%d+)\r\n$')
        if len == nil then
            load_error(load, key, 'mg: ' .. line)
        else
            local data = c:read(tonumber(len) + 2, LOAD_TIMEOUT)
            if data == nil then
                load_error(load, key, 'mg: no data')
                return false
            end
            if data:sub(1, -3) ~= value or got_cas ~= cas then
                load_error(
                    load, key,
                    ('mg: %s c%s, expected %s c%s'):format(
                        data:sub(1, -3),
                        got_cas,
                        value,
                        tostring(cas)
                    )
                )
            end
        end
    end
    return true
end

-- One round over all the keys; false when the connection is out of sync.
local function load_round(load, c, round)
    for from = 1, #load.keys, LOAD_CHUNK do
        local to = math.min(from + LOAD_CHUNK - 1, #load.keys)
        if not load_chunk(load, c, from, to, round) then
            return false
        end
    end
    return true
end

-- Run rounds in a fiber until load.stop.
local function load_start(load)
    fiber.create(function()
        local ok, err = pcall(function()
            local c = client.must_connect(load.port)
            local round = load.rounds
            while not load.stop do
                round = round + 1
                if not load_round(load, c, round) then
                    break
                end
                load.rounds = round
            end
            c:close()
        end)
        if not ok then
            table.insert(load.errors, tostring(err))
        end
        load.done = true
    end)
end

local function load_stop(load)
    load.stop = true
    t.helpers.retrying({timeout = LOAD_TIMEOUT * 2}, function()
        t.assert(load.done)
    end)
end

local r = t.group('vshard_rebalance')

-- The cluster options; weights: {[replicaset] = weight} or nil. Up to 5
-- buckets are sent at once.
local function rebalance_opts(port, weights)
    return {
        port = port,
        bucket_count = 600,
        rebalancer_mode = 'auto',
        rebalancer_max_sending = 5,
        timeout = 10,
        weights = weights,
    }
end

r.before_all(function(cg)
    cg.port = env.free_port()
    cg.cluster = vc.start(rebalance_opts(cg.port))
end)

r.after_all(function(cg)
    if cg.load ~= nil then
        cg.load.stop = true
    end
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)

r.test_rebalancing_under_load = function(cg)
    local cluster = cg.cluster
    local masters = {'s1-a', 's2-a'}
    for _, name in ipairs(masters) do
        t.assert_equals(vc.buckets(cluster[name]).active, 300, name)
    end
    local keys = {}
    for i = 1, 300 do
        keys[i] = 'r' .. i
    end
    local load = load_new(cg.port, keys)
    cg.load = load

    -- Preload.
    local c = client.must_connect(cg.port)
    t.assert(load_round(load, c, 0))
    c:close()
    t.assert_equals(load.errors, {})
    load_start(load)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_ge(load.rounds, 1)
    end)

    -- rs-2 gets three times the weight of rs-1: 150 buckets go to rs-2.
    local before = load.rounds
    local started = fiber.clock()
    cluster:reload(
        vc.config(
            rebalance_opts(
                cg.port,
                {['rs-1'] = 1, ['rs-2'] = 3}
            )
        )
    )
    t.helpers.retrying({timeout = 120}, function()
        local b1, b2 = vc.buckets(cluster['s1-a']), vc.buckets(cluster['s2-a'])
        t.assert_equals(
            {b1.active, b1.sending, b1.receiving},
            {150, 0, 0},
            's1-a'
        )
        t.assert_equals(
            {b2.active, b2.sending, b2.receiving},
            {450, 0, 0},
            's2-a'
        )
    end)
    local after = load.rounds
    local took = fiber.clock() - started
    load_stop(load)
    local resends = cluster.router:exec(function(name)
        return require('memcached').get(name).backend.resends
    end, {vc.NAME})
    require('log').info(
        'rebalancing took %.2f s, %d rounds, %d resends',
        took,
        after - before,
        resends
    )

    t.assert_equals(load.errors, {})
    -- At least one whole round ran while the buckets were moving.
    t.assert_ge(after - before, 2)

    -- Every key is in exactly one active bucket, the bucket of its key, with
    -- the last value stored.
    local bids = cluster.router:exec(function(keys_)
        local vshard = require('vshard')
        local res = {}
        for _, key in ipairs(keys_) do
            res[key] = vshard.router.bucket_id_strcrc32(key)
        end
        return res
    end, {keys})
    local found = {}
    for _, name in ipairs(masters) do
        local rows = cluster[name]:exec(function(space, keys_)
            local res = {}
            for _, key in ipairs(keys_) do
                local tuple = box.space[space]:get(key)
                if tuple ~= nil then
                    local bucket = box.space._bucket:get(tuple.bucket_id)
                    res[key] = {
                        value = tuple.value,
                        bucket_id = tuple.bucket_id,
                        status = bucket and bucket.status,
                    }
                end
            end
            return res
        end, {SPACE, keys})
        for key, row in pairs(rows) do
            if row.status == 'active' then
                found[key] = found[key] or {}
                table.insert(found[key], row)
            end
        end
    end
    for _, key in ipairs(keys) do
        local rows = found[key] or {}
        t.assert_equals(#rows, 1, key)
        t.assert_equals(rows[1].bucket_id, bids[key], key)
        t.assert_equals(rows[1].value, load.values[key], key)
    end
end

--
-- A storage master going down, and a leader switch.
--

local f = t.group('vshard_failover')

local FAILOVER_TIMEOUT = 0.5
local EXTRA_SPACE = '__mcc_extra'

f.before_all(function(cg)
    cg.port = env.free_port()
    cg.cluster = vc.start({port = cg.port, timeout = FAILOVER_TIMEOUT})
    cg.a = vc.keys_on(cg.cluster, 'rs-1', 10, 'a')
    cg.b = vc.keys_on(cg.cluster, 'rs-2', 10, 'b')
end)

f.after_all(function(cg)
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)

f.before_each(function(cg)
    reset_space(cg.cluster)
    cg.c = client.must_connect(cg.port)
end)

f.after_each(function(cg)
    cg.c:close()
    destroy_instances(cg.cluster, {'x'})
end)

-- Send a request and expect a one-line reply matching the pattern within
-- `limit` seconds.
local function check_fast(c, request, pattern, limit)
    local started = fiber.clock()
    c:send(request)
    local reply = c:line()
    local took = fiber.clock() - started
    t.assert_not_equals(reply, nil, request)
    t.assert_str_matches(reply, pattern)
    t.assert_le(took, limit, request)
end

f.test_master_down = function(cg)
    local cluster = cg.cluster
    local c = cg.c
    local a, b = cg.a, cg.b
    t.assert_equals(c:set(a[1], 'x'), 'STORED\r\n')
    t.assert_equals(c:set(b[1], 'y'), 'STORED\r\n')

    cluster['s2-a']:stop()
    local LIMIT = 3 * FAILOVER_TIMEOUT
    local ERR = 'SERVER_ERROR .*\r\n'
    -- rs-2: SERVER_ERROR after the timeout, no hang.
    check_fast(c, ('set %s 0 0 1\r\nz\r\n'):format(b[2]), ERR, LIMIT)
    check_fast(c, ('get %s\r\n'):format(b[1]), ERR, LIMIT)
    check_fast(c, ('mg %s v\r\n'):format(b[1]), ERR, LIMIT)
    -- rs-1 keeps working.
    t.assert_equals(c:set(a[2], 'w'), 'STORED\r\n')
    check(
        c,
        ('get %s\r\n'):format(a[1]),
        ('VALUE %s 0 1\r\nx\r\nEND\r\n'):format(a[1])
    )
    -- A mixed pipeline keeps the order.
    c:send(
        ('set %s 0 0 1\r\n1\r\nset %s 0 0 1\r\n2\r\nget %s\r\n'):format(
            a[3],
            b[3],
            a[3]
        )
    )
    t.assert_equals(c:line(), 'STORED\r\n')
    t.assert_str_matches(c:line(), ERR)
    t.assert_equals(
        c:read_until('END\r\n'),
        ('VALUE %s 0 1\r\n1\r\nEND\r\n'):format(a[3])
    )
    -- A multi-key get with a key on rs-2 fails as a whole.
    check_fast(c, ('get %s %s\r\n'):format(a[1], b[1]), ERR, LIMIT)
    -- flush_all needs every master.
    check_fast(c, 'flush_all\r\n', ERR, LIMIT)
    check(
        c,
        ('get %s\r\n'):format(a[1]),
        ('VALUE %s 0 1\r\nx\r\nEND\r\n'):format(a[1])
    )
    -- stats still answers.
    local started = fiber.clock()
    t.assert_equals(read_stats(c).curr_items, '0')
    t.assert_le(fiber.clock() - started, LIMIT)

    cluster['s2-a']:start()
    t.helpers.retrying({timeout = 60}, function()
        t.assert_equals(c:get(b[1], 'get'), 'y')
    end)
    t.assert_equals(c:set(b[2], 'z'), 'STORED\r\n')
    t.assert_equals(tonumber(read_stats(c).curr_items), 5)
end

f.test_leader_switch = function(cg)
    local cluster = cg.cluster
    local c = cg.c
    local a = cg.a
    local s1a, s1b = cluster['s1-a'], cluster['s1-b']
    local function sweepers(space_name)
        return {
            vc.space_info(s1a, space_name).sweeper,
            vc.space_info(s1b, space_name).sweeper,
        }
    end
    t.assert_equals(sweepers(), {true, false})
    -- A win token taken on s1-a is replicated with its item.
    local w = a[6]
    check(c, ('mg %s N30 s\r\n'):format(w), 'HD s0 W\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals((vc.tuple(s1b, w) or {}).token, true)
    end)

    -- s1-b becomes the leader of rs-1, and a new space is added.
    local opts = {
        port = cg.port,
        timeout = FAILOVER_TIMEOUT,
        leaders = {['rs-1'] = 's1-b'},
        spaces = {[EXTRA_SPACE] = {expire = {batch = 10}}},
    }
    cluster:reload(vc.config(opts))
    t.helpers.retrying({timeout = 30}, function()
        t.assert_equals(s1a:exec(function() return box.info.ro end), true)
        t.assert_equals(s1b:exec(function() return box.info.ro end), false)
        -- The sweepers moved to s1-b. The new space is created there and on
        -- rs-2, and replicated to s1-a.
        t.assert_equals(sweepers(), {false, true})
        t.assert_equals(sweepers(EXTRA_SPACE), {false, true})
        for _, name in ipairs(vc.STORAGES) do
            t.assert_equals(
                vc.space_info(cluster[name], EXTRA_SPACE).ready,
                true,
                name
            )
        end
    end)
    t.assert_equals(
        vc.space_info(cluster['s2-a'], EXTRA_SPACE).sweeper,
        true
    )
    -- Writes on rs-1 go to s1-b.
    t.helpers.retrying({timeout = 30}, function()
        t.assert_equals(c:set(a[1], 'v'), 'STORED\r\n')
    end)
    t.assert_equals(vc.tuple(s1b, a[1]).value, 'v')
    check(
        c,
        ('get %s\r\n'):format(a[1]),
        ('VALUE %s 0 1\r\nv\r\nEND\r\n'):format(a[1])
    )
    -- The new leader knows that the token is out.
    check(c, ('mg %s s\r\n'):format(w), 'HD s0 Z\r\n')
    -- The new space is served through the router.
    local port = env.free_port()
    create_instance(cluster, 'x', {
        listen = '127.0.0.1:' .. port,
        backend = 'vshard',
        space_name = EXTRA_SPACE,
        vshard = {timeout = FAILOVER_TIMEOUT},
    })
    local x = client.must_connect(port)
    t.assert_equals(x:set(a[2], 'e'), 'STORED\r\n')
    t.assert_equals(x:get(a[2], 'get'), 'e')
    x:close()
    t.assert_equals(
        s1b:exec(function(space, key)
            return box.space[space]:get(key).value
        end, {EXTRA_SPACE, a[2]}),
        'e'
    )

    -- Back to s1-a. The extra space is no longer served; its data stays.
    cluster:reload(vc.config({port = cg.port, timeout = FAILOVER_TIMEOUT}))
    t.helpers.retrying({timeout = 30}, function()
        t.assert_equals(sweepers(), {true, false})
        t.assert_equals(vc.space_info(s1a, EXTRA_SPACE), nil)
        t.assert_equals(vc.space_info(s1b, EXTRA_SPACE), nil)
    end)
    t.assert_equals(
        s1a:exec(function(space)
            return box.space[space]:len()
        end, {EXTRA_SPACE}),
        1
    )
    t.helpers.retrying({timeout = 30}, function()
        t.assert_equals(c:set(a[3], 'w'), 'STORED\r\n')
    end)
    t.assert_equals(vc.tuple(s1a, a[3]).value, 'w')
end

--
-- Options and the backend without a cluster, in this process.
--

local o = t.group('vshard_options')

o.before_all(function(cg)
    env.box_cfg()
    cg.mcc = require('memcached')
    cg.port = env.free_port()
end)

local STORAGE_SPACE = '__mcc_vstorage'

o.after_each(function(cg)
    if cg.mcc.get('vopts') ~= nil then
        cg.mcc.destroy('vopts')
    end
    require('memcached.storage').stop()
    if box.info.ro then
        box.cfg {read_only = false}
    end
    env.drop_spaces({'__mcc_vopts', STORAGE_SPACE})
end)

o.test_validation = function(cg)
    local listen = '127.0.0.1:' .. cg.port
    local cases = {
        {{listen = listen, vshard = true}, "Option 'vshard' must be a table"},
        {
            {listen = listen, vshard = {bogus = 1}},
            "Bad value for option 'vshard'",
        },
        {
            {listen = listen, vshard = {timeout = 0}},
            "Bad value for option 'vshard'",
        },
        {
            {listen = listen, vshard = {router = 5}},
            "Bad value for option 'vshard'",
        },
        {
            {listen = listen, backend = 'vshard', vshard = {router = {}}},
            "Option 'vshard.router' must be a vshard router or the name of one",
        },
    }
    for _, case in ipairs(cases) do
        t.assert_error_msg_contains(case[2], cg.mcc.create, 'vopts', case[1])
        t.assert_equals(cg.mcc.get('vopts'), nil)
    end
end

o.test_backend_switch = function(cg)
    local mcc = cg.mcc
    local inst = mcc.create('vopts', {
        listen = '127.0.0.1:' .. cg.port,
        backend = 'vshard',
    })
    t.assert_equals(inst.status, 'running')
    -- No local space and no sweeper.
    t.assert_equals(box.space.__mcc_vopts, nil)
    t.assert_equals(inst:info().expired, nil)
    local c = client.must_connect(cg.port)
    local NO_ROUTER = 'SERVER_ERROR vshard router is not configured\r\n'
    check(c, 'get a b\r\n', NO_ROUTER)
    t.assert_equals(c:set('a', 'x'), NO_ROUTER)
    check(c, 'flush_all\r\n', NO_ROUTER)
    t.assert_equals(tonumber(read_stats(c).curr_items), 0)

    -- To the local backend: the space and the sweeper appear.
    inst:cfg({backend = 'local'})
    t.assert_not_equals(box.space.__mcc_vopts, nil)
    t.assert_not_equals(inst:info().expired, nil)
    t.assert_equals(c:set('a', 'x'), 'STORED\r\n')
    -- And back: the sweeper stops, the data stays.
    inst:cfg({backend = 'vshard'})
    t.assert_equals(inst:info().expired, nil)
    check(c, 'get a\r\n', NO_ROUTER)
    t.assert_equals(box.space.__mcc_vopts:len(), 1)

    -- The vshard options make a new backend when they change.
    local backend = inst.backend
    inst:cfg({vshard = {timeout = 3}})
    t.assert_not_equals(inst.backend, backend)
    t.assert_equals(inst.backend.timeout, 3)
    backend = inst.backend
    inst:cfg({vshard = {timeout = 3}})
    t.assert_is(inst.backend, backend)
    inst:cfg({vshard = {router = 'nope'}})
    check(
        c,
        'get a\r\n',
        "SERVER_ERROR vshard router 'nope' does not exist\r\n"
    )
    -- A router object is kept by reference, not copied.
    local router = setmetatable({}, {__index = {callrw = function() end}})
    inst:cfg({vshard = {router = router}})
    t.assert_is(inst.opts.vshard.router, router)
    t.assert_is(inst.backend.router_ref, router)
    c:close()
end

o.test_storage_cfg = function()
    local storage = require('memcached.storage')
    local cases = {
        {5, 'cfg must be a table'},
        {{bogus = 1}, "unknown option 'bogus'"},
        {{spaces = 5}, "option 'spaces' must be a table"},
        {{spaces = {[''] = {}}}, 'a space name must be a non-empty string'},
        {{spaces = {s = 5}}, "space 's': options must be a table"},
        {
            {spaces = {s = {storage = 'tape'}}},
            "space 's': bad value for option 'storage'",
        },
        {
            {spaces = {s = {expire = {batch = 0}}}},
            "space 's': bad value for option 'expire'",
        },
        {{spaces = {s = {nope = true}}}, "space 's': unknown option 'nope'"},
        {
            {spaces = {s = {track_access = 'no'}}},
            "space 's': bad value for option 'track_access'",
        },
        {
            {spaces = {s = {commit_wait = 'async'}}},
            "space 's': bad value for option 'commit_wait'",
        },
        {
            {spaces = {s = {evict = {high = 2}}}},
            "space 's': bad value for option 'evict': 'high' must be a "
                .. 'number in (0, 1]',
        },
        {
            {spaces = {s = {evict = {low = 0.99}}}},
            "space 's': bad value for option 'evict': 'low' must be less than 'high'",
        },
    }
    for _, case in ipairs(cases) do
        t.assert_error_msg_contains(case[2], storage.cfg, case[1])
    end
    t.assert_equals((rawget(_G, 'mcc') or {}).storage_exec, nil)
    t.assert_equals(box.space.s, nil)
end

-- track_access per space: the gets that storage_exec runs follow it.
o.test_storage_track_access = function()
    local storage = require('memcached.storage')
    storage.cfg({spaces = {[STORAGE_SPACE] = {track_access = false}}})
    t.assert_equals(storage.info().spaces[STORAGE_SPACE].track_access, false)
    local exec = rawget(_G, 'mcc').storage_exec
    t.assert_covers(
        exec(STORAGE_SPACE, 1, {
            {
                op = 'store',
                mode = 'set',
                key = 'k',
                value = 'v',
                flags = 0,
                exptime = 0,
            },
            {op = 'get', key = 'k'},
        }),
        {{status = 'STORED'}, {status = 'HIT'}}
    )
    t.assert_equals(box.space[STORAGE_SPACE]:get('k').fetched, false)

    -- Back to the default: the next get is the first fetch.
    storage.cfg({spaces = {[STORAGE_SPACE] = {}}})
    t.assert_equals(storage.info().spaces[STORAGE_SPACE].track_access, true)
    exec(STORAGE_SPACE, 1, {{op = 'get', key = 'k'}})
    t.assert_equals(box.space[STORAGE_SPACE]:get('k').fetched, true)
end

-- commit_wait per space: the writes that storage_exec runs follow it. With
-- 'submit' a batch of sets doesn't wait for the WAL (no yield); with the
-- default every set does.
o.test_storage_commit_wait = function()
    local storage = require('memcached.storage')
    local function sets(n)
        local reqs = {}
        for i = 1, n do
            reqs[i] = {
                op = 'store',
                mode = 'set',
                key = 'k' .. i,
                value = 'v',
                flags = 0,
                exptime = 0,
                fast = 'set',
            }
        end
        return reqs
    end
    local function csw()
        return fiber.info({backtrace = false})[fiber.id()].csw
    end
    storage.cfg({spaces = {[STORAGE_SPACE] = {commit_wait = 'submit'}}})
    t.assert_equals(storage.info().spaces[STORAGE_SPACE].commit_wait, 'submit')
    local exec = rawget(_G, 'mcc').storage_exec
    box.ctl.wal_sync()
    local csw0 = csw()
    local resps = exec(STORAGE_SPACE, 1, sets(16))
    t.assert_equals(csw() - csw0, 0)
    t.assert_equals(resps[16].status, 'STORED')
    box.ctl.wal_sync()
    t.assert_equals(box.space[STORAGE_SPACE]:get('k16').bucket_id, 1)

    storage.cfg({spaces = {[STORAGE_SPACE] = {}}})
    t.assert_equals(
        storage.info().spaces[STORAGE_SPACE].commit_wait,
        'complete'
    )
    csw0 = csw()
    exec(STORAGE_SPACE, 1, sets(16))
    t.assert_ge(csw() - csw0, 16)
end

-- The evictor of a storage space: it evicts when the memory crosses the
-- watermarks, and follows the instance's role.
o.test_storage_evict = function()
    local storage = require('memcached.storage')
    local evict = require('memcached.evict')
    storage.cfg({spaces = {[STORAGE_SPACE] = {}}})
    local quota, used = evict.memory()
    local fill = used / quota
    -- About 8 MiB above the low watermark.
    local low = fill + 2 * 1024 * 1024 / quota
    local high = low + 4 * 1024 * 1024 / quota
    storage.cfg({
        spaces = {
            [STORAGE_SPACE] = {
                evict = {high = high, low = low, batch = 50},
            },
        },
    })
    t.assert_equals(storage.info().spaces[STORAGE_SPACE].evictor, true)
    local exec = rawget(_G, 'mcc').storage_exec
    local value = string.rep('v', 1000)
    for i = 1, 10000 do
        local resp = exec(STORAGE_SPACE, 1, {
            {
                op = 'store',
                mode = 'set',
                key = 'k' .. i,
                value = value,
                flags = 0,
                exptime = 0,
            },
        })[1]
        t.assert_equals(resp.status, 'STORED')
    end
    t.helpers.retrying({timeout = 10}, function()
        local info = storage.info().spaces[STORAGE_SPACE]
        t.assert_gt(info.evictions, 0)
        local q, u = evict.memory()
        t.assert_le(u / q, high)
        t.assert_equals(evict.info().spaces[STORAGE_SPACE].evicting, false)
    end)
    local stats = rawget(_G, 'mcc').storage_stats(STORAGE_SPACE)
    t.assert_equals(stats.evict, true)
    t.assert_gt(stats.evictions, 0)
    t.assert_equals(
        stats.curr_items + stats.evictions + stats.reclaimed,
        10000
    )
    -- Read-only: no evictor.
    box.cfg {read_only = true}
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(storage.info().spaces[STORAGE_SPACE].evictor, false)
    end)
    box.cfg {read_only = false}
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(storage.info().spaces[STORAGE_SPACE].evictor, true)
    end)
    storage.stop()
    t.assert_not(evict.enabled(STORAGE_SPACE))
end

-- expire.mode of a storage space: the exptime index is created or dropped
-- on a writable instance, also when it becomes writable; vinyl keeps it.
o.test_storage_expire_mode = function()
    local storage = require('memcached.storage')
    local function has_index()
        return box.space[STORAGE_SPACE].index.exptime ~= nil
    end
    local function info()
        return storage.info().spaces[STORAGE_SPACE]
    end
    storage.cfg({spaces = {[STORAGE_SPACE] = {expire = {mode = 'index'}}}})
    t.assert(has_index())
    t.assert_not_equals(box.space[STORAGE_SPACE].index.bucket_id, nil)
    t.assert_equals(info().expire_mode, 'index')
    box.space[STORAGE_SPACE]:insert({'k', 'v', 0, 1, 1, 0, 0, false, false, 7})
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(box.space[STORAGE_SPACE]:len(), 0)
        t.assert_equals(info().expired, 1)
    end)

    -- The default: scan.
    storage.cfg({spaces = {[STORAGE_SPACE] = {}}})
    t.assert_not(has_index())
    t.assert_equals(info().expire_mode, 'scan')
    box.space[STORAGE_SPACE]:insert({'k', 'v', 0, 1, 1, 0, 0, false, false, 7})
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(box.space[STORAGE_SPACE]:len(), 0)
        t.assert_equals(info().expired, 2)
    end)
    t.assert_gt(info().checked, 0)

    -- Read-only: the config is taken, the space is left as it is until
    -- the instance is writable again.
    box.cfg {read_only = true}
    storage.cfg({spaces = {[STORAGE_SPACE] = {expire = {mode = 'index'}}}})
    t.assert_not(has_index())
    box.cfg {read_only = false}
    t.helpers.retrying({timeout = 10}, function()
        t.assert(has_index())
    end)

    t.assert_error_msg_equals(
        "memcached.storage: space '" .. STORAGE_SPACE .. "': bad value for "
            .. "option 'expire': mode 'scan' needs storage = 'memory': vinyl "
            .. 'keeps the exptime index',
        storage.cfg,
        {
            spaces = {
                [STORAGE_SPACE] = {
                    storage = 'disk',
                    expire = {mode = 'scan'},
                },
            },
        }
    )
    t.assert_error_msg_equals(
        "memcached.storage: space '" .. STORAGE_SPACE
            .. "': bad value for " .. "option 'expire'",
        storage.cfg,
        {spaces = {[STORAGE_SPACE] = {expire = {mode = 'tree'}}}}
    )
    -- The failed calls changed nothing.
    t.assert_equals(info().expire_mode, 'index')
end

-- The space and the sweeper follow the instance's role through the
-- box.status watcher, without another cfg() call.
o.test_storage_deploy_on_rw = function()
    local storage = require('memcached.storage')
    box.cfg {read_only = true}
    storage.cfg({spaces = {[STORAGE_SPACE] = {expire = {batch = 10}}}})
    local mcc = rawget(_G, 'mcc')
    t.assert_type(mcc.storage_exec, 'function')
    t.assert_covers(storage.info(), {
        ro = true,
        spaces = {
            [STORAGE_SPACE] = {
                storage = 'memory',
                ready = false,
                sweeper = false,
            },
        },
    })
    t.assert_equals(box.space[STORAGE_SPACE], nil)
    -- The space is not there yet.
    t.assert_equals(mcc.storage_exec(STORAGE_SPACE, 1, {
        {op = 'get', key = 'k'},
    }), {
        {
            status = 'SERVER_ERROR',
            message = ("Space '%s' does not exist"):format(STORAGE_SPACE),
        },
    })

    box.cfg {read_only = false}
    t.helpers.retrying({timeout = 10}, function()
        local info = storage.info().spaces[STORAGE_SPACE]
        t.assert_equals({info.ready, info.sweeper}, {true, true})
    end)
    t.assert_not_equals(box.space[STORAGE_SPACE].index.bucket_id, nil)
    local resps = mcc.storage_exec(STORAGE_SPACE, 7, {
        {
            op = 'store',
            mode = 'set',
            key = 'k',
            value = 'v',
            flags = 0,
            exptime = 0,
        },
        {op = 'get', key = 'k'},
    })
    t.assert_equals(resps[1].status, 'STORED')
    t.assert_equals(resps[2].item.value, 'v')
    t.assert_equals(box.space[STORAGE_SPACE]:get('k').bucket_id, 7)
    t.assert_equals(mcc.storage_stats(STORAGE_SPACE).curr_items, 1)
    t.assert_equals(mcc.storage_flush(STORAGE_SPACE, 0).status, 'OK')
    t.assert_equals(
        mcc.storage_exec(STORAGE_SPACE, 7, {
            {op = 'get', key = 'k'},
        })[1].status,
        'MISS'
    )

    -- Read-only again: the sweeper stops, the space stays.
    box.cfg {read_only = true}
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(storage.info().spaces[STORAGE_SPACE].sweeper, false)
    end)
    t.assert_equals(storage.info().spaces[STORAGE_SPACE].ready, true)
    box.cfg {read_only = false}
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(storage.info().spaces[STORAGE_SPACE].sweeper, true)
    end)

    -- stop(): no globals, no sweeper; the data is kept.
    storage.stop()
    t.assert_equals((rawget(_G, 'mcc') or {}).storage_exec, nil)
    t.assert_equals(storage.info().spaces, {})
    t.assert_equals(
        require('memcached.state').sweepers[storage.SWEEPER_PREFIX
            .. STORAGE_SPACE],
        nil
    )
    t.assert_not_equals(box.space[STORAGE_SPACE], nil)
end
