-- vshard in application threads: the memcached front-ends of a router run
-- in an application thread group (threads = 'fe', backend = 'vshard').
-- Each thread has its own vshard router, made from the config of the TX
-- router, and calls the storages directly: TX is not in the data path.
--
-- The cluster is in test/helpers/vshard_threads.lua: rs-1 = {s1-a, s1-b},
-- rs-2 = {s2-a}, and a router with NTHREADS application threads running
-- memcached instance 'r'. Group vshard_threads_topology adds rs-3 = {s3-a}
-- by a config reload; group vshard_threads_failover stops the master of
-- rs-2.
--
-- MCC_BENCH=1 also runs a rough TX router vs thread routers benchmark.

local fiber = require('fiber')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')
local th = require('test.helpers.threads')
local vc = require('test.helpers.vshard_cluster')
local vt = require('test.helpers.vshard_threads')

local SPACE = vc.SPACE
local NAME = vc.NAME
local NTHREADS = vt.NTHREADS
local BIG_CAS = '1000000000000000000'

-- Send a request and expect exactly this reply. A one-line reply is read
-- as a line, so that a different one shows.
local function check(c, request, reply)
    c:send(request)
    local got
    if reply:find('\r\n') == #reply - 1 then
        got = c:line()
    else
        got = #reply > 0 and c:read(#reply) or ''
    end
    t.assert_equals(got, reply, request)
end

local function reset_space(cluster, masters)
    for _, name in ipairs(masters or {'s1-a', 's2-a'}) do
        cluster[name]:exec(function(space)
            box.space[space]:truncate()
            box.space._mcc_meta:delete('flush:' .. space)
        end, {SPACE})
    end
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

local function create_instance(cluster, name, opts)
    return cluster.router:exec(function(name_, opts_)
        return require('memcached').create(name_, opts_).status
    end, {name, opts})
end

-- n keys "<prefix><i>" in the bucket.
local function keys_in_bucket(cluster, bid, prefix, n)
    return cluster.router:exec(function(bid_, prefix_, n_)
        local vshard = require('vshard')
        local keys = {}
        local i = 0
        while #keys < n_ do
            i = i + 1
            local key = prefix_ .. i
            if vshard.router.bucket_id_strcrc32(key) == bid_ then
                table.insert(keys, key)
            end
        end
        return keys
    end, {bid, prefix, n})
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

-- Wait until the bucket is gone from the storage (garbage collected).
local function wait_bucket_gone(server, bid)
    server:exec(function(bid_)
        t.helpers.retrying({timeout = 30}, function()
            t.assert_equals(box.space._bucket:get(bid_), nil)
        end)
    end, {bid})
end

-- Read a dump up to its terminator; returns the item lines.
local function read_dump(c, terminator, sep)
    t.assert_equals(c:line(), 'OK\r\n')
    local lines = {}
    while true do
        local line = c.sock:read(sep, client.TIMEOUT)
        t.assert(
            line ~= nil and line ~= '',
            ('no terminator after %s'):format(table.concat(lines))
        )
        if line == terminator then
            break
        end
        table.insert(lines, line)
    end
    return lines
end

-- A sorted copy.
local function sorted(list)
    local res = {}
    for i, v in ipairs(list) do
        res[i] = v
    end
    table.sort(res)
    return res
end

local function copy(t_)
    local res = {}
    for k, v in pairs(t_) do
        res[k] = v
    end
    return res
end

local function info(cluster, name)
    return cluster.router:exec(function(name_)
        return require('memcached').get(name_):info()
    end, {name or NAME})
end

--
-- A router with front-ends in application threads.
--

local g = t.group('vshard_threads')

-- Instances a test may create on the router.
local EXTRA = {'bad', 'tx', 'named', 'obj', 'lc'}

g.before_all(function(cg)
    cg.port = env.free_port()
    cg.cluster = vt.start({port = cg.port})
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
end)

g.after_each(function(cg)
    for _, c in ipairs(cg.clients or {}) do
        pcall(c.close, c)
    end
    cg.clients = nil
    destroy_instances(cg.cluster, EXTRA)
    cg.cluster.router:exec(function(space)
        require('vshard').router.static:enable()
        -- A local space made by a switch to backend = 'local'.
        if box.space[space] ~= nil then
            box.space[space]:drop()
        end
    end, {SPACE})
end)

local function connect(cg, port)
    local c = client.must_connect(port or cg.port)
    cg.clients = cg.clients or {}
    table.insert(cg.clients, c)
    return c
end

-- Connections until every thread has one, at most 64. Returns the
-- connections and the thread id of each. Skips the test on macOS, which
-- doesn't spread the connections over the threads (README.md,
-- Requirements).
local function connect_all_threads(cg)
    t.skip_if(jit.os == 'OSX', 'macOS: the connections are not spread')
    local conns, ids = {}, {}
    local seen, nseen = {}, 0
    for _ = 1, 64 do
        local c = connect(cg)
        table.insert(conns, c)
        -- The front-end registers the connection when it accepts it.
        local port = c.sock:name().port
        local id
        t.helpers.retrying({timeout = 5}, function()
            id = vt.conn_threads(cg.cluster)[port]
            t.assert_not_equals(id, nil)
        end)
        table.insert(ids, id)
        if not seen[id] then
            seen[id] = true
            nseen = nseen + 1
        end
        if nseen == NTHREADS then
            break
        end
    end
    t.assert_equals(nseen, NTHREADS, 'every thread got a connection')
    return conns, ids
end

g.test_served_by_threads = function(cg)
    local cluster = cg.cluster
    local N = 12
    -- The connections of the tests before are gone; the counters start
    -- from zero.
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(info(cluster).curr_connections, 0)
    end)
    cluster.router:exec(function(name)
        require('memcached.threads').reset_stats(name)
    end, {NAME})
    local conns, keys = {}, {}
    for i = 1, N do
        conns[i] = connect(cg)
        keys[i] = i % 2 == 1 and cg.a[i] or cg.b[i]
        t.assert_equals(conns[i]:set(keys[i], 'v' .. i), 'STORED\r\n')
    end
    for i = 1, N do
        t.assert_equals(conns[i % N + 1]:get(keys[i], 'get'), 'v' .. i)
    end

    -- Every connection is in one application thread; they are spread.
    local where = vt.conn_threads(cluster)
    local used, nused = {}, 0
    for i = 1, N do
        local id = where[conns[i].sock:name().port]
        t.assert_not_equals(id, nil, 'the connection is in a thread')
        if used[id] == nil then
            nused = nused + 1
        end
        used[id] = (used[id] or 0) + 1
    end
    -- Not on macOS (README.md, Requirements).
    if jit.os ~= 'OSX' then
        t.assert_ge(nused, 2, 'the connections are spread over the threads')
    end

    -- TX doesn't listen.
    t.assert_equals(
        cluster.router:exec(function(name)
            local srv = require('memcached').get(name).server
            return {
                srv.running,
                srv.listener == nil,
                srv.stats.total_connections,
            }
        end, {NAME}),
        {false, true, 0}
    )

    -- Each thread has its own router: the static one of the thread, made
    -- from the config of the TX router.
    local routers = vt.thread_routers(cluster)
    t.assert_equals(#routers, NTHREADS)
    for i, r in ipairs(routers) do
        t.assert_equals(r.thread_id, i)
        t.assert(r.exists)
        t.assert_equals(r.name, '_static_router')
        t.assert_equals(r.replicasets, {'rs-1', 'rs-2'})
    end
    t.helpers.retrying({timeout = 10}, function()
        for _, r in ipairs(vt.thread_routers(cluster)) do
            t.assert_equals(r.available_rw, 30)
            t.assert_equals(r.unknown, 0)
        end
    end)

    -- TX is not in the data path: with the TX router disabled, the threads
    -- still serve reads and writes, and the storages see the batches.
    local disabled = cluster.router:exec(function()
        local vshard = require('vshard')
        vshard.router.static:disable()
        local ok, err = pcall(
            vshard.router.callrw,
            1,
            'mcc.storage_stats',
            {'__mcc_r'}
        )
        return {ok, tostring(err and err.message or err)}
    end)
    t.assert_equals(disabled[1], false)
    t.assert_str_contains(disabled[2], 'router is disabled explicitly')
    local before = vt.storage_batches(cluster)
    for i = 1, N do
        t.assert_equals(conns[i]:set(keys[i], 'w' .. i), 'STORED\r\n')
        t.assert_equals(conns[i % N + 1]:get(keys[i], 'get'), 'w' .. i)
    end
    t.assert_ge(vt.storage_batches(cluster) - before, 2 * N)
    -- stats: the items of the storages through the thread's router, the
    -- counters summed over the threads.
    local c = conns[1]
    c:send('stats\r\n')
    local stats = c:read_until('END\r\n')
    t.assert_str_contains(stats, 'STAT curr_items ' .. N .. '\r\n')
    t.assert_str_contains(stats, 'STAT curr_connections ' .. N .. '\r\n')
    t.assert_str_contains(stats, 'STAT cmd_set ' .. 2 * N .. '\r\n')
    t.assert_str_contains(stats, 'STAT get_hits ' .. 2 * N .. '\r\n')
    t.assert_str_contains(stats, 'STAT threads ' .. NTHREADS .. '\r\n')
    -- "stats conns": the listeners and connections of every thread.
    local reply = c:call('stats conns\r\n', 'END\r\n')
    local states = {}
    for state in reply:gmatch('STAT %d+:state (%S+)\r\n') do
        states[state] = (states[state] or 0) + 1
    end
    t.assert_equals(states, {
        conn_listening = NTHREADS,
        conn_read = N - 1,
        conn_parse_cmd = 1,
    })
    -- flush_all through a thread reaches every storage.
    t.assert_equals(c:call('flush_all\r\n'), 'OK\r\n')
    for i = 1, N do
        t.assert_equals(conns[i]:get(keys[i], 'get'), nil)
    end
    cluster.router:exec(function()
        require('vshard').router.static:enable()
    end)

    -- info() sums the threads up.
    local inf = info(cluster)
    t.assert_equals(inf.thread_group, vt.GROUP)
    t.assert_equals(#inf.threads, NTHREADS)
    t.assert_equals(inf.curr_connections, N)
    t.assert_equals(inf.cmd_set, 2 * N)
    for i, ti in ipairs(inf.threads) do
        t.assert_equals(ti.total_connections, used[i] or 0)
    end
    -- "stats reset" resets every thread.
    t.assert_equals(c:call('stats reset\r\n'), 'RESET\r\n')
    inf = info(cluster)
    t.assert_equals(inf.cmd_set, 0)
    t.assert_equals(inf.cmd_get, 0)
end

g.test_protocol = function(cg)
    local a, b = cg.a, cg.b
    local c = connect(cg)
    -- A pipeline of classic and meta commands over both replicasets.
    c:send(
        ('set %s 5 0 1\r\nx\r\n'):format(a[1])
            .. ('set %s 0 0 1 noreply\r\ny\r\n'):format(b[1])
            .. ('get %s %s %s\r\n'):format(a[1], b[1], a[2])
            .. ('incr %s 1\r\n'):format(b[2])
            .. ('set %s 0 0 2\r\n41\r\n'):format(b[2])
            .. ('incr %s 1\r\n'):format(b[2])
            .. ('mg %s v f t\r\n'):format(a[1])
            .. ('ms %s 2 T0\r\nhi\r\n'):format(b[3])
            .. ('mg %s s v q\r\n'):format(b[3])
            .. ('mg %s v q\r\n'):format(a[3])
            .. ('ma %s N0 J10 v\r\n'):format(a[4])
            .. ('md %s q\r\n'):format(a[4])
            .. 'mn\r\n'
    )
    t.assert_equals(c:line(), 'STORED\r\n')
    t.assert_equals(
        c:read_until('END\r\n'),
        ('VALUE %s 5 1\r\nx\r\nVALUE %s 0 1\r\ny\r\nEND\r\n'):format(
            a[1],
            b[1]
        )
    )
    t.assert_equals(c:line(), 'NOT_FOUND\r\n')
    t.assert_equals(c:line(), 'STORED\r\n')
    t.assert_equals(c:line(), '42\r\n')
    t.assert_equals(c:line(), 'VA 1 f5 t-1\r\n')
    t.assert_equals(c:line(), 'x\r\n')
    t.assert_equals(c:line(), 'HD\r\n')
    t.assert_equals(c:line(), 'VA 2 s2\r\n')
    t.assert_equals(c:line(), 'hi\r\n')
    t.assert_equals(c:line(), 'VA 2\r\n')
    t.assert_equals(c:line(), '10\r\n')
    t.assert_equals(c:line(), 'MN\r\n')
    -- The data is on the storages.
    t.assert_equals(vc.tuple(cg.cluster['s1-a'], a[1]).value, 'x')
    t.assert_equals(vc.tuple(cg.cluster['s2-a'], b[3]).value, 'hi')
    -- CAS works across connections, whatever their threads.
    local _, _, cas = c:get(a[1])
    local c2 = connect(cg)
    t.assert_equals(
        c2:call(('cas %s 0 0 1 %s\r\nz\r\n'):format(a[1], cas)),
        'STORED\r\n'
    )
    t.assert_equals(
        c:call(('cas %s 0 0 1 %s\r\nw\r\n'):format(a[1], cas)),
        'EXISTS\r\n'
    )
    -- Meta win tokens live in the items on the storage.
    check(c, ('mg %s N30 s\r\n'):format(b[5]), 'HD s0 W\r\n')
    check(c2, ('mg %s s\r\n'):format(b[5]), 'HD s0 Z\r\n')
    t.assert_equals(
        c:call('version\r\n'):match('^VERSION 1%.6%.%d+%-mcc'),
        'VERSION 1.6.45-mcc'
    )
end

g.test_multi_get = function(cg)
    local keys = {}
    for i = 1, 8 do
        table.insert(keys, cg.a[i])
        table.insert(keys, cg.b[i])
    end
    local conns = connect_all_threads(cg)
    local c = conns[1]
    local expected, stored = {}, {}
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
    local cas = {}
    for _, k in ipairs(stored) do
        cas[k] = select(3, c:get(k))
    end
    local line = table.concat(keys, ' ')
    -- The same answer through every thread, in the order of the keys.
    for _, conn in ipairs(conns) do
        t.assert_equals(
            conn:call('get ' .. line .. '\r\n', 'END\r\n'),
            table.concat(expected) .. 'END\r\n'
        )
        conn:send('gets ' .. line .. '\r\n')
        local _, items = conn:read_values()
        t.assert_equals(#items, #stored)
        for i, item in ipairs(items) do
            t.assert_equals(item.key, stored[i])
            t.assert_equals(item.cas, cas[item.key])
        end
    end
end

-- The thread routers follow a moved bucket while the client stays
-- connected to its thread's front-end.
g.test_bucket_move = function(cg)
    local cluster = cg.cluster
    local k = cg.a[7]
    local bid = vc.route(cluster, k)
    local w = keys_in_bucket(cluster, bid, 'w', 1)[1]
    local c = connect(cg)
    local port = c.sock:name().port
    local id
    t.helpers.retrying({timeout = 5}, function()
        id = vt.conn_threads(cluster)[port]
        t.assert_not_equals(id, nil)
    end)
    local function resends()
        return vt.thread_routers(cluster)[id].resends
    end
    -- Without discovery a thread router learns about the move only from
    -- the storage, so the move is certain to go through the resend path.
    local function discovery(mode)
        vt.thread_eval(
            cluster,
            [[
            local rec = require('memcached.state').thread_servers.r
            rec.backend:router():discovery_set(...)
        ]],
            {mode}
        )
    end
    discovery('off')

    check(c, ('ms %s 1 E%s\r\nv\r\n'):format(k, BIG_CAS), 'HD\r\n')
    check(c, ('mg %s N30 s\r\n'):format(w), 'HD s0 W\r\n')
    local before = resends()
    bucket_send(cluster['s1-a'], bid, 'rs-2')
    -- The item has moved with its CAS; the thread's router followed it.
    check(
        c,
        ('mg %s v c\r\n'):format(k),
        ('VA 1 c%s\r\nv\r\n'):format(BIG_CAS)
    )
    t.assert_gt(resends(), before)
    t.assert_equals(vc.tuple(cluster['s2-a'], k).bucket_id, bid)
    check(c, ('ms %s 1\r\nx\r\n'):format(k), 'HD\r\n')
    check(c, ('mg %s c\r\n'):format(k), 'HD c1000000000000000001\r\n')
    -- The win token has moved with the item.
    check(c, ('mg %s s\r\n'):format(w), 'HD s0 Z\r\n')
    -- Another thread's router finds the bucket as well.
    local conns, ids = connect_all_threads(cg)
    for i, conn in ipairs(conns) do
        if ids[i] ~= id then
            check(conn, ('mg %s v\r\n'):format(k), 'VA 1\r\nx\r\n')
        end
    end

    -- And back.
    wait_bucket_gone(cluster['s1-a'], bid)
    bucket_send(cluster['s2-a'], bid, 'rs-1')
    check(c, ('ms %s 1\r\ny\r\n'):format(k), 'HD\r\n')
    check(
        c,
        ('mg %s v c\r\n'):format(k),
        'VA 1 c1000000000000000002\r\ny\r\n'
    )
    t.assert_equals(vc.tuple(cluster['s1-a'], k).value, 'y')
    wait_bucket_gone(cluster['s2-a'], bid)
    discovery('on')
end

-- lru_crawler metadump|mgdump through the thread routers: every bucket, in
-- pages.
g.test_dump = function(cg)
    local cluster = cg.cluster
    local c = connect(cg)
    -- Flushed items are not dumped.
    t.assert_equals(c:set(cg.a[1], 'old'), 'STORED\r\n')
    t.assert_equals(c:call('flush_all\r\n'), 'OK\r\n')
    -- Six keys of one bucket, to have several pages.
    local bid = vc.route(cluster, cg.b[1])
    local keys = keys_in_bucket(cluster, bid, 'p', 6)
    for i = 1, 6 do
        table.insert(keys, cg.a[i])
        table.insert(keys, cg.b[i + 1])
    end
    local flags_of = {}
    for i, k in ipairs(keys) do
        t.assert_equals(c:set(k, 'v' .. i, {flags = i}), 'STORED\r\n')
        flags_of[k] = i
    end
    vt.thread_eval(
        cluster,
        [[
        local mod = require('memcached.backend.vshard')
        mod.DUMP_PAGE, mod.DUMP_WORKERS = ...
    ]],
        {2, 3}
    )

    c:send('lru_crawler metadump all\r\n')
    local lines = read_dump(c, 'END\r\n', '\n')
    local got = {}
    for _, line in ipairs(lines) do
        local key, flags = line:match(
            '^key=(%S+) exp=%-1 la=%d+ cas=%d+ fetch=no cls=1 size=%d+ '
                .. 'flags=(%d+) \n$'
        )
        t.assert_not_equals(key, nil, line)
        t.assert_equals(tonumber(flags), flags_of[key], line)
        table.insert(got, key)
    end
    t.assert_equals(sorted(got), sorted(keys))

    c:send('lru_crawler mgdump hash\r\n')
    lines = read_dump(c, 'EN\r\n', '\r\n')
    got = {}
    for _, line in ipairs(lines) do
        table.insert(got, line:match('^mg (%S+)\r\n$'))
    end
    t.assert_equals(sorted(got), sorted(keys))
    -- Classes without items.
    t.assert_equals(
        c:call('lru_crawler metadump 2\r\n', 'END\r\n'),
        'OK\r\nEND\r\n'
    )
    -- The connection goes on.
    t.assert_equals(c:get(keys[1], 'get'), 'v1')

    -- An error in the middle of a dump closes the connection.
    local port = env.free_port()
    t.assert_equals(
        create_instance(cluster, 'bad', {
            listen = '127.0.0.1:' .. port,
            backend = 'vshard',
            threads = vt.GROUP,
            space_name = 'nope',
        }),
        'running'
    )
    local bad = connect(cg, port)
    bad:send('lru_crawler metadump all\r\n')
    t.assert_equals(bad:line(), 'OK\r\n')
    t.assert_equals(
        bad:line(),
        "SERVER_ERROR space 'nope' is not served by this storage\r\n"
    )
    t.assert(bad:is_closed())

    -- The same in TX.
    port = env.free_port()
    create_instance(cluster, 'tx', {
        listen = '127.0.0.1:' .. port,
        backend = 'vshard',
        space_name = SPACE,
    })
    local tx = connect(cg, port)
    tx:send('lru_crawler mgdump all\r\n')
    t.assert_equals(#read_dump(tx, 'EN\r\n', '\r\n'), #keys)

    vt.thread_eval(
        cluster,
        [[
        local mod = require('memcached.backend.vshard')
        mod.DUMP_PAGE, mod.DUMP_WORKERS = 512, 8
    ]]
    )
end

-- mcc.reload() with vshard routers in the threads.
g.test_reload = function(cg)
    local cluster = cg.cluster
    local conns, ids = connect_all_threads(cg)
    for i, c in ipairs(conns) do
        t.assert_equals(c:set(cg.b[i], 'v' .. i), 'STORED\r\n')
    end
    vt.mark_routers(cluster, 'before')
    vt.thread_eval(
        cluster,
        [[
        package.loaded['memcached.backend.vshard'].mcc_test_old = true
    ]]
    )
    local function fibers()
        local res = cluster.router:exec(function()
            return require('memcached.reload').fiber_count()
        end)
        local list = vt.thread_eval(
            cluster,
            [[
            return require('memcached.reload').fiber_count()
        ]]
        )
        return {res, list}
    end
    local fibers_before = fibers()

    local summary = cluster.router:exec(function()
        return require('memcached').reload()
    end)
    t.assert_equals(#summary.threads[vt.GROUP], NTHREADS)

    -- The connections survive, in their threads, and work.
    local where = vt.conn_threads(cluster)
    for i, c in ipairs(conns) do
        t.assert_equals(where[c.sock:name().port], ids[i])
        t.assert_equals(c:get(cg.b[i], 'get'), 'v' .. i)
        t.assert_equals(c:set(cg.a[i], 'w' .. i), 'STORED\r\n')
        t.assert_equals(vc.tuple(cluster['s1-a'], cg.a[i]).value, 'w' .. i)
    end
    -- The routers are kept; the backend code is new.
    for _, r in ipairs(vt.thread_routers(cluster)) do
        t.assert_equals(r.mark, 'before')
    end
    for _, new in ipairs(
        vt.thread_eval(
            cluster,
            [[
        local mod = package.loaded['memcached.backend.vshard']
        return mod.mcc_test_old == nil
    ]]
        )
    ) do
        t.assert(new)
    end
    t.assert_equals(fibers(), fibers_before)

    -- The threads still follow the TX router: a reconfiguration of the TX
    -- router (here by hand) reaches them.
    local sync_timeout = cluster.router:exec(function()
        local vshard = require('vshard')
        local cfg = table.deepcopy( -- luacheck: ignore 143
            vshard.router.static.current_cfg
        )
        local old = cfg.sync_timeout
        cfg.sync_timeout = old + 6
        vshard.router.cfg(cfg)
        return old
    end)
    t.helpers.retrying({timeout = 10}, function()
        for _, r in ipairs(vt.thread_routers(cluster)) do
            t.assert_equals(r.sync_timeout, sync_timeout + 6)
        end
    end)
    cluster.router:exec(function(old)
        local vshard = require('vshard')
        local cfg = table.deepcopy( -- luacheck: ignore 143
            vshard.router.static.current_cfg
        )
        cfg.sync_timeout = old
        vshard.router.cfg(cfg)
    end, {sync_timeout})
    t.helpers.retrying({timeout = 10}, function()
        for _, r in ipairs(vt.thread_routers(cluster)) do
            t.assert_equals(r.sync_timeout, sync_timeout)
            t.assert_equals(r.mark, 'before')
        end
    end)
    for i, c in ipairs(conns) do
        t.assert_equals(c:get(cg.a[i], 'get'), 'w' .. i)
    end
end

-- A named router: it may appear after the instance; a router object in
-- the options reaches the threads as its name; read modes.
g.test_named_router = function(cg)
    local cluster = cg.cluster
    local a = cg.a[1]
    local port = env.free_port()
    t.assert_equals(
        create_instance(cluster, 'named', {
            listen = '127.0.0.1:' .. port,
            backend = 'vshard',
            threads = vt.GROUP,
            space_name = SPACE,
            read_mode = 'bro',
            vshard = {router = 'later', timeout = 1},
        }),
        'running'
    )
    local c = connect(cg, port)
    t.assert_equals(
        c:set(a, 'x'),
        "SERVER_ERROR vshard router 'later' does not exist\r\n"
    )
    -- TX makes the router: the threads make theirs from its config.
    cluster.router:exec(function()
        local vshard = require('vshard')
        assert(vshard.router.new('later', vshard.router.static.current_cfg))
    end)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(c:set(a, 'x'), 'STORED\r\n')
    end)
    -- Every thread makes its router from the config TX sends; the set
    -- went through one of them, the others may still be configuring theirs
    -- (router:info() raises until then).
    t.helpers.retrying({timeout = 10}, function()
        for _, r in ipairs(vt.thread_routers(cluster, 'named')) do
            t.assert_equals(r.name, 'later')
            t.assert_equals(r.replicasets, {'rs-1', 'rs-2'})
        end
    end)
    -- bro: the plain gets of a thread also reach the replica.
    local replica = cluster['s1-b']
    local r0 = vc.space_info(replica).batches
    t.helpers.retrying({timeout = 10}, function()
        check(
            c,
            ('get %s\r\n'):format(a),
            ('VALUE %s 0 1\r\nx\r\nEND\r\n'):format(a)
        )
        t.assert_gt(vc.space_info(replica).batches, r0)
    end)

    -- A router object in the options of another instance: its threads use
    -- the same thread routers.
    vt.mark_routers(cluster, 'later', 'named')
    local port2 = env.free_port()
    t.assert_equals(
        cluster.router:exec(function(listen, group, space)
            local vshard = require('vshard')
            return require('memcached').create('obj', {
                listen = listen,
                backend = 'vshard',
                threads = group,
                space_name = space,
                vshard = {router = vshard.router.internal.routers.later},
            }).status
        end, {'127.0.0.1:' .. port2, vt.GROUP, SPACE}),
        'running'
    )
    local c2 = connect(cg, port2)
    t.assert_equals(c2:get(a, 'get'), 'x')
    for _, r in ipairs(vt.thread_routers(cluster, 'obj')) do
        t.assert_equals(r.name, 'later')
        t.assert_equals(r.mark, 'later')
    end
end

g.test_lifecycle = function(cg)
    local cluster = cg.cluster
    local a = cg.a[1]
    local port = env.free_port()
    local function call(method, arg)
        return cluster.router:exec(function(method_, arg_)
            local inst = require('memcached').get('lc')
            inst[method_](inst, arg_)
            return {
                inst.status,
                require('memcached.threads').watching('lc'),
            }
        end, {method, arg})
    end
    t.assert_equals(
        create_instance(cluster, 'lc', {
            listen = '127.0.0.1:' .. port,
            backend = 'vshard',
            threads = vt.GROUP,
            space_name = SPACE,
        }),
        'running'
    )
    local c = connect(cg, port)
    t.assert_equals(c:set(a, '1'), 'STORED\r\n')
    vt.mark_routers(cluster, 'lc', 'lc')

    -- stop(): no listener, no watcher.
    t.assert_equals(call('stop'), {'stopped', false})
    t.assert(c:is_closed())
    t.assert_not(client.is_listening(port))
    -- start(): the thread routers are reused.
    t.assert_equals(call('start'), {'running', true})
    c = connect(cg, port)
    t.assert_equals(c:get(a, 'get'), '1')
    for _, r in ipairs(vt.thread_routers(cluster, 'lc')) do
        t.assert_equals(r.mark, 'lc')
    end

    -- The backend changes in the threads, the connection stays.
    t.assert_equals(call('cfg', {backend = 'local'}), {'running', false})
    t.assert_equals(c:get(a, 'get'), nil)
    t.assert_equals(c:set(a, 'local'), 'STORED\r\n')
    t.assert_equals(
        call('cfg', {
            backend = 'vshard',
            vshard = {timeout = 0.5},
        }),
        {'running', true}
    )
    t.assert_equals(c:get(a, 'get'), '1')
    t.assert_equals(
        vt.thread_eval(
            cluster,
            [[
        return require('memcached.state').thread_servers.lc.backend
               .timeout
    ]]
        ),
        {0.5, 0.5, 0.5}
    )

    -- destroy(): the front-ends go; the thread routers stay for the others.
    cluster.router:exec(function()
        require('memcached').destroy('lc')
    end)
    t.assert(c:is_closed())
    for _, r in ipairs(vt.thread_routers(cluster, 'lc')) do
        t.assert_equals(r.exists, false)
    end
    t.assert_equals(
        client.get(cg.port, a),
        ('VALUE %s 0 1\r\n1\r\nEND\r\n'):format(a)
    )
end

-- A rough benchmark: a pipelined get/set load from the application threads
-- of a client process against a TX front-end ('tx', with the TX router) and
-- the front-ends in the threads ('r', with the thread routers).
g.test_bench = function(cg)
    t.skip_if(os.getenv('MCC_BENCH') == nil, 'set MCC_BENCH=1 to run')
    local cluster = cg.cluster
    local port = env.free_port()
    create_instance(cluster, 'tx', {
        listen = '127.0.0.1:' .. port,
        backend = 'vshard',
        space_name = SPACE,
        vshard = {timeout = 5},
        max_item_size = 65536,
    })
    cluster.router:exec(function()
        local backend = require('memcached').get('tx').backend
        local value = string.rep('x', 32)
        for i = 0, 9999, 500 do
            local reqs = {}
            for j = i, i + 499 do
                table.insert(reqs, {
                    op = 'store',
                    key = 'k' .. j,
                    value = value,
                    flags = 0,
                    exptime = 0,
                    mode = 'set',
                })
            end
            for _, resp in ipairs(backend:exec(reqs)) do
                assert(resp.status == 'STORED', resp.message)
            end
        end
    end)
    local loader = th.start('mcc_vt_bench_client', 4)
    local duration = tonumber(os.getenv('MCC_BENCH_DURATION')) or 3
    local GETS = 1e9
    local profiles = {
        {
            '1 conn, depth 1, gets',
            {
                conns = 1,
                depth = 1,
                target = 1,
                ratio = GETS,
            },
        },
        {'4x8 conns, depth 16, get/set 9:1', {conns = 8, depth = 16}},
        {'4x8 conns, depth 16, gets', {conns = 8, depth = 16, ratio = GETS}},
        {'4x8 conns, depth 64, gets', {conns = 8, depth = 64, ratio = GETS}},
        {'4x64 conns, depth 1, gets', {conns = 64, depth = 1, ratio = GETS}},
    }
    local results = {}
    for _, p in ipairs(profiles) do
        for _, target in ipairs({
            {'tx router     ', port},
            {'thread routers', cg.port},
        }) do
            local load = copy(p[2])
            load.port = target[2]
            load.duration = duration
            local r = th.load(loader, load)
            t.assert_equals(r.errors, 0)
            table.insert(
                results,
                ('%s %-36s %8.0f ops/s  batch rtt avg '
                    .. '%7.3f ms p99 %7.3f ms'):format(
                        target[1],
                        p[1],
                        r.ops_per_sec,
                        r.avg * 1000,
                        r.p99 * 1000
                    )
            )
        end
    end
    loader:drop()
    io.stderr:write('\n' .. table.concat(results, '\n') .. '\n')
end

--
-- A replicaset joins by a config reload while clients are connected.
--

local tg = t.group('vshard_threads_topology')

tg.before_all(function(cg)
    cg.port = env.free_port()
    cg.cluster = vt.start({port = cg.port})
end)

tg.after_all(function(cg)
    if cg.load ~= nil then
        cg.load.stop = true
    end
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)

-- A client that keeps storing and reading keys until load.stop; records
-- the errors.
local function load_start(port, keys)
    local load = {rounds = 0, errors = {}, stop = false, done = false}
    fiber.create(function()
        local ok, err = pcall(function()
            local c = client.must_connect(port)
            while not load.stop do
                load.rounds = load.rounds + 1
                for _, key in ipairs(keys) do
                    local value = ('%s:%d'):format(key, load.rounds)
                    local reply = c:set(key, value)
                    if reply ~= 'STORED\r\n' then
                        table.insert(
                            load.errors,
                            key .. ': ' .. tostring(reply)
                        )
                    end
                    local got = c:get(key, 'get')
                    if got ~= value then
                        table.insert(
                            load.errors,
                            ('%s: got %s'):format(
                                key,
                                tostring(got)
                            )
                        )
                    end
                end
            end
            c:close()
        end)
        if not ok then
            table.insert(load.errors, tostring(err))
        end
        load.done = true
    end)
    return load
end

tg.test_topology_change = function(cg)
    local cluster = cg.cluster
    local a = vc.keys_on(cluster, 'rs-1', 6, 'a')
    local b = vc.keys_on(cluster, 'rs-2', 3, 'b')
    local k = a[1]
    local bid = vc.route(cluster, k)
    local conns = {}
    for i = 1, 6 do
        conns[i] = client.must_connect(cg.port)
    end
    local c = conns[1]
    t.assert_equals(c:set(k, 'v1'), 'STORED\r\n')
    local load = load_start(cg.port, {a[2], a[3], b[1], b[2]})
    cg.load = load
    t.helpers.retrying({timeout = 10}, function()
        t.assert_ge(load.rounds, 2)
    end)

    -- rs-3 joins the cluster.
    cluster:sync(vt.config({port = cg.port, rs3 = true}), {start_stop = true})
    cluster:reload()
    vc.wait_space(cluster['s3-a'])
    t.helpers.retrying({timeout = 30}, function()
        t.assert(
            cluster.router:exec(function()
                return require('vshard').router.static.replicasets['rs-3']
                    ~= nil
            end)
        )
        for _, r in ipairs(vt.thread_routers(cluster)) do
            t.assert_equals(r.replicasets, {'rs-1', 'rs-2', 'rs-3'})
        end
    end)

    -- A bucket moves to rs-3: the clients that stayed connected reach it
    -- through the routers of their threads.
    bucket_send(cluster['s1-a'], bid, 'rs-3')
    t.assert_equals(vc.tuple(cluster['s3-a'], k).value, 'v1')
    for i, conn in ipairs(conns) do
        t.assert_equals(conn:get(k, 'get'), i == 1 and 'v1' or 'v' .. i - 1)
        t.assert_equals(conn:set(k, 'v' .. i), 'STORED\r\n')
    end
    t.assert_equals(vc.tuple(cluster['s3-a'], k).value, 'v6')
    wait_bucket_gone(cluster['s1-a'], bid)
    t.assert_equals(vc.tuple(cluster['s1-a'], k), nil)
    local before = load.rounds
    t.helpers.retrying({timeout = 10}, function()
        t.assert_ge(load.rounds, before + 2)
    end)
    load.stop = true
    t.helpers.retrying({timeout = 10}, function()
        t.assert(load.done)
    end)
    t.assert_equals(load.errors, {})
    for _, conn in ipairs(conns) do
        conn:close()
    end
end

--
-- A storage master goes down while the front-ends run in the threads.
--

local fg = t.group('vshard_threads_failover')

local FAILOVER_TIMEOUT = 0.5

fg.before_all(function(cg)
    cg.port = env.free_port()
    cg.cluster = vt.start({port = cg.port, timeout = FAILOVER_TIMEOUT})
    cg.a = vc.keys_on(cg.cluster, 'rs-1', 10, 'a')
    cg.b = vc.keys_on(cg.cluster, 'rs-2', 10, 'b')
end)

fg.after_all(function(cg)
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)

fg.before_each(function(cg)
    reset_space(cg.cluster)
end)

fg.after_each(function(cg)
    for _, c in ipairs(cg.clients or {}) do
        pcall(c.close, c)
    end
    cg.clients = nil
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

-- As vshard_failover.test_master_down in vshard_test.lua, through a
-- connection in every thread: each thread router fails the requests of the
-- replicaset without a master within the batch timeout, keeps serving the
-- other one, and follows the master when it is back.
fg.test_master_down = function(cg)
    local cluster = cg.cluster
    local a, b = cg.a, cg.b
    -- A connection per thread.
    local all, ids = connect_all_threads(cg)
    local conns, seen = {}, {}
    for i, conn in ipairs(all) do
        if not seen[ids[i]] then
            seen[ids[i]] = true
            table.insert(conns, conn)
        end
    end
    t.assert_equals(#conns, NTHREADS)
    local c = conns[1]
    local stored = 0
    local function set(conn, key, value)
        t.assert_equals(conn:set(key, value), 'STORED\r\n')
        stored = stored + 1
    end
    set(c, a[1], 'x')
    set(c, b[1], 'y')

    cluster['s2-a']:stop()
    local LIMIT = 3 * FAILOVER_TIMEOUT
    local ERR = 'SERVER_ERROR .*\r\n'
    for i, conn in ipairs(conns) do
        -- rs-2: SERVER_ERROR after the timeout, no hang.
        check_fast(
            conn,
            ('set %s 0 0 1\r\nz\r\n'):format(b[1 + i]),
            ERR,
            LIMIT
        )
        check_fast(conn, ('get %s\r\n'):format(b[1]), ERR, LIMIT)
        check_fast(conn, ('mg %s v\r\n'):format(b[1]), ERR, LIMIT)
        -- rs-1 keeps working.
        set(conn, a[1 + i], 'w')
        check(
            conn,
            ('get %s\r\n'):format(a[1]),
            ('VALUE %s 0 1\r\nx\r\nEND\r\n'):format(a[1])
        )
    end
    -- A mixed pipeline keeps the order.
    c:send(
        ('set %s 0 0 1\r\n1\r\nset %s 0 0 1\r\n2\r\nget %s\r\n'):format(
            a[9],
            b[9],
            a[9]
        )
    )
    t.assert_equals(c:line(), 'STORED\r\n')
    stored = stored + 1
    t.assert_str_matches(c:line(), ERR)
    t.assert_equals(
        c:read_until('END\r\n'),
        ('VALUE %s 0 1\r\n1\r\nEND\r\n'):format(a[9])
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
    -- stats still answers: no items, the counters of every thread.
    local started = fiber.clock()
    c:send('stats\r\n')
    local stats = c:read_until('END\r\n')
    t.assert_le(fiber.clock() - started, LIMIT)
    t.assert_str_contains(stats, 'STAT curr_items 0\r\n')
    t.assert_str_contains(
        stats,
        ('STAT curr_connections %d\r\n'):format(
            #all
        )
    )
    t.assert_str_contains(stats, ('STAT threads %d\r\n'):format(NTHREADS))

    cluster['s2-a']:start()
    -- Every thread router finds the master again.
    for _, conn in ipairs(conns) do
        t.helpers.retrying({timeout = 60}, function()
            t.assert_equals(conn:get(b[1], 'get'), 'y')
        end)
    end
    for i, conn in ipairs(conns) do
        set(conn, b[1 + i], 'z')
    end
    c:send('stats\r\n')
    t.assert_str_contains(
        c:read_until('END\r\n'),
        ('STAT curr_items %d\r\n'):format(stored)
    )
end
