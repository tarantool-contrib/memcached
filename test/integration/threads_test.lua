-- Front-ends in application threads: the requests are served by the
-- threads (connections are spread over them, the data is executed in TX),
-- the lifecycle reaches the threads, counters are summed up.
--
-- MCC_BENCH=1 also runs a rough TX-only vs threads benchmark.

local socket = require('socket')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')
local th = require('test.helpers.threads')

local g = t.group('threads')

local NTHREADS = 4

g.before_all(function(cg)
    cg.server = th.start('mcc_threads', NTHREADS)
end)

g.after_all(function(cg)
    cg.server:drop()
end)

g.before_each(function(cg)
    cg.port = env.free_port()
end)

g.after_each(function(cg)
    for _, c in ipairs(cg.clients or {}) do
        pcall(c.close, c)
    end
    cg.clients = nil
    if cg.ipc_knobs then
        -- What the IPC tests change.
        cg.ipc_knobs = nil
        cg.server:exec(function()
            require('memcached.threads').IPC_ABI = 1
        end)
        th.thread_eval(
            cg.server,
            [[
            local mod = package.loaded['memcached.backend.thread']
            if mod ~= nil then
                for k, v in pairs(...) do
                    mod[k] = v
                end
            end
        ]],
            {cg.ipc_knobs_defaults}
        )
    end
    th.destroy(cg.server, 'fe')
    th.destroy(cg.server, 'ref')
    cg.server:exec(function()
        for _, name in ipairs({'__mcc_fe', '__mcc_ref'}) do
            if box.space[name] ~= nil then
                box.space[name]:drop()
            end
        end
        require('memcached').datapath(true)
    end)
end)

local function connect(cg, port)
    local c = client.must_connect(port or cg.port)
    cg.clients = cg.clients or {}
    table.insert(cg.clients, c)
    return c
end

local function exec_inst(cg, method, arg)
    return cg.server:exec(function(method_, arg_)
        local inst = require('memcached').get('fe')
        local res = inst[method_](inst, arg_)
        return res ~= nil and res.status or nil
    end, {method, arg})
end

-- Client port -> thread id of the connections of 'fe' in each thread.
local function conn_threads(cg)
    local list = th.thread_eval(
        cg.server,
        [[
        local state = require('memcached.state')
        local servers = state.thread_servers or {}
        local rec = servers.fe
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
    ]]
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

g.test_served_by_threads = function(cg)
    t.assert_equals(
        th.create(cg.server, 'fe', cg.port, {threads = 'app'}),
        'running'
    )
    local N = 32
    local conns = {}
    for i = 1, N do
        conns[i] = connect(cg)
        t.assert_equals(conns[i]:set('k' .. i, 'v' .. i), 'STORED\r\n')
    end
    -- Each value is read through another connection: the data is shared
    -- (it lives in TX).
    for i = 1, N do
        t.assert_equals(conns[i % N + 1]:get('k' .. i, 'get'), 'v' .. i)
    end

    -- Every client connection is served by exactly one application thread,
    -- and they are spread over the threads.
    local where = conn_threads(cg)
    local used = {}
    for i = 1, N do
        local port = conns[i].sock:name().port
        local id = where[port]
        t.assert_not_equals(id, nil, 'the connection is in a thread')
        used[id] = (used[id] or 0) + 1
    end
    local nused = 0
    for id in pairs(used) do
        t.assert(id >= 1 and id <= NTHREADS)
        nused = nused + 1
    end
    -- macOS's SO_REUSEPORT lets the threads bind the port but doesn't
    -- spread the connections over them (README.md, Requirements).
    if jit.os ~= 'OSX' then
        t.assert_ge(nused, 2, 'the connections are spread over the threads')
    end

    -- TX doesn't listen and has never accepted a connection.
    t.assert_equals(
        cg.server:exec(function()
            local srv = require('memcached').get('fe').server
            return {
                srv.running,
                srv.listener == nil,
                srv.stats.total_connections,
            }
        end),
        {false, true, 0}
    )

    -- info() sums up the counters of the threads and lists them.
    local info = th.info(cg.server, 'fe')
    t.assert_equals(info.thread_group, 'app')
    t.assert_equals(info.port, cg.port)
    t.assert_equals(#info.threads, NTHREADS)
    local total = 0
    for i, ti in ipairs(info.threads) do
        t.assert_equals(ti.thread_id, i)
        t.assert_equals(ti.total_connections, used[i] or 0)
        total = total + ti.cmd_set
    end
    t.assert_equals(total, N)
    t.assert_equals(info.curr_connections, N)
    t.assert_equals(info.total_connections, N)
    t.assert_equals(info.cmd_set, N)
    t.assert_equals(info.cmd_get, N)
    t.assert_equals(info.get_hits, N)

    -- So does "stats", in any thread.
    local c = conns[1]
    c:send('stats\r\n')
    local stats = c:read_until('END\r\n')
    t.assert_str_contains(stats, 'STAT curr_connections ' .. N .. '\r\n')
    t.assert_str_contains(stats, 'STAT cmd_set ' .. N .. '\r\n')
    t.assert_str_contains(stats, 'STAT get_hits ' .. N .. '\r\n')
    t.assert_str_contains(stats, 'STAT threads ' .. NTHREADS .. '\r\n')
    t.assert_str_contains(stats, 'STAT curr_items ' .. N .. '\r\n')
    c:send('stats settings\r\n')
    t.assert_str_contains(
        c:read_until('END\r\n'),
        'STAT num_threads ' .. NTHREADS .. '\r\n'
    )
    -- "stats conns" lists the listener and the connections of every
    -- thread, by fd.
    local reply = c:call('stats conns\r\n', 'END\r\n')
    local states, last = {}, -1
    for id, state in reply:gmatch('STAT (%d+):state (%S+)\r\n') do
        id = tonumber(id)
        t.assert_gt(id, last, 'by id, each once')
        last = id
        states[state] = (states[state] or 0) + 1
    end
    t.assert_equals(states, {
        conn_listening = NTHREADS,
        conn_read = N - 1,
        conn_parse_cmd = 1,
    })
    local own = 'STAT (%d+):addr tcp:127%.0%.0%.1:' .. c.sock:name().port
        .. '\r\n'
    local own_id = tonumber(reply:match(own))
    t.assert_str_contains(
        reply,
        ('STAT %d:state conn_parse_cmd\r\n'):format(
            own_id
        )
    )
    t.assert_str_contains(
        reply,
        ('STAT %d:listen_addr tcp:127.0.0.1:%d\r\n'):format(own_id, cg.port)
    )
    -- "stats reset" resets every thread.
    t.assert_equals(c:call('stats reset\r\n'), 'RESET\r\n')
    info = th.info(cg.server, 'fe')
    t.assert_equals(info.cmd_set, 0)
    t.assert_equals(info.cmd_get, 0)
end

g.test_protocol = function(cg)
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    local c = connect(cg)
    -- A pipeline of classic and meta commands is one batch for TX.
    c:send(
        'set a 5 0 1\r\nx\r\nset b 0 0 1 noreply\r\ny\r\n'
            .. 'get a b c\r\nincr n 1\r\n'
            .. 'set n 0 0 2\r\n41\r\nincr n 1\r\n'
            .. 'mg a v f t\r\nms m 2 T0\r\nhi\r\n'
            .. 'mg m s v q\r\nmg nope v q\r\n'
            .. 'mn\r\n'
    )
    t.assert_equals(c:line(), 'STORED\r\n')
    t.assert_equals(
        c:read_until('END\r\n'),
        'VALUE a 5 1\r\nx\r\nVALUE b 0 1\r\ny\r\nEND\r\n'
    )
    t.assert_equals(c:line(), 'NOT_FOUND\r\n')
    t.assert_equals(c:line(), 'STORED\r\n')
    t.assert_equals(c:line(), '42\r\n')
    t.assert_equals(c:line(), 'VA 1 f5 t-1\r\n')
    t.assert_equals(c:line(), 'x\r\n')
    t.assert_equals(c:line(), 'HD\r\n')
    t.assert_equals(c:line(), 'VA 2 s2\r\n')
    t.assert_equals(c:line(), 'hi\r\n')
    t.assert_equals(c:line(), 'MN\r\n')
    -- CAS works across threads.
    local _, _, cas = c:get('a')
    local c2 = connect(cg)
    t.assert_equals(
        c2:call(('cas a 0 0 1 %s\r\nz\r\n'):format(cas)),
        'STORED\r\n'
    )
    t.assert_equals(
        c:call(('cas a 0 0 1 %s\r\nw\r\n'):format(cas)),
        'EXISTS\r\n'
    )
    -- flush_all through a thread.
    t.assert_equals(c2:call('flush_all\r\n'), 'OK\r\n')
    t.assert_equals(c:get('a'), nil)
    t.assert_equals(
        c:call('version\r\n'):match('^VERSION 1%.6%.%d+%-mcc'),
        'VERSION 1.6.45-mcc'
    )
end

-- A socket listening on the port without SO_REUSEPORT.
local function occupy(port)
    local s = socket('AF_INET', 'SOCK_STREAM', 'tcp')
    s:setsockopt('SOL_SOCKET', 'SO_REUSEADDR', true)
    t.assert(s:bind('127.0.0.1', port))
    t.assert(s:listen(1))
    return s
end

g.test_lifecycle = function(cg)
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')

    -- stop() stops every thread: no listener, connections closed.
    t.assert_equals(exec_inst(cg, 'stop'), 'stopped')
    t.assert(c:is_closed())
    t.assert_not(client.is_listening(cg.port))
    t.assert_equals(exec_inst(cg, 'start'), 'running')
    c = connect(cg)
    t.assert_equals(c:get('a', 'get'), '1')

    -- A new URI: every thread listens on it, the connections survive.
    local port2 = env.free_port()
    exec_inst(cg, 'cfg', {listen = '127.0.0.1:' .. port2})
    t.assert_equals(c:get('a', 'get'), '1')
    t.assert_not(client.is_listening(cg.port))
    local list = th.thread_eval(
        cg.server,
        [[
        local srv = require('memcached.state').thread_servers.fe.srv
        return {srv.running, srv:info().port}
    ]]
    )
    t.assert_equals(#list, NTHREADS)
    for _, r in ipairs(list) do
        t.assert_equals(r, {true, port2})
    end
    local c2 = connect(cg, port2)
    t.assert_equals(c2:get('a', 'get'), '1')

    -- A URI that can't be bound: every thread keeps the old one.
    local busy_port = env.free_port()
    local busy = occupy(busy_port)
    t.assert_error_msg_contains(
        "can't bind",
        exec_inst,
        cg,
        'cfg',
        {listen = '127.0.0.1:' .. busy_port}
    )
    busy:close()
    t.assert_equals(c2:get('a', 'get'), '1')
    for _ = 1, 8 do
        local c3 = connect(cg, port2)
        t.assert_equals(c3:get('a', 'get'), '1')
    end
    t.assert_equals(th.info(cg.server, 'fe').listen, '127.0.0.1:' .. port2)

    -- The limits reach the threads.
    exec_inst(cg, 'cfg', {max_item_size = 10, max_connections = 100})
    list = th.thread_eval(
        cg.server,
        [[
        local srv = require('memcached.state').thread_servers.fe.srv
        return {srv.max_connections, srv.parse_opts.max_item_size}
    ]]
    )
    for _, r in ipairs(list) do
        t.assert_equals(r, {100, 10})
    end
    t.assert_equals(
        c2:set('big', string.rep('x', 11)),
        'SERVER_ERROR object too large for cache\r\n'
    )

    -- destroy() frees the front-ends in the threads.
    cg.server:exec(function()
        require('memcached').destroy('fe')
    end)
    t.assert(c2:is_closed())
    list = th.thread_eval(
        cg.server,
        [[
        local servers = require('memcached.state').thread_servers
        return servers.fe == nil
    ]]
    )
    t.assert_equals(list, {true, true, true, true})
end

g.test_move_to_threads = function(cg)
    th.create(cg.server, 'fe', cg.port)
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    t.assert_equals(th.info(cg.server, 'fe').thread_group, nil)
    -- The front-ends move from TX to the threads.
    exec_inst(cg, 'cfg', {threads = 'app'})
    t.assert(c:is_closed())
    c = connect(cg)
    t.assert_equals(c:get('a', 'get'), '1')
    local info = th.info(cg.server, 'fe')
    t.assert_equals(info.thread_group, 'app')
    t.assert_equals(info.status, 'running')
    t.assert_equals(info.curr_connections, 1)
    t.assert_equals(
        cg.server:exec(function()
            return require('memcached').get('fe').server.running
        end),
        false
    )
end

-- threads = false (or box.NULL) moves the front-ends back to TX.
g.test_move_back_to_tx = function(cg)
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    local function where()
        local res = cg.server:exec(function()
            local inst = require('memcached').get('fe')
            return {
                status = inst.status,
                threads = inst.opts.threads,
                fe_group = inst.fe_group,
                tx_running = inst.server.running,
                mark = rawget(inst, 'mcc_test_mark'),
            }
        end)
        -- A front-end in every thread, or in none.
        local list = th.thread_eval(
            cg.server,
            [[
            local servers = require('memcached.state').thread_servers
            return servers ~= nil and servers.fe ~= nil
        ]]
        )
        t.assert_equals(#list, NTHREADS)
        for _, v in ipairs(list) do
            t.assert_equals(v, list[1])
        end
        res.in_threads = list[1]
        return res
    end
    cg.server:exec(function()
        rawset(require('memcached').get('fe'), 'mcc_test_mark', true)
    end)
    for _, value in ipairs({false, box.NULL}) do
        exec_inst(cg, 'cfg', {threads = value})
        t.assert(c:is_closed(), 'moving the front-ends closes the connections')
        -- The same instance, in TX: no front-ends in the threads.
        t.assert_equals(where(), {
            status = 'running',
            tx_running = true,
            in_threads = false,
            mark = true,
        })
        c = connect(cg)
        t.assert_equals(c:get('a', 'get'), '1')
        t.assert_equals(th.info(cg.server, 'fe').curr_connections, 1)
        -- And to the threads again.
        exec_inst(cg, 'cfg', {threads = 'app'})
        t.assert(c:is_closed())
        t.assert_equals(where(), {
            status = 'running',
            threads = 'app',
            fe_group = 'app',
            tx_running = false,
            in_threads = true,
            mark = true,
        })
        c = connect(cg)
        t.assert_equals(c:get('a', 'get'), '1')
    end
    -- Nothing to move: the connection stays.
    exec_inst(cg, 'cfg', {threads = 'app'})
    t.assert_equals(c:get('a', 'get'), '1')
    exec_inst(cg, 'cfg', {threads = false})
    c = connect(cg)
    exec_inst(cg, 'cfg', {threads = false})
    t.assert_equals(c:get('a', 'get'), '1')
    -- A stopped instance moves too, and starts in TX.
    exec_inst(cg, 'cfg', {threads = 'app'})
    exec_inst(cg, 'stop')
    exec_inst(cg, 'cfg', {threads = false})
    t.assert_equals(exec_inst(cg, 'start'), 'running')
    t.assert_equals(where().tx_running, true)
    th.destroy(cg.server, 'fe')

    -- create() takes false as well.
    t.assert_equals(
        th.create(cg.server, 'fe', cg.port, {threads = false}),
        'running'
    )
    t.assert_equals(where().threads, nil)
    t.assert_equals(where().tx_running, true)
end

g.test_validation = function(cg)
    local listen = '127.0.0.1:' .. cg.port
    local cases = {
        {
            {listen = listen, threads = 'nope'},
            "Application thread group 'nope' does not exist",
        },
        {
            {listen = listen, threads = 'tx'},
            "Application thread group 'tx' does not exist",
        },
        {
            {listen = 'unix/:/tmp/mcc.sock', threads = 'app'},
            "Option 'threads' needs a TCP listen URI with a fixed port",
        },
        {
            {listen = '127.0.0.1:0', threads = 'app'},
            "Option 'threads' needs a TCP listen URI with a fixed port",
        },
        {
            {listen = listen, threads = true},
            "Bad value for option 'threads'",
        },
        {
            {listen = listen, threads = 1},
            "Option 'threads' must be a string or false, got number",
        },
    }
    local errors = cg.server:exec(function(cases_)
        local mcc = require('memcached')
        local res = {}
        for i, case in ipairs(cases_) do
            local ok, err = pcall(mcc.create, 'fe', case[1])
            res[i] = {ok, tostring(err), mcc.get('fe') == nil}
        end
        -- The same rules in cfg().
        local inst = mcc.create('fe', {listen = cases_[1][1].listen})
        for i, case in ipairs(cases_) do
            local ok, err = pcall(inst.cfg, inst, case[1])
            res[#cases_ + i] = {ok, tostring(err), inst.opts.threads == nil}
        end
        return res
    end, {cases})
    for i, res in ipairs(errors) do
        local case = cases[(i - 1) % #cases + 1]
        t.assert_equals(res[1], false)
        t.assert_str_contains(res[2], case[2])
        t.assert(res[3])
    end
end

-- The raw batch -----------------------------------------------------------
--
-- The classic get, gets and set at the head of a batch go to TX as bytes
-- and the C batch serves them there (backend/thread.lua batch()). The
-- replies and the counters are those of a front-end in TX: each script runs
-- against an instance in the threads ('fe') and one in TX ('ref').

-- The chunks of a script over the keys <p>* (@ in the chunks), each sent
-- in a write of its own: pipelines that mix the commands of the C batch
-- with others, commands split across reads, noreply, the forms the C batch
-- leaves to the parser.
local function raw_script(p)
    local big = string.rep('v', 2000)
    local chunks = {
        -- Only commands of the C batch.
        'set @a 5 0 1\r\nx\r\nset @b 0 0 1 noreply\r\ny\r\n'
            .. 'get @a @b @c\r\ngets @a @b\r\n',
        -- Then others, then the C batch's again.
        'get @a\r\nincr @n 1\r\nset @n 0 0 3\r\n100\r\nmg @a v f\r\n'
            .. 'get @b\r\nmn\r\nget @n\r\n',
        -- A get holds its item for the rest of the batch: the decrement
        -- after it makes a new item, "99" (in place, it would be "99 ").
        'set @h 0 0 3\r\n100\r\nget @h\r\ndecr @h 1\r\nget @h\r\n',
        -- No read before it: in place.
        'set @i 0 0 3\r\n100\r\n',
        'decr @i 1\r\nget @i\r\n',
        -- Split across reads: a value, a command line, a line end.
        'set @s 0 0 10\r\n01234',
        '56789\r\nget @s\r\nget @a',
        ' @s\r',
        '\nget @a\r\n',
        -- noreply, and a set in the middle of gets.
        'get @a\r\nset @c 3 0 1 noreply\r\nz\r\nset @d 0 0 2\r\nzz\r\n'
            .. 'get @c @d\r\n',
        -- Forms the C batch leaves to the parser: a bad data block, a key
        -- too long, a value too large, a negative exptime, gat.
        'get @a\r\nset @k 0 0 1\r\nxx\r\nget @a\r\n',
        'get @' .. string.rep('k', 251) .. '\r\nget @a\r\n',
        'get @a\r\nset @big 0 0 ' .. #big
            .. '\r\n' .. big
            .. '\r\nget @big @a\r\n',
        'set @e 0 -1 1\r\nx\r\nget @e\r\ngat 0 @a\r\n',
        -- A barrier after commands of the C batch.
        'get @a\r\nset @f 0 0 1\r\nf\r\nflush_all 100\r\nget @f\r\n',
        'flush_all 0\r\n',
        'set @g 0 0 1\r\ng\r\nget @g\r\nversion\r\n',
        -- A dump after commands of the C batch fails, as after any.
        'get @g\r\nlru_crawler metadump all\r\n',
        'mn\r\n',
    }
    for i, chunk in ipairs(chunks) do
        chunks[i] = chunk:gsub('@', p)
    end
    return chunks
end

-- Send the chunks of a script, a moment apart, and read every reply, with
-- the CAS values masked.
local function run_script(c, chunks)
    local fiber = require('fiber')
    local mns = 0
    for _, chunk in ipairs(chunks) do
        c:send(chunk)
        fiber.sleep(0.02)
        for _ in chunk:gmatch('mn\r\n') do
            mns = mns + 1
        end
    end
    -- Up to the reply of the last mn.
    local parts = {}
    for i = 1, mns do
        parts[i] = c:read_until('\r\nMN\r\n')
        t.assert(parts[i], 'the replies of the script')
    end
    local reply = table.concat(parts)
    reply = reply:gsub('(VALUE %S+ %d+ %d+) %d+\r\n', '%1 <cas>\r\n')
    return reply
end

-- The counters of an instance (summed over its threads).
local function counters(cg, name)
    return cg.server:exec(function(name_)
        local info = require('memcached').get(name_):info()
        local res = {}
        for _, c in ipairs(require('memcached.server').COUNTERS) do
            res[c] = info[c]
        end
        return res
    end, {name})
end

-- The raw batches and their commands, summed over the threads; with
-- ipc = true, also those of the batches through the channels.
local function raw_stats(cg, ipc)
    local list = th.thread_eval(
        cg.server,
        [[
        local rec = (require('memcached.state').thread_servers or {}).fe
        local b = rec ~= nil and rec.backend or {}
        return {b.raw_batches or 0, b.raw_commands or 0, b.ipc_batches or 0}
    ]]
    )
    local res = {0, 0, 0}
    for _, r in ipairs(list) do
        for i = 1, 3 do
            res[i] = res[i] + r[i]
        end
    end
    if not ipc then
        res[3] = nil
    end
    return res
end

local function datapath_enabled(cg)
    return cg.server:exec(function()
        return require('memcached').datapath().enabled
    end)
end

-- Run a script against 'fe' (threads) and 'ref' (TX), compare.
local function compare(cg, p)
    local c = connect(cg)
    local ref = connect(cg, cg.ref_port)
    local chunks = raw_script(p)
    local got = run_script(c, chunks)
    local want = run_script(ref, chunks)
    t.assert_equals(got, want)
    c:close()
    ref:close()
    return got
end

-- Whether memcached.ipc is built.
local function ipc_built(cg)
    return cg.server:exec(function()
        return (pcall(require, 'memcached.ipc'))
    end)
end

local function raw_instances(cg)
    local opts = {max_item_size = 1024}
    opts.threads = 'app'
    t.assert_equals(th.create(cg.server, 'fe', cg.port, opts), 'running')
    cg.ref_port = env.free_port()
    opts.threads = nil
    t.assert_equals(th.create(cg.server, 'ref', cg.ref_port, opts), 'running')
end

g.test_raw_batch = function(cg)
    raw_instances(cg)
    local reply = compare(cg, 'r')
    -- The holds of the C batch reach the Lua batch after it.
    t.assert_str_contains(
        reply,
        '99\r\nVALUE rh 0 2\r\n99\r\nEND\r\n',
        true
    )
    t.assert_str_contains(reply, '99\r\nVALUE ri 0 3\r\n99 \r\nEND\r\n', true)
    t.assert_str_contains(
        reply,
        'VALUE rs 0 10\r\n0123456789\r\nEND\r\n',
        true
    )
    -- The counters: those of TX, in the thread of the connection.
    local want = counters(cg, 'ref')
    t.assert_equals(counters(cg, 'fe'), want)
    local info = th.info(cg.server, 'fe')
    local busy = 0
    for _, ti in ipairs(info.threads) do
        if ti.cmd_get > 0 then
            busy = busy + 1
            t.assert_equals(ti.cmd_get, want.cmd_get)
            t.assert_equals(ti.get_hits, want.get_hits)
            t.assert_equals(ti.cmd_set, want.cmd_set)
            t.assert_equals(ti.total_items, want.total_items)
        end
    end
    t.assert_equals(busy, 1)
    local raw = raw_stats(cg, true)
    if datapath_enabled(cg) then
        t.assert_gt(raw[1], 5, 'the raw batch serves the batches')
        t.assert_gt(raw[2], 10)
        if ipc_built(cg) then
            t.assert_equals(raw[3], raw[1], 'through the channel')
        end
    else
        t.assert_equals(raw, {0, 0, 0})
    end
end

-- The C path switched off in TX: the threads fall back to the Lua batch,
-- and use the raw batch again after it is switched on.
g.test_raw_batch_fallback = function(cg)
    t.skip_if(not datapath_enabled(cg), 'the C data path is off')
    raw_instances(cg)
    th.thread_eval(
        cg.server,
        [[
        require('memcached.backend.thread').RAW_RETRY = 0.1
    ]]
    )
    cg.server:exec(function()
        require('memcached').datapath(false)
    end)
    compare(cg, 'x')
    local raw = raw_stats(cg)
    t.assert_equals(raw, {0, 0})
    cg.server:exec(function()
        require('memcached').datapath(true)
    end)
    require('fiber').sleep(0.2)
    compare(cg, 'y')
    t.assert_gt(raw_stats(cg)[1], 5)
    t.assert_equals(counters(cg, 'fe'), counters(cg, 'ref'))
end

-- At most RAW_MAX bytes of input go to TX: the C batch serves a prefix of
-- the input, and the Lua batch the rest, with the holds of the prefix even
-- when it ends at the end of a command.
g.test_raw_batch_limit = function(cg)
    raw_instances(cg)
    local held = 'set lh 0 0 3\r\n100\r\nget lh\r\n'
    local function set_max(n)
        th.thread_eval(
            cg.server,
            [[
            require('memcached.backend.thread').RAW_MAX = ...
        ]],
            {n}
        )
    end
    set_max(#held)
    local c = connect(cg)
    local chunks = {held .. 'decr lh 1\r\nget lh\r\nmn\r\n'}
    t.assert_equals(
        run_script(c, chunks),
        'STORED\r\nVALUE lh 0 3\r\n100\r\nEND\r\n99\r\n'
            .. 'VALUE lh 0 2\r\n99\r\nEND\r\nMN\r\n'
    )
    set_max(50)
    compare(cg, 'l')
    t.assert_equals(counters(cg, 'fe').cmd_get, counters(cg, 'ref').cmd_get + 2)
    if datapath_enabled(cg) then
        t.assert_gt(raw_stats(cg)[1], 5)
    end
end

-- mcc.reload() in threads mode: the raw batch runs the new code.
g.test_raw_batch_reload = function(cg)
    raw_instances(cg)
    compare(cg, 'p')
    local before = raw_stats(cg)
    local c = connect(cg)
    local ref = connect(cg, cg.ref_port)
    t.assert_equals(c:set('pk', 'v'), 'STORED\r\n')
    t.assert_equals(ref:set('pk', 'v'), 'STORED\r\n')
    cg.server:exec(function()
        require('memcached').reload()
    end)
    -- The connection is kept, and its next batch runs the new code.
    t.assert_equals(c:get('pk', 'get'), 'v')
    t.assert_equals(ref:get('pk', 'get'), 'v')
    compare(cg, 'q')
    local after = raw_stats(cg)
    if datapath_enabled(cg) then
        t.assert_gt(after[1], before[1] + 5)
    end
    t.assert_equals(counters(cg, 'fe'), counters(cg, 'ref'))
end

-- The channels (memcached.ipc) ---------------------------------------------
--
-- The raw batches of a thread backend object go to TX through a channel of
-- its own (backend/thread.lua "The channel", threads.lua "The IPC"), or by
-- threads.call when it can't be used.

-- A load: conns connections, each over keys of its own (so it knows every
-- reply), sending pipelines of up to depth gets, sets (of sizes from
-- opts.sizes) and deletes, each written by a fiber of its own while the
-- connection reads the replies, and checked byte for byte. With
-- opts.closing, a connection that the server closes ends quietly. Returns
-- a function that stops the load and returns its counters.
local load_runs = 0

local function start_load(port, opts)
    local fiber = require('fiber')
    load_runs = load_runs + 1
    local prefix = ('l%d_%d_'):format(load_runs, math.random(1e9))
    local sizes = opts.sizes or {1, 5, 50, 50, 500, 3000}
    local res = {
        pipelines = 0,
        gets = 0,
        hits = 0,
        sets = 0,
        errors = 0,
        closed = 0,
        mismatch = nil,
    }
    local stop = false
    local running = 0
    local function main(id)
        running = running + 1
        local c = client.connect(port)
        if c == nil then
            res.errors = res.errors + 1
            running = running - 1
            return
        end
        local model = {}
        local seq = 0
        while not stop do
            local cmds, want = {}, {}
            local gets, hits, sets = 0, 0, 0
            for i = 1, math.random(1, opts.depth or 16) do
                seq = seq + 1
                local key = ('%s%d_%d'):format(prefix, id, math.random(1, 8))
                local r = math.random()
                if r < 0.3 then
                    local size = sizes[math.random(#sizes)]
                    local v = string.rep(string.char(97 + seq % 26), size)
                    cmds[i] = ('set %s 0 0 %d\r\n%s\r\n'):format(key, size, v)
                    want[i] = 'STORED\r\n'
                    model[key] = v
                    sets = sets + 1
                elseif r < 0.35 then
                    cmds[i] = 'delete ' .. key .. '\r\n'
                    want[i] = model[key] ~= nil and 'DELETED\r\n'
                        or 'NOT_FOUND\r\n'
                    model[key] = nil
                else
                    local v = model[key]
                    cmds[i] = 'get ' .. key .. '\r\n'
                    want[i] = v ~= nil
                        and ('VALUE %s 0 %d\r\n%s\r\nEND\r\n'):format(
                            key,
                            #v,
                            v
                        ) or 'END\r\n'
                    gets = gets + 1
                    hits = hits + (v ~= nil and 1 or 0)
                end
            end
            local data = table.concat(cmds)
            local expect = table.concat(want)
            fiber.create(function()
                c.sock:write(data)
            end)
            local got = c.sock:read(#expect, 10)
            if got ~= expect then
                if opts.closing and (got == nil or #got < #expect) then
                    res.closed = res.closed + 1
                elseif res.mismatch == nil then
                    res.mismatch = {
                        got = got and got:sub(1, 200),
                        want = expect:sub(1, 200),
                    }
                    res.errors = res.errors + 1
                else
                    res.errors = res.errors + 1
                end
                break
            end
            res.pipelines = res.pipelines + 1
            res.gets = res.gets + gets
            res.hits = res.hits + hits
            res.sets = res.sets + sets
        end
        c:close()
        running = running - 1
    end
    for id = 1, opts.conns or 16 do
        fiber.create(main, id)
    end
    return function()
        stop = true
        local deadline = fiber.clock() + 30
        while running > 0 do
            t.assert_lt(fiber.clock(), deadline, 'the load stops')
            fiber.sleep(0.01)
        end
        return res
    end
end

-- Run a load for a while.
local function run_load(port, opts)
    local stop = start_load(port, opts)
    require('fiber').sleep(opts.duration or 0.5)
    return stop()
end

local function assert_load_ok(res)
    t.assert_equals(res.mismatch, nil)
    t.assert_equals(res.errors, 0)
    t.assert_gt(res.pipelines, 0)
end

-- The channels alive in the process and the fibers serving them in TX.
local function ipc_state(cg)
    return cg.server:exec(function()
        local n = 0
        for _, f in pairs(require('fiber').info()) do
            if tostring(f.name):find('^mcc/ipc/') then
                n = n + 1
            end
        end
        return {
            count = require('memcached.ipc').count(),
            served = #require('memcached.threads').ipc_info(),
            fibers = n,
        }
    end)
end

-- Every channel is let go: none alive, none served, no fibers.
local function assert_ipc_released(cg)
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(ipc_state(cg), {count = 0, served = 0, fibers = 0})
    end)
end

-- Set fields of backend/thread.lua in every thread.
local function thread_set(cg, fields)
    th.thread_eval(
        cg.server,
        [[
        local mod = require('memcached.backend.thread')
        for k, v in pairs(...) do
            mod[k] = v
        end
    ]],
        {fields}
    )
end

local THREAD_DEFAULTS = {
    IPC = true,
    IPC_MODULE = 'memcached.ipc',
    IPC_SLOTS = 1024,
    IPC_RETRY = 5,
    IPC_MAX = 4 * 1048576,
}

local function ipc_group(cg)
    t.skip_if(not ipc_built(cg), 'memcached.ipc is not built')
    cg.ipc_knobs = true
    cg.ipc_knobs_defaults = THREAD_DEFAULTS
end

-- The ids of the channels TX serves.
local function ipc_ids(cg)
    return cg.server:exec(function()
        local res = {}
        for _, c in ipairs(require('memcached.threads').ipc_info()) do
            table.insert(res, c.id)
        end
        return res
    end)
end

-- Many connections over every thread, pipelines that mix the commands of
-- the C batch with others, values over the slot buffer kept (64 KiB):
-- every reply and the counters as sent.
g.test_ipc_load = function(cg)
    ipc_group(cg)
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    local res = run_load(cg.port, {
        conns = 48,
        depth = 24,
        sizes = {1, 5, 50, 500, 3000, 70000, 200000},
        duration = 1,
    })
    assert_load_ok(res)
    local info = th.info(cg.server, 'fe')
    t.assert_equals(info.cmd_get, res.gets)
    t.assert_equals(info.get_hits, res.hits)
    t.assert_equals(info.cmd_set, res.sets)
    local raw = raw_stats(cg, true)
    if datapath_enabled(cg) then
        -- All but those of the connections that came while their thread
        -- opened its channel.
        t.assert_gt(raw[3], 50)
        t.assert_ge(raw[3], raw[1] * 0.9)
    end
    -- A channel per thread backend object.
    local st = ipc_state(cg)
    t.assert_le(st.count, NTHREADS)
    t.assert_equals(st.served, st.count)
    th.destroy(cg.server, 'fe')
    assert_ipc_released(cg)
end

-- No free slot, or a request too big: the raw batch goes by threads.call.
g.test_ipc_back_pressure = function(cg)
    ipc_group(cg)
    t.skip_if(not datapath_enabled(cg), 'the C data path is off')
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    thread_set(cg, {IPC_SLOTS = 2, IPC_MAX = 2000})
    local res = run_load(cg.port, {
        conns = 32,
        depth = 8,
        sizes = {5, 50, 3000},
        duration = 1,
    })
    thread_set(cg, THREAD_DEFAULTS)
    assert_load_ok(res)
    local raw = raw_stats(cg, true)
    t.assert_gt(raw[3], 10, 'some through the channels')
    t.assert_gt(raw[1] - raw[3], 10, 'some by threads.call')
    local info = th.info(cg.server, 'fe')
    t.assert_equals(info.cmd_get, res.gets)
    t.assert_equals(info.cmd_set, res.sets)
    th.destroy(cg.server, 'fe')
    assert_ipc_released(cg)
end

-- Without the module, with the channels off, or when TX doesn't serve
-- them, the raw batch goes by threads.call; a channel is opened again
-- IPC_RETRY after TX refused one.
g.test_ipc_fallback = function(cg)
    ipc_group(cg)
    t.skip_if(not datapath_enabled(cg), 'the C data path is off')
    local function check(fields, want_ipc)
        th.create(cg.server, 'fe', cg.port, {threads = 'app'})
        thread_set(cg, fields)
        local res = run_load(cg.port, {conns = 8, duration = 0.3})
        assert_load_ok(res)
        local raw = raw_stats(cg, true)
        t.assert_gt(raw[1], 10)
        if want_ipc then
            t.assert_gt(raw[3], 0)
        else
            t.assert_equals(raw[3], 0)
            t.assert_equals(ipc_state(cg).count, 0)
        end
        thread_set(cg, THREAD_DEFAULTS)
        th.destroy(cg.server, 'fe')
        assert_ipc_released(cg)
    end
    -- The .so is missing.
    check({IPC_MODULE = 'memcached.no_such_ipc'}, false)
    -- Off.
    check({IPC = false}, false)
    -- TX refuses the channels (another ABI): threads.call, then the
    -- channels once TX serves them.
    cg.server:exec(function()
        require('memcached.threads').IPC_ABI = -1
    end)
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    thread_set(cg, {IPC_RETRY = 0.2})
    local stop = start_load(cg.port, {conns = 8})
    require('fiber').sleep(0.3)
    t.assert_equals(raw_stats(cg, true)[3], 0)
    t.assert_equals(ipc_state(cg).served, 0)
    cg.server:exec(function()
        require('memcached.threads').IPC_ABI = 1
    end)
    t.helpers.retrying({timeout = 5}, function()
        t.assert_gt(raw_stats(cg, true)[3], 10)
    end)
    assert_load_ok(stop())
    thread_set(cg, THREAD_DEFAULTS)
    th.destroy(cg.server, 'fe')
    assert_ipc_released(cg)
end

-- The fibers that serve the channels in TX are cancelled: TX lets the
-- channels go, the threads go on by threads.call and open new channels.
g.test_ipc_tx_cancelled = function(cg)
    ipc_group(cg)
    t.skip_if(not datapath_enabled(cg), 'the C data path is off')
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    thread_set(cg, {IPC_RETRY = 0.3})
    local stop = start_load(cg.port, {conns = 16, sizes = {5, 50, 70000}})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_gt(ipc_state(cg).served, 0)
    end)
    local before = ipc_ids(cg)
    cg.server:exec(function()
        for id, f in pairs(require('fiber').info()) do
            if tostring(f.name):find('^mcc/ipc/') then
                pcall(function()
                    require('fiber').find(id):cancel()
                end)
            end
        end
    end)
    require('fiber').sleep(0.1)
    -- New channels.
    t.helpers.retrying({timeout = 5}, function()
        local ids = ipc_ids(cg)
        t.assert_gt(#ids, 0)
        t.assert_gt(ids[1], before[#before])
    end)
    local res = stop()
    assert_load_ok(res)
    t.assert_equals(th.info(cg.server, 'fe').cmd_set, res.sets)
    thread_set(cg, THREAD_DEFAULTS)
    th.destroy(cg.server, 'fe')
    assert_ipc_released(cg)
end

-- mcc.reload() under load: the connections and the channels stay, the
-- requests in flight are served, nothing leaks.
g.test_ipc_reload = function(cg)
    ipc_group(cg)
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    local stop = start_load(cg.port, {conns = 24, sizes = {5, 500, 70000}})
    require('fiber').sleep(0.3)
    local before = ipc_state(cg)
    for _ = 1, 2 do
        cg.server:exec(function()
            require('memcached').reload()
        end)
        require('fiber').sleep(0.2)
    end
    local res = stop()
    assert_load_ok(res)
    local after = ipc_state(cg)
    t.assert_equals(after.count, before.count)
    t.assert_equals(after.served, before.served)
    t.assert_le(after.fibers, after.served * 128)
    local info = th.info(cg.server, 'fe')
    t.assert_equals(info.cmd_get, res.gets)
    t.assert_equals(info.cmd_set, res.sets)
    th.destroy(cg.server, 'fe')
    assert_ipc_released(cg)
end

-- stop(), destroy() and the moves between TX and the threads with
-- requests in flight (sets that wait for the WAL, big values): the
-- connections are closed, no request hangs, the channels are let go.
g.test_ipc_stop_in_flight = function(cg)
    ipc_group(cg)
    local load = {
        conns = 32,
        depth = 32,
        sizes = {50, 3000, 70000, 200000},
        closing = true,
    }
    local function in_flight(action)
        local stop = start_load(cg.port, load)
        require('fiber').sleep(0.3)
        action()
        local res = stop()
        t.assert_equals(res.mismatch, nil)
        t.assert_equals(res.errors, 0)
        t.assert_gt(res.pipelines + res.closed, 0)
    end
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    in_flight(function()
        t.assert_equals(exec_inst(cg, 'stop'), 'stopped')
    end)
    assert_ipc_released(cg)
    t.assert_equals(exec_inst(cg, 'start'), 'running')
    in_flight(function()
        exec_inst(cg, 'cfg', {threads = false})
    end)
    assert_ipc_released(cg)
    in_flight(function()
        exec_inst(cg, 'cfg', {threads = 'app'})
    end)
    assert_load_ok(run_load(cg.port, {conns = 8, duration = 0.2}))
    in_flight(function()
        th.destroy(cg.server, 'fe')
    end)
    assert_ipc_released(cg)
    -- The server serves on.
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    assert_load_ok(run_load(cg.port, {conns = 8, duration = 0.2}))
end

-- A rough benchmark: a pipelined get/set load from the application threads
-- of a client process against TX-only and threaded front-ends, the latter
-- with the raw batch through the channels and by threads.call.
g.test_bench = function(cg)
    t.skip_if(os.getenv('MCC_BENCH') == nil, 'set MCC_BENCH=1 to run')
    local loader = th.start('mcc_bench_client', 4)
    local duration = tonumber(os.getenv('MCC_BENCH_DURATION')) or 3
    local results = {}
    local function run(label, opts, load, ipc)
        opts.max_item_size = 65536
        th.create(cg.server, 'fe', cg.port, opts)
        if opts.threads ~= nil then
            -- Before the first batch, which opens the channel.
            th.thread_eval(
                cg.server,
                [[
                require('memcached.backend.thread').IPC = ...
            ]],
                {ipc}
            )
        end
        cg.server:exec(function()
            local s = box.space.__mcc_fe
            for i = 0, 9999 do
                s:replace({
                    'k' .. i,
                    string.rep('x', 32),
                    0,
                    0,
                    i,
                    0,
                    0,
                    false,
                    false,
                    0,
                })
            end
        end)
        load.port = cg.port
        load.duration = duration
        local r = th.load(loader, load)
        t.assert_equals(r.errors, 0)
        table.insert(
            results,
            ('%-44s %8.0f ops/s  batch rtt avg %7.3f ms '
                .. 'p50 %7.3f ms p99 %7.3f ms'):format(
                    label,
                    r.ops_per_sec,
                    r.avg * 1000,
                    r.p50 * 1000,
                    r.p99 * 1000
                )
        )
        th.destroy(cg.server, 'fe')
    end
    -- ratio: gets per set; GETS: gets only.
    local GETS = 1e9
    local profiles = {
        {
            '1 conn, depth 1, get/set 9:1',
            {conns = 1, depth = 1, target = 1, ratio = 9},
        },
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
        {'4x64 conns, depth 1, get/set 9:1', {conns = 64, depth = 1}},
        {'4x64 conns, depth 1, gets', {conns = 64, depth = 1, ratio = GETS}},
    }
    -- threads: through the channels (memcached.ipc); call: by threads.call.
    for _, p in ipairs(profiles) do
        run('tx      ' .. p[1], {}, p[2])
        run('call    ' .. p[1], {threads = 'app'}, p[2], false)
        run('threads ' .. p[1], {threads = 'app'}, p[2], true)
    end
    loader:drop()
    io.stderr:write('\n' .. table.concat(results, '\n') .. '\n')
end
