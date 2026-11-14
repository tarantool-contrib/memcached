-- The C connection loop (memcached/cloop.c, server.lua conn_step()) against
-- the Lua connection loop with the C batch (mcc.datapath_loop(false)): the
-- same bytes on the wire, the same stored tuples and the same counters, and
-- the connection's life: idle timeouts, closes, srv:cfg() and flushes while
-- it waits in C, the server stopping, a client that doesn't read, many
-- connections, the stats from another connection. Skipped when the data
-- path is not built. The hot reload of a connection in the C loop is in
-- reload_test.lua.
--
-- Both runs of a script start from the same state (an empty space, the
-- same CAS counter and created stamp, a frozen clock), as in
-- datapath_test.lua.

local ffi = require('ffi')
local fiber = require('fiber')
local msgpack = require('msgpack')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local g = t.group('cloop')

local NAME = 'cl'
local SPACE = '__mcc_' .. NAME

local mcc
local schema
local state

local real_time = fiber.time
local clock = {now = 1800000000.25}

g.before_all(function(cg)
    env.box_cfg()
    mcc = require('memcached')
    schema = require('memcached.schema')
    state = require('memcached.state')
    cg.saved = {
        cas_counter = state.cas_counter,
        stamp_last = state.stamp_last,
        datapath = not state.datapath_off,
        loop = not state.cloop_off,
    }
    cg.available = mcc.datapath_loop().available
    cg.port = env.free_port()
end)

g.before_each(function(cg)
    t.skip_if(not cg.available, env.datapath_so .. ' is not built')
    cg.clients = {}
end)

g.after_each(function(cg)
    fiber.time = real_time
    for _, c in ipairs(cg.clients) do
        pcall(c.sock.close, c.sock)
    end
    if box.info.ro then
        box.cfg {read_only = false}
    end
    mcc.datapath(cg.saved.datapath)
    mcc.datapath_loop(cg.saved.loop)
    if mcc.get(NAME) ~= nil then
        mcc.destroy(NAME)
    end
    env.drop_spaces({SPACE})
    if box.space._mcc_meta ~= nil then
        box.space._mcc_meta:delete(schema.flush_key(SPACE))
    end
end)

g.after_all(function(cg)
    fiber.time = real_time
    if cg.saved ~= nil then
        mcc.datapath(cg.saved.datapath)
        mcc.datapath_loop(cg.saved.loop)
        state.cas_counter = cg.saved.cas_counter
        state.stamp_last = cg.saved.stamp_last
    end
end)

local function connect(cg)
    local c = client.must_connect(cg.port)
    table.insert(cg.clients, c)
    return c
end

-- The commands the C loops of the instance served so far.
local function served()
    local inst = mcc.get(NAME)
    local counters = inst and inst.server.dp_counters
    if not counters then
        return 0
    end
    return (require('memcached.datapath').served(counters))
end

-- A new instance (these options) on an empty space with the C loop on or
-- off, from the same state, with a frozen clock.
local function reset(cg, loop, opts)
    for _, c in ipairs(cg.clients) do
        pcall(c.sock.close, c.sock)
    end
    cg.clients = {}
    if mcc.get(NAME) ~= nil then
        mcc.destroy(NAME)
    end
    -- MEMCACHED_DATAPATH=lua or batch runs this file as it is.
    mcc.datapath(true)
    t.assert_equals(mcc.datapath_loop(loop).enabled, loop)
    local o = {listen = '127.0.0.1:' .. cg.port, expire = {enabled = false}}
    for k, v in pairs(opts or {}) do
        o[k] = v
    end
    mcc.create(NAME, o)
    box.space[SPACE]:truncate()
    box.space._mcc_meta:delete(schema.flush_key(SPACE))
    state.cas_counter = ffi.cast('uint64_t', 1000000)
    state.stamp_last = 0
    state.tokens[SPACE] = nil
    clock.now = 1800000000.25
    fiber.time = function()
        return clock.now
    end
    cg.client = connect(cg)
end

-- Everything the server sends until the reply of an mn sent after data.
local function call(c, data)
    c:send(data .. 'mn\r\n')
    local reply = c:read_until('MN\r\n')
    t.assert(reply, 'no reply to ' .. data)
    return reply:sub(1, -5)
end

local function dump()
    local res = {}
    for _, tuple in box.space[SPACE]:pairs() do
        res[tuple[1]] = msgpack.encode(tuple)
    end
    return res
end

-- The counters of "stats" that don't depend on the time or the process.
local VOLATILE = {pid = true, uptime = true, time = true, version = true}

local function counters(c)
    local reply = c:call('stats\r\n', 'END\r\n')
    local res = {}
    for name, value in reply:gmatch('STAT (%S+) ([^\r]*)\r\n') do
        if not VOLATILE[name] then
            res[name] = value
        end
    end
    return res
end

-- Run a script (strings: sent with an mn after them, the reply read up to
-- its MN; functions: fn(cg) -> a string or nil) with the C loop on or off.
-- Returns the replies, the tuples and the counters of "stats" from another
-- connection.
local function run(cg, script, loop, opts)
    reset(cg, loop, opts)
    local replies = {}
    for _, step in ipairs(script) do
        if type(step) == 'function' then
            replies[#replies + 1] = step(cg) or ''
        else
            replies[#replies + 1] = call(cg.client, step)
        end
    end
    local tuples = dump()
    local stats = counters(connect(cg))
    if loop then
        t.assert(served() > 0, 'the C loop served nothing')
    else
        t.assert_equals(served(), 0)
    end
    return replies, tuples, stats
end

-- The C and the Lua loop give the same replies, tuples and counters.
local function same(cg, script, opts)
    local lua_replies, lua_tuples, lua_stats = run(cg, script, false, opts)
    local c_replies, c_tuples, c_stats = run(cg, script, true, opts)
    for i = 1, #script do
        t.assert_equals(
            c_replies[i],
            lua_replies[i],
            ('step %d: %s'):format(i, tostring(script[i]))
        )
    end
    t.assert_equals(c_tuples, lua_tuples)
    t.assert_equals(c_stats, lua_stats)
    return c_replies
end

local function set_cmd(key, value, flags, exptime, extra)
    return ('set %s %d %d %d%s\r\n%s\r\n'):format(
        key,
        flags or 0,
        exptime or 0,
        #value,
        extra or '',
        value
    )
end

local function tick(seconds)
    return function()
        clock.now = clock.now + seconds
    end
end

-- Served and unserved commands in one pipeline, in every order: the C
-- loop returns to Lua at the first one it doesn't serve, and Lua goes on
-- with the same batch (back into the C batch for the rest).
g.test_mixed_pipelines = function(cg)
    local served_cmds = {
        set_cmd('a', '1'),
        'get a b\r\n',
        'gets a\r\n',
        'mg a v f c t\r\n',
        'ms b 2 T0\r\nxy\r\n',
        'incr a 5\r\n',
        'delete b\r\n',
        'touch a 100\r\n',
        'md nope q\r\n',
        'ma a D2\r\n',
    }
    local unserved = {
        'version\r\n',
        'verbosity 1\r\n',
        'me a\r\n',
        'bogus\r\n',
        'get ' .. ('k'):rep(251) .. '\r\n',
        'incr a x\r\n',
        'mg a N30 v\r\n',
        set_cmd('big', ('v'):rep(2000000)),
        'flush_all 1000\r\n',
        'cas a 0 0 1 1\r\nz\r\n',
    }
    local script = {}
    for i, u in ipairs(unserved) do
        local s1 = served_cmds[i]
        local s2 = served_cmds[#served_cmds - i + 1]
        table.insert(script, u .. s1 .. s2)
        table.insert(script, s1 .. u .. s2)
        table.insert(script, s1 .. s2 .. u)
        table.insert(script, s1 .. u .. u .. s2 .. s1 .. u)
    end
    table.insert(
        script,
        table.concat(served_cmds) .. table.concat(unserved)
            .. table.concat(served_cmds)
    )
    same(cg, script)
end

-- A pipeline split in two reads at every byte: the C loop stops at the
-- incomplete command and Lua reads the rest, as its loop does.
g.test_split_reads = function(cg)
    local data = set_cmd('a', 'hello') .. 'get a\r\n'
        .. 'mg a v c\r\n' .. 'ms b 3 T0\r\nabc\r\n'
        .. 'version\r\n' .. 'gets a b\r\n'
        .. 'delete a noreply\r\n' .. 'get a b\r\n'
    local script = {}
    for i = 1, #data - 1 do
        table.insert(script, function(cg_)
            local c = cg_.client
            c:send(data:sub(1, i))
            fiber.sleep(0.001)
            c:send(data:sub(i + 1) .. 'mn\r\n')
            return c:read_until('MN\r\n')
        end)
    end
    same(cg, script)
end

-- Values larger than a read (16 KiB) and the input and output buffers
-- (BUFFER_KEEP): the C loop returns to Lua for an incomplete value, and
-- after a batch that grew the buffers.
g.test_big_values = function(cg)
    local script = {}
    for _, n in ipairs({16383, 16384, 65536, 65537, 300000, 1000000}) do
        local v = ('%08d'):format(n):rep(math.ceil(n / 8)):sub(1, n)
        local key = 'k' .. n
        table.insert(script, set_cmd(key, v))
        table.insert(script, 'get ' .. key .. '\r\n')
        table.insert(script, 'mg ' .. key .. ' v s\r\n')
        table.insert(
            script,
            'ms m' .. n
                .. ' ' .. n
                .. ' T0\r\n' .. v
                .. '\r\nget m' .. n
                .. ' ' .. key
                .. '\r\n'
        )
        table.insert(script, 'get a\r\nget ' .. key .. ' ' .. key .. '\r\n')
    end
    same(cg, script)
end

g.test_noreply_and_quiet = function(cg)
    same(cg, {
        set_cmd('a', '1', 0, 0, ' noreply') .. 'get a\r\n',
        set_cmd('b', '2', 0, 0, ' noreply')
            .. set_cmd(
                'c',
                '3',
                0,
                0,
                ' noreply'
            ),
        'incr a 1 noreply\r\ndecr a 1 noreply\r\ntouch b 0 noreply\r\n',
        'delete b noreply\r\nget b\r\n',
        'mg a v q\r\nmg nope v q\r\nms d 1 q\r\nx\r\nmd nope q\r\n',
        'ma a q\r\nma nope q\r\nmg d v q k\r\n',
        'get a c d\r\n',
    })
end

-- quit: the replies before it, then the close, in both loops.
g.test_quit = function(cg)
    local function quit(cg_)
        local c = cg_.client
        c:send(set_cmd('q', 'v') .. 'get q\r\nmg q v\r\nquit\r\nget q\r\n')
        local res = {}
        while true do
            local data = c.sock:read(1, 2)
            if data == nil or data == '' then
                break
            end
            res[#res + 1] = data
        end
        return table.concat(res) .. (c:is_closed(0.5) and '<closed>' or '')
    end
    local lua = run(cg, {quit}, false)
    local c = run(cg, {quit}, true)
    t.assert_equals(c, lua)
    t.assert_str_contains(c[1], '<closed>')
    t.assert_str_contains(c[1], 'STORED\r\nVALUE q 0 1\r\nv\r\nEND\r\n')
end

-- The counters of the commands the C loop serves are in "stats" from any
-- connection at once, and in info() and stats reset, while the connection
-- stays in the loop.
g.test_stats_from_another_connection = function(cg)
    for _, loop in ipairs({false, true}) do
        reset(cg, loop)
        local c = cg.client
        local other = connect(cg)
        local before = counters(other)
        for i = 1, 20 do
            t.assert_equals(c:set('k' .. i, 'v' .. i), 'STORED\r\n')
        end
        for i = 1, 30 do
            c:send('get k' .. i .. '\r\n')
            c:read_until('END\r\n')
        end
        c:send('mg k1 v\r\nmg k2 v\r\n')
        c:read_until('VA 2\r\nv2\r\n')
        local after = counters(other)
        t.assert_equals(after.cmd_get - before.cmd_get, 32, loop)
        t.assert_equals(after.get_hits - before.get_hits, 22, loop)
        t.assert_equals(after.get_misses - before.get_misses, 10, loop)
        t.assert_equals(after.cmd_set - before.cmd_set, 20, loop)
        t.assert_equals(after.cmd_meta - before.cmd_meta, 2, loop)
        t.assert_equals(after.total_items - before.total_items, 20, loop)
        local sent = 0
        for i = 1, 20 do
            sent = sent + #set_cmd('k' .. i, 'v' .. i)
        end
        for i = 1, 30 do
            sent = sent + #('get k' .. i .. '\r\n')
        end
        sent = sent + #'mg k1 v\r\nmg k2 v\r\n'
        -- The other connection's stats line came before.
        t.assert_equals(
            after.bytes_read - before.bytes_read,
            sent + #'stats\r\n',
            loop
        )
        local info = mcc.get(NAME):info()
        t.assert_equals(info.cmd_get, tonumber(after.cmd_get), loop)
        t.assert_equals(info.bytes_read, tonumber(after.bytes_read), loop)
        -- And the reply of that stats.
        t.assert(info.bytes_written > tonumber(after.bytes_written), loop)
        t.assert_equals(other:call('stats reset\r\n'), 'RESET\r\n')
        t.assert_equals(c:get('k1', 'get'), 'v1')
        local reset_stats = counters(other)
        t.assert_equals(reset_stats.cmd_get, '1', loop)
        t.assert_equals(reset_stats.get_hits, '1', loop)
        if loop then
            t.assert(served() >= 53, served())
        end
    end
end

-- flush_all, srv:cfg() and the switches, from another connection or Lua,
-- while the connection waits in C: its next command sees them.
g.test_changes_while_in_c = function(cg)
    local function other(data, terminator)
        return function(cg_)
            local c = connect(cg_)
            local reply = c:call(data, terminator)
            c.sock:close()
            return reply
        end
    end
    local function lua(fn)
        return function()
            fn()
            -- The connection's fiber waits again.
            fiber.sleep(0.01)
        end
    end
    same(cg, {
        set_cmd('a', '1') .. set_cmd('b', '2'),
        'get a b\r\n',
        other('flush_all\r\n'),
        'get a b\r\n',
        set_cmd('a', '3'),
        other('flush_all 10\r\n'),
        'get a\r\n',
        tick(11),
        'get a\r\n',
        set_cmd('a', '4'),
        lua(function()
            mcc.get(NAME):cfg({max_item_size = 4})
        end),
        set_cmd('a', '12345') .. set_cmd('b', '1234') .. 'get a b\r\n',
        lua(function()
            mcc.get(NAME):cfg({max_item_size = 1048576, track_access = false})
        end),
        set_cmd('t', 'v') .. 'get t\r\n' .. 'mg t h l\r\n',
        function()
            return tostring(box.space[SPACE]:get('t')[schema.FETCHED])
        end,
        lua(function()
            mcc.get(NAME):cfg({track_access = true})
        end),
        'get t\r\n' .. 'mg t h\r\n',
        function()
            return tostring(box.space[SPACE]:get('t')[schema.FETCHED])
        end,
        lua(function()
            box.cfg {read_only = true}
        end),
        'get a t\r\n' .. set_cmd('r', 'v')
            .. 'mg t v N30\r\n' .. 'mg nope v N30\r\n'
            .. 'mg nope v\r\n',
        lua(function()
            box.cfg {read_only = false}
        end),
        set_cmd('r', 'v') .. 'get r nope\r\n',
        lua(function()
            mcc.datapath(false)
        end),
        'get r\r\n' .. set_cmd('r', 'w') .. 'gets r\r\n',
        lua(function()
            mcc.datapath(true)
        end),
        'get r\r\n',
    })
end

-- idle_timeout: a connection waiting in C for a command is closed after it
-- and counted; a change applies to it at once; one that keeps sending
-- stays.
g.test_idle_timeout = function(cg)
    for _, loop in ipairs({false, true}) do
        reset(cg, loop, {idle_timeout = 0.3})
        local idle = cg.client
        t.assert_equals(idle:set('a', '1'), 'STORED\r\n')
        local busy = connect(cg)
        local deadline = fiber.clock() + 0.6
        while fiber.clock() < deadline do
            t.assert_equals(busy:get('a', 'get'), '1')
            fiber.sleep(0.05)
        end
        t.assert(idle:is_closed(1), loop)
        t.assert_equals(busy:get('a', 'get'), '1')
        t.assert_equals(counters(busy).idle_kicks, '1')
        -- A longer timeout, then a shorter one: the waiting connection
        -- takes each at once.
        mcc.get(NAME):cfg({idle_timeout = 100})
        local waiting = connect(cg)
        t.assert_equals(waiting:get('a', 'get'), '1')
        fiber.sleep(0.4)
        t.assert_equals(waiting:get('a', 'get'), '1')
        local states = {}
        for _, conn in pairs(mcc.get(NAME).server.conns) do
            states[#states + 1] = conn.state
        end
        t.assert_items_equals(states, {'conn_read', 'conn_read'})
        mcc.get(NAME):cfg({idle_timeout = 0.2})
        t.assert(waiting:is_closed(1), loop)
        t.assert(busy:is_closed(1), loop)
        t.assert_equals(mcc.get(NAME):info().idle_kicks, 3)
    end
end

-- A client that sends gets of a large value and doesn't read: the C loop
-- waits for the socket in conn_mwrite; the server stops anyway, and so
-- does a destroy.
g.test_blocked_write = function(cg)
    for _, loop in ipairs({false, true}) do
        reset(cg, loop)
        local c = cg.client
        t.assert_equals(c:set('big', ('x'):rep(500000)), 'STORED\r\n')
        c:send(('get big\r\n'):rep(200))
        local srv = mcc.get(NAME).server
        t.helpers.retrying({timeout = 5}, function()
            local states = {}
            for _, conn in pairs(srv.conns) do
                states[#states + 1] = conn.state
            end
            t.assert_equals(states, {'conn_mwrite'})
        end)
        local other = connect(cg)
        t.assert_str_contains(
            other:call('stats conns\r\n', 'END\r\n'),
            ':state conn_mwrite\r\n'
        )
        local fibers = {}
        for _, conn in pairs(srv.conns) do
            fibers[#fibers + 1] = conn.fiber
        end
        mcc.get(NAME):stop()
        t.assert_equals(srv.nconns, 0)
        t.helpers.retrying({timeout = 5}, function()
            for _, f in ipairs(fibers) do
                t.assert_equals(f:status(), 'dead')
            end
        end)
        t.assert(other:is_closed(1), loop)
        mcc.get(NAME):start()
        local again = connect(cg)
        t.assert_equals(again:get('big', 'get'), ('x'):rep(500000))
        -- Blocked again, then destroyed.
        again:send(('get big\r\n'):rep(200))
        t.helpers.retrying({timeout = 5}, function()
            local n = 0
            for _, conn in pairs(srv.conns) do
                t.assert_equals(conn.state, 'conn_mwrite')
                n = n + 1
            end
            t.assert_equals(n, 1)
        end)
        mcc.destroy(NAME)
        t.assert_equals(srv.nconns, 0)
    end
end

-- The server stops with connections waiting in C and in the middle of
-- pipelines: they are closed, their fibers end.
g.test_server_stop = function(cg)
    reset(cg, true)
    local conns = {cg.client}
    for _ = 1, 9 do
        conns[#conns + 1] = connect(cg)
    end
    for i, c in ipairs(conns) do
        t.assert_equals(c:set('k' .. i, 'v'), 'STORED\r\n')
    end
    local srv = mcc.get(NAME).server
    local fibers = {}
    for _, conn in pairs(srv.conns) do
        fibers[#fibers + 1] = conn.fiber
        t.assert_equals(conn.state, 'conn_read')
    end
    t.assert_equals(#fibers, 10)
    -- A load in flight on half of them.
    local stop = false
    for i = 1, 5 do
        fiber.create(function()
            local c = conns[i]
            while not stop do
                local ok = pcall(c.send, c, 'get k1\r\nget k2\r\n')
                if not ok or c:read_until('END\r\n', 0.5) == nil then
                    break
                end
            end
        end)
    end
    fiber.sleep(0.05)
    mcc.get(NAME):stop()
    stop = true
    t.helpers.retrying({timeout = 5}, function()
        for _, f in ipairs(fibers) do
            t.assert_equals(f:status(), 'dead')
        end
    end)
    for i = 6, 10 do
        t.assert(conns[i]:is_closed(1), i)
    end
    t.assert_equals(srv.nconns, 0)
end

-- Many connections at once, each with its own pipelines, in both loops:
-- the same replies, every reply in order.
g.test_many_connections = function(cg)
    local N = 200
    local function load(cg_)
        local conns = {}
        for i = 1, N do
            conns[i] = connect(cg_)
        end
        local res = {}
        local done = 0
        for i = 1, N do
            fiber.create(function()
                local c = conns[i]
                local out = {}
                for j = 1, 10 do
                    local key = ('k%d_%d'):format(i, j % 3)
                    out[#out + 1] = call(
                        c,
                        set_cmd(key, ('v'):rep(j)) .. 'get '
                            .. key .. ' nope\r\n'
                            .. 'mg ' .. key
                            .. ' s v\r\n' .. (j % 4 == 0 and 'version\r\n' or '')
                    )
                end
                res[i] = table.concat(out)
                done = done + 1
            end)
        end
        t.helpers.retrying({timeout = 20}, function()
            t.assert_equals(done, N)
        end)
        return table.concat(res, '|')
    end
    -- The CAS values depend on the order of the sets: not compared.
    local function strip(r)
        return (r:gsub('VALUE (%S+) (%d+) (%d+) %d+', 'VALUE %1 %2 %3'))
    end
    local lua = run(cg, {load}, false)
    local c = run(cg, {load}, true)
    t.assert_equals(strip(c[1]), strip(lua[1]))
    t.assert(served() > N * 10 * 3, served())
end
