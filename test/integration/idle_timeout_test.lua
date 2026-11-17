-- The instance option idle_timeout, as memcached's -o idle_timeout: a
-- connection that waits for a command (a new one, or the rest of a command
-- line) longer than that is closed and counted in idle_kicks; one inside a
-- value, or over a unix socket, is not. In TX and in application threads,
-- with runtime changes through inst:cfg().

local fiber = require('fiber')
local fio = require('fio')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')
local th = require('test.helpers.threads')

local IDLE = 0.5

-- Whether the server closes the connection within `timeout` seconds.
local function closed_within(c, timeout)
    return c:is_closed(timeout)
end

-- The connection is still open: nothing to read for `timeout` seconds.
local function still_open(c, timeout)
    local data = c.sock:read(1, timeout or 0.2)
    return data == nil
end

-- "stats [args]" as a map and the raw reply.
local function read_stats(c, args)
    local reply = c:call(
        args and ('stats ' .. args .. '\r\n') or 'stats\r\n',
        'END\r\n'
    )
    local stats = {}
    for name, value in reply:gmatch('STAT (%S+) ([^\r]*)\r\n') do
        stats[name] = value
    end
    return stats, reply
end

--
-- In TX.
--

local g = t.group('idle_timeout')

local NAMES = {'idle', 'idle_unix', 'idle_rt'}

local mcc

g.before_all(function()
    env.box_cfg()
    mcc = require('memcached')
end)

g.before_each(function(cg)
    cg.port = env.free_port()
    cg.clients = {}
end)

g.after_each(function(cg)
    for _, c in ipairs(cg.clients) do
        pcall(c.close, c)
    end
    for _, name in ipairs(NAMES) do
        if mcc.get(name) ~= nil then
            mcc.destroy(name)
        end
    end
    local spaces = {}
    for _, name in ipairs(NAMES) do
        table.insert(spaces, '__mcc_' .. name)
    end
    env.drop_spaces(spaces)
end)

local function connect(cg, port)
    local c = client.must_connect(port or cg.port)
    table.insert(cg.clients, c)
    return c
end

g.test_idle_connections = function(cg)
    local inst = mcc.create('idle', {
        listen = '127.0.0.1:' .. cg.port,
        idle_timeout = IDLE,
    })
    -- Accepted, no command yet.
    local fresh = connect(cg)
    -- Idle after a command.
    local after = connect(cg)
    t.assert_equals(after:set('a', 'x'), 'STORED\r\n')
    -- The start of a command line.
    local partial = connect(cg)
    partial:send('get fo')
    -- The command line of a set, and a part of its value.
    local value = connect(cg)
    value:send('set v 0 0 10\r\nabc')
    -- Busy: a command every IDLE / 4 seconds.
    local busy = connect(cg)
    -- Over a unix socket.
    local path = fio.pathjoin(fio.tempdir(), 'idle.sock')
    mcc.create('idle_unix', {listen = 'unix/:' .. path, idle_timeout = IDLE})
    local s = require('socket').tcp_connect('unix/', path, 1)
    t.assert(s)
    local unix = setmetatable({sock = s}, getmetatable(fresh))
    table.insert(cg.clients, unix)

    local deadline = fiber.clock() + 3 * IDLE
    while fiber.clock() < deadline do
        t.assert_equals(busy:call('version\r\n'):match('^VERSION'), 'VERSION')
        fiber.sleep(IDLE / 4)
    end
    local stats, reply = read_stats(busy)
    t.assert_equals(stats.idle_kicks, '3')
    -- After auth_errors, as in memcached.
    t.assert_str_contains(reply, 'STAT auth_errors 0\r\nSTAT idle_kicks 3\r\n')
    t.assert_equals(read_stats(busy, 'settings').idle_timeout, '0.5')
    t.assert_equals(inst:info().idle_kicks, 3)
    t.assert_equals(inst:info().curr_connections, 2)
    -- stats reset resets it.
    t.assert_equals(busy:call('stats reset\r\n'), 'RESET\r\n')
    t.assert_equals(read_stats(busy).idle_kicks, '0')

    t.assert(closed_within(fresh, 0.1), 'a new connection times out')
    t.assert(closed_within(after, 0.1), 'an idle connection times out')
    t.assert(closed_within(partial, 0.1), 'a partial command line times out')
    t.assert(still_open(value, 0.1), 'a connection inside a value does not')
    t.assert(still_open(unix, 0.1), 'a unix socket connection does not')
    -- The value goes on.
    t.assert_equals(value:call('defghij\r\n'), 'STORED\r\n')
    t.assert_equals(unix:call('version\r\n'):match('^VERSION'), 'VERSION')
end

-- Without the option: no timeout, no idle_kicks in "stats".
g.test_default = function(cg)
    local inst = mcc.create('idle', {listen = '127.0.0.1:' .. cg.port})
    t.assert_equals(inst.opts.idle_timeout, 0)
    local c = connect(cg)
    local stats = read_stats(c)
    t.assert_equals(stats.idle_kicks, nil)
    t.assert_not_equals(stats.auth_errors, nil)
    t.assert_equals(read_stats(c, 'settings').idle_timeout, '0')
    t.assert_equals(inst:info().idle_kicks, 0)
end

-- inst:cfg() changes the timeout of the connections that are waiting.
g.test_runtime_change = function(cg)
    local inst = mcc.create('idle_rt', {listen = '127.0.0.1:' .. cg.port})
    local old = connect(cg)
    t.assert_equals(old:set('a', 'x'), 'STORED\r\n')
    fiber.sleep(2 * IDLE)
    t.assert(still_open(old, 0.1))
    -- On: the connection idle for 2 * IDLE is closed at once, a fresh one
    -- after IDLE.
    local fresh = connect(cg)
    local started = fiber.clock()
    inst:cfg({idle_timeout = IDLE})
    t.assert(closed_within(old, 0.3), 'closed when the timeout is set')
    t.assert_lt(fiber.clock() - started, 0.3)
    t.assert(still_open(fresh, 0.1))
    t.assert(closed_within(fresh, 2 * IDLE))
    t.assert_equals(inst:info().idle_kicks, 2)

    -- Longer: a connection waits for the new timeout.
    local c = connect(cg)
    inst:cfg({idle_timeout = 4 * IDLE})
    t.assert(still_open(c, 2 * IDLE))
    -- Off: never closed.
    inst:cfg({idle_timeout = 0})
    t.assert(still_open(c, 4 * IDLE))
    t.assert_equals(c:call('version\r\n'):match('^VERSION'), 'VERSION')
    t.assert_equals(inst:info().idle_kicks, 2)
    t.assert_equals(read_stats(c).idle_kicks, nil)
end

g.test_validation = function(cg)
    local listen = '127.0.0.1:' .. cg.port
    for _, case in ipairs({
        {-1, "Bad value for option 'idle_timeout'"},
        {math.huge, "Bad value for option 'idle_timeout'"},
        {0 / 0, "Bad value for option 'idle_timeout'"},
        {'1', "Option 'idle_timeout' must be a number, got string"},
    }) do
        t.assert_error_msg_equals(
            case[2],
            mcc.create,
            'idle',
            {listen = listen, idle_timeout = case[1]}
        )
        t.assert_equals(mcc.get('idle'), nil)
    end
    local inst = mcc.create('idle', {listen = listen, idle_timeout = 30})
    t.assert_error_msg_equals(
        "Bad value for option 'idle_timeout'",
        inst.cfg,
        inst,
        {idle_timeout = -0.5}
    )
    t.assert_equals(inst.opts.idle_timeout, 30)
end

--
-- In application threads.
--

local tg = t.group('idle_timeout_threads')

local NTHREADS = 2

tg.before_all(function(cg)
    cg.server = th.start('mcc_idle', NTHREADS)
end)

tg.after_all(function(cg)
    cg.server:drop()
end)

tg.before_each(function(cg)
    cg.port = env.free_port()
    cg.clients = {}
end)

tg.after_each(function(cg)
    for _, c in ipairs(cg.clients) do
        pcall(c.close, c)
    end
    th.destroy(cg.server, 'fe')
end)

-- Connections until both threads have one. Skips the test on macOS, which
-- doesn't spread the connections over the threads (README.md,
-- Requirements).
local function connect_all_threads(cg)
    t.skip_if(jit.os == 'OSX', 'macOS: the connections are not spread')
    local seen, nseen = {}, 0
    local conns = {}
    for _ = 1, 64 do
        local c = client.must_connect(cg.port)
        table.insert(cg.clients, c)
        table.insert(conns, c)
        t.assert_equals(c:set('k', 'v'), 'STORED\r\n')
        local port = c.sock:name().port
        local list = th.thread_eval(
            cg.server,
            [[
            local port = ...
            local rec = require('memcached.state').thread_servers.fe
            for _, conn in pairs(rec.srv.conns) do
                local peer = conn.sock:peer()
                if peer ~= nil and peer.port == port then
                    return require('threads').info().thread_id
                end
            end
            return false
        ]],
            {port}
        )
        for _, id in ipairs(list) do
            if id and not seen[id] then
                seen[id] = true
                nseen = nseen + 1
            end
        end
        if nseen == NTHREADS then
            return conns
        end
    end
    t.fail('the connections are not spread over the threads')
end

tg.test_threads = function(cg)
    th.create(cg.server, 'fe', cg.port, {
        threads = 'app',
        idle_timeout = IDLE,
    })
    local conns = connect_all_threads(cg)
    for _, c in ipairs(conns) do
        t.assert(closed_within(c, 4 * IDLE))
    end
    local c = client.must_connect(cg.port)
    table.insert(cg.clients, c)
    local stats = read_stats(c)
    t.assert_equals(stats.idle_kicks, tostring(#conns))
    t.assert_equals(read_stats(c, 'settings').idle_timeout, '0.5')
    local info = th.info(cg.server, 'fe')
    t.assert_equals(info.idle_kicks, #conns)
    local kicked = 0
    for _, ti in ipairs(info.threads) do
        t.assert_gt(ti.idle_kicks, 0, 'every thread closed connections')
        kicked = kicked + ti.idle_kicks
    end
    t.assert_equals(kicked, #conns)
end

-- inst:cfg() reaches the connections waiting in the threads.
tg.test_threads_runtime_change = function(cg)
    th.create(cg.server, 'fe', cg.port, {threads = 'app'})
    local conns = connect_all_threads(cg)
    fiber.sleep(2 * IDLE)
    for _, c in ipairs(conns) do
        t.assert(still_open(c, 0.05))
    end
    cg.server:exec(function(idle)
        require('memcached').get('fe'):cfg({idle_timeout = idle})
    end, {IDLE})
    for _, c in ipairs(conns) do
        t.assert(closed_within(c, 0.3))
    end
    local list = th.thread_eval(
        cg.server,
        [[
        return require('memcached.state').thread_servers.fe.srv.idle_timeout
    ]]
    )
    t.assert_equals(list, {IDLE, IDLE})
    cg.server:exec(function()
        require('memcached').get('fe'):cfg({idle_timeout = 0})
    end)
    local c = client.must_connect(cg.port)
    table.insert(cg.clients, c)
    t.assert(still_open(c, 2 * IDLE))
    t.assert_equals(th.info(cg.server, 'fe').idle_kicks, #conns)
end
