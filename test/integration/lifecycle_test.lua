-- The instance lifecycle API: stop/destroy with connected clients,
-- reconfiguration, option validation, commit errors, read-only start.
-- Ported from the tarantool/memcached tarantool3-role branch.

local fiber = require('fiber')
local fio = require('fio')
local socket = require('socket')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local g = t.group('lifecycle')

local mcc

-- Names of the instances and spaces a test may create.
local NAMES = {
    'idle',
    'writer',
    'gone',
    'rebind',
    'other',
    'immutable',
    'opts',
    'commit',
    'ro',
    'ro2',
    'maxconn',
    'failed',
    'expire',
    'disk',
    'forms',
    'unix',
}
local SPACES = {
    'gone_space',
    'immutable_space',
    'commit_space',
    'ro_space',
    'ro2_space',
    'disk_space',
}

local function copy(t_)
    local res = {}
    for k, v in pairs(t_) do
        res[k] = v
    end
    return res
end

g.before_all(function(cg)
    env.box_cfg()
    mcc = require('memcached')
    cg.port = env.free_port()
end)

g.after_each(function()
    box.cfg {read_only = false}
    for _, name in ipairs(NAMES) do
        if mcc.get(name) ~= nil then
            mcc.destroy(name)
        end
    end
    local names = copy(SPACES)
    for _, name in ipairs(NAMES) do
        table.insert(names, '__mcc_' .. name)
    end
    env.drop_spaces(names)
end)

-- A socket listening on the port.
local function occupy(port)
    local s = socket('AF_INET', 'SOCK_STREAM', 'tcp')
    s:setsockopt('SOL_SOCKET', 'SO_REUSEADDR', true)
    t.assert(s:bind('127.0.0.1', port))
    t.assert(s:listen(1))
    return s
end

local function uri(port, host)
    return (host or '127.0.0.1') .. ':' .. port
end

g.test_stop_with_idle_client = function(cg)
    local inst = mcc.create('idle', {listen = uri(cg.port)})
    t.assert_equals(inst.status, 'running')
    local c = client.must_connect(cg.port)
    t.assert_equals(c:set('a', 'x'), 'STORED\r\n')
    t.assert_equals(inst:info().curr_connections, 1)

    local started = fiber.clock()
    inst:stop()
    t.assert_lt(fiber.clock() - started, 1, 'stop() does not wait for clients')
    t.assert_equals(inst:info().curr_connections, 0)
    t.assert_equals(inst.status, 'stopped')
    -- The client sees the connection closed.
    t.assert(c:is_closed())
    c:close()
    t.assert_not(client.is_listening(cg.port))

    -- The instance can be started again.
    inst:start()
    t.assert_equals(inst.status, 'running')
    t.assert_equals(client.get(cg.port, 'a'), 'VALUE a 0 1\r\nx\r\nEND\r\n')
end

g.test_stop_with_blocked_writer = function(cg)
    local inst = mcc.create('writer', {listen = uri(cg.port)})
    local value = string.rep('v', 512 * 1024)
    t.assert_equals(client.set(cg.port, 'big', value), 'STORED\r\n')

    -- Request a lot of data and don't read it: the connection fiber blocks
    -- on writing.
    local c = client.must_connect(cg.port)
    c:send(string.rep('get big\r\n', 64))
    fiber.sleep(0.3)
    t.assert_equals(inst:info().curr_connections, 1)
    -- Other clients are served.
    t.assert_equals(client.get(cg.port, 'none'), 'END\r\n')

    local started = fiber.clock()
    inst:stop()
    t.assert_lt(fiber.clock() - started, 1, 'stop() does not wait for clients')
    t.assert_equals(inst:info().curr_connections, 0)
    c:close()
    -- The connection fibers are gone.
    t.helpers.retrying({}, function()
        for _, f in pairs(fiber.info({backtrace = false})) do
            t.assert_not_str_contains(f.name, 'mcc/writer/')
        end
    end)
end

g.test_destroy = function(cg)
    local inst = mcc.create('gone', {
        listen = uri(cg.port),
        space_name = 'gone_space',
    })
    local c = client.must_connect(cg.port)
    t.assert_equals(c:set('a', 'x'), 'STORED\r\n')

    inst:destroy()
    t.assert_equals(inst.status, 'destroyed')
    t.assert_equals(mcc.get('gone'), nil)
    t.assert_not(client.is_listening(cg.port))
    t.assert(c:is_closed())
    c:close()

    -- The data is kept.
    t.assert_equals(box.space.gone_space:len(), 1)

    for _, method in ipairs({'start', 'stop', 'info', 'destroy'}) do
        t.assert_error_msg_contains(
            "Memcached instance 'gone' is destroyed",
            inst[method],
            inst
        )
    end
    t.assert_error_msg_contains(
        "Memcached instance 'gone' is destroyed",
        inst.cfg,
        inst,
        {}
    )

    -- The name can be reused.
    inst = mcc.create('gone', {
        listen = uri(cg.port),
        space_name = 'gone_space',
    })
    t.assert_equals(client.get(cg.port, 'a'), 'VALUE a 0 1\r\nx\r\nEND\r\n')

    -- The module-level function.
    mcc.destroy('gone')
    t.assert_equals(inst.status, 'destroyed')
    t.assert_error_msg_contains(
        "Memcached instance 'gone' does not exist",
        mcc.destroy,
        'gone'
    )

    -- A stopped instance can be destroyed too.
    inst = mcc.create('gone', {listen = uri(cg.port)})
    inst:stop()
    inst:destroy()
    t.assert_equals(mcc.get('gone'), nil)

    -- A name can't be taken twice.
    mcc.create('gone', {listen = uri(cg.port)})
    t.assert_error_msg_contains(
        "Memcached instance 'gone' already exists",
        mcc.create,
        'gone',
        {listen = uri(cg.port)}
    )
    mcc.destroy('gone')
end

g.test_cfg_listen = function(cg)
    local port1, port2, port3 = cg.port, env.free_port(), env.free_port()
    local inst = mcc.create('rebind', {listen = port1})
    local c = client.must_connect(port1)
    t.assert_equals(c:set('a', 'x'), 'STORED\r\n')

    inst:cfg({listen = tostring(port2)})
    t.assert_equals(inst.opts.listen, tostring(port2))
    t.assert_equals(inst:info().listen, tostring(port2))
    t.assert_not(client.is_listening(port1))
    t.assert_equals(client.get(port2, 'a'), 'VALUE a 0 1\r\nx\r\nEND\r\n')
    -- The existing connection is kept.
    t.assert_equals(
        c:call('get a\r\n', 'END\r\n'),
        'VALUE a 0 1\r\nx\r\nEND\r\n'
    )

    -- The same port on another interface: the old socket has to be
    -- released before binding the new one.
    inst:cfg({listen = uri(port2)})
    inst:cfg({listen = uri(port2, '0.0.0.0')})
    t.assert_equals(inst.opts.listen, uri(port2, '0.0.0.0'))
    t.assert(client.is_listening(port2))

    -- A busy port: the instance keeps listening on the old one.
    local other = mcc.create('other', {listen = uri(port3)})
    t.assert_error_msg_contains(
        "can't bind",
        inst.cfg,
        inst,
        {listen = uri(port3)}
    )
    t.assert_equals(inst.opts.listen, uri(port2, '0.0.0.0'))
    t.assert_equals(inst.status, 'running')
    t.assert(client.is_listening(port2))
    t.assert_equals(client.get(port2, 'a'), 'VALUE a 0 1\r\nx\r\nEND\r\n')
    other:destroy()
    t.assert_equals(
        c:call('get a\r\n', 'END\r\n'),
        'VALUE a 0 1\r\nx\r\nEND\r\n'
    )
    c:close()

    -- A stopped instance listens on the new URI when started.
    inst:stop()
    inst:cfg({listen = uri(port1)})
    t.assert_not(client.is_listening(port1))
    inst:start()
    t.assert(client.is_listening(port1))
    t.assert_not(client.is_listening(port2))
end

g.test_listen_forms = function(cg)
    -- A port number: all interfaces.
    local inst = mcc.create('forms', {listen = cg.port})
    t.assert_equals(client.set(cg.port, 'a', 'x'), 'STORED\r\n')
    -- A host name: the first address it resolves to.
    inst:cfg({listen = 'localhost:' .. cg.port})
    local host = inst.server.listener:name().host
    t.assert_items_include({'127.0.0.1', '::1'}, {host})
    local c = client.must_connect(cg.port, host)
    t.assert_equals(
        c:call('get a\r\n', 'END\r\n'),
        'VALUE a 0 1\r\nx\r\nEND\r\n'
    )
    c:close()
    inst:destroy()

    -- A unix socket.
    local path = fio.pathjoin(fio.tempdir(), 'mcc.sock')
    inst = mcc.create('unix', {listen = 'unix/:' .. path})
    local s = socket.tcp_connect('unix/', path, 1)
    t.assert(s)
    s:write('set a 0 0 1\r\ny\r\n')
    t.assert_equals(s:read('\r\n', 1), 'STORED\r\n')
    s:close()
    inst:stop()
    t.assert_not(fio.path.exists(path))
    -- A stale socket file is replaced.
    local stale = socket('AF_UNIX', 'SOCK_STREAM', 0)
    stale:bind('unix/', path)
    stale:close()
    t.assert(fio.path.exists(path))
    inst:start()
    s = socket.tcp_connect('unix/', path, 1)
    t.assert(s)
    s:write('get a\r\n')
    t.assert_equals(s:read('END\r\n', 1), 'VALUE a 0 1\r\ny\r\nEND\r\n')
    s:close()
    inst:destroy()

    t.assert_error_msg_contains(
        "Bad value for option 'listen'",
        mcc.create,
        'forms',
        {listen = 'host:'}
    )
end

g.test_cfg_immutable_options = function(cg)
    local inst = mcc.create('immutable', {
        listen = uri(cg.port),
        space_name = 'immutable_space',
    })
    t.assert_error_msg_contains(
        "Option 'space_name' can't be changed after the instance is created",
        inst.cfg,
        inst,
        {space_name = 'other_space'}
    )
    t.assert_error_msg_contains(
        "Option 'storage' can't be changed after the instance is created",
        inst.cfg,
        inst,
        {storage = 'disk'}
    )
    -- The same values are fine.
    inst:cfg({space_name = 'immutable_space', storage = 'memory'})
    t.assert_equals(inst.status, 'running')
    t.assert_equals(inst.space_name, 'immutable_space')
end

g.test_option_validation = function(cg)
    local listen = uri(cg.port)
    local cases = {
        {{}, "Option 'listen' is required"},
        {{listen = listen, bogus = 1}, "Unknown option 'bogus'"},
        {{listen = true}, "Option 'listen' must be a number or string"},
        {{listen = listen, storage = 'mem'}, "Bad value for option 'storage'"},
        {
            {listen = listen, backend = 'bogus'},
            "Bad value for option 'backend'",
        },
        {
            {listen = listen, max_item_size = 0},
            "Bad value for option 'max_item_size'",
        },
        {
            {listen = listen, max_connections = 1.5},
            "Bad value for option 'max_connections'",
        },
        {
            {listen = listen, expire = {batch = 0}},
            "Bad value for option 'expire'",
        },
        {
            {listen = listen, expire = {bogus = true}},
            "Bad value for option 'expire'",
        },
        {
            {listen = listen, expire = true},
            "Option 'expire' must be a table",
        },
        {
            {listen = listen, read_mode = 'x'},
            "Bad value for option 'read_mode'",
        },
        {
            {listen = listen, snapshot_refresh = 0.01},
            "Bad value for option 'snapshot_refresh'",
        },
        {
            {listen = listen, read_mode = 'ro'},
            "read_mode 'ro' is not supported by the local backend",
        },
        {
            {listen = listen, threads = 'fe'},
            "Application thread group 'fe' does not exist",
        },
    }
    for _, case in ipairs(cases) do
        t.assert_error_msg_contains(case[2], mcc.create, 'opts', case[1])
        t.assert_equals(mcc.get('opts'), nil)
    end
    t.assert_error_msg_contains(
        'options must be a table',
        mcc.create,
        'opts',
        'x'
    )
    t.assert_error_msg_contains(
        'instance name must be a non-empty string',
        mcc.create,
        1,
        {listen = listen}
    )
    if not pcall(require, 'memcached.backend.vshard') then
        t.assert_error_msg_contains(
            "Backend 'vshard' is not available",
            mcc.create,
            'opts',
            {listen = listen, backend = 'vshard'}
        )
    end

    -- The same rules in cfg(); a failed cfg() changes nothing.
    local inst = mcc.create('opts', {listen = listen})
    t.assert_equals(inst.opts.max_item_size, 1048576)
    t.assert_equals(inst.opts.max_connections, 4096)
    t.assert_equals(inst.opts.expire, {enabled = true, batch = 100})
    t.assert_equals(inst.opts.space_name, '__mcc_opts')
    for _, case in ipairs(cases) do
        if case[1].listen == listen then
            local opts = copy(case[1])
            opts.listen = nil
            t.assert_error_msg_contains(case[2], inst.cfg, inst, opts)
        end
    end
    t.assert_equals(inst.opts.max_item_size, 1048576)
    t.assert_equals(inst.opts.read_mode, 'rw')
    inst:cfg({
        max_item_size = 100,
        max_connections = 10,
        expire = {batch = 10},
    })
    t.assert_equals(inst.opts.expire, {enabled = true, batch = 10})
    t.assert_equals(inst.server.max_connections, 10)
    t.assert_equals(inst.server.parse_opts.max_item_size, 100)
end

g.test_statuses = function(cg)
    local inst = mcc.create('failed', {listen = uri(cg.port)})
    t.assert_error_msg_contains(
        "Memcached instance 'failed' is already started",
        inst.start,
        inst
    )
    inst:stop()
    t.assert_error_msg_contains(
        "Memcached instance 'failed' is already stopped",
        inst.stop,
        inst
    )
    -- Start fails on a busy port.
    local busy = occupy(cg.port)
    t.assert_error_msg_contains("can't bind", inst.start, inst)
    t.assert_equals(inst.status, 'failed')
    busy:close()
    inst:start()
    t.assert_equals(inst.status, 'running')
    t.assert_equals(client.set(cg.port, 'a', 'x'), 'STORED\r\n')
    -- A failed instance can be stopped.
    inst:stop()
    busy = occupy(cg.port)
    t.assert_error_msg_contains("can't bind", inst.start, inst)
    inst:stop()
    t.assert_equals(inst.status, 'stopped')
    -- create() on a busy port fails and frees the name.
    inst:destroy()
    t.assert_error_msg_contains(
        "can't bind",
        mcc.create,
        'failed',
        {listen = uri(cg.port)}
    )
    t.assert_equals(mcc.get('failed'), nil)
    busy:close()
end

g.test_max_connections = function(cg)
    local inst = mcc.create('maxconn', {
        listen = uri(cg.port),
        max_connections = 2,
    })
    local c1 = client.must_connect(cg.port)
    local c2 = client.must_connect(cg.port)
    t.assert_equals(c1:call('version\r\n'):sub(1, 8), 'VERSION ')
    t.assert_equals(c2:call('version\r\n'):sub(1, 8), 'VERSION ')
    local c3 = client.must_connect(cg.port)
    t.assert_equals(c3:line(), 'ERROR Too many open connections\r\n')
    t.assert(c3:is_closed())
    c3:close()
    t.assert_equals(inst:info().rejected_connections, 1)
    t.assert_equals(inst:info().curr_connections, 2)
    c1:close()
    t.helpers.retrying({}, function()
        t.assert_equals(inst:info().curr_connections, 1)
    end)
    c3 = client.must_connect(cg.port)
    t.assert_equals(c3:call('get a\r\n'), 'END\r\n')
    -- The limit can be changed at runtime.
    inst:cfg({max_connections = 3})
    local c4 = client.must_connect(cg.port)
    t.assert_equals(c4:call('get a\r\n'), 'END\r\n')
    c2:close()
    c3:close()
    c4:close()
end

g.test_commit_error = function(cg)
    local inst = mcc.create('commit', {
        listen = uri(cg.port),
        space_name = 'commit_space',
    })
    -- A yield inside a memtx transaction aborts it, so the commit fails.
    local function trigger()
        fiber.yield()
    end
    box.space.commit_space:on_replace(trigger)
    t.assert_equals(
        client.set(cg.port, 'a', 'x'),
        'SERVER_ERROR Transaction has been aborted by a fiber yield\r\n'
    )
    t.assert_equals(client.get(cg.port, 'a'), 'END\r\n')
    t.assert_equals(box.space.commit_space:len(), 0)
    box.space.commit_space:on_replace(nil, trigger)
    t.assert_equals(client.set(cg.port, 'a', 'x'), 'STORED\r\n')
    inst:stop()
end

g.test_start_on_read_only = function(cg)
    box.cfg {read_only = true}
    local inst = mcc.create('ro', {
        listen = uri(cg.port),
        space_name = 'ro_space',
    })
    t.assert_equals(inst.status, 'waiting')
    t.assert_equals(box.space.ro_space, nil)
    t.assert_not(client.is_listening(cg.port))
    t.assert_error_msg_contains(
        "Memcached instance 'ro' is already started",
        inst.start,
        inst
    )
    t.assert_equals(inst:info().status, 'waiting')

    -- The space appears when the instance becomes writable (on a replica
    -- it arrives by replication).
    box.cfg {read_only = false}
    t.helpers.retrying({}, function()
        t.assert_equals(inst.status, 'running')
    end)
    t.assert_not_equals(box.space.ro_space, nil)
    t.assert_equals(client.set(cg.port, 'a', 'x'), 'STORED\r\n')

    -- An existing space is used on a read-only instance right away: reads
    -- work, writes fail.
    inst:destroy()
    box.cfg {read_only = true}
    inst = mcc.create('ro', {listen = uri(cg.port), space_name = 'ro_space'})
    t.assert_equals(inst.status, 'running')
    t.assert_equals(client.get(cg.port, 'a'), 'VALUE a 0 1\r\nx\r\nEND\r\n')
    t.assert_str_contains(
        client.set(cg.port, 'b', 'y'),
        "SERVER_ERROR Can't modify data on a read-only instance"
    )
    local c = client.must_connect(cg.port)
    t.assert_str_contains(c:call('flush_all\r\n'), 'SERVER_ERROR')
    t.assert_str_contains(c:call('touch a 10\r\n'), 'SERVER_ERROR')
    t.assert_equals(
        c:call('incr a 1\r\n'),
        'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n'
    )
    c:close()
    inst:destroy()

    -- A waiting instance can be stopped and destroyed.
    inst = mcc.create('ro2', {
        listen = uri(cg.port),
        space_name = 'ro2_space',
    })
    t.assert_equals(inst.status, 'waiting')
    inst:stop()
    t.assert_equals(inst.status, 'stopped')
    inst:start()
    t.assert_equals(inst.status, 'waiting')
    inst:destroy()
    box.cfg {read_only = false}
    fiber.sleep(0.3)
    t.assert_equals(box.space.ro2_space, nil)
    t.assert_not(client.is_listening(cg.port))
end

g.test_expire_cfg = function(cg)
    local inst = mcc.create('expire', {
        listen = uri(cg.port),
        expire = {enabled = false},
    })
    local space = box.space.__mcc_expire
    local c = client.must_connect(cg.port)
    t.assert_equals(c:call('set a 0 1 1\r\nx\r\n'), 'STORED\r\n')
    fiber.sleep(1.5)
    -- Dead, but nobody deletes it.
    t.assert_equals(c:call('get b\r\n'), 'END\r\n')
    t.assert_equals(space:len(), 1)
    t.assert_equals(inst:info().expired, nil)
    inst:cfg({expire = {enabled = true, batch = 1}})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(space:len(), 0)
        t.assert_equals(inst:info().expired, 1)
    end)
    c:close()
end

g.test_disk_storage = function(cg)
    local inst = mcc.create('disk', {
        listen = uri(cg.port),
        space_name = 'disk_space',
        storage = 'disk',
    })
    t.assert_equals(box.space.disk_space.engine, 'vinyl')
    t.assert_equals(box.space.disk_space.index.primary.type, 'TREE')
    local c = client.must_connect(cg.port)
    t.assert_equals(c:set('a', 'x'), 'STORED\r\n')
    t.assert_equals(c:call('set b 0 1 1\r\ny\r\n'), 'STORED\r\n')
    t.assert_equals(
        c:call('incr a 1\r\n'),
        'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n'
    )
    t.assert_equals(
        c:call('get a b\r\n', 'END\r\n'),
        'VALUE a 0 1\r\nx\r\nVALUE b 0 1\r\ny\r\nEND\r\n'
    )
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(box.space.disk_space:select({'b'}), {})
    end)
    t.assert_equals(c:call('flush_all\r\n'), 'OK\r\n')
    t.assert_equals(c:call('get a\r\n'), 'END\r\n')
    c:close()
    inst:destroy()
end
