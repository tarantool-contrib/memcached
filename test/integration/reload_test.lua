-- Hot reload: mcc.reload() in a child process that runs a copy of the code,
-- which the tests patch. Listeners, connections, counters, sweepers and read
-- views survive; TX and the application threads run the new code.

local fio = require('fio')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')
local th = require('test.helpers.threads')
local rvh = require('test.helpers.readview')

local g = t.group('reload')

local NTHREADS = 2
local NAME = 'rl'

-- Start a child with a copy of the code that it finds first, before it is
-- loaded. By default the copy is made the search root: Tarantool searches
-- it before package.path (luatest sets it to its own, the repository), and
-- it is the same in every thread. With opts.path_only the search root is
-- unset (the current directory, the work dir) and only package.path finds
-- the copy.
local function start(cg, opts)
    cg.port = env.free_port()
    cg.server = th.start(
        'mcc_reload',
        NTHREADS,
        nil,
        {setsearchroot = not opts.path_only}
    )
    cg.copy = cg.server:exec(function(root, searchroot)
        local fio_ = require('fio')
        local dir = fio_.tempdir()
        assert(
            fio_.copytree(
                root .. '/memcached',
                dir .. '/memcached'
            )
        )
        if searchroot then
            package.setsearchroot(dir) -- luacheck: ignore 143
        end
        package.path = dir .. '/?.lua;' .. dir .. '/?/init.lua;' .. package.path
        local mcc = require('memcached')
        local source = debug.getinfo(mcc.create, 'S').source
        assert(source == '@' .. dir .. '/memcached/init.lua', source)
        return dir
    end, {env.root, not opts.path_only})
end

local function stop(cg)
    for _, c in ipairs(cg.clients or {}) do
        pcall(c.close, c)
    end
    cg.clients = nil
    th.destroy(cg.server, NAME)
    cg.server:drop()
    for _, dir in ipairs({cg.copy, cg.copy2}) do
        fio.rmtree(dir)
    end
    cg.copy, cg.copy2 = nil, nil
end

g.before_each(function(cg)
    start(cg, {})
end)

g.after_each(stop)

local function connect(cg)
    local c = client.must_connect(cg.port)
    cg.clients = cg.clients or {}
    table.insert(cg.clients, c)
    return c
end

-- Change a file of the copy: fn(text) -> new text.
local function patch(cg, file, fn, dir)
    local path = fio.pathjoin(dir or cg.copy, 'memcached', file)
    local f = assert(fio.open(path, {'O_RDONLY'}))
    local text = f:read()
    f:close()
    local new = fn(text)
    t.assert_not_equals(new, text, 'the patch changes ' .. file)
    f = assert(fio.open(path, {'O_WRONLY', 'O_TRUNC'}))
    f:write(new)
    f:close()
    return text
end

-- The version of the copy, which server.lua reads from memcached/version.lua
-- (in the rock, tt writes it).
local function set_version(cg, version, dir)
    local path = fio.pathjoin(dir or cg.copy, 'memcached', 'version.lua')
    local f = assert(
        fio.open(
            path,
            {'O_WRONLY', 'O_CREAT', 'O_TRUNC'},
            tonumber('644', 8)
        )
    )
    f:write(('return {version = %q}\n'):format(version))
    f:close()
end

-- The version reply of the copy before a patch: the copy has no
-- memcached/version.lua, so it is that of the tree tt built, or 'scm'.
local function built_version_reply()
    local ok, version = pcall(require, 'memcached.version')
    return ('VERSION 1.6.45-mcc-%s\r\n'):format(
        ok and version.version or 'scm'
    )
end

-- mcc.reload() in the child: its summary, or nil and the error.
local function reload(cg)
    return cg.server:exec(function()
        local ok, res = pcall(require('memcached').reload)
        if not ok then
            return nil, tostring(res)
        end
        return res
    end)
end

-- Mark the loaded server module: a reload drops the mark.
local function mark(cg)
    cg.server:exec(function()
        require('memcached.server').test_mark = true
    end)
end

local function marked(cg)
    return cg.server:exec(function()
        return require('memcached.server').test_mark == true
    end)
end

-- The listener fd and the accept fiber of the TX front-end.
local function tx_listener(cg)
    return cg.server:exec(function(name)
        local srv = require('memcached').get(name).server
        local f = srv.accept_fiber
        return {fd = srv.listener:fd(), accept = f:id()}
    end, {NAME})
end

local function fiber_status(cg, id)
    return cg.server:exec(function(id_)
        local f = require('fiber').find(id_)
        return f ~= nil and f:status() or 'dead'
    end, {id})
end

-- The fibers named mcc/* and the open fds of the child.
local function resources(cg)
    return cg.server:exec(function()
        local fio_ = require('fio')
        return {
            fibers = require('memcached.reload').fiber_count(),
            fds = #fio_.listdir('/proc/self/fd'),
        }
    end)
end

g.test_listener_and_connections_survive = function(cg)
    th.create(cg.server, NAME, cg.port)
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    local before = tx_listener(cg)
    mark(cg)

    local summary = reload(cg)
    t.assert_equals(summary.instances, 1)
    t.assert_items_include(summary.modules, {
        'memcached',
        'memcached.engine',
        'memcached.server',
    })
    t.assert_not_equals(marked(cg), true, 'the code is reloaded')

    -- The same listener with a new accept loop; the old one is gone.
    local after = tx_listener(cg)
    t.assert_equals(after.fd, before.fd)
    t.assert_not_equals(after.accept, before.accept)
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(fiber_status(cg, before.accept), 'dead')
    end)
    -- The connection works, and so do new ones.
    t.assert_equals(c:get('a', 'get'), '1')
    t.assert_equals(c:set('b', '2'), 'STORED\r\n')
    local c2 = connect(cg)
    t.assert_equals(c2:get('b', 'get'), '2')
    t.assert_equals(
        cg.server:exec(function(name)
            return require('memcached').get(name):info().curr_connections
        end, {NAME}),
        2
    )
end

g.test_new_code_on_the_same_connection = function(cg)
    th.create(cg.server, NAME, cg.port)
    local c = connect(cg)
    t.assert_equals(c:call('version\r\n'), built_version_reply())
    set_version(cg, 'patched')
    reload(cg)
    t.assert_equals(c:call('version\r\n'), 'VERSION 1.6.45-mcc-patched\r\n')
    c:send('stats\r\n')
    t.assert_str_contains(
        c:read_until('END\r\n'),
        'STAT version 1.6.45-mcc-patched\r\n'
    )
    t.assert_equals(
        cg.server:exec(function()
            return require('memcached').VERSION
        end),
        'patched'
    )
end

-- A connection in the C connection loop (memcached/cloop.c) takes the new
-- code at its next batch: the loop returns when the reload bumps the
-- generation of its inputs, and the new conn_step() calls the new
-- engine.loop(), which then keeps the connection in C.
g.test_c_loop_takes_the_new_code = function(cg)
    th.create(cg.server, NAME, cg.port)
    local available = cg.server:exec(function()
        return require('memcached').datapath_loop().enabled
    end)
    t.skip_if(not available, 'the C connection loop is not available')
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    t.assert_equals(c:get('a', 'get'), '1')
    local function loop_state()
        return cg.server:exec(function(name)
            local srv = require('memcached').get(name).server
            local dp = require('memcached.datapath')
            return {
                served = (dp.served(srv.dp_counters)),
                calls = require('memcached.state').test_loop_calls,
            }
        end, {NAME})
    end
    local before = loop_state()
    t.assert(before.served >= 2, before)
    t.assert_equals(before.calls, nil)
    patch(cg, 'engine.lua', function(text)
        return (text:gsub(
            '\nfunction M%.loop%(ctx, srv, conn, ibuf, obuf, idle, new_cmd, limits%)\n',
            '%0    state.test_loop_calls = (state.test_loop_calls or 0) + 1\n'
        ))
    end)
    t.assert_not_equals(reload(cg), nil)
    -- The connection waits in the old loop: its next command returns it.
    t.assert_equals(c:get('a', 'get'), '1')
    local after = loop_state()
    t.assert_equals(after.calls, 1)
    t.assert_equals(after.served, before.served + 1)
    -- And it stays in the new one.
    for i = 1, 10 do
        t.assert_equals(c:set('k' .. i, 'v'), 'STORED\r\n')
        t.assert_equals(c:get('k' .. i, 'get'), 'v')
    end
    after = loop_state()
    t.assert_equals(after.calls, 1)
    t.assert_equals(after.served, before.served + 21)
    -- A new connection runs the new code from the start.
    local c2 = connect(cg)
    t.assert_equals(c2:get('a', 'get'), '1')
    t.assert_equals(loop_state().calls, 2)
end

-- The state module is not reloaded, but gets the fields of the new one.
g.test_state_upgrade = function(cg)
    th.create(cg.server, NAME, cg.port)
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    patch(cg, 'state.lua', function(text)
        return (text:gsub(
            '\nlocal DEFAULTS = {\n',
            '\nlocal DEFAULTS = {\n    test_field = {},\n'
        ))
    end)
    local res = cg.server:exec(function()
        local state = require('memcached.state')
        -- As if the state was made by an older version.
        state.dump_readviews = nil
        local ok, err = pcall(require('memcached').reload)
        local now = require('memcached.state')
        return {
            ok,
            tostring(err),
            now == state,
            type(now.test_field),
            type(now.dump_readviews),
            now.instances.rl ~= nil,
        }
    end)
    t.assert_equals(res[1], true, res[2])
    t.assert_equals(
        {res[3], res[4], res[5], res[6]},
        {true, 'table', 'table', true}
    )
    t.assert_equals(c:get('a', 'get'), '1')
end

g.test_no_leaks = function(cg)
    th.create(cg.server, NAME, cg.port, {
        read_mode = 'snapshot',
        snapshot_refresh = 100,
    })
    rvh.wait_id(cg.server, NAME)
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    local before = resources(cg)
    -- accept, sweeper, evictor, refresher, connection.
    t.assert_equals(before.fibers, 5)
    for i = 1, 20 do
        t.assert_not_equals(reload(cg), nil, 'reload ' .. i)
    end
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(resources(cg), before)
    end)
    t.assert_equals(rvh.views(cg.server, 'mcc/' .. NAME), {'open'})
    t.assert_equals(c:get('a', 'gat 0'), '1')
    t.assert_equals(c:set('b', '2'), 'STORED\r\n')
end

g.test_failed_reload_keeps_the_old_code = function(cg)
    th.create(cg.server, NAME, cg.port)
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    local before = tx_listener(cg)
    mark(cg)
    local cases = {
        {
            'engine.lua',
            function(text)
                return text .. '\nlocal = 1\n'
            end,
        },
        {
            'server.lua',
            function(text)
                return text:gsub('\nreturn M\n$', "\nerror('boom')\nreturn M\n")
            end,
        },
        {
            'state.lua',
            function(text)
                return 'syntax error ' .. text
            end,
        },
    }
    for _, case in ipairs(cases) do
        local file, fn = case[1], case[2]
        local orig = patch(cg, file, fn)
        local res, err = reload(cg)
        t.assert_equals(res, nil, file)
        t.assert_str_contains(
            err,
            'reload failed, the old code is kept',
            false,
            file
        )
        -- The old code keeps serving.
        t.assert(marked(cg), file)
        t.assert_equals(tx_listener(cg), before)
        t.assert_equals(c:get('a', 'get'), '1')
        t.assert_equals(c:set('a', '1'), 'STORED\r\n')
        t.assert_equals(
            cg.server:exec(function()
                return require('memcached.state').reload_in_progress
            end),
            false
        )
        patch(cg, file, function()
            return orig
        end)
    end
    -- Fixed: the reload works.
    set_version(cg, 'fixed')
    t.assert_not_equals(reload(cg), nil)
    t.assert_not(marked(cg))
    t.assert_equals(c:call('version\r\n'), 'VERSION 1.6.45-mcc-fixed\r\n')
end

g.test_sweepers_and_refreshers = function(cg)
    local srv = cg.server
    th.create(srv, NAME, cg.port, {
        read_mode = 'snapshot',
        snapshot_refresh = 100,
    })
    local id = rvh.wait_id(srv, NAME)
    local c = connect(cg)
    local function info()
        return srv:exec(function(name)
            local state = require('memcached.state')
            local inst = require('memcached').get(name)
            return {
                expired = inst:info().expired,
                checked = inst:info().checked,
                mode = state.sweepers[name].mode,
                sweeper = state.sweepers[name].fiber:id(),
                refresher = state.readviews[name].fiber:id(),
            }
        end, {NAME})
    end
    t.assert_equals(c:set('x', '1', {exptime = 1}), 'STORED\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(info().expired, 1)
    end)
    -- Stale: the read view predates it.
    t.assert_equals(c:set('s', '1'), 'STORED\r\n')
    t.assert_equals(c:get('s', 'get'), nil)
    local before = info()

    t.assert_not_equals(reload(cg), nil)
    local after = info()
    t.assert_not_equals(after.sweeper, before.sweeper)
    t.assert_not_equals(after.refresher, before.refresher)
    t.assert_equals(after.expired, 1, 'the counters are kept')
    -- expire.mode = 'scan' (the default): the scan goes on with the new
    -- code, from a new cursor.
    t.assert_equals(before.mode, 'scan')
    t.assert_ge(after.checked, before.checked)
    t.assert_gt(before.checked, 0)
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(fiber_status(cg, before.sweeper), 'dead')
        t.assert_equals(fiber_status(cg, before.refresher), 'dead')
    end)
    -- The refresher keeps its read view.
    t.assert_equals(rvh.current_id(srv, NAME), id)
    t.assert_equals(c:get('s', 'get'), nil)
    t.assert_equals(rvh.views(srv, 'mcc/' .. NAME), {'open'})

    -- The new sweeper expires items.
    t.assert_equals(c:set('y', '1', {exptime = 1}), 'STORED\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(info().expired, 2)
    end)
    -- And switches the mode of the space.
    local index = srv:exec(function(name)
        local inst = require('memcached').get(name)
        inst:cfg({expire = {mode = 'index'}})
        return box.space[inst.space_name].index.exptime ~= nil
    end, {NAME})
    t.assert(index)
    t.assert_equals(c:set('z', '1', {exptime = 1}), 'STORED\r\n')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(info().expired, 3)
        t.assert_equals(info().mode, 'index')
    end)
    -- The new refresher refreshes.
    srv:exec(function(name)
        require('memcached').get(name):cfg({snapshot_refresh = 0.1})
    end, {NAME})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(c:get('s', 'get'), '1')
    end)
    t.assert_gt(rvh.current_id(srv, NAME), id)
end

-- The listener fds, accept fibers and mcc/* fiber counts in the threads.
local function thread_state(cg)
    return th.thread_eval(
        cg.server,
        [[
        local state = require('memcached.state')
        local srv = state.thread_servers[...].srv
        return {
            fd = srv.listener:fd(),
            accept = srv.accept_fiber:id(),
            fibers = require('memcached.reload').fiber_count(),
        }
    ]],
        {NAME}
    )
end

g.test_threads = function(cg)
    local srv = cg.server
    th.create(srv, NAME, cg.port, {
        threads = 'app',
        read_mode = 'snapshot',
        snapshot_refresh = 100,
    })
    rvh.wait_id(srv, NAME, nil, {threads = true})
    local conns = {}
    for i = 1, 8 do
        conns[i] = connect(cg)
        t.assert_equals(conns[i]:call('version\r\n'), built_version_reply())
    end
    t.assert_equals(conns[1]:set('a', '1'), 'STORED\r\n')
    local before = thread_state(cg)
    t.assert_equals(#before, NTHREADS)
    local ids = rvh.thread_ids(srv, NAME)

    set_version(cg, 'patched')
    local summary = reload(cg)
    t.assert_not_equals(summary, nil)
    t.assert_not_equals(summary.threads.app, nil)
    -- Every thread serves the new code on the old connections.
    for _, c in ipairs(conns) do
        t.assert_equals(
            c:call('version\r\n'),
            'VERSION 1.6.45-mcc-patched\r\n'
        )
    end
    local after = thread_state(cg)
    for i, th_state in ipairs(after) do
        t.assert_equals(th_state.fd, before[i].fd, 'the same listener')
        t.assert_not_equals(
            th_state.accept,
            before[i].accept,
            'a new accept loop'
        )
    end
    -- New connections, the same read views (stale reads).
    for _ = 1, 8 do
        local c = connect(cg)
        t.assert_equals(
            c:call('version\r\n'),
            'VERSION 1.6.45-mcc-patched\r\n'
        )
        t.assert_equals(c:get('a', 'get'), nil)
    end
    t.assert_equals(rvh.thread_ids(srv, NAME), ids)

    -- No fibers leak in the threads.
    after = thread_state(cg)
    for i = 1, 10 do
        t.assert_not_equals(reload(cg), nil, 'reload ' .. i)
    end
    t.helpers.retrying({timeout = 5}, function()
        local now = thread_state(cg)
        for i, th_state in ipairs(now) do
            t.assert_equals(th_state.fibers, after[i].fibers)
            t.assert_equals(th_state.fd, before[i].fd)
        end
    end)
    -- The data path works.
    for i, c in ipairs(conns) do
        t.assert_equals(c:set('k' .. i, tostring(i)), 'STORED\r\n')
        t.assert_equals(c:get('a', 'gat 0'), '1')
    end
end

-- The threads load the code by TX's package.path, at the first use and at
-- every reload.
local gp = t.group('reload_path')

gp.before_each(function(cg)
    start(cg, {path_only = true})
end)

gp.after_each(stop)

gp.test_threads_follow_package_path = function(cg)
    local srv = cg.server
    th.create(srv, NAME, cg.port, {threads = 'app'})
    local function sources()
        return th.thread_eval(
            srv,
            [[
            local server = require('memcached.server')
            return debug.getinfo(server.version, 'S').source
        ]]
        )
    end
    local source = '@' .. cg.copy .. '/memcached/server.lua'
    t.assert_equals(sources(), {source, source})
    local conns = {}
    for i = 1, 6 do
        conns[i] = connect(cg)
    end
    set_version(cg, 'patched')
    t.assert_not_equals(reload(cg), nil)
    for _, c in ipairs(conns) do
        t.assert_equals(
            c:call('version\r\n'),
            'VERSION 1.6.45-mcc-patched\r\n'
        )
    end
    -- TX's package.path changes: the next reload follows it everywhere.
    cg.copy2 = srv:exec(function(root)
        local fio_ = require('fio')
        local dir = fio_.tempdir()
        assert(
            fio_.copytree(
                root .. '/memcached',
                dir .. '/memcached'
            )
        )
        package.path = dir .. '/?.lua;' .. package.path
        return dir
    end, {env.root})
    set_version(cg, 'second', cg.copy2)
    t.assert_not_equals(reload(cg), nil)
    source = '@' .. cg.copy2 .. '/memcached/server.lua'
    t.assert_equals(sources(), {source, source})
    for _, c in ipairs(conns) do
        t.assert_equals(
            c:call('version\r\n'),
            'VERSION 1.6.45-mcc-second\r\n'
        )
        t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    end
end
