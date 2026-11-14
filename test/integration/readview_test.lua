-- Read views: the stale reads of read_mode = 'snapshot' (TX-only and with
-- the front-ends in application threads) and the lru_crawler metadump /
-- mgdump dumps.

local clock = require('clock')
local fiber = require('fiber')
local socket = require('socket')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')
local th = require('test.helpers.threads')
local rvh = require('test.helpers.readview')

local g = t.group('readview')

local NTHREADS = 2
local NAMES = {'snap', 'rvt', 'rvdisk', 'dmp'}

g.before_all(function(cg)
    cg.server = th.start('mcc_readview', NTHREADS)
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
    for _, name in ipairs(NAMES) do
        th.destroy(cg.server, name)
    end
    cg.server:exec(function(names)
        for _, name in ipairs(names) do
            local s = box.space['__mcc_' .. name]
            if s ~= nil then
                s:drop()
            end
        end
        if box.space._mcc_meta ~= nil then
            box.space._mcc_meta:truncate()
        end
    end, {NAMES})
end)

local function connect(cg)
    local c = client.must_connect(cg.port)
    cg.clients = cg.clients or {}
    table.insert(cg.clients, c)
    return c
end

local function cfg(cg, name, opts)
    return cg.server:exec(function(name_, opts_)
        return require('memcached').get(name_):cfg(opts_).status
    end, {name, opts})
end

-- The tuple of a key in the data space of an instance, as an array.
local function tuple(cg, name, key)
    return cg.server:exec(function(name_, key_)
        local tup = box.space['__mcc_' .. name_]:get(key_)
        return tup ~= nil and tup:totable() or nil
    end, {name, key})
end

-- A reply of a known number of lines.
local function lines(c, n)
    local res = {}
    for i = 1, n do
        res[i] = c:line()
    end
    return table.concat(res)
end

-- Snapshot mode, TX only ---------------------------------------------------

g.test_snapshot_tx = function(cg)
    local srv = cg.server
    th.create(srv, 'snap', cg.port)
    local c = connect(cg)
    t.assert_equals(c:set('b', 'old'), 'STORED\r\n')
    t.assert_equals(c:set('k', 'v'), 'STORED\r\n')
    t.assert_equals(c:call('md k I\r\n'), 'HD\r\n')
    -- An item that expires in 3 seconds.
    srv:exec(function()
        local now = require('fiber').time()
        box.space.__mcc_snap:replace({
            'e',
            'x',
            0,
            now + 3,
            1ULL,
            now,
            now,
            false,
            false,
            0,
        })
    end)

    t.assert_equals(
        cfg(cg, 'snap', {
            read_mode = 'snapshot',
            snapshot_refresh = 100,
        }),
        'running'
    )
    local id = rvh.wait_id(srv, 'snap')
    t.assert_equals(rvh.fibers(srv, '^mcc/rv/snap$'), {'mcc/rv/snap'})

    -- The plain gets are served from the read view and are stale.
    t.assert_equals(c:get('b', 'get'), 'old')
    t.assert_equals(c:set('b', 'new'), 'STORED\r\n')
    t.assert_equals(c:get('b', 'get'), 'old')
    t.assert_equals(c:get('b', 'gets'), 'old')
    t.assert_equals(c:call('mg b v\r\n') .. c:line(), 'VA 3\r\nold\r\n')
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    t.assert_equals(c:get('a', 'get'), nil)
    t.assert_equals(c:call('mg a v\r\n'), 'EN\r\n')
    -- Without a bump of last_access or fetched in the space.
    local tup = tuple(cg, 'snap', 'b')
    t.assert_equals(tup[2], 'new')
    t.assert_equals(tup[8], false, 'not fetched')

    -- Touch, vivify and recache go to TX.
    c:send('gat 0 a\r\n')
    t.assert_equals(lines(c, 3), 'VALUE a 0 1\r\n1\r\nEND\r\n')
    t.assert_equals(c:call('touch a 0\r\n'), 'TOUCHED\r\n')
    t.assert_equals(c:call('mg a v T0\r\n') .. c:line(), 'VA 1\r\n1\r\n')
    t.assert_equals(c:call('mg a v N30\r\n') .. c:line(), 'VA 1\r\n1\r\n')
    t.assert_equals(c:call('mg a v R30\r\n') .. c:line(), 'VA 1\r\n1\r\n')
    t.assert_equals(c:call('mg viv v N30\r\n') .. c:line(), 'VA 0 W\r\n\r\n')
    -- A plain get still doesn't see it.
    t.assert_equals(c:get('a', 'get'), nil)

    -- A stale item hands out the win token: TX serves it.
    t.assert_equals(c:call('mg k v\r\n') .. c:line(), 'VA 1 X W\r\nv\r\n')
    t.assert_equals(c:call('mg k v\r\n') .. c:line(), 'VA 1 Z X\r\nv\r\n')

    -- Expiry is checked against the current time, not the read view's.
    t.assert_equals(c:get('e', 'get'), 'x')
    t.helpers.retrying({timeout = 10}, function()
        t.assert_equals(c:get('e', 'get'), nil)
    end)
    t.assert_equals(rvh.current_id(srv, 'snap'), id, 'the same read view')

    -- The win tokens are in the items: once in a read view, a stale item
    -- whose token is out is served from it with Z, and so is the Z of a
    -- recache win.
    t.assert_equals(c:set('r', 'v', {exptime = 100}), 'STORED\r\n')
    t.assert_equals(c:call('mg r R200\r\n'), 'HD W\r\n')
    rvh.refresh(srv, 'snap')
    id = rvh.wait_id(srv, 'snap', id)
    -- New versions in the space, without tokens: not in the read view.
    t.assert_equals(c:set('k', 'new'), 'STORED\r\n')
    t.assert_equals(c:set('r', 'new'), 'STORED\r\n')
    t.assert_equals(c:call('mg k v\r\n') .. c:line(), 'VA 1 Z X\r\nv\r\n')
    t.assert_equals(c:call('mg r\r\n'), 'HD Z\r\n')
    t.assert_equals(c:get('r', 'get'), 'v')
    t.assert_equals(tuple(cg, 'snap', 'k')[11], false)

    -- flush_all is visible at once on the same connection, and the next read
    -- view has it.
    t.assert_equals(c:call('flush_all\r\n'), 'OK\r\n')
    t.assert_equals(c:get('b', 'get'), nil)
    t.assert_equals(c:get('k', 'get'), nil)
    id = rvh.wait_id(srv, 'snap', id)
    t.assert_equals(c:get('b', 'get'), nil)

    -- A shorter interval makes new data visible.
    t.assert_equals(c:set('x', 'y'), 'STORED\r\n')
    t.assert_equals(c:get('x', 'get'), nil)
    cfg(cg, 'snap', {snapshot_refresh = 0.1})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(c:get('x', 'get'), 'y')
    end)
    -- Several refreshes later, only the current read view is open.
    fiber.sleep(0.5)
    t.assert_ge(rvh.current_id(srv, 'snap') - id, 3, 'refreshed')
    t.assert_equals(rvh.views(srv, 'mcc/snap'), {'open'})

    -- Back to 'rw': the refresher and its read view are gone, the reads are
    -- fresh.
    cfg(cg, 'snap', {read_mode = 'rw'})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(rvh.fibers(srv, '^mcc/rv/snap$'), {})
        t.assert_equals(rvh.views(srv, 'mcc/snap'), {})
    end)
    t.assert_equals(rvh.current_id(srv, 'snap'), nil)
    t.assert_equals(c:set('x', 'z'), 'STORED\r\n')
    t.assert_equals(c:get('x', 'get'), 'z')
    -- A get bumps fetched again.
    t.assert_equals(tuple(cg, 'snap', 'x')[8], true)
end

g.test_snapshot_validation = function(cg)
    local res = cg.server:exec(function(port)
        local mcc = require('memcached')
        local listen = '127.0.0.1:' .. port
        local out = {}
        local ok, err = pcall(mcc.create, 'rvdisk', {
            listen = listen,
            storage = 'disk',
            read_mode = 'snapshot',
        })
        table.insert(out, {ok, tostring(err), mcc.get('rvdisk') == nil})
        local inst = mcc.create('rvdisk', {listen = listen, storage = 'disk'})
        ok, err = pcall(inst.cfg, inst, {read_mode = 'snapshot'})
        table.insert(out, {ok, tostring(err), inst.opts.read_mode == 'rw'})
        ok, err = pcall(inst.cfg, inst, {snapshot_refresh = 0.05})
        table.insert(out, {ok, tostring(err), inst.opts.snapshot_refresh == 1})
        return out
    end, {cg.port})
    t.assert_equals(res[1][1], false)
    t.assert_str_contains(
        res[1][2],
        "read_mode 'snapshot' needs storage 'memory'"
    )
    t.assert(res[1][3])
    t.assert_equals(res[2][1], false)
    t.assert_str_contains(res[2][2], "needs storage 'memory'")
    t.assert(res[2][3])
    t.assert_equals(res[3][1], false)
    t.assert_str_contains(res[3][2], "Bad value for option 'snapshot_refresh'")
    t.assert(res[3][3])
end

-- Snapshot mode with the front-ends in application threads ---------------

g.test_snapshot_threads = function(cg)
    local srv = cg.server
    th.create(srv, 'rvt', cg.port, {
        threads = 'app',
        read_mode = 'snapshot',
        snapshot_refresh = 100,
    })
    local id = rvh.wait_id(srv, 'rvt', nil, {threads = true})
    -- Several connections, spread over the threads.
    local conns = {}
    for i = 1, 8 do
        conns[i] = connect(cg)
    end
    t.assert_equals(conns[1]:set('k', 'v1'), 'STORED\r\n')
    for _, c in ipairs(conns) do
        t.assert_equals(c:get('k', 'get'), nil, 'stale')
    end
    -- Touches go to TX.
    conns[2]:send('gat 0 k\r\n')
    t.assert_equals(lines(conns[2], 3), 'VALUE k 0 2\r\nv1\r\nEND\r\n')

    -- A new read view reaches every thread.
    rvh.refresh(srv, 'rvt')
    id = rvh.wait_id(srv, 'rvt', id, {threads = true})
    for _, c in ipairs(conns) do
        t.assert_equals(c:get('k', 'get'), 'v1')
    end
    t.assert_equals(tuple(cg, 'rvt', 'k')[8], true, 'fetched by gat only')
    -- Each thread has its own reference to the current read view only; TX
    -- closes the old one once the threads release it.
    t.assert_equals(rvh.thread_views(srv, 'mcc/rvt'), {1, 1})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(rvh.views(srv, 'mcc/rvt'), {'open'})
    end)

    -- A flush is visible at once through its connection; the other
    -- connections see it after the next publication.
    t.assert_equals(conns[1]:set('k', 'v2'), 'STORED\r\n')
    t.assert_equals(conns[1]:call('flush_all\r\n'), 'OK\r\n')
    t.assert_equals(conns[1]:get('k', 'get'), nil)
    rvh.wait_id(srv, 'rvt', id, {threads = true})
    for _, c in ipairs(conns) do
        t.assert_equals(c:get('k', 'get'), nil)
    end

    -- A shorter interval: new data reaches every connection.
    cfg(cg, 'rvt', {snapshot_refresh = 0.1})
    t.assert_equals(conns[3]:set('z', '1'), 'STORED\r\n')
    t.helpers.retrying({timeout = 5}, function()
        for _, c in ipairs(conns) do
            t.assert_equals(c:get('z', 'get'), '1')
        end
    end)

    -- Back to 'rw': the threads drop their read views, the reads are fresh.
    cfg(cg, 'rvt', {read_mode = 'rw'})
    t.assert_equals(rvh.current_id(srv, 'rvt'), nil)
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(rvh.thread_ids(srv, 'rvt'), {false, false})
        t.assert_equals(rvh.thread_views(srv, 'mcc/rvt'), {0, 0})
        t.assert_equals(rvh.views(srv, 'mcc/rvt'), {})
    end)
    t.assert_equals(conns[4]:set('z', '2'), 'STORED\r\n')
    for _, c in ipairs(conns) do
        t.assert_equals(c:get('z', 'get'), '2')
    end
end

g.test_snapshot_threads_stop = function(cg)
    local srv = cg.server
    th.create(srv, 'rvt', cg.port, {
        threads = 'app',
        read_mode = 'snapshot',
        snapshot_refresh = 0.1,
    })
    rvh.wait_id(srv, 'rvt', nil, {threads = true})
    -- Stopping (and destroying) while the read views are being published
    -- leaves no read view behind, in TX or in the threads.
    srv:exec(function()
        local inst = require('memcached').get('rvt')
        for _ = 1, 5 do
            inst:stop()
            require('fiber').sleep(0.05)
            inst:start()
            require('fiber').sleep(0.15)
        end
        inst:destroy()
    end)
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(rvh.thread_ids(srv, 'rvt'), {false, false})
        t.assert_equals(rvh.thread_views(srv, 'mcc/rvt'), {0, 0})
        t.assert_equals(rvh.views(srv, 'mcc/rvt'), {})
        t.assert_equals(rvh.fibers(srv, '^mcc/rv/rvt$'), {})
    end)
end

-- Dumps ------------------------------------------------------------------

-- memcached's uriencode(): all but [0-9A-Za-z._~-] as %XX.
local function uriencode(s)
    return (s:gsub('[^0-9A-Za-z_~%.%-]', function(ch)
        return ('%%%02X'):format(ch:byte())
    end))
end

-- The expected metadump lines of the live items of a space, sorted.
local function expected_metadump(cg, name)
    local items = cg.server:exec(function(name_)
        local now = require('fiber').time()
        local res = {}
        for _, tup in box.space['__mcc_' .. name_]:pairs() do
            if tup[4] == 0 or tup[4] > now then
                local cas = tostring(tup[5]):gsub('ULL$', '')
                table.insert(res, {
                    tup[1],
                    #tup[2],
                    tup[3],
                    tup[4],
                    cas,
                    tup[7],
                    tup[8],
                })
            end
        end
        return res
    end, {name})
    local res = {}
    for _, it in ipairs(items) do
        local key, size, flags, exptime, cas, la, fetched = unpack(it)
        table.insert(
            res,
            ('key=%s exp=%s la=%d cas=%s fetch=%s cls=1 '
                .. 'size=%d flags=%d \n'):format(
                    uriencode(key),
                    exptime == 0 and '-1' or ('%d'):format(math.floor(exptime)),
                    math.floor(la),
                    cas,
                    fetched and 'yes' or 'no',
                    #key + size,
                    flags
                )
        )
    end
    table.sort(res)
    return res
end

-- Send a dump command; returns the first line and the sorted item lines up
-- to the terminator.
local function dump(c, cmd)
    c:send(cmd .. '\r\n')
    local first = c:line()
    local items = {}
    local term = cmd:find('mgdump') and 'EN\r\n' or 'END\r\n'
    while true do
        -- metadump lines end with '\n' only.
        local line = c.sock:read('\n', client.TIMEOUT)
        t.assert_not_equals(line, nil, 'the dump ends')
        if line == term then
            break
        end
        table.insert(items, line)
    end
    table.sort(items)
    return first, items
end

local function dump_cases(cg, opts)
    local srv = cg.server
    th.create(srv, 'dmp', cg.port, opts)
    local c = connect(cg)
    t.assert_equals(c:set('a', 'xyz', {flags = 5}), 'STORED\r\n')
    t.assert_equals(c:set('b', 'hello', {exptime = 1000}), 'STORED\r\n')
    t.assert_equals(c:set('u/v%w~x.y-z_1', 'q'), 'STORED\r\n')
    -- A key with a space, set with base64.
    t.assert_equals(c:call('ms d2l0aCBzcGFjZQ== 2 b\r\nhi\r\n'), 'HD\r\n')
    t.assert_equals(c:get('a', 'get'), 'xyz')
    -- An expired item, not deleted yet.
    srv:exec(function()
        local now = require('fiber').time()
        box.space.__mcc_dmp:replace({
            'old',
            'v',
            0,
            now - 10,
            7ULL,
            now - 20,
            now - 20,
            false,
            false,
            0,
        })
    end)
    local expected = expected_metadump(cg, 'dmp')
    t.assert_equals(#expected, 4)
    t.assert_str_contains(
        table.concat(expected),
        'key=with%20space exp=-1 '
    )
    t.assert_str_contains(table.concat(expected), 'key=u%2Fv%25w~x.y-z_1 ')
    t.assert_str_contains(
        table.concat(expected),
        ' fetch=yes cls=1 size=4 flags=5 \n'
    )

    for _, classes in ipairs({'all', 'hash', '1', '2,1', '63,1'}) do
        local first, items = dump(c, 'lru_crawler metadump ' .. classes)
        t.assert_equals(first, 'OK\r\n')
        t.assert_equals(items, expected, classes)
    end
    -- Dumps have no side effects.
    t.assert_equals(expected_metadump(cg, 'dmp'), expected)
    local first, items = dump(c, 'lru_crawler mgdump all')
    t.assert_equals(first, 'OK\r\n')
    t.assert_equals(items, {
        'mg a\r\n',
        'mg b\r\n',
        'mg d2l0aCBzcGFjZQ== b\r\n',
        'mg u/v%w~x.y-z_1\r\n',
    })
    -- Other classes are empty.
    t.assert_equals(
        c:call('lru_crawler metadump 2\r\n', 'END\r\n'),
        'OK\r\nEND\r\n'
    )
    t.assert_equals(
        c:call('lru_crawler mgdump 2,3\r\n', 'EN\r\n'),
        'OK\r\nEN\r\n'
    )
    -- Bad commands.
    for _, cmd in ipairs({
        'lru_crawler metadump 64',
        'lru_crawler metadump x',
        'lru_crawler mgdump 0',
        'lru_crawler metadump 1,64',
        'lru_crawler metadump -1',
    }) do
        t.assert_equals(
            c:call(cmd .. '\r\n'),
            'BADCLASS invalid class id\r\n',
            cmd
        )
    end
    for _, cmd in ipairs({
        'lru_crawler bogus',
        'lru_crawler metadump',
        'lru_crawler crawl all',
        'lru_crawler',
    }) do
        t.assert_equals(c:call(cmd .. '\r\n'), 'ERROR\r\n', cmd)
    end

    -- A dump can't follow other commands in a batch; the commands after it
    -- are answered after it.
    c:send('get nope\r\nlru_crawler metadump all\r\n')
    t.assert_equals(
        lines(c, 2),
        'END\r\nERROR cannot pipeline other commands before metadump\r\n'
    )
    c:send('get nope\r\nlru_crawler mgdump all\r\nversion\r\n')
    t.assert_equals(
        lines(c, 2),
        'END\r\nERROR cannot pipeline other commands before mgdump\r\n'
    )
    t.assert_str_contains(c:line(), 'VERSION ')
    c:send('lru_crawler mgdump 2\r\nget a\r\nmn\r\n')
    t.assert_equals(
        lines(c, 6),
        'OK\r\nEN\r\nVALUE a 5 3\r\nxyz\r\nEND\r\nMN\r\n'
    )

    -- Flushed items are not dumped, newer ones are.
    t.assert_equals(c:call('flush_all\r\n'), 'OK\r\n')
    t.assert_equals(
        c:call('lru_crawler metadump all\r\n', 'END\r\n'),
        'OK\r\nEND\r\n'
    )
    t.assert_equals(c:set('n', '1'), 'STORED\r\n')
    first, items = dump(c, 'lru_crawler mgdump hash')
    t.assert_equals(first, 'OK\r\n')
    t.assert_equals(items, {'mg n\r\n'})

    -- The read views of the dumps are closed, in TX and in the threads.
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(rvh.views(srv, 'mcc/dump/dmp'), {})
        if opts.threads ~= nil then
            t.assert_equals(rvh.thread_views(srv, 'mcc/dump/dmp'), {0, 0})
        end
    end)
end

g.test_dump_tx = function(cg)
    dump_cases(cg, {})
end

g.test_dump_threads = function(cg)
    dump_cases(cg, {threads = 'app'})
end

g.test_dump_unsupported = function(cg)
    th.create(cg.server, 'dmp', cg.port, {storage = 'disk'})
    local c = connect(cg)
    t.assert_equals(c:set('a', '1'), 'STORED\r\n')
    t.assert_equals(
        c:call('lru_crawler metadump all\r\n'),
        "SERVER_ERROR no read view of space '__mcc_dmp': dumps "
            .. "need storage 'memory'\r\n"
    )
    t.assert_equals(c:get('a', 'get'), '1')
    -- A backend with remote data (vshard) has no local read view, even if a
    -- local space has the same name.
    local out = cg.server:exec(function()
        local rv = require('memcached.readview')
        local res = {}
        local srv = {
            name = 'x',
            backend = {
                REMOTE = true,
                space_name = '__mcc_dmp',
            },
        }
        rv.dump(
            srv,
            {op = 'lru_crawler', sub = 'metadump', items = true},
            function(data)
                table.insert(res, data)
                return true
            end
        )
        return table.concat(res)
    end)
    t.assert_equals(
        out,
        'SERVER_ERROR dumps are not supported by this backend\r\n'
    )
end

-- TX closes the read view of a dump that a thread doesn't take.
g.test_dump_timeout = function(cg)
    local res = cg.server:exec(function()
        local fiber_ = require('fiber')
        local rv = require('memcached.readview')
        local state = require('memcached.state')
        local function count()
            local n = 0
            for _, v in ipairs(box.read_view.list()) do
                if v.name == 'mcc/dump/lost' then
                    n = n + 1
                end
            end
            return n
        end
        local timeout = rv.DUMP_TIMEOUT
        rv.DUMP_TIMEOUT = 0.1
        local id = rv.tx_open('lost')
        local out = {count(), state.dump_readviews[id] ~= nil}
        fiber_.sleep(0.3)
        rv.DUMP_TIMEOUT = timeout
        table.insert(out, count())
        table.insert(out, state.dump_readviews[id] == nil)
        table.insert(out, rv.tx_close(id))
        -- Taken in time: closed at once.
        id = rv.tx_open('lost')
        table.insert(out, rv.tx_close(id))
        table.insert(out, count())
        return out
    end)
    t.assert_equals(res, {1, true, 0, true, false, true, 0})
end

-- A client with a small receive buffer.
local function slow_client(port)
    local s = socket('AF_INET', 'SOCK_STREAM', 'tcp')
    t.assert(s:setsockopt('SOL_SOCKET', 'SO_RCVBUF', 4096))
    if not s:sysconnect('127.0.0.1', port) then
        t.assert(s:writable(client.TIMEOUT))
    end
    return s
end

local BIG = 50000
local KEY_PAD = string.rep('x', 100)

local function big_key(i)
    return ('k%05d'):format(i) .. KEY_PAD
end

-- A big dump to a client that doesn't read doesn't block the other clients
-- and is consistent: it is a dump of the read view taken when it started.
local function big_dump(cg, opts)
    local srv = cg.server
    th.create(srv, 'dmp', cg.port, opts)
    srv:exec(function(n, pad)
        local fiber_ = require('fiber')
        local s = box.space.__mcc_dmp
        local now = fiber_.time()
        box.begin()
        for i = 1, n do
            s:replace({
                ('k%05d'):format(i) .. pad,
                'v',
                0,
                0,
                i,
                now,
                now,
                false,
                false,
                0,
            })
            if i % 1000 == 0 then
                box.commit()
                box.begin()
            end
        end
        box.commit()
    end, {BIG, KEY_PAD})
    local slow = slow_client(cg.port)
    local dump_rv = 'mcc/dump/dmp'
    t.assert(slow:write('lru_crawler metadump all\r\n'))
    -- The dump is stuck on the client.
    local function dumping()
        if opts.threads ~= nil then
            local n = 0
            for _, k in ipairs(rvh.thread_views(srv, dump_rv)) do
                n = n + k
            end
            return n
        end
        return #rvh.views(srv, dump_rv)
    end
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(dumping(), 1)
    end)
    fiber.sleep(0.3)
    t.assert_equals(dumping(), 1, 'the dump is blocked')

    -- The others are served meanwhile.
    local c = connect(cg)
    local t0 = clock.monotonic()
    t.assert_equals(c:set(big_key(1), 'longer value'), 'STORED\r\n')
    t.assert_equals(c:set('new', '1'), 'STORED\r\n')
    t.assert_equals(
        c:call(('delete %s\r\n'):format(big_key(2))),
        'DELETED\r\n'
    )
    t.assert_equals(c:get(big_key(1), 'get'), 'longer value')
    t.assert_lt(clock.monotonic() - t0, 1)
    t.assert_equals(dumping(), 1, 'the dump is still in progress')

    -- The dump has the data as of its start.
    t.assert_equals(slow:read('\n', 10), 'OK\r\n')
    local n, seen = 0, {}
    while true do
        local line = slow:read('\n', 10)
        t.assert_not_equals(line, nil)
        t.assert_not_equals(line, '')
        if line == 'END\r\n' then
            break
        end
        n = n + 1
        local key, size = line:match('^key=(%S+) .* size=(%d+) ')
        if key == big_key(1) or key == big_key(2) or key == 'new' then
            seen[key] = tonumber(size)
        end
    end
    slow:close()
    t.assert_equals(n, BIG)
    t.assert_equals(seen, {
        [big_key(1)] = #big_key(1) + 1,
        [big_key(2)] = #big_key(2) + 1,
    })
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(dumping(), 0)
        t.assert_equals(rvh.views(srv, dump_rv), {})
    end)
end

g.test_dump_big_tx = function(cg)
    big_dump(cg, {})
end

g.test_dump_big_threads = function(cg)
    big_dump(cg, {threads = 'app'})
end

-- Publishing in a thread -------------------------------------------------

-- Open read views in TX (kept open until close_views); returns their ids,
-- ascending.
local function open_views(cg, n)
    return cg.server:exec(function(n_)
        _G.test_views = _G.test_views or {}
        local ids = {}
        for i = 1, n_ do
            local rv = box.read_view.open({name = 'mcc/test'})
            table.insert(_G.test_views, rv)
            ids[i] = rv.id
        end
        return ids
    end, {n})
end

local function close_views(cg)
    cg.server:exec(function()
        for _, rv in ipairs(_G.test_views or {}) do
            pcall(rv.close, rv)
        end
        _G.test_views = nil
    end)
end

g.test_publish_order = function(cg)
    -- Load the code in the threads.
    th.create(cg.server, 'rvt', cg.port, {threads = 'app'})
    local ids = open_views(cg, 3)
    local res = th.thread_eval(
        cg.server,
        [[
        local fiber = require('fiber')
        local rv = require('memcached.readview')
        local a, b, c = ...
        local out = {}
        -- A late publication of an older read view is ignored, even when
        -- both are being opened at once.
        local f1 = fiber.new(rv.publish, 'x', b)
        f1:set_joinable(true)
        local f2 = fiber.new(rv.publish, 'x', a)
        f2:set_joinable(true)
        local _, r1 = f1:join()
        f2:join()
        table.insert(out, {r1, rv.current_id('x')})
        table.insert(out, {rv.publish('x', a), rv.current_id('x')})
        -- After a flush through the wrapper, read views opened before it
        -- (by TX's count: rv_floor) are not used.
        local inner = {flush_all = function()
            return {status = 'OK', rv_floor = b}
        end}
        local w = rv.wrap(inner, {name = 'x', space_name = 'none'})
        table.insert(out, {w:flush_all(0).status, rv.current_id('x')})
        table.insert(out, {rv.publish('x', b), rv.current_id('x')})
        table.insert(out, {rv.publish('x', c), rv.current_id('x')})
        -- A newer read view than the floor is kept.
        table.insert(out, {w:flush_all(0).status, rv.current_id('x')})
        rv.drop('x')
        table.insert(out, {rv.current_id('x')})
        return out
    ]],
        ids
    )
    close_views(cg)
    local a, b, c = unpack(ids)
    for _, r in ipairs(res) do
        t.assert_equals(r, {
            {true, b},
            {false, b},
            {'OK', nil},
            {false, nil},
            {true, c},
            {'OK', c},
            {},
        })
    end
    t.assert_lt(a, b)
end

g.test_publish_stopped = function(cg)
    local srv = cg.server
    th.create(srv, 'rvt', cg.port, {
        threads = 'app',
        read_mode = 'snapshot',
        snapshot_refresh = 100,
    })
    rvh.wait_id(srv, 'rvt', nil, {threads = true})
    srv:exec(function()
        require('memcached').get('rvt'):stop()
    end)
    t.assert_equals(rvh.thread_ids(srv, 'rvt'), {false, false})
    -- A publication that reaches a thread after the stop is dropped.
    local ids = open_views(cg, 1)
    local res = th.thread_eval(
        srv,
        [[
        local threads = require('memcached.threads')
        local ok = threads.fe.rv_publish({name = 'rvt', id = ...})
        return {ok, require('memcached.readview').current_id('rvt')}
    ]],
        ids
    )
    close_views(cg)
    t.assert_equals(res, {{false}, {false}})
    t.assert_equals(rvh.thread_views(srv, 'mcc/test'), {0, 0})
    -- So is one to a front-end that is not in snapshot mode.
    srv:exec(function()
        require('memcached').get('rvt'):cfg({read_mode = 'rw'})
        require('memcached').get('rvt'):start()
    end)
    ids = open_views(cg, 1)
    res = th.thread_eval(
        srv,
        [[
        local threads = require('memcached.threads')
        local ok = threads.fe.rv_publish({name = 'rvt', id = ...})
        return {ok, require('memcached.readview').current_id('rvt')}
    ]],
        ids
    )
    close_views(cg)
    t.assert_equals(res, {{false}, {false}})
end
