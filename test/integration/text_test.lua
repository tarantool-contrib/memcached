-- The classic text protocol over real sockets. The cases come from the
-- tarantool/memcached text tests (test/text/*.test.py and their .result
-- files) and from memcached's protocol.txt.

local fiber = require('fiber')
local fio = require('fio')
local socket = require('socket')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local g = t.group('text_protocol')

local NAME = 'text'
local SPACE = '__mcc_text'

g.before_all(function(cg)
    env.box_cfg()
    cg.mcc = require('memcached')
    cg.port = env.free_port()
    cg.inst = cg.mcc.create(NAME, {listen = '127.0.0.1:' .. cg.port})
end)

g.after_all(function(cg)
    cg.inst:destroy()
    env.drop_spaces({SPACE})
    box.space._mcc_meta:delete('flush:' .. SPACE)
end)

g.before_each(function(cg)
    box.space[SPACE]:truncate()
    box.space._mcc_meta:delete('flush:' .. SPACE)
    cg.c = client.must_connect(cg.port)
end)

g.after_each(function(cg)
    cg.c:close()
    for _, c in ipairs(cg.extra or {}) do
        c:close()
    end
    cg.extra = nil
end)

local function connect(cg)
    local c = client.must_connect(cg.port)
    cg.extra = cg.extra or {}
    table.insert(cg.extra, c)
    return c
end

local function shorten(s)
    if #s > 200 then
        return s:sub(1, 200) .. ('... (%d bytes)'):format(#s)
    end
    return s
end

-- Send a request and expect exactly this reply.
local function check(c, request, reply)
    c:send(request)
    local got = #reply > 0 and c:read(#reply) or ''
    if got ~= reply then
        -- Show the rest of the reply, if any.
        local rest = c.sock:read(65536, 0.1) or ''
        t.fail(
            ('%s\nexpected: %q\nactual: %q'):format(
                shorten(request),
                shorten(reply),
                shorten((got or '') .. rest)
            )
        )
    end
end

-- The full reply to a retrieval command.
local function retrieve(c, request)
    c:send(request)
    return c:read_values()
end

local function cas_of(c, key)
    local _, _, cas = c:get(key)
    t.assert(cas, 'cas of ' .. key)
    return cas
end

g.test_getset = function(cg)
    local c = cg.c
    check(c, 'set foo 0 0 6\r\nfooval\r\n', 'STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 0 6\r\nfooval\r\nEND\r\n')
    check(c, 'set bar 0 0 6\r\nbarval\r\n', 'STORED\r\n')
    check(c, 'get bar\r\n', 'VALUE bar 0 6\r\nbarval\r\nEND\r\n')
    -- add foo: not stored, the old value stays.
    check(c, 'add foo 0 0 5\r\nfoov2\r\n', 'NOT_STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 0 6\r\nfooval\r\nEND\r\n')
    check(c, 'replace bar 0 0 6\r\nbarva2\r\n', 'STORED\r\n')
    check(c, 'get bar\r\n', 'VALUE bar 0 6\r\nbarva2\r\nEND\r\n')
    check(c, 'replace notexist 0 0 6\r\nbarva2\r\n', 'NOT_STORED\r\n')
    check(c, 'get notexist\r\n', 'END\r\n')
    check(c, 'delete foo\r\n', 'DELETED\r\n')
    check(c, 'get foo\r\n', 'END\r\n')
    check(c, 'delete foo\r\n', 'NOT_FOUND\r\n')
    check(c, 'get foo\r\n', 'END\r\n')
    check(c, 'add moo 0 0 6\r\nmooval\r\n', 'STORED\r\n')
    check(c, 'get moo\r\n', 'VALUE moo 0 6\r\nmooval\r\nEND\r\n')
    -- cas with a wrong unique value.
    check(c, 'cas moo 0 0 6 0\r\nMOOVAL\r\n', 'EXISTS\r\n')
    check(c, 'get moo\r\n', 'VALUE moo 0 6\r\nmooval\r\nEND\r\n')
    local cas = cas_of(c, 'moo')
    check(c, ('cas moo 0 0 6 %s\r\nMOOVAL\r\n'):format(cas), 'STORED\r\n')
    check(c, 'get moo\r\n', 'VALUE moo 0 6\r\nMOOVAL\r\nEND\r\n')
    -- Pipelined.
    check(
        c,
        'set foo 0 0 6\r\nfooval\r\ndelete foo\r\n'
            .. 'set foo 0 0 6\r\nfooval\r\ndelete foo\r\n',
        'STORED\r\nDELETED\r\nSTORED\r\nDELETED\r\n'
    )
    -- Multi-get.
    check(
        c,
        'set a 1 0 1\r\nA\r\nset b 2 0 1\r\nB\r\n',
        'STORED\r\nSTORED\r\n'
    )
    check(
        c,
        'get a nope b a\r\n',
        'VALUE a 1 1\r\nA\r\nVALUE b 2 1\r\nB\r\nVALUE a 1 1\r\nA\r\nEND\r\n'
    )
end

g.test_big_values = function(cg)
    local c = cg.c
    for _, len in ipairs({1024, 1024 + 512 * 1024}) do
        check(
            c,
            ('set foo_%d 0 0 %d\r\n%s\r\n'):format(
                len,
                len,
                string.rep('B', len)
            ),
            'STORED\r\n'
        )
    end
    -- Causing a memory overflow doesn't leave stale data.
    local len = 1024 * 1024 + 1024
    check(c, ('set foo_%d 0 0 3\r\nMOO\r\n'):format(len), 'STORED\r\n')
    check(
        c,
        ('get foo_%d\r\n'):format(len),
        ('VALUE foo_%d 0 3\r\nMOO\r\nEND\r\n'):format(len)
    )
    check(
        c,
        ('set foo_%d 0 0 %d\r\n%s\r\n'):format(
            len,
            len,
            string.rep('B', len)
        ),
        'SERVER_ERROR object too large for cache\r\n'
    )
    check(c, ('get foo_%d\r\n'):format(len), 'END\r\n')
    -- Other commands keep the old value.
    check(c, 'set x 0 0 1\r\nx\r\n', 'STORED\r\n')
    check(
        c,
        ('append x 0 0 %d\r\n%s\r\n'):format(len, string.rep('B', len)),
        'SERVER_ERROR object too large for cache\r\n'
    )
    check(c, 'get x\r\n', 'VALUE x 0 1\r\nx\r\nEND\r\n')
    -- Close to the limit (memtx_max_tuple_size is 1 MiB by default and the
    -- tuple has some overhead, like memcached's items).
    len = 1024 * 1024 - 512
    local value = string.rep('x', len)
    check(c, ('set max 0 0 %d\r\n%s\r\n'):format(len, value), 'STORED\r\n')
    t.assert(c:get('max') == value)
end

g.test_case_sensitive = function(cg)
    -- As in memcached: command names are exact, so "SET" is unknown and its
    -- data line is a command too.
    local c = cg.c
    check(c, 'SET foo 0 0 6\r\nfooval\r\n', 'ERROR\r\nERROR\r\n')
    check(c, 'set foo 0 0 6\r\nfooval\r\n', 'STORED\r\n')
    check(c, 'GET foo\r\n', 'ERROR\r\n')
    check(c, 'Gets foo\r\n', 'ERROR\r\n')
    check(
        c,
        'DELETE foo noreply\r\nget foo\r\n',
        'VALUE foo 0 6\r\nfooval\r\nEND\r\n'
    )
end

g.test_binary_get = function(cg)
    local c = cg.c
    local blobs = {
        'mooo\0',
        'mumble\0\0\0\0\r\rblarg',
        '\0',
        '\r',
        '\r\n',
        'END\r\n',
    }
    for i, blob in ipairs(blobs) do
        local key = 'foo_' .. i
        check(
            c,
            ('set %s 0 0 %d\r\n%s\r\n'):format(key, #blob, blob),
            'STORED\r\n'
        )
        check(
            c,
            ('get %s\r\n'):format(key),
            ('VALUE %s 0 %d\r\n%s\r\nEND\r\n'):format(key, #blob, blob)
        )
    end
    check(c, 'flush_all\r\n', 'OK\r\n')
end

g.test_bogus_commands = function(cg)
    local c = cg.c
    check(c, 'boguscommand slkdsldkfjsd\r\n', 'ERROR\r\n')
    -- As memcached 1.6.45: a line shorter than 4 bytes and too few
    -- arguments are "bad command line format".
    local FORMAT = 'CLIENT_ERROR bad command line format\r\n'
    check(c, '\r\n', FORMAT)
    check(c, 'gets\r\n', FORMAT)
    check(c, 'set foo 0 0\r\n', FORMAT)
    -- The connection is still usable.
    check(c, 'set foo 0 0 1\r\nx\r\n', 'STORED\r\n')
    -- A bad length: the data block isn't known, so it is parsed as a
    -- command (a short line here).
    check(c, 'set foo 0 0 x\r\ny\r\n', FORMAT .. FORMAT)
    -- Other errors swallow the data block.
    check(c, 'set foo x 0 1\r\ny\r\n', FORMAT)
    check(c, 'get foo\r\n', 'VALUE foo 0 1\r\nx\r\nEND\r\n')
end

g.test_cas = function(cg)
    local c = cg.c
    for _, line in ipairs({
        'cas bad blah 0 0 0',
        'cas bad 0 blah 0 0',
        'cas bad 0 0 0 blah',
    }) do
        check(
            c,
            line .. '\r\n\r\n',
            'CLIENT_ERROR bad command line format\r\n'
        )
    end
    -- A bad length: nothing to swallow.
    check(
        c,
        'cas bad 0 0 blah 0\r\n',
        'CLIENT_ERROR bad command line format\r\n'
    )
    check(c, 'gets foo\r\n', 'END\r\n')
    check(c, 'set foo 0 0 6\r\nbarval\r\n', 'STORED\r\n')
    c:send('gets foo\r\n')
    t.assert_str_matches(
        c:read_values(),
        'VALUE foo 0 6 %d+\r\nbarval\r\nEND\r\n'
    )
    check(c, 'cas foo 0 0 6 123\r\nbarva2\r\n', 'EXISTS\r\n')
    local cas = cas_of(c, 'foo')
    check(c, ('cas foo 0 0 6 %s\r\nbarva2\r\n'):format(cas), 'STORED\r\n')
    -- Reusing the same unique value.
    check(c, ('cas foo 0 0 6 %s\r\nbarva2\r\n'):format(cas), 'EXISTS\r\n')
    check(c, 'delete foo\r\n', 'DELETED\r\n')
    check(c, ('cas foo 0 0 6 %s\r\nbarva2\r\n'):format(cas), 'NOT_FOUND\r\n')

    check(c, 'set foo1 0 0 1\r\n1\r\n', 'STORED\r\n')
    check(c, 'set foo2 0 0 1\r\n2\r\n', 'STORED\r\n')
    t.assert_not_equals(cas_of(c, 'foo1'), cas_of(c, 'foo2'))

    -- Two clients race with the same unique value: one wins.
    local c2 = connect(cg)
    local cas1 = cas_of(c, 'foo1')
    local cas2 = cas_of(c2, 'foo1')
    t.assert_equals(cas1, cas2)
    c:send(('cas foo1 0 0 6 %s\r\nbarva2\r\n'):format(cas1))
    c2:send(('cas foo1 0 0 4 %s\r\npear\r\n'):format(cas2))
    local replies = {c:line(), c2:line()}
    table.sort(replies)
    t.assert_equals(replies, {'EXISTS\r\n', 'STORED\r\n'})

    -- incr changes the unique value.
    check(c, 'set bug15 0 0 1\r\n0\r\n', 'STORED\r\n')
    local before = cas_of(c, 'bug15')
    check(c, 'incr bug15 1\r\n', '1\r\n')
    t.assert_not_equals(cas_of(c, 'bug15'), before)

    -- Every store gives a bigger unique value; they are 64-bit.
    local prev = tonumber64(cas_of(c, 'bug15'))
    for _ = 1, 5 do
        check(c, 'set bug15 0 0 1\r\n0\r\n', 'STORED\r\n')
        local cur = tonumber64(cas_of(c, 'bug15'))
        t.assert(cur > prev)
        prev = cur
    end
end

g.test_incrdecr = function(cg)
    local c = cg.c
    -- A big value.
    check(c, 'set bug21 0 0 19\r\n9223372036854775807\r\n', 'STORED\r\n')
    check(c, 'incr bug21 1\r\n', '9223372036854775808\r\n')
    check(c, 'incr bug21 1\r\n', '9223372036854775809\r\n')
    check(c, 'decr bug21 1\r\n', '9223372036854775808\r\n')
    -- Underflow protection.
    check(c, 'set num 0 0 1\r\n1\r\n', 'STORED\r\n')
    check(c, 'incr num 1\r\n', '2\r\n')
    check(c, 'incr num 8\r\n', '10\r\n')
    check(c, 'decr num 1\r\n', '9\r\n')
    -- As memcached: a shorter number is padded with spaces to the old
    -- length; a longer one has its own.
    check(c, 'get num\r\n', 'VALUE num 0 2\r\n9 \r\nEND\r\n')
    check(c, 'decr num 9\r\n', '0\r\n')
    check(c, 'decr num 5\r\n', '0\r\n')
    check(c, 'get num\r\n', 'VALUE num 0 2\r\n0 \r\nEND\r\n')
    check(c, 'incr num 100\r\n', '100\r\n')
    check(c, 'get num\r\n', 'VALUE num 0 3\r\n100\r\nEND\r\n')
    -- A read earlier in the batch holds the item (memcached's refcount): a
    -- new value of its own length, until an mn writes the replies out.
    check(
        c,
        'get num\r\ndecr num 1\r\nget num\r\n',
        'VALUE num 0 3\r\n100\r\nEND\r\n99\r\nVALUE num 0 2\r\n99\r\nEND\r\n'
    )
    check(
        c,
        'set num 0 0 3\r\n100\r\nget num\r\nmn\r\ndecr num 1\r\n',
        'STORED\r\nVALUE num 0 3\r\n100\r\nEND\r\nMN\r\n99\r\n'
    )
    check(
        c,
        'touch num 0\r\ndecr num 1\r\nget num\r\n',
        'TOUCHED\r\n98\r\nVALUE num 0 3\r\n98 \r\nEND\r\n'
    )
    -- A 32-bit value.
    check(c, 'set num 0 0 10\r\n4294967296\r\n', 'STORED\r\n')
    check(c, 'incr num 1\r\n', '4294967297\r\n')
    -- incr wraps around 2^64, as in memcached.
    check(c, 'set num 0 0 20\r\n18446744073709551615\r\n', 'STORED\r\n')
    check(c, 'incr num 1\r\n', '0\r\n')
    check(c, 'set num 0 0 20\r\n18446744073709551615\r\n', 'STORED\r\n')
    check(c, 'incr num 3\r\n', '2\r\n')
    -- Missing keys.
    check(c, 'decr bogus 1\r\n', 'NOT_FOUND\r\n')
    check(c, 'decr incr 1\r\n', 'NOT_FOUND\r\n')
    -- A big increment.
    check(c, 'set bigincr 0 0 1\r\n0\r\n', 'STORED\r\n')
    check(
        c,
        'incr bigincr 18446744073709551610\r\n',
        '18446744073709551610\r\n'
    )
    -- Non-numeric values.
    local NON_NUMERIC = 'CLIENT_ERROR cannot increment or decrement '
        .. 'non-numeric value\r\n'
    check(c, 'set text 0 0 2\r\nhi\r\n', 'STORED\r\n')
    check(c, 'incr text 1\r\n', NON_NUMERIC)
    check(c, 'set empty 0 0 0\r\n\r\n', 'STORED\r\n')
    check(c, 'decr empty 1\r\n', NON_NUMERIC)
    check(c, 'set huge 0 0 20\r\n18446744073709551616\r\n', 'STORED\r\n')
    check(c, 'incr huge 1\r\n', NON_NUMERIC)
    -- Bad deltas.
    check(
        c,
        'incr num x\r\n',
        'CLIENT_ERROR invalid numeric delta argument\r\n'
    )
    check(
        c,
        'incr num -1\r\n',
        'CLIENT_ERROR invalid numeric delta argument\r\n'
    )
    -- Flags and TTL are kept.
    check(c, 'set kept 42 1000 1\r\n5\r\n', 'STORED\r\n')
    check(c, 'incr kept 5\r\n', '10\r\n')
    check(c, 'get kept\r\n', 'VALUE kept 42 2\r\n10\r\nEND\r\n')
    local t_ = box.space[SPACE]:get('kept')
    t.assert_gt(t_.exptime, fiber.time() + 900)
end

g.test_expirations = function(cg)
    local c = cg.c
    -- Relative: after 1 second.
    check(c, 'set foo 0 1 6\r\nfooval\r\n', 'STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 0 6\r\nfooval\r\nEND\r\n')
    t.helpers.retrying({timeout = 3}, function()
        t.assert_equals(retrieve(c, 'get foo\r\n'), 'END\r\n')
    end)
    -- Absolute, in the past.
    local now = math.floor(fiber.time())
    check(c, ('set foo 0 %d 6\r\nfooval\r\n'):format(now - 1), 'STORED\r\n')
    check(c, 'get foo\r\n', 'END\r\n')
    check(c, ('set boo 0 %d 6\r\nbooval\r\n'):format(now - 20), 'STORED\r\n')
    check(c, 'get boo\r\n', 'END\r\n')
    -- Absolute, in the future.
    check(c, ('set foo 0 %d 6\r\nfooval\r\n'):format(now + 2), 'STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 0 6\r\nfooval\r\nEND\r\n')
    t.helpers.retrying({timeout = 4}, function()
        t.assert_equals(retrieve(c, 'get foo\r\n'), 'END\r\n')
    end)
    -- Negative: expired at once, the old value is gone.
    check(c, 'set neg 0 0 1\r\nx\r\n', 'STORED\r\n')
    check(c, 'set neg 0 -1 1\r\ny\r\n', 'STORED\r\n')
    check(c, 'get neg\r\n', 'END\r\n')
    -- add succeeds once the item expires.
    check(c, 'add add 0 1 6\r\naddval\r\n', 'STORED\r\n')
    check(c, 'add add 0 1 7\r\naddval1\r\n', 'NOT_STORED\r\n')
    t.helpers.retrying({timeout = 3}, function()
        t.assert_equals(
            c:call('add add 0 1 7\r\naddval2\r\n'),
            'STORED\r\n'
        )
    end)
end

g.test_sweeper = function(cg)
    local c = cg.c
    local space = box.space[SPACE]
    check(c, 'set forever 0 0 1\r\nx\r\n', 'STORED\r\n')
    for i = 1, 250 do
        check(c, ('set k%d 0 1 1\r\nx\r\n'):format(i), 'STORED\r\n')
    end
    -- The sweeper deletes expired items without anyone touching them.
    -- Deletions are visible before they commit; the counter is updated
    -- after the commit.
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(space:len(), 1)
        t.assert_ge(cg.inst:info().expired, 250)
    end)

    -- A read-only instance doesn't delete; dead items aren't returned.
    check(c, 'add key 0 1 3\r\nfoo\r\n', 'STORED\r\n')
    box.cfg {read_only = true}
    fiber.sleep(1.5)
    check(c, 'get key\r\n', 'END\r\n')
    t.assert_equals(space:len(), 2)
    box.cfg {read_only = false}
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(space:len(), 1)
    end)
end

g.test_touch_gat = function(cg)
    local c = cg.c
    check(c, 'set foo 3 100 3\r\nbar\r\n', 'STORED\r\n')
    check(c, 'touch foo 1\r\n', 'TOUCHED\r\n')
    check(c, 'touch nope 1\r\n', 'NOT_FOUND\r\n')
    check(c, 'touch foo 1 noreply\r\n', '')
    check(c, 'touch foo x\r\n', 'CLIENT_ERROR invalid exptime argument\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 3 3\r\nbar\r\nEND\r\n')
    t.helpers.retrying({timeout = 3}, function()
        t.assert_equals(retrieve(c, 'get foo\r\n'), 'END\r\n')
    end)
    -- gat returns the value and sets the TTL; gats adds the CAS.
    check(c, 'set foo 3 1 3\r\nbar\r\n', 'STORED\r\n')
    check(c, 'set baz 0 0 1\r\nz\r\n', 'STORED\r\n')
    check(
        c,
        'gat 0 foo nope baz\r\n',
        'VALUE foo 3 3\r\nbar\r\nVALUE baz 0 1\r\nz\r\nEND\r\n'
    )
    t.assert_equals(box.space[SPACE]:get('foo').exptime, 0)
    fiber.sleep(1.1)
    check(c, 'get foo\r\n', 'VALUE foo 3 3\r\nbar\r\nEND\r\n')
    local cas = cas_of(c, 'foo')
    check(
        c,
        'gats 100 foo\r\n',
        ('VALUE foo 3 3 %s\r\nbar\r\nEND\r\n'):format(cas)
    )
    -- touch doesn't change the CAS.
    t.assert_equals(cas_of(c, 'foo'), cas)
    -- A negative TTL: the value is returned and expires at once.
    check(c, 'gat -1 foo\r\n', 'VALUE foo 3 3\r\nbar\r\nEND\r\n')
    check(c, 'get foo\r\n', 'END\r\n')
    check(c, 'gat x foo\r\n', 'CLIENT_ERROR invalid exptime argument\r\n')
    check(c, 'gat 10\r\n', 'CLIENT_ERROR bad command line format\r\n')
end

g.test_append_prepend = function(cg)
    local c = cg.c
    check(c, 'append foo 0 0 1\r\nx\r\n', 'NOT_STORED\r\n')
    check(c, 'prepend foo 0 0 1\r\nx\r\n', 'NOT_STORED\r\n')
    check(c, 'set foo 7 100 3\r\nbar\r\n', 'STORED\r\n')
    -- The flags and the TTL of the command are ignored.
    check(c, 'append foo 1 1 3\r\nbaz\r\n', 'STORED\r\n')
    check(c, 'prepend foo 2 1 3\r\nfoo\r\n', 'STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 7 9\r\nfoobarbaz\r\nEND\r\n')
    t.assert_gt(box.space[SPACE]:get('foo').exptime, fiber.time() + 50)
    check(c, 'append foo 0 0 0\r\n\r\n', 'STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 7 9\r\nfoobarbaz\r\nEND\r\n')
end

g.test_flags = function(cg)
    local c = cg.c
    for _, flags in ipairs({0, 0x7b, 0xffff, 0xffffffff}) do
        check(c, ('set foo %d 0 6\r\nfooval\r\n'):format(flags), 'STORED\r\n')
        local value, ret_flags = c:get('foo')
        t.assert_equals(value, 'fooval')
        t.assert_equals(ret_flags, flags)
    end
    -- Truncated to 32 bits, as memcached does.
    check(c, 'set foo 4294967297 0 6\r\nfooval\r\n', 'STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 1 6\r\nfooval\r\nEND\r\n')
    check(
        c,
        'set foo 18446744073709551616 0 6\r\nfooval\r\n',
        'CLIENT_ERROR bad command line format\r\n'
    )
    check(c, 'flush_all\r\n', 'OK\r\n')
end

g.test_flush_all = function(cg)
    local c = cg.c
    local space = box.space[SPACE]
    check(c, 'set foo 0 0 6\r\nfooval\r\n', 'STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 0 6\r\nfooval\r\nEND\r\n')
    check(c, 'flush_all\r\n', 'OK\r\n')
    check(c, 'get foo\r\n', 'END\r\n')
    -- An item set right after the flush is not flushed, even in the same
    -- batch.
    check(c, 'set foo 0 0 3\r\nnew\r\n', 'STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 0 3\r\nnew\r\nEND\r\n')
    check(
        c,
        'set bar 0 0 1\r\nx\r\nflush_all\r\nset foo 0 0 3\r\nnew\r\n'
            .. 'get foo bar\r\n',
        'STORED\r\nOK\r\nSTORED\r\nVALUE foo 0 3\r\nnew\r\nEND\r\n'
    )
    -- The flushed items are deleted by the sweeper.
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(space:len(), 1)
    end)

    -- A delayed flush, as an absolute time: everything stored before it is
    -- gone once the time comes.
    local when = math.floor(fiber.time()) + 2
    check(c, ('flush_all %d\r\n'):format(when), 'OK\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 0 3\r\nnew\r\nEND\r\n')
    check(c, 'set foo 0 0 3\r\n123\r\n', 'STORED\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 0 3\r\n123\r\nEND\r\n')
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(space:len(), 0)
    end)
    check(c, 'get foo\r\n', 'END\r\n')
    -- The flush time is persisted.
    t.assert_equals(box.space._mcc_meta:get('flush:' .. SPACE)[2], when)

    -- A relative delay.
    check(c, 'set foo 0 0 1\r\nx\r\n', 'STORED\r\n')
    check(c, 'flush_all 1\r\n', 'OK\r\n')
    check(c, 'get foo\r\n', 'VALUE foo 0 1\r\nx\r\nEND\r\n')
    t.helpers.retrying({timeout = 4}, function()
        t.assert_equals(retrieve(c, 'get foo\r\n'), 'END\r\n')
    end)
    check(c, 'flush_all x\r\n', 'CLIENT_ERROR invalid exptime argument\r\n')
end

g.test_noreply = function(cg)
    local c = cg.c
    check(c, 'flush_all noreply\r\n', '')
    check(c, 'flush_all 0 noreply\r\n', '')
    check(c, 'get noreply:foo\r\n', 'END\r\n')
    check(c, 'add noreply:foo 0 0 1 noreply\r\n1\r\n', '')
    check(c, 'get noreply:foo\r\n', 'VALUE noreply:foo 0 1\r\n1\r\nEND\r\n')
    check(c, 'set noreply:foo 0 0 1 noreply\r\n2\r\n', '')
    check(c, 'get noreply:foo\r\n', 'VALUE noreply:foo 0 1\r\n2\r\nEND\r\n')
    check(c, 'replace noreply:foo 0 0 1 noreply\r\n3\r\n', '')
    check(c, 'get noreply:foo\r\n', 'VALUE noreply:foo 0 1\r\n3\r\nEND\r\n')
    check(c, 'append noreply:foo 0 0 1 noreply\r\n4\r\n', '')
    check(c, 'get noreply:foo\r\n', 'VALUE noreply:foo 0 2\r\n34\r\nEND\r\n')
    check(c, 'prepend noreply:foo 0 0 1 noreply\r\n5\r\n', '')
    check(
        c,
        'get noreply:foo\r\n',
        'VALUE noreply:foo 0 3\r\n534\r\nEND\r\n'
    )
    local cas = cas_of(c, 'noreply:foo')
    check(c, ('cas noreply:foo 0 0 1 %s noreply\r\n6\r\n'):format(cas), '')
    check(c, 'get noreply:foo\r\n', 'VALUE noreply:foo 0 1\r\n6\r\nEND\r\n')
    check(c, 'incr noreply:foo 3 noreply\r\n', '')
    check(c, 'get noreply:foo\r\n', 'VALUE noreply:foo 0 1\r\n9\r\nEND\r\n')
    check(c, 'decr noreply:foo 2 noreply\r\n', '')
    check(c, 'get noreply:foo\r\n', 'VALUE noreply:foo 0 1\r\n7\r\nEND\r\n')
    check(c, 'delete noreply:foo noreply\r\n', '')
    check(c, 'get noreply:foo\r\n', 'END\r\n')
    -- Failures and errors are quiet too, as in memcached.
    check(c, 'add noreply:foo 0 0 1 noreply\r\n1\r\n', '')
    check(c, 'add noreply:foo 0 0 1 noreply\r\n2\r\n', '')
    check(c, 'incr noreply:foo x noreply\r\n', '')
    -- A bad data chunk: 'x' and two bytes instead of \r\n.
    check(c, 'set noreply:foo 0 0 1 noreply\r\nxyz', '')
    check(c, 'verbosity 1 noreply\r\n', '')
    check(c, 'delete noreply:foo 0 noreply\r\n', '')
    check(c, 'get noreply:foo\r\n', 'END\r\n')
end

g.test_multiversioning = function(cg)
    local size = 256 * 1024
    local lower = string.rep('0123456789abcdef', size / 16)
    local upper = lower:upper()
    local c1 = cg.c
    local c2 = connect(cg)
    check(c1, ('set big 0 0 %d\r\n%s\r\n'):format(size, lower), 'STORED\r\n')
    -- A get in flight sees the value it started with.
    c1:send('get big\r\n')
    fiber.sleep(0.01)
    check(c2, 'delete big\r\n', 'DELETED\r\n')
    check(c2, ('set big 0 0 %d\r\n%s\r\n'):format(size, upper), 'STORED\r\n')
    local _, items = c1:read_values()
    t.assert_equals(#items, 1)
    t.assert(items[1].value == lower, 'the old value')
    t.assert(c2:get('big') == upper, 'the new value')
end

g.test_key_validation = function(cg)
    local c = cg.c
    local FORMAT = 'CLIENT_ERROR bad command line format\r\n'
    local key = string.rep('k', 250)
    check(c, ('set %s 0 0 1\r\nx\r\n'):format(key), 'STORED\r\n')
    check(
        c,
        ('get %s\r\n'):format(key),
        ('VALUE %s 0 1\r\nx\r\nEND\r\n'):format(key)
    )
    local long = key .. 'k'
    check(c, ('get %s\r\n'):format(long), FORMAT)
    check(c, ('get a %s\r\n'):format(long), FORMAT)
    check(c, ('delete %s\r\n'):format(long), FORMAT)
    check(c, ('incr %s 1\r\n'):format(long), FORMAT)
    check(c, ('touch %s 1\r\n'):format(long), FORMAT)
    -- The data block of a rejected set is swallowed (memcached 1.6.45).
    check(c, ('set %s 0 0 1\r\nx\r\n'):format(long), FORMAT)
    -- Control characters are fine, as in memcached.
    check(c, 'get a\tb\r\n', 'END\r\n')
    check(c, 'set a\1b 0 0 1\r\nx\r\n', 'STORED\r\n')
    check(c, 'get a\1b\r\n', 'VALUE a\1b 0 1\r\nx\r\nEND\r\n')
    check(c, 'set a\127b 0 0 1\r\nx\r\n', 'STORED\r\n')
    check(c, 'get a\127b\r\n', 'VALUE a\127b 0 1\r\nx\r\nEND\r\n')
    -- Any other byte is fine.
    check(c, 'set \xff\xfe 0 0 1\r\nx\r\n', 'STORED\r\n')
    check(c, 'get \xff\xfe\r\n', 'VALUE \xff\xfe 0 1\r\nx\r\nEND\r\n')
end

g.test_bad_data_chunk = function(cg)
    local c = cg.c
    -- The value is longer than announced: the announced bytes plus two are
    -- consumed, the rest is parsed as a command.
    -- "\r\n" alone is a short line (memcached 1.6.45).
    check(
        c,
        'set foo 0 0 3\r\nabcdefg\r\n',
        'CLIENT_ERROR bad data chunk\r\nERROR\r\n'
    )
    check(c, 'get foo\r\n', 'END\r\n')
    check(
        c,
        'set foo 0 0 3\r\nabcde\r\n',
        'CLIENT_ERROR bad data chunk\r\nCLIENT_ERROR bad command line format\r\n'
    )
    check(
        c,
        'set foo 0 0 3\r\nabc\n\r\n',
        'CLIENT_ERROR bad data chunk\r\nCLIENT_ERROR bad command line format\r\n'
    )
    check(c, 'set foo 0 0 3\r\nabc\r\n', 'STORED\r\n')
end

-- append/prepend: the joined item must fit max_item_size, as in memcached
-- (do_store_item); otherwise NOT_STORED (meta NS) and the old item stays.
g.test_append_over_limit = function(cg)
    local c = cg.c
    local base = 1024 * 1024 - 2000
    check(
        c,
        ('set big 0 0 %d\r\n%s\r\n'):format(base, ('v'):rep(base)),
        'STORED\r\n'
    )
    for _, cmd in ipairs({'append', 'prepend'}) do
        check(
            c,
            ('%s big 0 0 3000\r\n%s\r\n'):format(cmd, ('a'):rep(3000)),
            'NOT_STORED\r\n'
        )
    end
    check(c, ('ms big 3000 MA\r\n%s\r\n'):format(('a'):rep(3000)), 'NS\r\n')
    check(c, 'mg big s\r\n', ('HD s%d\r\n'):format(base))
    check(
        c,
        ('append big 0 0 500\r\n%s\r\n'):format(('a'):rep(500)),
        'STORED\r\n'
    )
    check(c, 'mg big s\r\n', ('HD s%d\r\n'):format(base + 500))
end

g.test_meta_ms_too_large_removes_old_value = function(cg)
    local c = cg.c
    -- 10 bytes with a 3-byte key (proto/text.lua item_size()).
    cg.inst:cfg({max_item_size = 72})
    -- As memcached: a meta store that doesn't fit removes the old value in
    -- any mode, and the data block is swallowed even when it comes later.
    check(c, 'set foo 0 0 3\r\nabc\r\n', 'STORED\r\n')
    c:send('ms foo 11 MA q\r\n012345')
    fiber.sleep(0.05)
    check(
        c,
        '6789a\r\nmg foo v\r\n',
        'SERVER_ERROR object too large for cache\r\nEN\r\n'
    )
    cg.inst:cfg({max_item_size = 1048576})
end

g.test_max_item_size = function(cg)
    local c = cg.c
    -- The item size as memcached counts it: 10 bytes with a 3-byte key.
    cg.inst:cfg({max_item_size = 72})
    check(c, 'set foo 0 0 10\r\n0123456789\r\n', 'STORED\r\n')
    -- The data is swallowed, even when it comes later.
    c:send('set foo 0 0 11\r\n012345')
    fiber.sleep(0.05)
    check(
        c,
        '6789a\r\nget foo\r\n',
        'SERVER_ERROR object too large for cache\r\nEND\r\n'
    )
    check(
        c,
        'set foo 0 0 12 noreply\r\n0123456789ab\r\nget foo\r\n',
        'END\r\n'
    )
    cg.inst:cfg({max_item_size = 1048576})
    check(c, 'set foo 0 0 11\r\n0123456789a\r\n', 'STORED\r\n')
end

g.test_split_input = function(cg)
    local c = cg.c
    -- A command arriving byte by byte.
    local cmd = 'set foo 0 0 5\r\nhello\r\nget foo\r\n'
    for i = 1, #cmd do
        c:send(cmd:sub(i, i))
        if i % 7 == 0 then
            fiber.sleep(0.001)
        end
    end
    t.assert_equals(
        c:read(#'STORED\r\nVALUE foo 0 5\r\nhello\r\nEND\r\n'),
        'STORED\r\nVALUE foo 0 5\r\nhello\r\nEND\r\n'
    )
    -- A big value in chunks.
    local value = string.rep('0123456789', 100000)
    c:send(('set big 0 0 %d\r\n'):format(#value))
    local chunk = 4096
    for i = 1, #value, chunk do
        c:send(value:sub(i, i + chunk - 1))
        if i % (chunk * 16) == 1 then
            fiber.sleep(0.001)
        end
    end
    check(c, '\r\n', 'STORED\r\n')
    t.assert(c:get('big') == value)
end

g.test_pipelining = function(cg)
    local c = cg.c
    local request, reply = {}, {}
    for i = 1, 2000 do
        table.insert(
            request,
            ('set k%d 0 0 %d\r\n%d\r\n'):format(
                i,
                #tostring(i),
                i
            )
        )
        table.insert(reply, 'STORED\r\n')
        table.insert(request, ('get k%d\r\n'):format(i))
        table.insert(
            reply,
            ('VALUE k%d 0 %d\r\n%d\r\nEND\r\n'):format(
                i,
                #tostring(i),
                i
            )
        )
        if i % 100 == 0 then
            table.insert(request, 'bogus\r\n')
            table.insert(reply, 'ERROR\r\n')
        end
    end
    check(c, table.concat(request), table.concat(reply))
    -- A multi-get of many keys.
    local keys = {}
    for i = 1, 2000 do
        table.insert(keys, 'k' .. i)
    end
    c:send('get ' .. table.concat(keys, ' ') .. '\r\n')
    local _, items = c:read_values()
    t.assert_equals(#items, 2000)
    t.assert_equals(items[2000].value, '2000')
end

-- limit_maxbytes is the memory limit of the space's engine.
g.test_stats_limit_maxbytes = function(cg)
    local function maxbytes(c)
        local reply = c:call('stats\r\n', 'END\r\n')
        return tonumber(reply:match('STAT limit_maxbytes (%d+)\r\n'))
    end
    t.assert_equals(maxbytes(cg.c), box.cfg.memtx_memory)
    local port = env.free_port()
    local inst = cg.mcc.create('text_disk', {
        listen = '127.0.0.1:' .. port,
        storage = 'disk',
    })
    local c = client.must_connect(port)
    local ok, err = pcall(function()
        t.assert_not_equals(box.cfg.vinyl_memory, box.cfg.memtx_memory)
        t.assert_equals(maxbytes(c), box.cfg.vinyl_memory)
    end)
    c:close()
    inst:destroy()
    env.drop_spaces({'__mcc_text_disk'})
    if not ok then
        error(err, 0)
    end
end

g.test_stats = function(cg)
    local c = cg.c
    check(c, 'stats reset\r\n', 'RESET\r\n')
    check(
        c,
        'set a 0 0 1\r\nx\r\nget a b\r\n',
        'STORED\r\nVALUE a 0 1\r\nx\r\nEND\r\n'
    )
    check(
        c, 'gets a\r\ndelete b\r\nincr a 1\r\ntouch a 0\r\n',
        ('VALUE a 0 1 %s\r\nx\r\nEND\r\nNOT_FOUND\r\n'
            .. 'CLIENT_ERROR cannot increment or decrement non-numeric '
            .. 'value\r\nTOUCHED\r\n'):format(
                tostring(
                    box.space[SPACE]:get('a').cas
                ):gsub('ULL', '')
            )
    )
    c:send('stats\r\n')
    local reply = c:read_until('END\r\n')
    local stats = {}
    for name, value in reply:gmatch('STAT (%S+) ([^\r]*)\r\n') do
        stats[name] = value
    end
    t.assert_equals(stats.pid, tostring(box.info.pid))
    t.assert_str_matches(stats.version, '1%.6%.%d+%-mcc%-.*')
    t.assert_equals(stats.cmd_get, '3')
    t.assert_equals(stats.get_hits, '2')
    t.assert_equals(stats.get_misses, '1')
    t.assert_equals(stats.cmd_set, '1')
    t.assert_equals(stats.delete_misses, '1')
    t.assert_equals(stats.cmd_touch, '1')
    t.assert_equals(stats.touch_hits, '1')
    t.assert_equals(stats.curr_items, '1')
    t.assert_equals(stats.total_items, '1')
    t.assert_equals(stats.curr_connections, '1')
    t.assert_equals(stats.max_connections, '4096')
    for _, name in ipairs({
        'uptime',
        'time',
        'bytes',
        'bytes_read',
        'bytes_written',
        'total_connections',
        'limit_maxbytes',
        'threads',
        'pointer_size',
    }) do
        t.assert_str_matches(stats[name], '%d+', nil, nil, name)
    end

    c:send('stats settings\r\n')
    reply = c:read_until('END\r\n')
    t.assert_str_contains(reply, 'STAT item_size_max 1048576\r\n')
    t.assert_str_contains(reply, 'STAT maxconns 4096\r\n')
    t.assert_str_contains(reply, ('STAT tcpport %d\r\n'):format(cg.port))

    check(c, 'stats reset\r\n', 'RESET\r\n')
    c:send('stats\r\n')
    reply = c:read_until('END\r\n')
    t.assert_str_contains(reply, 'STAT cmd_get 0\r\n')
    check(c, 'stats items\r\n', 'END\r\n')
    check(c, 'stats slabs\r\n', 'END\r\n')
    check(c, 'stats bogus\r\n', 'ERROR\r\n')
    -- stats detail: memcached's replies with detail off.
    local usage = 'CLIENT_ERROR usage: stats detail on|off|dump\r\n'
    check(c, 'stats detail\r\n', usage)
    check(c, 'stats detail x\r\n', usage)
    check(c, 'stats detail onx\r\n', usage)
    check(c, 'stats detail on\r\n', 'OK\r\n')
    check(c, 'stats detail o\r\n', 'OK\r\n')
    check(c, 'stats detail of\r\n', 'OK\r\n')
    check(c, 'stats detail off\r\n', 'OK\r\n')
    check(c, 'stats detail dump\r\n', 'END\r\n')
    check(c, 'stats detail d\r\n', 'END\r\n')
end

-- As memcached without -o track_sizes.
g.test_stats_sizes = function(cg)
    check(cg.c, 'stats sizes\r\n', 'STAT sizes_status disabled\r\nEND\r\n')
end

-- "stats conns": id -> {field = value}, and the ids in reply order.
local function stats_conns(c)
    local reply = c:call('stats conns\r\n', 'END\r\n')
    local conns, order = {}, {}
    for id, field, value in reply:gmatch('STAT (%d+):(%S+) ([^\r]*)\r\n') do
        id = tonumber(id)
        if conns[id] == nil then
            conns[id] = {}
            table.insert(order, id)
        end
        conns[id][field] = value
    end
    t.assert_str_matches(reply, '^STAT .*END\r\n$')
    return conns, order
end

-- The connection of a client in stats_conns() by its local port.
local function conn_of(conns, c)
    local addr = 'tcp:127.0.0.1:' .. c.sock:name().port
    for id, conn in pairs(conns) do
        if conn.addr == addr then
            return conn, id
        end
    end
    t.fail('no connection ' .. addr)
end

-- The listener and the connections, with memcached's fields and states.
g.test_stats_conns = function(cg)
    local c = cg.c
    -- The connections of the tests before are gone.
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(cg.inst:info().curr_connections, 1)
    end)
    t.assert_equals(c:set('big', string.rep('v', 512 * 1024)), 'STORED\r\n')
    -- Accepted, no command yet.
    local fresh = connect(cg)
    -- After a command.
    local idle = connect(cg)
    t.assert_equals(idle:set('a', 'x'), 'STORED\r\n')
    -- The start of a command line.
    local partial = connect(cg)
    partial:send('get fo')
    -- Inside a value.
    local value = connect(cg)
    value:send('set v 0 0 10\r\nabc')
    -- Discarding a too large value.
    local swallow = connect(cg)
    swallow:send('set huge 0 0 2000000\r\nabc')
    -- Replies the client doesn't read.
    local writer = connect(cg)
    writer:send(string.rep('get big\r\n', 64))
    fiber.sleep(0.3)

    local conns, order = stats_conns(c)
    local sorted = {unpack(order)}
    table.sort(sorted)
    t.assert_equals(order, sorted, 'by id')
    t.assert_equals(#order, 8)
    local listen = 'tcp:127.0.0.1:' .. cg.port
    -- The ids are the fds.
    local srv = cg.inst.server
    local lid = srv.listener:fd()
    t.assert_str_matches(conns[lid].secs_since_last_cmd, '%d+')
    conns[lid].secs_since_last_cmd = nil
    t.assert_equals(conns[lid], {addr = listen, state = 'conn_listening'})
    local function expect(client_, state)
        local conn, id = conn_of(conns, client_)
        t.assert_equals(conn.listen_addr, listen)
        t.assert_equals(conn.state, state, conn.addr)
        t.assert_equals(conn.secs_since_last_cmd, '0')
        local fd
        for _, sc in pairs(srv.conns) do
            if sc.sock:peer().port == client_.sock:name().port then
                fd = sc.sock:fd()
            end
        end
        t.assert_equals(id, fd)
    end
    expect(c, 'conn_parse_cmd')
    expect(fresh, 'conn_new_cmd')
    expect(idle, 'conn_read')
    expect(partial, 'conn_read')
    expect(value, 'conn_nread')
    expect(swallow, 'conn_swallow')
    expect(writer, 'conn_mwrite')

    -- secs_since_last_cmd counts from the last command.
    fiber.sleep(1.1)
    t.assert_equals(idle:call('version\r\n'):match('^VERSION'), 'VERSION')
    conns = stats_conns(c)
    t.assert_equals(conn_of(conns, idle).secs_since_last_cmd, '0')
    t.assert_equals(conn_of(conns, fresh).secs_since_last_cmd, '1')
    t.assert_equals(conn_of(conns, c).secs_since_last_cmd, '0')
    -- Over a unix socket: memcached shows no peer address.
    local path = fio.pathjoin(fio.tempdir(), 'mcc.sock')
    local inst = cg.mcc.create('text_unix', {listen = 'unix/:' .. path})
    local ok, err = pcall(function()
        local s = socket.tcp_connect('unix/', path, 1)
        t.assert(s)
        local u = setmetatable({sock = s}, getmetatable(c))
        local list = stats_conns(u)
        u:close()
        local states = {}
        for _, conn in pairs(list) do
            if conn.state == 'conn_listening' then
                t.assert_equals(conn.addr, 'unix:' .. path)
                t.assert_equals(conn.listen_addr, nil)
            else
                t.assert_equals(conn.addr, '?:<AF 0>')
                t.assert_equals(conn.listen_addr, 'unix:' .. path)
            end
            table.insert(states, conn.state)
        end
        table.sort(states)
        t.assert_equals(states, {'conn_listening', 'conn_parse_cmd'})
    end)
    inst:destroy()
    env.drop_spaces({'__mcc_text_unix'})
    if not ok then
        error(err, 0)
    end
end

g.test_version_verbosity = function(cg)
    local c = cg.c
    local reply = c:call('version\r\n')
    t.assert_str_matches(reply, 'VERSION 1%.6%.%d+%-mcc%-%S+\r\n')
    check(c, 'verbosity 1\r\n', 'OK\r\n')
    check(c, 'verbosity\r\n', 'CLIENT_ERROR bad command line format\r\n')
    check(c, 'verbosity x\r\n', 'CLIENT_ERROR bad command line format\r\n')
    check(c, 'verbosity 0\r\n', 'OK\r\n')
end

g.test_quit = function(cg)
    local c = cg.c
    -- The replies before quit are sent, the commands after it are not run.
    check(c, 'set a 0 0 1\r\nx\r\nquit\r\nset b 0 0 1\r\ny\r\n', 'STORED\r\n')
    t.assert(c:is_closed())
    local c2 = connect(cg)
    check(c2, 'get a b\r\n', 'VALUE a 0 1\r\nx\r\nEND\r\n')
    check(c2, 'quit\r\n', '')
    t.assert(c2:is_closed())
    t.helpers.retrying({}, function()
        t.assert_equals(cg.inst:info().curr_connections, 0)
    end)
end

g.test_long_line = function(cg)
    local c = cg.c
    -- No newline in 2048 bytes: memcached closes the connection, without a
    -- reply.
    c:send(string.rep('x', 4096))
    t.assert(c:is_closed())
    -- A long multi-get is fine.
    local c2 = connect(cg)
    local keys = string.rep(' key', 1000)
    c2:send('get' .. keys)
    fiber.sleep(0.01)
    check(c2, '\r\n', 'END\r\n')
end

g.test_meta_dispatch = function(cg)
    local c = cg.c
    local has_meta = pcall(require, 'memcached.proto.meta')
    -- Meta commands go to proto/meta.lua when it exists.
    check(c, 'mn\r\n', has_meta and 'MN\r\n' or 'ERROR\r\n')
    -- Not meta commands.
    check(c, 'mnx\r\n', 'ERROR\r\n')
    check(c, 'mx foo\r\n', 'ERROR\r\n')
    check(c, 'MN\r\n', 'ERROR\r\n')
    if has_meta then
        -- Both protocols in one batch, in order.
        check(
            c,
            'set foo 0 0 3\r\nbar\r\nmg foo v\r\nget foo\r\nmn\r\n',
            'STORED\r\nVA 3\r\nbar\r\nVALUE foo 0 3\r\nbar\r\nEND\r\nMN\r\n'
        )
    end
end

g.test_meta_invalidate_peek_win = function(cg)
    t.skip_if(
        not pcall(require, 'memcached.proto.meta'),
        'proto/meta.lua is not available'
    )
    local c = cg.c
    check(c, 'set foo 0 0 3\r\nbar\r\n', 'STORED\r\n')
    local cas = cas_of(c, 'foo')
    -- md I marks the item stale and bumps the CAS.
    check(c, 'md foo I\r\n', 'HD\r\n')
    local stale_cas = tostring(box.space[SPACE]:get('foo').cas):gsub(
        'ULL',
        ''
    )
    t.assert_not_equals(stale_cas, cas)
    -- me has no side effects: it doesn't take the win token.
    local me = c:line()
    t.assert_equals(me, nil)
    c:send('me foo\r\n')
    me = c:line()
    t.assert_str_matches(
        me,
        ('ME foo exp=%%-1 la=%%d+ cas=%s fetch=%%a+ cls=1 size=%%d+\r\n'):format(
            stale_cas
        )
    )
    -- The first mg of a stale item wins the recache: X W.
    check(
        c,
        'mg foo v c t\r\n',
        ('VA 3 c%s t-1 X W\r\nbar\r\n'):format(stale_cas)
    )
    -- The next ones see that someone has won: Z X.
    check(
        c,
        'mg foo v c t\r\n',
        ('VA 3 c%s t-1 Z X\r\nbar\r\n'):format(stale_cas)
    )
    check(c, 'me foo\r\n', me)
    check(c, 'mg foo c\r\n', ('HD c%s Z X\r\n'):format(stale_cas))
    -- A new value clears both.
    check(c, 'set foo 0 0 3\r\nnew\r\n', 'STORED\r\n')
    local new_cas = cas_of(c, 'foo')
    check(c, 'mg foo v c t\r\n', ('VA 3 c%s t-1\r\nnew\r\n'):format(new_cas))
end

g.test_commit_error = function(cg)
    local c = cg.c
    local space = box.space[SPACE]
    check(c, 'set a 0 0 1\r\nx\r\n', 'STORED\r\n')
    -- A yield inside a memtx transaction aborts it, so the commit fails;
    -- only the requests that write fail.
    local function trigger()
        fiber.yield()
    end
    space:on_replace(trigger)
    local ok, err = pcall(function()
        -- The first get of an item would write the fetched flag: a read
        -- doesn't fail because of it.
        check(
            c,
            'set b 0 0 1\r\ny\r\nget a\r\nincr a 1\r\ndelete a\r\n'
                .. 'touch a 10\r\ngets b\r\n',
            'SERVER_ERROR Transaction has been aborted by a fiber '
                .. 'yield\r\nVALUE a 0 1\r\nx\r\nEND\r\n'
                .. 'CLIENT_ERROR cannot increment or decrement non-numeric '
                .. 'value\r\nSERVER_ERROR Transaction has been aborted by a '
                .. 'fiber yield\r\nSERVER_ERROR Transaction has been aborted '
                .. 'by a fiber yield\r\nEND\r\n'
        )
    end)
    space:on_replace(nil, trigger)
    t.assert(ok, err)
    check(c, 'get a b\r\n', 'VALUE a 0 1\r\nx\r\nEND\r\n')
end

--
-- Regressions found by the differential compat suite (test/compat), end to
-- end; each test names its former allowlist entry.
--

local function has_meta()
    return pcall(require, 'memcached.proto.meta')
end

-- classic_read_takes_token: get, gets, gat, gats and touch of a stale item
-- leave its win token to the next mg.
g.test_compat_classic_read_keeps_token = function(cg)
    t.skip_if(not has_meta(), 'proto/meta.lua is not available')
    local c = cg.c
    local reads = {
        {'get k\r\n', 'VALUE k 1 1\r\nx\r\nEND\r\n'},
        {'gets k\r\n', nil},
        {'gat 100 k\r\n', 'VALUE k 1 1\r\nx\r\nEND\r\n'},
        {'gats 100 k\r\n', nil},
        {'touch k 100\r\n', 'TOUCHED\r\n'},
    }
    for _, read in ipairs(reads) do
        check(c, 'ms k 1 T100 F1\r\nx\r\n', 'HD\r\n')
        check(c, 'md k I\r\n', 'HD\r\n')
        if read[2] ~= nil then
            check(c, read[1], read[2])
        else
            local raw = retrieve(c, read[1])
            t.assert_str_matches(raw, 'VALUE k 1 1 %d+\r\nx\r\nEND\r\n')
        end
        check(c, 'mg k v\r\n', 'VA 1 X W\r\nx\r\n')
        check(c, 'mg k v\r\n', 'VA 1 Z X\r\nx\r\n')
    end
end

-- vivify_fetched, vivify_negative_ttl.
g.test_compat_vivify = function(cg)
    t.skip_if(not has_meta(), 'proto/meta.lua is not available')
    local c = cg.c
    check(c, 'mg k N30 h\r\n', 'HD h0 W\r\n')
    check(c, 'mg k h\r\n', 'HD h1 Z\r\n')
    check(c, 'mg u N30 u\r\n', 'HD W\r\n')
    check(c, 'mg u h\r\n', 'HD h0 Z\r\n')
    check(c, 'mg n N-1 v\r\n', 'VA 0 W\r\n\r\n')
    check(c, 'mg n v\r\n', 'EN\r\n')
end

-- md_x_new_item, md_x_cas.
g.test_compat_md_x = function(cg)
    t.skip_if(not has_meta(), 'proto/meta.lua is not available')
    local c = cg.c
    check(c, 'ms k 1\r\nx\r\n', 'HD\r\n')
    local cas = cas_of(c, 'k')
    check(c, 'mg k v\r\n', 'VA 1\r\nx\r\n')
    check(c, 'md k x T100\r\n', 'HD\r\n')
    check(c, 'mg k h s c\r\n', ('HD h0 s0 c%s\r\n'):format(cas))
    check(c, 'ms s 1\r\nx\r\n', 'HD\r\n')
    check(c, 'md s I\r\n', 'HD\r\n')
    check(c, 'md s x T100\r\n', 'HD\r\n')
    check(c, 'mg s\r\n', 'HD\r\n')
end

-- cas_zero_means_none, arith_empty_cas_order, arith_strtoull_value.
g.test_compat_cas_zero_and_arith = function(cg)
    t.skip_if(not has_meta(), 'proto/meta.lua is not available')
    local c = cg.c
    check(c, 'ms k 1\r\nx\r\n', 'HD\r\n')
    check(c, 'ms k 1 MA C0\r\ny\r\n', 'HD\r\n')
    check(c, 'ms k 1 MP C0\r\nz\r\n', 'HD\r\n')
    check(c, 'ms k 1 C0\r\nn\r\n', 'EX\r\n')
    check(c, 'ms k 1 C0 I\r\nn\r\n', 'EX\r\n')
    check(c, 'mg k v\r\n', 'VA 3\r\nzxy\r\n')
    check(c, 'ms n 1\r\n1\r\n', 'HD\r\n')
    check(c, 'ma n C0 v\r\n', 'VA 1\r\n2\r\n')
    check(c, 'ms e 0\r\n\r\n', 'HD\r\n')
    check(
        c,
        'ma e C18446744073709551615\r\n',
        'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n'
    )
    check(c, 'set s 0 0 5\r\n12 ab\r\n', 'STORED\r\n')
    check(c, 'incr s 1\r\n', '13\r\n')
    check(c, 'set p 0 0 2\r\n+5\r\n', 'STORED\r\n')
    check(c, 'incr p 1\r\n', '6\r\n')
    check(c, 'decr p 1\r\n', '5\r\n')
end

-- flush_all_bad_exptime.
g.test_compat_flush_all_bad_exptime = function(cg)
    local c = cg.c
    check(c, 'flush_all abc\r\n', 'CLIENT_ERROR invalid exptime argument\r\n')
    check(
        c,
        'flush_all abc noreply\r\nmn\r\n',
        has_meta() and 'MN\r\n' or 'ERROR\r\n'
    )
end

-- http_close: the connection closes without a reply.
g.test_compat_http_close = function(cg)
    for _, line in ipairs({
        'foo / HTTP/1.1',
        'GET / HTTP/1.0',
        'PUT /asdf/asd/fasdfasdf/sadf HTTP/1.1',
        'DELETE HTTP/1.1',
    }) do
        local c = connect(cg)
        c:send(line .. '\r\n')
        t.assert_equals(c.sock:read(1, client.TIMEOUT), '', line)
    end
end

-- meta_long_line: a meta line over 2048 bytes sent with its \n in one write
-- is processed, and the connection stays open.
g.test_compat_meta_long_line = function(cg)
    t.skip_if(not has_meta(), 'proto/meta.lua is not available')
    local c = cg.c
    check(
        c,
        'mg ' .. ('k'):rep(3000) .. ' v\r\n',
        'CLIENT_ERROR bad command line format\r\n'
    )
    check(c, 'mn\r\n', 'MN\r\n')
end
