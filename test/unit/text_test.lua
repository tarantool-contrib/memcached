-- proto/text.lua: the parser and the encoder, no box needed.

local t = require('luatest')

local text = require('memcached.proto.text')

local g = t.group('text')

local function parse(buf, opts)
    return text.parse(buf, 1, opts)
end

-- The parse() results as a table.
local function parsed(buf, opts)
    return {text.parse(buf, 1, opts)}
end

g.test_need_more_data = function()
    t.assert_equals(parsed(''), {nil, 1})
    t.assert_equals(parsed('get foo'), {nil, 1})
    t.assert_equals(parsed('get foo\r'), {nil, 1})
    -- The data block is incomplete: the hint is the size of the command.
    local cmd = 'set foo 0 0 5\r\n'
    t.assert_equals(parsed(cmd), {nil, 1, #cmd + 7})
    t.assert_equals(parsed(cmd .. 'abc'), {nil, 1, #cmd + 7})
    t.assert_equals(parsed(cmd .. 'abcde\r'), {nil, 1, #cmd + 7})
    -- From a position other than 1.
    local buf = 'version\r\n' .. cmd .. 'ab'
    t.assert_equals({text.parse(buf, 10)}, {nil, 10, #cmd + 7})
end

g.test_set = function()
    local buf = 'set foo 5 10 3\r\nbar\r\nget foo\r\n'
    local req, pos = parse(buf)
    t.assert_equals(req, {
        op = 'store',
        cmd = 'set',
        mode = 'set',
        key = 'foo',
        value = 'bar',
        flags = 5,
        exptime = 10,
        noreply = false,
        fast = 'set',
    })
    t.assert_equals(buf:sub(pos), 'get foo\r\n')
    for _, cmd in ipairs({'add', 'replace', 'append', 'prepend'}) do
        req = parse(cmd .. ' k 0 0 1 noreply\r\nx\r\n')
        t.assert_equals(req.mode, cmd)
        t.assert_equals(req.noreply, true)
        -- Only a plain set takes the C data path.
        t.assert_equals(req.fast, nil)
    end
    t.assert_equals(parse('cas k 0 0 1 5\r\nx\r\n').fast, nil)
    -- Extra arguments are ignored, as in memcached 1.6.45; noreply is the
    -- last token.
    req = parse('set k 0 0 1 whatever\r\nx\r\n')
    t.assert_equals(req.noreply, false)
    t.assert_equals(req.value, 'x')
    t.assert_equals(parse('set k 0 0 1 noreply x\r\nx\r\n').noreply, false)
    t.assert_equals(parse('set k 0 0 1 x noreply\r\nx\r\n').noreply, true)
    -- Only the first 23 tokens count.
    local extra = string.rep(' x', 17)
    t.assert_equals(
        parse('set k 0 0 1' .. extra .. ' noreply\r\nx\r\n')
            .noreply,
        true
    )
    t.assert_equals(
        parse('set k 0 0 1' .. extra .. ' x noreply\r\nx\r\n')
            .noreply,
        false
    )
    -- Binary data and an empty value.
    req = parse('set k 0 0 6\r\na\r\n\0\r\n\r\n')
    t.assert_equals(req.value, 'a\r\n\0\r\n')
    req = parse('set k 0 0 0\r\n\r\n')
    t.assert_equals(req.value, '')
    -- Flags are read with strtoul and cast to 32 bits, as memcached does:
    -- above 2^64-1 or a negative long is refused, the rest is truncated.
    for _, case in ipairs({
        {'4294967296', 0},
        {'4294967297', 1},
        {'18446744073709551615', 4294967295},
        {'+5', 5},
        {'-0', 0},
        {'\t5\t', 5},
        {'-18446744073709551615', 1},
    }) do
        t.assert_equals(
            parse('set k ' .. case[1] .. ' 0 1\r\nx\r\n').flags,
            case[2],
            case[1]
        )
    end
    for _, flags in ipairs({
        '18446744073709551616',
        '-1',
        '-9223372036854775808',
        '1x',
        '0x10',
    }) do
        t.assert_equals(
            parsed('set k ' .. flags .. ' 0 1\r\nx\r\n')[3],
            'CLIENT_ERROR bad command line format\r\n',
            flags
        )
    end
    -- Exptime may be negative. The exptime is read with
    -- strtol, as memcached does: a sign, leading whitespace, then a cast
    -- to int32_t.
    req = parse('set k 4294967295 -1 1\r\nx\r\n')
    t.assert_equals(req.flags, 4294967295)
    t.assert_equals(req.exptime, -1)
    t.assert_equals(parse('set k 0 +7 1\r\nx\r\n').exptime, 7)
    t.assert_equals(parse('set k 0 \t-0 1\r\nx\r\n').exptime, 0)
    t.assert_equals(parse('set k 0 4294967306 1\r\nx\r\n').exptime, 10)
    t.assert_equals(
        parse('set k 0 2147483648 1\r\nx\r\n').exptime,
        -2147483648
    )
    -- Lines may end with \n only.
    req, pos = parse('set k 0 0 1\nx\r\n')
    t.assert_equals(req.value, 'x')
    t.assert_equals(pos, 16)
end

g.test_cas = function()
    local req = parse('cas k 1 2 1 18446744073709551615 noreply\r\nx\r\n')
    t.assert_equals(req.op, 'store')
    t.assert_equals(req.mode, 'set')
    t.assert_equals(req.cas, 18446744073709551615ULL)
    t.assert_equals(type(req.cas), 'cdata')
    t.assert_equals(req.noreply, true)
end

g.test_storage_errors = function()
    local FORMAT = 'CLIENT_ERROR bad command line format\r\n'
    -- As memcached 1.6.45: too few tokens or a bad length fail before the
    -- length is known, and noreply doesn't hide that.
    for _, line in ipairs({
        'set k 0 0\r\n',
        'set k 0 0 -1\r\n',
        'set k 0 0 0x10\r\n',
        'set k 0 0 +1\r\n',
        'set k 0 0 2147483646\r\n',
        'set k 0 0 noreply\r\n',
    }) do
        t.assert_equals(parsed(line), {false, #line + 1, FORMAT}, line)
    end
    -- The other errors swallow the data block (1 byte + \r\n here).
    local cases = {
        -- Bad numbers, as in the tarantool/memcached cas test.
        'cas bad blah 0 1 0\r\n',
        'cas bad 0 blah 1 0\r\n',
        'cas bad 0 0 1 blah\r\n',
        'set k -1 0 1\r\n',
        'set k 0 1.5 1\r\n',
        'set k 0 9223372036854775808 1\r\n',
        'cas k 0 0 1 18446744073709551616\r\n',
        'cas k 0 0 1 -1\r\n',
        -- No cas unique.
        'cas k 0 0 1\r\n',
        -- Keys.
        'set ' .. string.rep('k', 251) .. ' 0 0 1\r\n',
    }
    for _, line in ipairs(cases) do
        local req, pos, err = parse(line .. 'x\r\n')
        t.assert_equals(req, false, line)
        t.assert_equals(pos, #line + 4, line)
        t.assert_equals(err, FORMAT, line)
    end
    -- The data block may not have arrived: it is skipped as it does.
    t.assert_equals(parsed('set k abc 0 5\r\n'), {false, 23, FORMAT})
    -- A key of 250 bytes is fine, and so are control characters: memcached
    -- splits on spaces only.
    local key = string.rep('k', 250)
    t.assert_equals(parse('set ' .. key .. ' 0 0 1\r\nx\r\n').key, key)
    for _, k in ipairs({'a\tb', 'a\1b', 'a\127', '\0', 'a\rb'}) do
        t.assert_equals(parse('set ' .. k .. ' 0 0 1\r\nx\r\n').key, k)
        t.assert_equals(parse('get ' .. k .. '\r\n').multi[1].key, k)
        t.assert_equals(parse('delete ' .. k .. '\r\n').key, k)
    end
    -- noreply hides the errors once the length is known.
    t.assert_equals(
        parsed('set k x 0 1 noreply\r\nx\r\n'),
        {false, 25, nil}
    )
    t.assert_equals(
        parsed('cas k 0 0 1 noreply\r\nx\r\n'),
        {false, 25, nil}
    )
end

g.test_bad_data_chunk = function()
    -- The data block is followed by anything but \r\n: the bytes are
    -- consumed and parsing goes on after them.
    local buf = 'set k 0 0 3\r\nabcde\r\n'
    local req, pos, err = parse(buf)
    t.assert_equals(req, false)
    t.assert_equals(err, 'CLIENT_ERROR bad data chunk\r\n')
    t.assert_equals(buf:sub(pos), '\r\n')
    t.assert_equals(parsed('set k 0 0 1 noreply\r\nxyz'), {false, 25, nil})
    t.assert_equals(parsed('set k 0 0 1\r\nx\n\r'), {
        false,
        17,
        'CLIENT_ERROR bad data chunk\r\n',
    })
end

g.test_too_large = function()
    -- The item size as memcached counts it: 48 + key + 1 + flags (4 when
    -- not 0) + value + 2 + 8, here 70 for 10 bytes.
    t.assert_equals(text.item_size(1, 0, 10), 70)
    t.assert_equals(text.item_size(1, 5, 10), 74)
    local opts = {max_item_size = 70}
    local line = 'set k 0 0 11\r\n'
    -- The data is skipped even if it hasn't arrived: next_pos is beyond
    -- the buffer. A failed set deletes the old value.
    local req, pos = parse(line, opts)
    t.assert_equals(req, {
        op = 'delete',
        key = 'k',
        cmd = 'set',
        noreply = false,
        too_large = true,
    })
    t.assert_equals(pos, #line + 11 + 2 + 1)
    t.assert_equals(
        text.encode(req, {status = 'DELETED'}),
        'SERVER_ERROR object too large for cache\r\n'
    )
    t.assert_equals(
        text.encode(req, {status = 'NOT_FOUND'}),
        'SERVER_ERROR object too large for cache\r\n'
    )
    req = parse('set k 0 0 11 noreply\r\n', opts)
    t.assert_equals(text.encode(req, {status = 'DELETED'}), '')
    -- Other commands just fail.
    line = 'add k 0 0 11\r\n'
    t.assert_equals(parsed(line, opts), {
        false,
        #line + 14,
        'SERVER_ERROR object too large for cache\r\n',
    })
    line = 'cas k 0 0 11 1\r\n'
    t.assert_equals(parsed(line, opts), {
        false,
        #line + 14,
        'SERVER_ERROR object too large for cache\r\n',
    })
    -- The limit itself is fine; flags count.
    t.assert_equals(
        parse('set k 0 0 10\r\n0123456789\r\n', opts).value,
        '0123456789'
    )
    t.assert_equals(
        parse('set k 1 0 10\r\n0123456789\r\n', opts).too_large,
        true
    )
    -- The default is 1 MiB: a value of 1 MiB is too large, as in memcached.
    t.assert_equals(parse('set k 0 0 1048576\r\n').too_large, true)
    t.assert_equals(parse('set k 0 0 1048517\r\n').too_large, true)
    line = 'set k 0 0 1048516\r\n'
    t.assert_equals(parsed(line), {nil, 1, #line + 1048516 + 2})
end

g.test_retrieval = function()
    local req, pos = parse('get a b  c\r\n')
    t.assert_equals(pos, 13)
    t.assert_equals(req, {
        op = 'retrieve',
        cmd = 'get',
        with_cas = false,
        multi = {
            {op = 'get', key = 'a', fast = 'get', hold = true},
            {op = 'get', key = 'b', fast = 'get', hold = true},
            {op = 'get', key = 'c', fast = 'get', hold = true},
        },
    })
    req = parse('gets a\r\n')
    t.assert_equals(req.with_cas, true)
    t.assert_equals(req.multi, {
        {
            op = 'get',
            key = 'a',
            fast = 'gets',
            hold = true,
        },
    })
    req = parse('gat 100 a b\r\n')
    t.assert_equals(req.with_cas, false)
    t.assert_equals(req.multi, {
        {op = 'get', key = 'a', touch_ttl = 100, hold = true},
        {op = 'get', key = 'b', touch_ttl = 100, hold = true},
    })
    req = parse('gats -1 a\r\n')
    t.assert_equals(req.with_cas, true)
    t.assert_equals(req.multi, {
        {
            op = 'get',
            key = 'a',
            touch_ttl = -1,
            hold = true,
        },
    })
    local FORMAT = 'CLIENT_ERROR bad command line format\r\n'
    t.assert_equals(parsed('gat 10\r\n'), {false, 9, FORMAT})
    t.assert_equals(parsed('get\r\n'), {false, 6, FORMAT})
    t.assert_equals(parsed('gets \r\n'), {false, 8, FORMAT})
    t.assert_equals(
        parsed('gat x a\r\n'),
        {false, 10, 'CLIENT_ERROR invalid exptime argument\r\n'}
    )
    t.assert_equals(
        parsed('gat +1 a\r\n'),
        {false, 11, 'CLIENT_ERROR invalid exptime argument\r\n'}
    )
    -- noreply is a key, and so is anything past the 23rd token.
    t.assert_equals(#parse('get a noreply\r\n').multi, 2)
    t.assert_equals(#parse('get' .. string.rep(' k', 30) .. '\r\n').multi, 30)
    local long = string.rep('x', 251)
    t.assert_equals(
        parsed('get a ' .. long .. '\r\n'),
        {false, 260, 'CLIENT_ERROR bad command line format\r\n'}
    )
end

g.test_delete = function()
    t.assert_equals(parse('delete k\r\n'), {
        op = 'delete',
        cmd = 'delete',
        key = 'k',
        noreply = false,
    })
    t.assert_equals(parse('delete k noreply\r\n').noreply, true)
    t.assert_equals(parse('delete k 0\r\n').noreply, false)
    t.assert_equals(parse('delete k 0 noreply\r\n').noreply, true)
    -- As memcached 1.6.45: extra arguments are ignored.
    t.assert_equals(parse('delete k 1\r\n').key, 'k')
    t.assert_equals(parse('delete k 1 noreply\r\n').noreply, true)
    t.assert_equals(parse('delete k 0 noreply x\r\n').noreply, false)
    t.assert_equals(
        parsed('delete\r\n'),
        {false, 9, 'CLIENT_ERROR bad command line format\r\n'}
    )
end

g.test_arith = function()
    t.assert_equals(parse('incr k 5\r\n'), {
        op = 'arith',
        cmd = 'incr',
        key = 'k',
        incr = true,
        delta = 5ULL,
        noreply = false,
    })
    local req = parse('decr k 18446744073709551615 noreply\r\n')
    t.assert_equals(req.incr, false)
    t.assert_equals(req.delta, 18446744073709551615ULL)
    t.assert_equals(req.noreply, true)
    t.assert_equals(parsed('incr k x\r\n'), {
        false,
        11,
        'CLIENT_ERROR invalid numeric delta argument\r\n',
    })
    t.assert_equals(parsed('incr k -1\r\n'), {
        false,
        12,
        'CLIENT_ERROR invalid numeric delta argument\r\n',
    })
    t.assert_equals(parsed('incr k 18446744073709551616\r\n'), {
        false,
        30,
        'CLIENT_ERROR invalid numeric delta argument\r\n',
    })
    -- mcmc_toktou64: digits only, at most 22 of them.
    t.assert_equals(parsed('incr k +1\r\n'), {
        false,
        12,
        'CLIENT_ERROR invalid numeric delta argument\r\n',
    })
    t.assert_equals(
        parse('incr k ' .. string.rep('0', 21) .. '1\r\n').delta,
        1ULL
    )
    t.assert_equals(parsed('incr k ' .. string.rep('0', 22) .. '1\r\n'), {
        false,
        33,
        'CLIENT_ERROR invalid numeric delta argument\r\n',
    })
    t.assert_equals(
        parsed('incr k\r\n'),
        {false, 9, 'CLIENT_ERROR bad command line format\r\n'}
    )
    t.assert_equals(parse('incr k 1 x\r\n').delta, 1ULL)
end

g.test_touch = function()
    t.assert_equals(parse('touch k 10\r\n'), {
        op = 'get',
        cmd = 'touch',
        key = 'k',
        touch_ttl = 10,
        touch_only = true,
        noreply = false,
    })
    t.assert_equals(parse('touch k 10 noreply\r\n').noreply, true)
    t.assert_equals(parsed('touch k x\r\n'), {
        false,
        12,
        'CLIENT_ERROR invalid exptime argument\r\n',
    })
    t.assert_equals(
        parsed('touch k\r\n'),
        {false, 10, 'CLIENT_ERROR bad command line format\r\n'}
    )
    t.assert_equals(parse('touch k 1 2\r\n').touch_ttl, 1)
end

g.test_other_commands = function()
    t.assert_equals(parse('flush_all\r\n'), {
        op = 'flush',
        cmd = 'flush_all',
        delay = 0,
        noreply = false,
    })
    t.assert_equals(parse('flush_all 10\r\n').delay, 10)
    t.assert_equals(parse('flush_all noreply\r\n').noreply, true)
    local req = parse('flush_all 0 noreply\r\n')
    t.assert_equals(req.delay, 0)
    t.assert_equals(req.noreply, true)
    -- compat flush_all_bad_exptime: memcached's message.
    t.assert_equals(parsed('flush_all x\r\n'), {
        false,
        14,
        'CLIENT_ERROR invalid exptime argument\r\n',
    })
    t.assert_equals(parsed('flush_all abc noreply\r\n'), {false, 24})
    t.assert_equals(parsed('flush_all +1\r\n'), {
        false,
        15,
        'CLIENT_ERROR invalid exptime argument\r\n',
    })
    -- Extra arguments are ignored (memcached 1.6.45).
    t.assert_equals(parse('flush_all 1 2 3\r\n').delay, 1)
    -- A leading space: memcached's second table of commands still works.
    t.assert_equals(parse(' flush_all\r\n').op, 'flush')

    t.assert_equals(parse('version\r\n'), {op = 'version', cmd = 'version'})
    t.assert_equals(parse('quit\r\n'), {op = 'quit', cmd = 'quit'})
    t.assert_equals(parse('verbosity 1\r\n'), {
        op = 'verbosity',
        cmd = 'verbosity',
        level = 1,
        noreply = false,
    })
    t.assert_equals(parse('verbosity 10 noreply\r\n').level, 2)
    t.assert_equals(parse('verbosity 1 2 3\r\n').level, 1)
    t.assert_equals(parse('  verbosity 1\r\n').level, 1)
    t.assert_equals(parsed('verbosity\r\n'), {
        false,
        12,
        'CLIENT_ERROR bad command line format\r\n',
    })
    t.assert_equals(parsed('verbosity noreply\r\n'), {false, 20})
    t.assert_equals(parsed('verbosity x\r\n'), {
        false,
        14,
        'CLIENT_ERROR bad command line format\r\n',
    })
    t.assert_equals(
        parse('stats\r\n'),
        {op = 'stats', cmd = 'stats', args = {}}
    )
    t.assert_equals(parse('stats settings\r\n').args, {'settings'})
end

g.test_unknown_commands = function()
    t.assert_equals(
        parsed('boguscommand slkdsldkfjsd\r\n'),
        {false, 28, 'ERROR\r\n'}
    )
    -- noreply hides it too.
    t.assert_equals(parsed('bogus noreply\r\n'), {false, 16})
    -- Lines shorter than 4 bytes (memcached 1.6.45).
    local FORMAT = 'CLIENT_ERROR bad command line format\r\n'
    t.assert_equals(parsed('\r\n'), {false, 3, FORMAT})
    t.assert_equals(parsed('\n'), {false, 2, FORMAT})
    t.assert_equals(parsed('x\r\n'), {false, 4, FORMAT})
    t.assert_equals(parsed('   \n'), {false, 5, 'ERROR\r\n'})
    -- A leading space makes the command name empty.
    t.assert_equals(parsed(' get a\r\n'), {false, 9, 'ERROR\r\n'})
    -- Meta commands are not classic ones.
    t.assert_equals(parsed('mn\r\n'), {false, 5, 'ERROR\r\n'})
end

-- compat http_close: an unknown command whose last token starts with
-- "HTTP/" closes the connection without a reply (t/malicious-commands.t).
-- Command names are exact there: GET is unknown.
g.test_http_close = function()
    for _, line in ipairs({
        'foo / HTTP/1.1',
        'GET / HTTP/1.0',
        'PUT /asdf/asd/fasdfasdf/sadf HTTP/1.1',
        'DELETE HTTP/1.1',
        'Set k HTTP/2',
    }) do
        local buf = line .. '\r\n'
        t.assert_equals(parsed(buf), {false, #buf + 1, nil, true}, line)
    end
    -- Known commands, a lone token, HTTP/ elsewhere: as before.
    t.assert_equals(parse('get / HTTP/1.1\r\n').multi[2].key, 'HTTP/1.1')
    t.assert_equals(parsed('HTTP/1.1\r\n'), {false, 11, 'ERROR\r\n'})
    t.assert_equals(parsed('foo HTTP/1.1 x\r\n'), {false, 17, 'ERROR\r\n'})
    t.assert_equals(parsed('foo / http/1.1\r\n'), {false, 17, 'ERROR\r\n'})
end

-- Command names are exact, as in memcached.
g.test_case_sensitive = function()
    for _, line in ipairs({
        'SET foo 0 0 1',
        'GETS foo',
        'Delete foo',
        'Flush_all',
        'VERSION',
        'Get foo',
    }) do
        local buf = line .. '\r\n'
        t.assert_equals(parsed(buf), {false, #buf + 1, 'ERROR\r\n'}, line)
    end
    -- An unknown command: noreply hides its ERROR.
    t.assert_equals(parsed('GET foo noreply\r\n'), {false, 18})
end

g.test_long_lines = function()
    local line = string.rep('x', text.MAX_LINE + 1)
    -- Closed without a reply, as memcached does.
    t.assert_equals(parsed(line), {false, #line + 1, nil, true})
    -- A multi-key get may be long.
    line = 'get' .. string.rep(' key', 1000)
    t.assert_equals(parsed(line), {nil, 1})
    t.assert_equals(#parse(line .. '\r\n').multi, 1000)
    line = string.rep('x', 4000) .. '\r\n'
    t.assert_equals(parsed(line), {false, #line + 1, 'ERROR\r\n'})
end

g.test_numbers = function()
    t.assert_equals(text.parse_u64('0'), 0ULL)
    t.assert_equals(text.parse_u64('000123'), 123ULL)
    t.assert_equals(
        text.parse_u64('18446744073709551615'),
        18446744073709551615ULL
    )
    t.assert_equals(text.parse_u64('18446744073709551616'), nil)
    t.assert_equals(text.parse_u64('99999999999999999999'), nil)
    t.assert_equals(text.parse_u64(''), nil)
    t.assert_equals(text.parse_u64('1e5'), nil)
    t.assert_equals(text.parse_u64(' 1'), nil)
    t.assert_equals(text.u64str(0), '0')
    t.assert_equals(text.u64str(123), '123')
    t.assert_equals(
        text.u64str(18446744073709551615ULL),
        '18446744073709551615'
    )
    t.assert_equals(text.u64str(9007199254740993ULL), '9007199254740993')
    t.assert_equals(text.parse_u64('+1'), nil)
    t.assert_equals(text.parse_u64(string.rep('0', 23)), nil)
    t.assert_equals(text.parse_i32('-20'), -20)
    t.assert_equals(text.parse_i32('-'), 0)
    t.assert_equals(text.parse_i32('+20'), nil)
    t.assert_equals(text.parse_i32('20x'), nil)
    t.assert_equals(text.parse_i32('2147483647'), 2147483647)
    t.assert_equals(text.parse_i32('2147483648'), nil)
    t.assert_equals(text.parse_i32('-2147483648'), -2147483648)
    t.assert_equals(text.parse_i32('000000000001'), nil)
    t.assert_equals(text.parse_u32('4294967295'), 4294967295)
    t.assert_equals(text.parse_u32('4294967296'), nil)
    t.assert_equals(text.parse_exptime(' -5'), -5)
    t.assert_equals(text.parse_exptime('5x'), nil)
    t.assert_equals(text.parse_exptime('9223372036854775807'), -1)
    t.assert_equals(text.parse_exptime('-9223372036854775808'), 0)
    t.assert_equals(text.parse_exptime('9223372036854775808'), nil)
end

local function item(key, value, flags, cas)
    return {
        status = 'HIT',
        item = {
            key = key,
            value = value,
            flags = flags,
            cas = cas,
        },
    }
end

g.test_encode_retrieval = function()
    local req = parse('get a b c\r\n')
    t.assert_equals(
        text.encode(req, {
            item('a', 'xy', 0, 1ULL),
            {status = 'MISS'},
            item('c', '', 42, 2ULL),
        }),
        'VALUE a 0 2\r\nxy\r\nVALUE c 42 0\r\n\r\nEND\r\n'
    )
    req = parse('gets a b\r\n')
    t.assert_equals(
        text.encode(req, {
            item('a', 'xy', 0, 18446744073709551615ULL),
            item('b', 'z', 1, 5),
        }),
        'VALUE a 0 2 18446744073709551615\r\nxy\r\nVALUE b 1 1 5\r\nz\r\nEND\r\n'
    )
    t.assert_equals(
        text.encode(req, {{status = 'MISS'}, {status = 'MISS'}}),
        'END\r\n'
    )
    -- An error drops the partial results.
    t.assert_equals(
        text.encode(req, {
            item('a', 'xy', 0, 1ULL),
            {status = 'SERVER_ERROR', message = 'oops\nnewline'},
        }),
        'SERVER_ERROR oops newline\r\n'
    )
end

g.test_encode_replies = function()
    local set = parse('set k 0 0 1\r\nx\r\n')
    t.assert_equals(text.encode(set, {status = 'STORED'}), 'STORED\r\n')
    t.assert_equals(
        text.encode(set, {status = 'NOT_STORED'}),
        'NOT_STORED\r\n'
    )
    t.assert_equals(text.encode(set, {status = 'EXISTS'}), 'EXISTS\r\n')
    t.assert_equals(
        text.encode(set, {status = 'NOT_FOUND'}),
        'NOT_FOUND\r\n'
    )
    t.assert_equals(
        text.encode(set, {
            status = 'SERVER_ERROR',
            message = 'WAL error',
        }),
        'SERVER_ERROR WAL error\r\n'
    )
    local quiet = parse('set k 0 0 1 noreply\r\nx\r\n')
    t.assert_equals(text.encode(quiet, {status = 'STORED'}), '')
    t.assert_equals(
        text.encode(quiet, {
            status = 'SERVER_ERROR',
            message = 'x',
        }),
        ''
    )

    local touch = parse('touch k 1\r\n')
    t.assert_equals(text.encode(touch, item('k', 'x', 0, 1)), 'TOUCHED\r\n')
    t.assert_equals(text.encode(touch, {status = 'MISS'}), 'NOT_FOUND\r\n')

    local delete = parse('delete k\r\n')
    t.assert_equals(
        text.encode(delete, {status = 'DELETED'}),
        'DELETED\r\n'
    )
    t.assert_equals(
        text.encode(delete, {status = 'NOT_FOUND'}),
        'NOT_FOUND\r\n'
    )

    local incr = parse('incr k 1\r\n')
    t.assert_equals(
        text.encode(incr, {
            status = 'STORED',
            value = 18446744073709551615ULL,
        }),
        '18446744073709551615\r\n'
    )
    t.assert_equals(
        text.encode(incr, {status = 'NOT_FOUND'}),
        'NOT_FOUND\r\n'
    )
    t.assert_equals(
        text.encode(incr, {
            status = 'CLIENT_ERROR',
            message = 'x',
        }),
        'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n'
    )

    t.assert_equals(
        text.encode(parse('flush_all\r\n'), {status = 'OK'}),
        'OK\r\n'
    )
    t.assert_equals(
        text.encode(
            parse('flush_all noreply\r\n'),
            {status = 'OK'}
        ),
        ''
    )
    t.assert_equals(
        text.encode(parse('verbosity 1\r\n'), {status = 'OK'}),
        'OK\r\n'
    )
    t.assert_equals(
        text.encode(
            parse('version\r\n'),
            {status = 'VERSION', version = '1.6.0-x'}
        ),
        'VERSION 1.6.0-x\r\n'
    )
    t.assert_equals(text.encode(parse('quit\r\n'), nil), '')

    local stats = parse('stats\r\n')
    t.assert_equals(
        text.encode(stats, {
            status = 'STATS',
            stats = {
                {'pid', 1},
                {'version', '1.6'},
                {'big', 18446744073709551615ULL},
                {'ratio', 0.5},
                {'flag', true},
            },
        }),
        'STAT pid 1\r\nSTAT version 1.6\r\n'
            .. 'STAT big 18446744073709551615\r\nSTAT ratio 0.500000\r\n'
            .. 'STAT flag yes\r\nEND\r\n'
    )
    t.assert_equals(text.encode(stats, {status = 'RESET'}), 'RESET\r\n')
    t.assert_equals(text.encode(stats, {status = 'ERROR'}), 'ERROR\r\n')
end

-- "get <key>" and "gets <key>" have a fast path in parse(): it must give
-- what the general parser (parse_general()) gives.
g.test_get_fast_path = function()
    local max_key = string.rep('k', text.MAX_KEY_LENGTH)
    local lines = {
        'get a',
        'gets a',
        'get ' .. max_key,
        'gets ' .. max_key,
        'get ' .. max_key .. 'x',
        'get a b',
        'get  a',
        'get a ',
        'get ',
        'gets ',
        'get',
        'gets',
        'get\ta',
        'get a\tb',
        'get a\1',
        'get a\127',
        'get \128\255',
        'get a\rb',
        'gat 1 a',
        'getx a',
    }
    for _, line in ipairs(lines) do
        for _, eol in ipairs({'\r\n', '\n'}) do
            local buf = line .. eol .. 'version\r\n'
            t.assert_equals(
                {text.parse(buf, 1)},
                {text.parse_general(buf, 1)},
                line
            )
        end
    end
    t.assert_equals(
        parse('get a\r\n').multi,
        {{op = 'get', key = 'a', fast = 'get', hold = true}}
    )
    -- Not an HTTP request: "get" is a command (the HTTP check is exact,
    -- so "GET HTTP/1.1" closes the connection).
    t.assert_equals(
        parse('get HTTP/1.1\r\n').multi,
        {{op = 'get', key = 'HTTP/1.1', fast = 'get', hold = true}}
    )
    t.assert_equals(parse('gets ' .. max_key .. '\r\n').multi[1].key, max_key)
    -- Incomplete lines.
    t.assert_equals(parsed('get a\r'), {nil, 1})
    t.assert_equals({text.parse('xget a\r\n', 2)}, {parse('get a\r\n'), 9})
end

-- encode_retrieve: flags as a Lua number or as a 64-bit cdata.
g.test_encode_flags = function()
    local req = parse('get a\r\n')
    local function hit(flags)
        return {
            {
                status = 'HIT',
                item = {
                    key = 'a',
                    value = 'v',
                    flags = flags,
                    cas = 1ULL,
                },
            },
        }
    end
    t.assert_equals(
        text.encode(req, hit(4294967295)),
        'VALUE a 4294967295 1\r\nv\r\nEND\r\n'
    )
    t.assert_equals(
        text.encode(req, hit(7ULL)),
        'VALUE a 7 1\r\nv\r\nEND\r\n'
    )
    t.assert_equals(text.encode(req, hit(0)), 'VALUE a 0 1\r\nv\r\nEND\r\n')
end
