-- Unit tests for the meta protocol parser and encoder (no box needed).

local t = require('luatest')
local ffi = require('ffi')
local digest = require('digest')

local meta = require('memcached.proto.meta')

local g = t.group('meta')

local CRLF = '\r\n'
local NOW = 1700000000

local E_FORMAT = 'CLIENT_ERROR bad command line format\r\n'
local E_TOKEN = 'CLIENT_ERROR bad token in command line format\r\n'
local E_DUPLICATE = 'CLIENT_ERROR duplicate flag\r\n'
local E_FLAG = 'CLIENT_ERROR invalid flag\r\n'
local E_TOO_MANY = 'CLIENT_ERROR options flags are too long\r\n'
local E_KEY_DECODE = 'CLIENT_ERROR error decoding key\r\n'
local E_OPAQUE = 'CLIENT_ERROR opaque token too long\r\n'
local E_MS_MODE = 'CLIENT_ERROR invalid mode for ms M token\r\n'
local E_MA_MODE = 'CLIENT_ERROR invalid mode for ma M token\r\n'
local E_MA_FLAG = 'CLIENT_ERROR invalid or duplicate flag\r\n'
local E_INITIAL = 'CLIENT_ERROR invalid numeric initial value\r\n'
local E_DELTA = 'CLIENT_ERROR invalid numeric delta value\r\n'
local E_CHUNK = 'CLIENT_ERROR bad data chunk\r\n'
local E_TOO_LARGE = 'SERVER_ERROR object too large for cache\r\n'

local U64_MAX = 18446744073709551615ULL

-- Parse a buffer that holds exactly one command and return the request.
local function parse1(s, opts)
    local req, pos, err = meta.parse(s, 1, opts)
    if not req then
        t.fail(
            ('expected a request from %q, got %s %s'):format(
                s:sub(1, 200),
                tostring(req),
                tostring(err)
            )
        )
    end
    t.assert_equals(pos, #s + 1, 'whole buffer consumed')
    return req
end

-- Parse a buffer that holds exactly one bad command; return err, pos, close.
local function parse_err(s, opts)
    local req, pos, err, close = meta.parse(s, 1, opts)
    t.assert_equals(req, false, ('expected an error from %q'):format(s))
    return err, pos, close
end

local function assert_err(line, err, opts)
    local s = line .. CRLF
    local e, pos = parse_err(s, opts)
    t.assert_equals(e, err, line)
    t.assert_equals(pos, #s + 1, line)
end

local function is_u64(v)
    return type(v) == 'cdata' and ffi.istype('uint64_t', v)
end

local function item(fields)
    local it = {
        key = 'foo',
        value = 'hi',
        flags = 0,
        exptime = 0,
        cas = 1ULL,
        created = NOW - 100,
        last_access = NOW - 10,
        fetched = false,
        stale = false,
    }
    for k, v in pairs(fields or {}) do
        it[k] = v
    end
    return it
end

local function enc(req, resp)
    return meta.encode(req, resp, {now = NOW})
end

-- Parse one command line (CRLF added) and encode resp for it.
local function reply(line, resp)
    return enc(parse1(line .. CRLF), resp)
end

-- Feed chunks the way server.lua does: append to the buffer, parse until
-- more data is needed, keep the rest; a next_pos past the end of the buffer
-- skips the bytes as they arrive. Errors are collected as {err = ...}.
local function feed(chunks, opts)
    local buf = ''
    local skip = 0
    local out = {}
    for _, chunk in ipairs(chunks) do
        buf = buf .. chunk
        if skip > 0 then
            local n = math.min(skip, #buf)
            buf = buf:sub(n + 1)
            skip = skip - n
        end
        local pos = 1
        while pos <= #buf do
            local req, next_pos, err = meta.parse(buf, pos, opts)
            if req == nil then
                break
            end
            t.assert(next_pos > pos, 'the parser advances')
            out[#out + 1] = req == false and {err = err} or req
            pos = next_pos
        end
        if pos > #buf + 1 then
            skip = pos - #buf - 1
            pos = #buf + 1
        end
        buf = buf:sub(pos)
    end
    t.assert_equals(skip, 0, 'nothing left to skip')
    return out, buf
end

--
-- mg
--

g.test_mg_basic = function()
    -- win_token: mg takes win tokens, the classic reads don't (compat
    -- classic_read_takes_token).
    t.assert_equals(parse1('mg foo\r\n'), {
        op = 'get',
        key = 'foo',
        bump_la = true,
        win_token = true,
        hold = true,
        meta = {cmd = 'mg', ret = {}},
    })
end

g.test_mg_bare_newline = function()
    t.assert_equals(parse1('mg foo v\n').key, 'foo')
    t.assert_equals(parse1('mg foo\n').key, 'foo')
end

g.test_mg_extra_spaces = function()
    local req = parse1('mg  foo   v  s \r\n')
    t.assert_equals(req.key, 'foo')
    t.assert_equals(req.meta.value, true)
    t.assert_equals(req.meta.ret, {'s'})
end

g.test_mg_all_flags = function()
    local req = parse1(
        'mg foo s t c f l h k Oop q u v N30 R10 T60 E123 C456 P1 Lx\r\n'
    )
    t.assert_equals(req, {
        op = 'get',
        key = 'foo',
        touch_ttl = 60,
        vivify_ttl = 30,
        recache_ttl = 10,
        bump_la = false,
        no_lru_bump = true,
        new_cas = 123ULL,
        win_token = true,
        hold = true,
        meta = {
            cmd = 'mg',
            quiet = true,
            value = true,
            compare_cas = 456ULL,
            ret = {'s', 't', 'c', 'f', 'l', 'h', 'k', 'Oop'},
        },
    })
    t.assert(is_u64(req.new_cas))
    t.assert(is_u64(req.meta.compare_cas))
end

g.test_mg_each_flag = function()
    local cases = {
        {'v', function(r) return r.meta.value == true end},
        {'q', function(r) return r.meta.quiet == true end},
        {
            'u',
            function(r)
                return r.bump_la == false and r.no_lru_bump == true
            end,
        },
        {'T0', function(r) return r.touch_ttl == 0 end},
        {'T-1', function(r) return r.touch_ttl == -1 end},
        {'T-', function(r) return r.touch_ttl == 0 end},
        {'T2592001', function(r) return r.touch_ttl == 2592001 end},
        {'T2147483647', function(r) return r.touch_ttl == 2147483647 end},
        {'T-2147483648', function(r) return r.touch_ttl == -2147483648 end},
        {'N0', function(r) return r.vivify_ttl == 0 end},
        {'R30', function(r) return r.recache_ttl == 30 end},
        {'E1', function(r) return r.new_cas == 1ULL end},
        {'C0', function(r) return r.meta.compare_cas == 0ULL end},
        {'O', function(r) return r.meta.ret[1] == 'O' end},
        {'Oabc', function(r) return r.meta.ret[1] == 'Oabc' end},
        {'k', function(r) return r.meta.ret[1] == 'k' end},
        {'s', function(r) return r.meta.ret[1] == 's' end},
        {'t', function(r) return r.meta.ret[1] == 't' end},
        {'c', function(r) return r.meta.ret[1] == 'c' end},
        {'f', function(r) return r.meta.ret[1] == 'f' end},
        {'l', function(r) return r.meta.ret[1] == 'l' end},
        {'h', function(r) return r.meta.ret[1] == 'h' end},
        -- Accepted by memcached's mg and ignored.
        {'P/path', function(r) return #r.meta.ret == 0 end},
        {'Lpath', function(r) return #r.meta.ret == 0 end},
        {'x', function(r) return #r.meta.ret == 0 end},
        {'I', function(r) return #r.meta.ret == 0 end},
        {'F5', function(r) return #r.meta.ret == 0 end},
        {'MS', function(r) return #r.meta.ret == 0 end},
        {'J1', function(r) return #r.meta.ret == 0 end},
        {'D1', function(r) return #r.meta.ret == 0 end},
        -- Only the first character of a flag matters.
        {'vfoo', function(r) return r.meta.value == true end},
    }
    for _, c in ipairs(cases) do
        local req = parse1('mg foo ' .. c[1] .. CRLF)
        t.assert(c[2](req), c[1])
    end
end

g.test_mg_errors = function()
    assert_err('mg', E_FORMAT)
    assert_err('mg foo m', E_FLAG)
    -- From memcached's t/metaget.t.
    assert_err('mg foo m o o o o o o o o o', E_FLAG)
    assert_err('mg foo v v', E_DUPLICATE)
    assert_err('mg foo Oa Ob', E_DUPLICATE)
    -- memcached 1.6.45's tokenizer: a flag starts with a letter or a digit.
    assert_err('mg foo \xc3\xa9', E_FORMAT)
    assert_err('mg foo +', E_FORMAT)
    assert_err('mg foo {', E_FORMAT)
    assert_err('mg foo 1', E_FLAG)
    assert_err('mg foo [', E_FLAG)
    for _, f in ipairs({
        'N',
        'T',
        'R',
        'Nx',
        'T1x',
        'R--1',
        'T2147483648',
        'T-2147483649',
        'T123456789012',
        'C',
        'Cx',
        'C-1',
        'C18446744073709551616',
        'E',
        'E1.5',
    }) do
        assert_err('mg foo ' .. f, E_TOKEN)
    end
    -- memcached sets no message for a bad F.
    assert_err('mg foo F', E_FORMAT)
    assert_err('mg foo F4294967296', E_FORMAT)
    assert_err('mg foo Fx', E_FORMAT)
end

g.test_mg_error_precedence = function()
    -- An unknown flag stops the scan; a bad argument doesn't, the last
    -- message wins.
    assert_err('mg foo Nx zz', E_FLAG)
    assert_err('mg foo Nx v v', E_DUPLICATE)
    assert_err('mg foo Nx Fx', E_TOKEN)
    assert_err('mg foo Fx Nx', E_TOKEN)
    assert_err('mg foo Cx Jx', E_INITIAL)
    assert_err('mg foo Jx Dx', E_DELTA)
    assert_err('mg foo Dx Ex', E_TOKEN)
    -- The key is checked before the flags.
    assert_err('mg ' .. ('a'):rep(251) .. ' zz', E_FORMAT)
end

g.test_mg_key_rules = function()
    local key = ('k'):rep(250)
    t.assert_equals(parse1('mg ' .. key .. ' v\r\n').key, key)
    assert_err('mg ' .. key .. 'k v', E_FORMAT)
    -- memcached splits on spaces only: control characters are fine.
    for _, k in ipairs({'foo\tbar', 'foo\1bar', 'foo\127', 'foo\0', 'foo\rbar'}) do
        t.assert_equals(parse1('mg ' .. k .. ' v\r\n').key, k)
        t.assert_equals(parse1('md ' .. k .. '\r\n').key, k)
        t.assert_equals(parse1('ma ' .. k .. '\r\n').key, k)
        t.assert_equals(parse1('me ' .. k .. '\r\n').key, k)
        t.assert_equals(parse1('ms ' .. k .. ' 1\r\nx\r\n').key, k)
    end
    -- Bytes above 127 are allowed, like in memcached.
    t.assert_equals(parse1('mg ключ v\r\n').key, 'ключ')
    t.assert_equals(
        parse1('mg response/quiet t q O4\r\n').key,
        'response/quiet'
    )
end

g.test_mg_token_limit = function()
    -- 20 tokens with the command and the key are allowed, 21 are not.
    local flags = {
        's',
        't',
        'c',
        'f',
        'l',
        'h',
        'k',
        'O1',
        'q',
        'u',
        'v',
        'N1',
        'R1',
        'T1',
        'E1',
        'C1',
        'P1',
        'L1',
    }
    local req = parse1('mg foo ' .. table.concat(flags, ' ') .. CRLF)
    t.assert_equals(#req.meta.ret, 8)
    assert_err('mg foo ' .. table.concat(flags, ' ') .. ' x', E_TOO_MANY)
    assert_err('mg foo' .. (' v'):rep(19), E_TOO_MANY)
end

g.test_mg_opaque_length = function()
    -- 32 bytes counting the 'O', as memcached's check.
    local ok = 'O' .. ('9'):rep(31)
    t.assert_equals(parse1('mg foo ' .. ok .. CRLF).meta.ret, {ok})
    assert_err('mg foo O' .. ('9'):rep(32), E_OPAQUE)
end

g.test_mg_uint64 = function()
    local req = parse1('mg foo C9007199254740993 E18446744073709551615\r\n')
    t.assert(is_u64(req.meta.compare_cas))
    t.assert_equals(tostring(req.meta.compare_cas), '9007199254740993ULL')
    t.assert(is_u64(req.new_cas))
    t.assert_equals(req.new_cas, U64_MAX)
    -- memcached allows up to 22 characters, leading zeros included.
    req = parse1('mg foo C' .. ('0'):rep(20) .. '42\r\n')
    t.assert_equals(req.meta.compare_cas, 42ULL)
    t.assert(is_u64(req.meta.compare_cas))
    assert_err('mg foo C' .. ('0'):rep(21) .. '42', E_TOKEN)
    assert_err('mg foo C99999999999999999999', E_TOKEN)
end

g.test_mg_base64_key = function()
    local req = parse1('mg Zm9v b v k\r\n')
    t.assert_equals(req.key, 'foo')
    t.assert_equals(req.meta.base64, true)
    -- "tesuto" in katakana, from memcached's t/metaget.t.
    req = parse1('mg 44OG44K544OI b v\r\n')
    t.assert_equals(req.key, '\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88')
    -- Binary keys may hold anything, spaces and control characters too.
    local bin = '\0 \r\n\t\255'
    req = parse1('mg ' .. meta.b64encode(bin) .. ' b\r\n')
    t.assert_equals(req.key, bin)
    -- Without b the key is taken as is.
    t.assert_equals(parse1('mg Zm9v v\r\n').key, 'Zm9v')
end

g.test_mg_base64_key_errors = function()
    assert_err('mg abc b', E_KEY_DECODE)
    assert_err('mg ==== b', E_KEY_DECODE)
    assert_err('mg A=== b', E_KEY_DECODE)
    assert_err('mg *** b', E_KEY_DECODE)
    -- The length limit applies to the encoded key.
    local long = ('QUFB'):rep(63)
    assert_err('mg ' .. long .. ' b', E_FORMAT)
    t.assert_equals(
        parse1('mg ' .. long:sub(1, 248) .. ' b\r\n').key,
        ('AAA'):rep(62)
    )
    -- A bad key doesn't stop the scan.
    assert_err('mg abc b zz', E_FLAG)
    assert_err('mg abc b Nx', E_TOKEN)
end

g.test_base64 = function()
    t.assert_equals(meta.b64encode(''), '')
    for n = 1, 40 do
        local s = digest.urandom(n)
        local e = meta.b64encode(s)
        t.assert_equals(
            e,
            digest.base64_encode(s, {nowrap = true}),
            'length ' .. n
        )
        t.assert_equals(meta.b64decode(e), s, 'length ' .. n)
    end
    -- memcached's decoder skips characters outside the alphabet and stops at
    -- the first padded block.
    t.assert_equals(meta.b64decode('Zm9v!'), 'foo')
    t.assert_equals(meta.b64decode('Zg==Zm9v'), 'f')
    t.assert_equals(meta.b64decode('Zm8='), 'fo')
    t.assert_equals(meta.b64decode('Zm9'), nil)
    t.assert_equals(meta.b64decode(''), nil)
end

--
-- ms
--

g.test_ms_basic = function()
    t.assert_equals(parse1('ms foo 2\r\nhi\r\n'), {
        op = 'store',
        key = 'foo',
        value = 'hi',
        flags = 0,
        exptime = 0,
        mode = 'set',
        meta = {cmd = 'ms', ret = {}},
    })
end

g.test_ms_all_flags = function()
    local req = parse1(
        'ms foo 2 c C123 E456 F789 I k Oop q s T60 MR N30 P1 Lx\r\nhi\r\n'
    )
    t.assert_equals(req, {
        op = 'store',
        key = 'foo',
        value = 'hi',
        flags = 789,
        exptime = 60,
        -- A CAS turns replace into the cas command.
        mode = 'set',
        cas = 123ULL,
        invalidate = true,
        new_cas = 456ULL,
        meta = {cmd = 'ms', quiet = true, ret = {'c', 'k', 'Oop', 's'}},
    })
    t.assert(is_u64(req.cas))
    t.assert(is_u64(req.new_cas))
end

g.test_ms_modes = function()
    local modes = {
        E = 'add',
        A = 'append',
        P = 'prepend',
        R = 'replace',
        S = 'set',
        Efoo = 'add',
    }
    for m, mode in pairs(modes) do
        t.assert_equals(parse1('ms foo 2 M' .. m .. '\r\nhi\r\n').mode, mode, m)
    end
    for _, m in ipairs({'Z', 'e', 'a', '', 'I'}) do
        local s = 'ms foo 2 M' .. m .. '\r\nhi\r\n'
        local err, pos = parse_err(s)
        t.assert_equals(err, E_MS_MODE, m)
        t.assert_equals(pos, #s + 1, 'data swallowed')
    end
end

g.test_ms_cas_and_invalidate = function()
    local req = parse1('ms foo 2 C5\r\nhi\r\n')
    t.assert_equals({req.mode, req.cas, req.invalidate}, {'set', 5ULL, nil})
    req = parse1('ms foo 2 C5 MR\r\nhi\r\n')
    t.assert_equals({req.mode, req.cas}, {'set', 5ULL})
    -- add ignores the CAS, as memcached does.
    req = parse1('ms foo 2 C5 ME I\r\nhi\r\n')
    t.assert_equals({req.mode, req.cas, req.invalidate}, {'add', nil, nil})
    -- append and prepend check it.
    req = parse1('ms foo 2 C5 MA\r\nhi\r\n')
    t.assert_equals({req.mode, req.cas}, {'append', 5ULL})
    req = parse1('ms foo 2 C5 MP I\r\nhi\r\n')
    t.assert_equals({req.mode, req.cas, req.invalidate}, {'prepend', 5ULL, nil})
    -- I works only with a CAS.
    t.assert_equals(parse1('ms foo 2 I\r\nhi\r\n').invalidate, nil)
    t.assert_equals(parse1('ms foo 2 I C1\r\nhi\r\n').invalidate, true)
end

g.test_ms_vivify = function()
    local req = parse1('ms foo 2 MA N30 T10\r\nhi\r\n')
    t.assert_equals({req.mode, req.vivify_ttl, req.exptime}, {'append', 30, 10})
    t.assert_equals(parse1('ms foo 2 MP N0\r\nhi\r\n').vivify_ttl, 0)
    -- N only means something with append and prepend.
    t.assert_equals(parse1('ms foo 2 N30\r\nhi\r\n').vivify_ttl, nil)
    t.assert_equals(parse1('ms foo 2 MS N30\r\nhi\r\n').vivify_ttl, nil)
end

g.test_ms_flags_and_ttl = function()
    t.assert_equals(parse1('ms foo 2 F4294967295\r\nhi\r\n').flags, 4294967295)
    t.assert_equals(parse1('ms foo 2 T-1\r\nhi\r\n').exptime, -1)
    t.assert_equals(
        parse1('ms foo 2 T1700000000\r\nhi\r\n').exptime,
        1700000000
    )
    local s = 'ms foo 2 F4294967296\r\nhi\r\n'
    local err, pos = parse_err(s)
    t.assert_equals({err, pos}, {E_FORMAT, #s + 1})
end

g.test_ms_binary_data = function()
    local data = 'a\r\nb\0\r\n\r\nEND\r\nmn\r\n\255'
    local s = ('ms foo %d T30\r\n'):format(#data) .. data .. CRLF
    t.assert_equals(parse1(s).value, data)
    -- Empty data.
    t.assert_equals(parse1('ms foo 0\r\n\r\n').value, '')
    -- The command line may end with a bare \n.
    t.assert_equals(parse1('ms foo 2\nhi\r\n').value, 'hi')
end

g.test_ms_base64_key = function()
    local req = parse1('ms 44OG44K544OI 2 b k\r\npo\r\n')
    t.assert_equals(req.key, '\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88')
    t.assert_equals(req.meta.base64, true)
    local s = 'ms abc 2 b\r\nhi\r\n'
    local err, pos = parse_err(s)
    t.assert_equals({err, pos}, {E_KEY_DECODE, #s + 1})
end

g.test_ms_bad_data_chunk = function()
    -- memcached reads <datalen> + 2 bytes and checks the last two.
    local s = 'ms foo 2\r\nhix\r\nmn\r\n'
    local err, pos = parse_err(s)
    t.assert_equals(err, E_CHUNK)
    t.assert_equals(pos, #'ms foo 2\r\nhix\r' + 1)
    err, pos = parse_err('ms foo 2\r\nhi\n\n')
    t.assert_equals({err, pos}, {E_CHUNK, 15})
    err, pos = parse_err('ms foo 2\r\nhi\r\r')
    t.assert_equals({err, pos}, {E_CHUNK, 15})
end

g.test_ms_bad_datalen = function()
    -- Without a valid <datalen> nothing is swallowed.
    for _, line in ipairs({
        'ms',
        'ms foo',
        'ms foo abc',
        'ms foo -1',
        'ms foo 1.5',
        'ms foo 99999999999',
        'ms foo 2147483646',
        'ms foo 0x10',
    }) do
        local s = line .. '\r\nhi\r\n'
        local err, pos = parse_err(s)
        t.assert_equals({err, pos}, {E_FORMAT, #line + 3}, line)
    end
end

g.test_ms_errors_swallow_data = function()
    local cases = {
        {'ms foo 2 zz', E_FLAG},
        {'ms foo 2 v v', E_DUPLICATE},
        {'ms foo 2 Tx', E_TOKEN},
        {'ms foo 2 Cx', E_TOKEN},
        {'ms foo 2 Ex', E_TOKEN},
        {'ms foo 2 Fx', E_FORMAT},
        {'ms foo 2 O' .. ('o'):rep(32), E_OPAQUE},
        {'ms ' .. ('k'):rep(251) .. ' 2', E_FORMAT},
        {'ms foo 2' .. (' v'):rep(18), E_TOO_MANY},
    }
    for _, c in ipairs(cases) do
        local out, rest = feed({c[1] .. '\r\nhi\r\nmn\r\n'})
        t.assert_equals(
            out,
            {{err = c[2]}, {op = 'mn', meta = {cmd = 'mn'}}},
            c[1]
        )
        t.assert_equals(rest, '')
    end
    -- 17 flags after <datalen> (20 tokens) are still fine.
    local req = parse1(
        'ms foo 2 c C1 E1 F1 I k O1 q s T1 MS N1 P L h l t\r\nhi\r\n'
    )
    t.assert_equals(req.meta.ret, {'c', 'k', 'O1', 's'})
end

g.test_ms_error_swallows_ahead = function()
    -- The error comes as soon as the line is there; next_pos is past the data
    -- block, which may not have arrived yet.
    local s = 'ms foo 5 zz\r\nhello\r\nmn\r\n'
    local line = #'ms foo 5 zz\r\n'
    for k = 0, line - 1 do
        t.assert_equals(
            {meta.parse(s:sub(1, k), 1)},
            {nil, 1},
            'split at ' .. k
        )
    end
    for k = line, #s do
        local req, pos, err = meta.parse(s:sub(1, k), 1)
        t.assert_equals(
            {req, pos, err},
            {false, line + 8, E_FLAG},
            'split at ' .. k
        )
    end
    -- Every split into two reads gives the error and then mn.
    local mn = {op = 'mn', meta = {cmd = 'mn'}}
    for k = 0, #s do
        local out, rest = feed({s:sub(1, k), s:sub(k + 1)})
        t.assert_equals(out, {{err = E_FLAG}, mn}, 'split at ' .. k)
        t.assert_equals(rest, '')
    end
end

g.test_ms_too_large = function()
    -- The item size as memcached counts it (proto/text.lua item_size()):
    -- 66 for a 3-byte key and 4 bytes, the key decoded with b.
    local opts = {max_item_size = 66}
    t.assert_equals(parse1('ms foo 4\r\nabcd\r\n', opts).value, 'abcd')
    t.assert_equals(
        parse1('ms foo 4 F1\r\nabcd\r\n', opts).meta.too_large,
        true
    )
    t.assert_equals(parse1('ms Zm9v 4 b\r\nabcd\r\n', opts).value, 'abcd')
    -- A store that doesn't fit removes the old value (as memcached), so it
    -- is parsed to a delete answered with the error, in any mode, quiet too.
    local s = 'ms foo 5\r\nabcde\r\nmn\r\n'
    local req, pos = meta.parse(s, 1, opts)
    t.assert_equals(pos, #s - 3)
    t.assert_equals(
        {req.op, req.key, req.meta.too_large},
        {'delete', 'foo', true}
    )
    t.assert_equals(meta.encode(req, {status = 'DELETED'}), E_TOO_LARGE)
    req = meta.parse('ms Zm9v 5 b q MA\r\nabcde\r\n', 1, opts)
    t.assert_equals({req.op, req.key}, {'delete', 'foo'})
    t.assert_equals(meta.encode(req, {status = 'NOT_FOUND'}), E_TOO_LARGE)
    local err
    -- Flag errors come first.
    err = parse_err('ms foo 5 zz\r\nabcde\r\n', opts)
    t.assert_equals(err, E_FLAG)
    err = parse_err('ms foo 5 MZ\r\nabcde\r\n', opts)
    t.assert_equals(err, E_MS_MODE)
    -- The default limit is 1 MiB, with the item header: 1048514 bytes for
    -- a 3-byte key.
    local big = ('x'):rep(1048514)
    t.assert_equals(#parse1('ms foo 1048514\r\n' .. big .. CRLF).value, 1048514)
    req = meta.parse('ms foo 1048515\r\n' .. big .. 'x' .. CRLF, 1)
    t.assert_equals(req.meta.too_large, true)
    -- A too large block isn't buffered: it is skipped as it arrives.
    local next_pos
    req, next_pos = meta.parse('ms foo 1000000000\r\n', 1)
    t.assert_equals({req.op, next_pos}, {
        'delete',
        #'ms foo 1000000000\r\n' + 1000000000 + 3,
    })
end

g.test_ms_partial_every_split = function()
    local data = 'a\r\nb\r\n'
    local s = 'ms foo 6 T30 c\r\n' .. data .. CRLF
    local line = #'ms foo 6 T30 c\r\n'
    for k = 0, #s - 1 do
        local req, pos, need = meta.parse(s:sub(1, k), 1)
        t.assert_equals(req, nil, 'split at ' .. k)
        t.assert_equals(pos, 1, 'split at ' .. k)
        if k >= line then
            t.assert_equals(need, #s, 'split at ' .. k)
        end
    end
    local req = parse1(s)
    t.assert_equals(req.value, data)
    t.assert_equals(req.exptime, 30)
    -- Every split into two reads gives the same request.
    for k = 0, #s do
        local out, rest = feed({s:sub(1, k), s:sub(k + 1)})
        t.assert_equals(out, {req}, 'split at ' .. k)
        t.assert_equals(rest, '')
    end
end

--
-- md
--

g.test_md_basic = function()
    t.assert_equals(parse1('md foo\r\n'), {
        op = 'delete',
        key = 'foo',
        meta = {
            cmd = 'md',
            ret = {},
        },
    })
end

g.test_md_all_flags = function()
    local req = parse1('md foo C5 E6 I T30 k Oo q x F7 P1 Lx\r\n')
    t.assert_equals(req, {
        op = 'delete',
        key = 'foo',
        cas = 5ULL,
        invalidate = true,
        ttl = 30,
        clear_value = true,
        flags = 7,
        new_cas = 6ULL,
        meta = {cmd = 'md', quiet = true, ret = {'k', 'Oo'}},
    })
    t.assert(is_u64(req.cas))
end

g.test_md_ttl = function()
    -- T only works with I or x.
    t.assert_equals(parse1('md foo T30\r\n').ttl, nil)
    t.assert_equals(parse1('md foo T30 I\r\n').ttl, 30)
    t.assert_equals(parse1('md foo T30 x\r\n').ttl, 30)
    -- md x without T: the new item has no TTL.
    local req = parse1('md foo x\r\n')
    t.assert_equals({req.clear_value, req.flags, req.ttl}, {true, 0, 0})
    t.assert_equals(parse1('md foo x I\r\n').ttl, 0)
    t.assert_equals(parse1('md foo I\r\n').ttl, nil)
    req = parse1('md Zm9v b k\r\n')
    t.assert_equals({req.key, req.meta.base64}, {'foo', true})
end

g.test_md_errors = function()
    assert_err('md', E_FORMAT)
    assert_err('md foo zz', E_FLAG)
    assert_err('md foo q q', E_DUPLICATE)
    assert_err('md foo Cx', E_TOKEN)
    assert_err('md foo Tx', E_TOKEN)
    assert_err('md foo O' .. ('o'):rep(32), E_OPAQUE)
    assert_err('md abc b', E_KEY_DECODE)
    assert_err('md ' .. ('k'):rep(251), E_FORMAT)
end

--
-- ma
--

g.test_ma_basic = function()
    local req = parse1('ma foo\r\n')
    t.assert_equals(req, {
        op = 'arith',
        key = 'foo',
        incr = true,
        delta = 1ULL,
        meta = {
            cmd = 'ma',
            ret = {},
        },
    })
    t.assert(is_u64(req.delta))
end

g.test_ma_all_flags = function()
    local req = parse1('ma foo N0 J13 D30 MD T300 C5 E6 q v t c k Oo P1 Lx\r\n')
    t.assert_equals(req, {
        op = 'arith',
        key = 'foo',
        incr = false,
        delta = 30ULL,
        initial = 13ULL,
        vivify_ttl = 0,
        ttl = 300,
        cas = 5ULL,
        new_cas = 6ULL,
        meta = {
            cmd = 'ma',
            quiet = true,
            value = true,
            ret = {'t', 'c', 'k', 'Oo'},
        },
    })
    for _, f in ipairs({'delta', 'initial', 'cas', 'new_cas'}) do
        t.assert(is_u64(req[f]), f)
    end
end

g.test_ma_modes = function()
    for m, incr in pairs({
        I = true,
        ['+'] = true,
        D = false,
        ['-'] = false,
        Ifoo = true,
    }) do
        t.assert_equals(parse1('ma foo M' .. m .. CRLF).incr, incr, m)
    end
    assert_err('ma foo MZ', E_MA_MODE)
    assert_err('ma foo Mi', E_MA_MODE)
    assert_err('ma foo M', E_MA_MODE)
end

g.test_ma_initial = function()
    -- J only matters with N; the initial value defaults to 0.
    t.assert_equals(parse1('ma foo J5\r\n').initial, nil)
    local req = parse1('ma foo N10\r\n')
    t.assert_equals({req.initial, req.vivify_ttl}, {0ULL, 10})
    t.assert(is_u64(req.initial))
end

g.test_ma_uint64 = function()
    local req = parse1(
        'ma foo D18446744073709551615 N0 J18446744073709551614\r\n'
    )
    t.assert_equals(req.delta, U64_MAX)
    t.assert_equals(tostring(req.initial), '18446744073709551614ULL')
    t.assert(is_u64(req.delta) and is_u64(req.initial))
end

g.test_ma_errors = function()
    -- memcached gives one message for every flag error of ma.
    for _, f in ipairs({
        'zz',
        'v v',
        'Dx',
        'D',
        'D18446744073709551616',
        'Jx',
        'Nx',
        'Cx',
        'Ex',
        'Tx',
        'Fx',
    }) do
        assert_err('ma foo ' .. f, E_MA_FLAG)
    end
    assert_err('ma abc b', E_MA_FLAG)
    assert_err('ma ' .. ('k'):rep(251), E_MA_FLAG)
    assert_err('ma foo' .. (' v'):rep(19), E_MA_FLAG)
    assert_err('ma', E_FORMAT)
    assert_err('ma foo O' .. ('o'):rep(32), E_OPAQUE)
end

--
-- mn, me and other commands
--

g.test_mn = function()
    t.assert_equals(parse1('mn\r\n'), {op = 'mn', meta = {cmd = 'mn'}})
    t.assert_equals(parse1('mn\r\n').op, 'mn')
    -- Shorter than 4 bytes (memcached 1.6.45).
    t.assert_equals({meta.parse('mn\n', 1)}, {false, 4, E_FORMAT})
    -- mn takes no arguments; memcached ignores anything after it.
    t.assert_equals(parse1('mn foo q\r\n').op, 'mn')
end

g.test_me = function()
    t.assert_equals(parse1('me foo\r\n'), {
        op = 'get',
        key = 'foo',
        bump_la = false,
        no_lru_bump = true,
        peek = true,
        meta = {
            cmd = 'me',
        },
    })
    local req = parse1('me Zm9v b\r\n')
    t.assert_equals({req.key, req.meta.base64}, {'foo', true})
    -- Other tokens are ignored, like in memcached.
    t.assert_equals(parse1('me foo zz yy\r\n').key, 'foo')
    assert_err('me', E_FORMAT)
    assert_err('me abc b', E_FORMAT)
    assert_err('me ' .. ('k'):rep(251), E_FORMAT)
end

g.test_unknown_commands = function()
    for _, line in ipairs(
        {'mx foo', 'mgx foo', 'm foo', 'get foo', '   ', 'MG foo'}
    ) do
        assert_err(line, 'ERROR\r\n')
    end
end

g.test_need_more_data = function()
    for _, s in ipairs({
        '',
        'm',
        'mg',
        'mg foo v',
        'mg foo v\r',
        'ms foo 2\r\n',
        'ms foo 2\r\nhi',
        'ms foo 2\r\nhi\r',
        'mn',
    }) do
        local req, pos = meta.parse(s, 1)
        t.assert_equals({req, pos}, {nil, 1}, s)
    end
end

g.test_pos_argument = function()
    local s = 'xxxxmg foo v\r\nms bar 2\r\nhi\r\n'
    local req, pos = meta.parse(s, 5)
    t.assert_equals({req.key, pos}, {'foo', 15})
    req, pos = meta.parse(s, pos)
    t.assert_equals({req.key, req.value, pos}, {'bar', 'hi', #s + 1})
    t.assert_equals({meta.parse(s, pos)}, {nil, pos})
    -- pos defaults to 1.
    t.assert_equals(meta.parse('mn\r\n').op, 'mn')
end

g.test_line_too_long = function()
    -- memcached closes the connection without a \n in 2048 bytes.
    local s = 'mg ' .. ('k'):rep(3000)
    local req, pos, err, close = meta.parse(s, 1)
    t.assert_equals({req, pos, err, close}, {false, #s + 1, nil, true})
    -- Not before.
    local line = 'mg foo ' .. ('P'):rep(2041)
    t.assert_equals(#line, 2048)
    t.assert_equals({meta.parse(line, 1)}, {nil, 1})
    t.assert_equals(parse1(line .. CRLF).key, 'foo')
    t.assert_equals((meta.parse(line .. 'P', 1)), false)
end

-- compat meta_long_line: a line over 2048 bytes whose \n is already in the
-- buffer is processed, as in memcached's try_read_command_ascii.
g.test_long_line_with_newline = function()
    local s = 'mg ' .. ('k'):rep(3000) .. ' v'
    t.assert_equals(
        {meta.parse(s .. CRLF .. 'mn\r\n', 1)},
        {false, #s + 3, E_FORMAT}
    )
    local line = 'mg foo ' .. ('P'):rep(2042)
    t.assert_equals(parse1(line .. CRLF).key, 'foo')
    -- ms too: the data block follows as usual.
    local ms = 'ms ' .. ('k'):rep(3000) .. ' 2\r\nhi\r\n'
    t.assert_equals({meta.parse(ms, 1)}, {false, #ms + 1, E_FORMAT})
end

g.test_err_line_ends_with_crlf = function()
    -- As in proto/text.lua: the error line is sent as is.
    local _, _, err = meta.parse('mg foo zz\r\n', 1)
    t.assert_equals(err, 'CLIENT_ERROR invalid flag\r\n')
    local _, _, err2 = meta.parse('ms foo 2 Fx\r\nhi\r\n', 1)
    t.assert_equals(err2, 'CLIENT_ERROR bad command line format\r\n')
end

--
-- Pipelining
--

local PIPELINE = table.concat({
    'mg a v\r\n',
    'ms b 3 T5\r\nx\r\n\r\n',
    'md c q\r\n',
    'ma d MD D2\r\n',
    'mg e zz\r\n',
    'ms f 2 zz\r\nhi\r\n',
    'me g\r\n',
    'mn\r\n',
})

local function pipeline_shape(out)
    local shape = {}
    for _, r in ipairs(out) do
        shape[#shape + 1] = r.err or (r.op .. ' ' .. (r.key or ''))
    end
    return shape
end

g.test_pipeline_one_buffer = function()
    local out, rest = feed({PIPELINE})
    t.assert_equals(rest, '')
    t.assert_equals(pipeline_shape(out), {
        'get a',
        'store b',
        'delete c',
        'arith d',
        E_FLAG,
        E_FLAG,
        'get g',
        'mn ',
    })
    t.assert_equals(out[2].value, 'x\r\n')
    t.assert_equals(out[4].delta, 2ULL)
    t.assert_equals(out[4].incr, false)
end

g.test_pipeline_every_split = function()
    local expected = feed({PIPELINE})
    for k = 0, #PIPELINE do
        local out, rest = feed({PIPELINE:sub(1, k), PIPELINE:sub(k + 1)})
        t.assert_equals(out, expected, 'split at ' .. k)
        t.assert_equals(rest, '')
    end
    local chunks = {}
    for i = 1, #PIPELINE do
        chunks[i] = PIPELINE:sub(i, i)
    end
    local out, rest = feed(chunks)
    t.assert_equals(out, expected, 'byte by byte')
    t.assert_equals(rest, '')
end

g.test_mg_partial_every_split = function()
    local s = 'mg foo s v t Oabc\r\nmn\r\n'
    local first = #'mg foo s v t Oabc\r\n'
    for k = 0, #s do
        local req, pos = meta.parse(s:sub(1, k), 1)
        if k < first then
            t.assert_equals({req, pos}, {nil, 1}, 'split at ' .. k)
        else
            t.assert_equals(
                {req.key, pos},
                {'foo', first + 1},
                'split at ' .. k
            )
        end
    end
end

--
-- Encoding: mg
--

g.test_encode_mg_hit = function()
    local resp = {
        status = 'HIT',
        item = item({
            value = 'hi',
            flags = 5,
            exptime = NOW + 30,
            cas = 123ULL,
            last_access = NOW - 10,
            fetched = true,
        }),
    }
    t.assert_equals(
        reply('mg foo s t c f l h k Oop v', resp),
        'VA 2 s2 t30 c123 f5 l10 h1 kfoo Oop\r\nhi\r\n'
    )
    -- The flags come back in request order.
    t.assert_equals(
        reply('mg foo Oop k v c', resp),
        'VA 2 Oop kfoo c123\r\nhi\r\n'
    )
    -- Without v there is no value.
    t.assert_equals(reply('mg foo s', resp), 'HD s2\r\n')
    t.assert_equals(reply('mg foo', resp), 'HD\r\n')
    -- Flags that only modify the item add nothing.
    t.assert_equals(reply('mg foo T30 N30 R30 E5 u q P1 Lx', resp), 'HD\r\n')
end

g.test_encode_mg_values = function()
    local function hit(fields)
        return {status = 'HIT', item = item(fields)}
    end
    t.assert_equals(reply('mg foo t', hit({exptime = 0})), 'HD t-1\r\n')
    t.assert_equals(
        reply('mg foo t', hit({exptime = NOW + 29.2})),
        'HD t30\r\n'
    )
    t.assert_equals(reply('mg foo t', hit({exptime = NOW - 1})), 'HD t0\r\n')
    t.assert_equals(reply('mg foo h', hit({fetched = false})), 'HD h0\r\n')
    t.assert_equals(
        reply('mg foo l', hit({last_access = NOW - 2.5})),
        'HD l2\r\n'
    )
    t.assert_equals(
        reply('mg foo l', hit({last_access = NOW + 5})),
        'HD l0\r\n'
    )
    t.assert_equals(
        reply('mg foo f', hit({flags = 4294967295})),
        'HD f4294967295\r\n'
    )
    t.assert_equals(
        reply('mg foo c', hit({cas = U64_MAX})),
        'HD c18446744073709551615\r\n'
    )
    -- msgpack gives small CAS values back as Lua numbers.
    t.assert_equals(reply('mg foo c', hit({cas = 42})), 'HD c42\r\n')
    t.assert_equals(reply('mg foo s v', hit({value = ''})), 'VA 0 s0\r\n\r\n')
end

g.test_encode_mg_binary_value = function()
    local value = 'a\r\nEN\r\n\0'
    local resp = {status = 'HIT', item = item({value = value})}
    t.assert_equals(reply('mg foo s v', resp), 'VA 8 s8\r\n' .. value .. '\r\n')
end

g.test_encode_mg_item_array = function()
    -- The item may come as an array in the space format order.
    local resp = {
        status = 'HIT',
        item = {'foo', 'hey', 7, NOW + 60, 99ULL, NOW - 5, NOW - 3, true, false},
    }
    t.assert_equals(
        reply('mg foo s t c f l h v', resp),
        'VA 3 s3 t60 c99 f7 l3 h1\r\nhey\r\n'
    )
end

g.test_encode_mg_compare_cas = function()
    local resp = {status = 'HIT', item = item({cas = 77ULL})}
    -- C matches: no value, HD.
    t.assert_equals(reply('mg foo C77 v c', resp), 'HD c77\r\n')
    -- C doesn't match: the value.
    t.assert_equals(reply('mg foo C99999 v c', resp), 'VA 2 c77\r\nhi\r\n')
    -- The engine may give the CAS as a Lua number.
    t.assert_equals(
        reply('mg foo C77 v', {
            status = 'HIT',
            item = item({
                cas = 77,
            }),
        }),
        'HD\r\n'
    )
    t.assert_equals(
        reply('mg foo C9007199254740993 v', {
            status = 'HIT',
            item = item({cas = 9007199254740992}),
        }),
        'VA 2\r\nhi\r\n'
    )
end

g.test_encode_mg_win_stale = function()
    local it = item()
    t.assert_equals(
        reply('mg foo c', {status = 'HIT', item = it, won = true}),
        'HD c1 W\r\n'
    )
    t.assert_equals(
        reply('mg foo', {
            status = 'HIT',
            item = it,
            already_won = true,
        }),
        'HD Z\r\n'
    )
    t.assert_equals(
        reply('mg foo', {
            status = 'HIT',
            item = it,
            stale = true,
            won = true,
        }),
        'HD X W\r\n'
    )
    t.assert_equals(
        reply('mg foo', {
            status = 'HIT',
            item = it,
            stale = true,
            already_won = true,
        }),
        'HD Z X\r\n'
    )
    -- Z, X, W go after the requested flags and before the value.
    t.assert_equals(
        reply('mg foo s v Oo', {
            status = 'HIT',
            item = it,
            stale = true,
            won = true,
        }),
        'VA 2 s2 Oo X W\r\nhi\r\n'
    )
    -- Without resp.stale the item's stale field counts.
    t.assert_equals(
        reply('mg foo', {
            status = 'HIT',
            item = item({stale = true}),
        }),
        'HD X\r\n'
    )
    t.assert_equals(
        reply('mg foo', {
            status = 'HIT',
            item = item({stale = true}),
            stale = false,
        }),
        'HD\r\n'
    )
end

g.test_encode_mg_vivify = function()
    -- From memcached's t/metaget.t: a miss with N creates an empty item and
    -- wins; q doesn't hide it.
    local resp = {
        status = 'HIT',
        won = true,
        item = item({
            value = '',
            exptime = NOW + 30,
        }),
    }
    t.assert_equals(
        reply('mg needwin s c v N30 t', resp),
        'VA 0 s0 c1 t30 W\r\n\r\n'
    )
    t.assert_equals(reply('mg quietautov s N30 t q', resp), 'HD s0 t30 W\r\n')
end

g.test_encode_mg_miss = function()
    t.assert_equals(reply('mg foo v', {status = 'MISS'}), 'EN\r\n')
    -- O and k come back on a miss too, as memcached does.
    t.assert_equals(
        reply('mg foo s v k Oo t', {status = 'MISS'}),
        'EN kfoo Oo\r\n'
    )
    t.assert_equals(reply('mg Zm9v b k', {status = 'MISS'}), 'EN kZm9v b\r\n')
end

g.test_encode_mg_quiet = function()
    t.assert_equals(reply('mg foo v q', {status = 'MISS'}), '')
    t.assert_equals(reply('mg foo v q k Oo', {status = 'MISS'}), '')
    -- Hits are not hidden.
    local resp = {status = 'HIT', item = item({value = 'bo'})}
    t.assert_equals(reply('mg foo v q', resp), 'VA 2\r\nbo\r\n')
    t.assert_equals(reply('mg foo t q O4', resp), 'HD t-1 O4\r\n')
    -- Errors are not hidden.
    t.assert_equals(
        reply('mg foo v q', {
            status = 'SERVER_ERROR',
            message = 'read only',
        }),
        'SERVER_ERROR read only\r\n'
    )
end

g.test_encode_mg_base64_key = function()
    local resp = {status = 'HIT', item = item({key = '\0\1\2'})}
    local req = parse1('mg ' .. meta.b64encode('\0\1\2') .. ' b k v\r\n')
    t.assert_equals(enc(req, resp), 'VA 2 kAAEC b\r\nhi\r\n')
    -- The key comes back in canonical base64.
    t.assert_equals(
        reply('mg Zm9v! b k', {status = 'HIT', item = item()}),
        'HD kZm9v b\r\n'
    )
end

g.test_encode_quiet_pipeline = function()
    -- From memcached's t/metaget.t: hits, a quiet miss, then mn.
    local hit = {status = 'HIT', item = item({value = 'bo'})}
    local out, rest = feed(
        {'mg quiet v q\r\nmg quiet v q\r\nmg quietmiss v q\r\nmn\r\n'}
    )
    t.assert_equals(rest, '')
    local resps = {hit, hit, {status = 'MISS'}, nil}
    local wire = {}
    for i, req in ipairs(out) do
        wire[i] = enc(req, resps[i])
    end
    t.assert_equals(table.concat(wire), 'VA 2\r\nbo\r\nVA 2\r\nbo\r\nMN\r\n')
    -- From memcached's t/proxyinternal.t: the miss of a non-quiet mg in the
    -- middle.
    out = feed({'mg q1 v t q O1\r\nmg q2 v t O2\r\nmg q3 v t q O3\r\nmn\r\n'})
    wire = {}
    for i, req in ipairs(out) do
        wire[i] = enc(req, i < 4 and {status = 'MISS'} or nil)
    end
    t.assert_equals(table.concat(wire), 'EN O2\r\nMN\r\n')
end

--
-- Encoding: ms
--

g.test_encode_ms = function()
    t.assert_equals(
        reply('ms foo 2\r\nhi', {status = 'STORED', cas = 5ULL}),
        'HD\r\n'
    )
    t.assert_equals(reply('ms foo 2\r\nhi', {status = 'NOT_STORED'}), 'NS\r\n')
    t.assert_equals(reply('ms foo 2\r\nhi', {status = 'EXISTS'}), 'EX\r\n')
    t.assert_equals(reply('ms foo 2\r\nhi', {status = 'NOT_FOUND'}), 'NF\r\n')
    -- From memcached's t/metaget.t: flag reflection.
    local ok = {status = 'STORED', cas = 18446744073709551615ULL}
    t.assert_equals(reply('ms a 1 c\r\n1', ok), 'HD c18446744073709551615\r\n')
    t.assert_equals(reply('ms a 1 Otest\r\n1', ok), 'HD Otest\r\n')
    t.assert_equals(reply('ms a 1 s\r\n1', ok), 'HD s1\r\n')
    t.assert_equals(reply('ms a 1 k\r\n1', ok), 'HD ka\r\n')
    t.assert_equals(
        reply('ms a 1 s k Oo c T5 F1\r\n1', ok),
        'HD s1 ka Oo c18446744073709551615\r\n'
    )
    -- c0 unless stored.
    t.assert_equals(
        reply('ms msetcas 2 c C5\r\nio', {
            status = 'EXISTS',
            cas = 7ULL,
        }),
        'EX c0\r\n'
    )
    t.assert_equals(
        reply('ms foo 2 c k Oo s\r\nio', {status = 'NOT_STORED'}),
        'NS c0 kfoo Oo s2\r\n'
    )
    -- The size after append.
    t.assert_equals(
        reply('ms appendviv 2 MA N30 s\r\nko', {
            status = 'STORED',
            size = 4,
        }),
        'HD s4\r\n'
    )
    t.assert_equals(reply('ms Zm9v 2 b k\r\nhi', ok), 'HD kZm9v b\r\n')
end

g.test_encode_ms_quiet = function()
    t.assert_equals(reply('ms foo 2 q\r\nhi', {status = 'STORED'}), '')
    t.assert_equals(
        reply('ms foo 2 q c k\r\nhi', {
            status = 'STORED',
            cas = 1ULL,
        }),
        ''
    )
    t.assert_equals(
        reply('ms foo 2 q\r\nhi', {status = 'NOT_STORED'}),
        'NS\r\n'
    )
    t.assert_equals(reply('ms foo 2 q\r\nhi', {status = 'EXISTS'}), 'EX\r\n')
    t.assert_equals(
        reply('ms foo 2 q Oo\r\nhi', {status = 'NOT_FOUND'}),
        'NF Oo\r\n'
    )
    t.assert_equals(
        reply('ms foo 2 q\r\nhi', {
            status = 'SERVER_ERROR',
            message = 'Failed to write to disk',
        }),
        'SERVER_ERROR Failed to write to disk\r\n'
    )
end

--
-- Encoding: md
--

g.test_encode_md = function()
    t.assert_equals(reply('md foo', {status = 'DELETED'}), 'HD\r\n')
    t.assert_equals(reply('md foo', {status = 'NOT_FOUND'}), 'NF\r\n')
    t.assert_equals(reply('md foo', {status = 'EXISTS'}), 'EX\r\n')
    -- O and k with every code.
    t.assert_equals(
        reply('md foo k Oo C5', {status = 'DELETED'}),
        'HD kfoo Oo\r\n'
    )
    t.assert_equals(
        reply('md foo Oo k', {status = 'NOT_FOUND'}),
        'NF Oo kfoo\r\n'
    )
    t.assert_equals(reply('md foo Oo', {status = 'EXISTS'}), 'EX Oo\r\n')
    t.assert_equals(
        reply('md Zm9v b k', {status = 'DELETED'}),
        'HD kZm9v b\r\n'
    )
end

g.test_encode_md_quiet = function()
    t.assert_equals(reply('md foo q', {status = 'DELETED'}), '')
    t.assert_equals(reply('md foo q I T30', {status = 'DELETED'}), '')
    -- memcached hides only HD: NF and EX still come back.
    t.assert_equals(reply('md foo q', {status = 'NOT_FOUND'}), 'NF\r\n')
    t.assert_equals(reply('md foo q Oo', {status = 'EXISTS'}), 'EX Oo\r\n')
    t.assert_equals(
        reply('md foo q', {status = 'SERVER_ERROR', message = 'x'}),
        'SERVER_ERROR x\r\n'
    )
end

--
-- Encoding: ma
--

g.test_encode_ma = function()
    t.assert_equals(
        reply('ma foo', {
            status = 'STORED',
            value = 1ULL,
            cas = 2ULL,
        }),
        'HD\r\n'
    )
    t.assert_equals(
        reply('ma foo v', {status = 'STORED', value = 13ULL}),
        'VA 2\r\n13\r\n'
    )
    t.assert_equals(
        reply('ma foo v c k Oo', {
            status = 'STORED',
            value = 0ULL,
            cas = 9ULL,
        }),
        'VA 1 c9 kfoo Oo\r\n0\r\n'
    )
    t.assert_equals(
        reply('ma foo v', {status = 'STORED', value = U64_MAX}),
        'VA 20\r\n18446744073709551615\r\n'
    )
    -- msgpack may give small values back as Lua numbers.
    t.assert_equals(
        reply('ma foo v c', {status = 'STORED', value = 44, cas = 3}),
        'VA 2 c3\r\n44\r\n'
    )
    t.assert_equals(
        reply('ma foo Oo k c t v', {status = 'NOT_FOUND'}),
        'NF Oo kfoo\r\n'
    )
    t.assert_equals(reply('ma foo N0 Oo', {status = 'NOT_STORED'}), 'NS Oo\r\n')
    t.assert_equals(reply('ma foo C5 v', {status = 'EXISTS'}), 'EX\r\n')
    t.assert_equals(
        reply('ma Zm9v b k', {status = 'STORED', value = 1ULL}),
        'HD kZm9v b\r\n'
    )
end

g.test_encode_ma_ttl = function()
    t.assert_equals(
        reply('ma foo t', {
            status = 'STORED',
            value = 1ULL,
            exptime = NOW + 300,
        }),
        'HD t300\r\n'
    )
    t.assert_equals(
        reply('ma foo t', {
            status = 'STORED',
            value = 1ULL,
            exptime = 0,
        }),
        'HD t-1\r\n'
    )
    -- Without resp.exptime the TTL comes from T.
    t.assert_equals(
        reply('ma foo T300 t', {status = 'STORED', value = 1ULL}),
        'HD t300\r\n'
    )
    t.assert_equals(
        reply('ma foo T0 t', {status = 'STORED', value = 1ULL}),
        'HD t-1\r\n'
    )
    t.assert_equals(
        reply('ma foo T-1 t', {status = 'STORED', value = 1ULL}),
        'HD t0\r\n'
    )
    t.assert_equals(
        reply('ma foo T' .. (NOW + 100) .. ' t', {
            status = 'STORED',
            value = 1ULL,
        }),
        'HD t100\r\n'
    )
    t.assert_equals(
        reply('ma foo t', {status = 'STORED', value = 1ULL}),
        'HD t-1\r\n'
    )
end

g.test_encode_ma_quiet = function()
    -- From memcached's t/metaget.t: "ma mi q D1" then mn gives only MN.
    t.assert_equals(reply('ma mi q D1', {status = 'STORED', value = 2ULL}), '')
    -- q hides VA too.
    t.assert_equals(reply('ma foo q v', {status = 'STORED', value = 2ULL}), '')
    t.assert_equals(reply('ma foo q', {status = 'NOT_FOUND'}), 'NF\r\n')
    t.assert_equals(reply('ma foo q', {status = 'NOT_STORED'}), 'NS\r\n')
    t.assert_equals(reply('ma foo q', {status = 'EXISTS'}), 'EX\r\n')
end

g.test_encode_ma_errors = function()
    local non_numeric = 'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n'
    t.assert_equals(reply('ma foo q', {status = 'CLIENT_ERROR'}), non_numeric)
    t.assert_equals(
        reply('ma foo', {
            status = 'CLIENT_ERROR',
            message = 'cannot increment or decrement non-numeric value',
        }),
        non_numeric
    )
    t.assert_equals(
        reply('ma foo', {
            status = 'CLIENT_ERROR',
            message = 'CLIENT_ERROR cannot increment or decrement non-numeric value',
        }),
        non_numeric
    )
    t.assert_equals(
        reply('ma foo q v', {
            status = 'SERVER_ERROR',
            message = 'Transaction conflict',
        }),
        'SERVER_ERROR Transaction conflict\r\n'
    )
end

--
-- Encoding: mn, me, errors
--

g.test_encode_mn = function()
    t.assert_equals(reply('mn', nil), 'MN\r\n')
    t.assert_equals(reply('mn', {status = 'OK'}), 'MN\r\n')
end

g.test_encode_me = function()
    local resp = {
        status = 'HIT',
        item = item({
            value = 'hello',
            exptime = NOW + 60,
            cas = 5ULL,
            last_access = NOW - 3,
            fetched = true,
        }),
    }
    t.assert_equals(
        reply('me foo', resp),
        'ME foo exp=60 la=3 cas=5 fetch=yes cls=1 size=8\r\n'
    )
    resp.item.exptime = 0
    resp.item.fetched = false
    t.assert_equals(
        reply('me foo', resp),
        'ME foo exp=-1 la=3 cas=5 fetch=no cls=1 size=8\r\n'
    )
    t.assert_equals(
        reply('me Zm9v b', resp),
        'ME Zm9v exp=-1 la=3 cas=5 fetch=no cls=1 size=8\r\n'
    )
    t.assert_equals(reply('me foo', {status = 'MISS'}), 'EN\r\n')
    -- Answered by the front-end without the engine: always a miss.
    t.assert_equals(reply('me foo', nil), 'EN\r\n')
    t.assert_equals(
        reply('me foo', {status = 'SERVER_ERROR', message = 'x'}),
        'SERVER_ERROR x\r\n'
    )
end

g.test_encode_errors = function()
    t.assert_equals(
        reply('mg foo', {status = 'SERVER_ERROR'}),
        'SERVER_ERROR\r\n'
    )
    t.assert_equals(
        reply('mg foo', {
            status = 'SERVER_ERROR',
            message = 'SERVER_ERROR out of memory',
        }),
        'SERVER_ERROR out of memory\r\n'
    )
    t.assert_equals(
        reply('md foo', {status = 'BOGUS'}),
        'SERVER_ERROR unexpected engine status BOGUS\r\n'
    )
    t.assert_equals(reply('mg foo', nil), 'SERVER_ERROR no response\r\n')
end

g.test_encode_default_clock = function()
    -- Without opts.now the clock is fiber.time().
    local req = parse1('mg foo t\r\n')
    local resp = {
        status = 'HIT',
        item = item({
            exptime = require('fiber').time() + 100,
        }),
    }
    local out = meta.encode(req, resp)
    local ttl = tonumber(out:match('^HD t(%d+)\r\n$'))
    t.assert(ttl ~= nil and ttl >= 99 and ttl <= 100, out)
end

--
-- detect
--

g.test_detect = function()
    for _, s in ipairs(
        {'mg foo', 'ms foo 2', 'md foo', 'ma foo', 'mn\r\n', 'mn\n', 'me foo'}
    ) do
        t.assert_equals(meta.detect(s, 1), true, s)
    end
    for _, s in ipairs({'get foo', 'mx foo', 'mgx', 'm foo', 'MG foo', 'stats'}) do
        t.assert_equals(meta.detect(s, 1), false, s)
    end
    for _, s in ipairs({'', 'm', 'mg', 'mn'}) do
        t.assert_equals(meta.detect(s, 1), nil, s)
    end
    t.assert_equals(meta.detect('get a\r\nmg b', 8), true)
end
