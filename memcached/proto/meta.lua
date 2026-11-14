-- memcached meta protocol: mg, ms, md, ma, mn and me (memcached 1.6).
--
-- parse(buf, pos[, opts]) turns one command into an engine request (see
-- "Engine contract" in DESIGN.md), and encode(req, resp[, opts]) turns the
-- engine response into the reply. The semantics, including the error strings,
-- follow memcached's proto_parser.c.
--
-- This module may run in an application thread: no box.* here, only plain
-- Lua and string operations. 64-bit values (CAS, delta, initial value) are
-- uint64_t cdata, never Lua numbers.

local bit = require('bit')
local fiber = require('fiber')

local band, bor, lshift, rshift = bit.band, bit.bor, bit.lshift, bit.rshift
local byte, char, find, format, sub = string.byte,
    string.char, string.find,
    string.format, string.sub
local concat = table.concat
local ceil, floor = math.ceil, math.floor

local M = {}

M.MAX_KEY_LENGTH = 250   -- KEY_MAX_LENGTH
M.MAX_LINE_LENGTH = 2048 -- memcached closes on a longer line without \n
M.MAX_TOKENS = 20        -- MFLAG_MAX_OPT_LENGTH, command and key included
M.MAX_OPAQUE_LENGTH = 32 -- MFLAG_MAX_OPAQUE_LENGTH, the 'O' included
M.DEFAULT_MAX_ITEM_SIZE = 1048576

local MAX_KEY_LENGTH = M.MAX_KEY_LENGTH
local MAX_LINE_LENGTH = M.MAX_LINE_LENGTH
local MAX_TOKENS = M.MAX_TOKENS
local MAX_OPAQUE_LENGTH = M.MAX_OPAQUE_LENGTH
local DEFAULT_MAX_ITEM_SIZE = M.DEFAULT_MAX_ITEM_SIZE
local MAX_DATA_LENGTH = 2147483645                    -- INT_MAX - 2

-- Error lines returned by parse(), with the \r\n as in proto/text.lua.
local E_UNKNOWN = 'ERROR\r\n'
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

-- The default message of an arith CLIENT_ERROR response.
local NON_NUMERIC = 'cannot increment or decrement non-numeric value'

--
-- base64, as memcached's base64.c: the decoder skips characters outside the
-- alphabet, needs a multiple of 4 alphabet characters and stops at the first
-- padded block.
--

local B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
local B64_ENC = {}
local B64_DEC = {}
for i = 0, 63 do
    B64_ENC[i] = sub(B64, i + 1, i + 1)
    B64_DEC[byte(B64, i + 1)] = i
end
B64_DEC[61] = 0 -- '='

local function b64encode(s)
    local out = {}
    local n = #s
    local i = 1
    while i + 2 <= n do
        local a, b, c = byte(s, i, i + 2)
        out[#out + 1] = B64_ENC[rshift(a, 2)]
            .. B64_ENC[bor(lshift(band(a, 3), 4), rshift(b, 4))]
            .. B64_ENC[bor(lshift(band(b, 15), 2), rshift(c, 6))]
            .. B64_ENC[band(c, 63)]
        i = i + 3
    end
    local rest = n - i + 1
    if rest == 1 then
        local a = byte(s, i)
        out[#out + 1] = B64_ENC[rshift(a, 2)] .. B64_ENC[lshift(band(a, 3), 4)]
            .. '=='
    elseif rest == 2 then
        local a, b = byte(s, i, i + 1)
        out[#out + 1] = B64_ENC[rshift(a, 2)]
            .. B64_ENC[bor(lshift(band(a, 3), 4), rshift(b, 4))]
            .. B64_ENC[lshift(band(b, 15), 2)]
            .. '='
    end
    return concat(out)
end

local function b64decode(s)
    local n = #s
    local count = 0
    for i = 1, n do
        if B64_DEC[byte(s, i)] ~= nil then
            count = count + 1
        end
    end
    if count == 0 or count % 4 ~= 0 then
        return nil
    end
    local out = {}
    local k, pad = 0, 0
    local v1, v2, v3
    for i = 1, n do
        local c = byte(s, i)
        local v = B64_DEC[c]
        if v ~= nil then
            if c == 61 then
                pad = pad + 1
            end
            k = k + 1
            if k == 1 then
                v1 = v
            elseif k == 2 then
                v2 = v
            elseif k == 3 then
                v3 = v
            else
                k = 0
                local chunk = char(
                    band(bor(lshift(v1, 2), rshift(v2, 4)), 255),
                    band(bor(lshift(v2, 4), rshift(v3, 2)), 255),
                    band(bor(lshift(v3, 6), v), 255)
                )
                if pad > 0 then
                    if pad > 2 then
                        return nil
                    end
                    out[#out + 1] = sub(chunk, 1, 3 - pad)
                    break
                end
                out[#out + 1] = chunk
            end
        end
    end
    local res = concat(out)
    if res == '' then
        return nil
    end
    return res
end

M.b64encode = b64encode
M.b64decode = b64decode

--
-- Numbers, as memcached's mcmc_tokto32 / mcmc_toktou32 / mcmc_toktou64.
--

-- Signed 32 bit: an optional '-', decimal digits, at most 11 characters.
local function parse_i32(s)
    if #s > 11 then
        return nil
    end
    local neg = byte(s, 1) == 45
    local digits = neg and sub(s, 2) or s
    if find(digits, '[^0-9]') then
        return nil
    end
    local v = tonumber(digits) or 0
    if neg then
        v = 0 - v
        if v < -2147483648 then
            return nil
        end
    elseif v > 2147483647 then
        return nil
    end
    return v
end

local function parse_u32(s)
    if #s > 11 or find(s, '[^0-9]') then
        return nil
    end
    local v = tonumber(s) or 0
    if v > 4294967295 then
        return nil
    end
    return v
end

local U64_MAX_STR = '18446744073709551615'

-- Unsigned 64 bit as uint64_t cdata: decimal digits, at most 22 characters.
local function parse_u64(s)
    if #s > 22 or find(s, '[^0-9]') then
        return nil
    end
    local digits = s:match('^0*(%d*)$')
    local n = #digits
    if n > 20 or (n == 20 and digits > U64_MAX_STR) then
        return nil
    end
    return 0ULL + (tonumber64(digits) or 0)
end

M.parse_u64 = parse_u64

-- A uint64_t cdata or a Lua number in decimal, without the ULL suffix.
local function u64str(v)
    if v == nil then
        return '0'
    end
    if type(v) == 'number' then
        return format('%d', v)
    end
    return tostring(v):match('^%-?%d+') or '0'
end

M.u64str = u64str

--
-- Parser.
--

local META_COMMANDS = {
    mg = true,
    ms = true,
    md = true,
    ma = true,
    mn = true,
    me = true,
}

local MS_MODES = {
    E = 'add',
    A = 'append',
    P = 'prepend',
    R = 'replace',
    S = 'set',
}

-- Flags reflected in the response, per command, in request order.
local RET_MG = {
    s = true,
    t = true,
    c = true,
    f = true,
    l = true,
    h = true,
    O = true,
    k = true,
}
local RET_MS = {O = true, k = true, c = true, s = true}
local RET_MD = {O = true, k = true}
local RET_MA = {c = true, t = true, O = true, k = true}

-- Flags that take no argument or whose argument is ignored.
local PLAIN_FLAGS = {
    l = true,
    h = true,
    O = true,
    P = true,
    L = true,
    k = true,
    s = true,
    t = true,
    c = true,
    f = true,
    v = true,
    u = true,
    q = true,
    x = true,
    I = true,
}

-- Split a command line on spaces.
local function tokenize(line)
    local toks = {}
    local n = 0
    local i = 1
    while true do
        local s, e = find(line, '[^ ]+', i)
        if s == nil then
            break
        end
        n = n + 1
        toks[n] = sub(line, s, e)
        i = e + 2
    end
    return toks, n
end

-- memcached's _meta_flag_preparse. Returns a table of parsed flags or nil and
-- the error line. Like memcached, a bad argument doesn't stop the scan, and
-- the last error message wins; an unknown or repeated flag stops it at once.
local function preparse(toks, ntok, start, ret_set)
    local key = toks[2]
    if #key > MAX_KEY_LENGTH then
        return nil, E_FORMAT
    end
    if ntok > MAX_TOKENS then
        return nil, E_TOO_MANY
    end
    local of = {key = key}
    local ret = {}
    local nret = 0
    local seen = {}
    local err
    local failed = false
    for i = start, ntok do
        local tok = toks[i]
        local o = byte(tok, 1)
        if o >= 127 or seen[o] then
            return nil, E_DUPLICATE
        end
        seen[o] = true
        local f = sub(tok, 1, 1)
        if ret_set[f] then
            nret = nret + 1
            ret[nret] = tok
        end
        if PLAIN_FLAGS[f] then
            of[f] = true
            if f == 'O' and #tok > MAX_OPAQUE_LENGTH then
                of.long_opaque = true
            end
        elseif f == 'b' then
            local decoded = b64decode(key)
            if decoded == nil then
                err = E_KEY_DECODE
                failed = true
            else
                of.key = decoded
            end
            of.b = true
        elseif f == 'N' or f == 'T' or f == 'R' then
            local v = #tok > 1 and parse_i32(sub(tok, 2)) or nil
            if v == nil then
                err = E_TOKEN
                failed = true
            else
                of[f] = v
            end
        elseif f == 'F' then
            -- memcached doesn't set a message for a bad F.
            local v = #tok > 1 and parse_u32(sub(tok, 2)) or nil
            if v == nil then
                failed = true
            else
                of.F = v
            end
        elseif f == 'C' or f == 'E' or f == 'J' or f == 'D' then
            local v = #tok > 1 and parse_u64(sub(tok, 2)) or nil
            if v == nil then
                err = (f == 'J' and E_INITIAL) or (f == 'D' and E_DELTA)
                    or E_TOKEN
                failed = true
            else
                of[f] = v
            end
        elseif f == 'M' then
            of.M = sub(tok, 2, 2)
        else
            return nil, E_FLAG
        end
    end
    if failed then
        return nil, err or E_FORMAT
    end
    of.ret = ret
    return of
end

local function parse_mg(toks, ntok)
    local of, err = preparse(toks, ntok, 3, RET_MG)
    if of == nil then
        return nil, err
    end
    if of.long_opaque then
        return nil, E_OPAQUE
    end
    return {
        op = 'get',
        key = of.key,
        touch_ttl = of.T,
        vivify_ttl = of.N,
        recache_ttl = of.R,
        bump_la = not of.u,
        no_lru_bump = of.u,
        new_cas = of.E,
        -- Only mg hands out win tokens (W, Z), not the classic reads.
        win_token = true,
        -- A hit holds the item for the rest of the batch (engine.lua).
        hold = true,
        meta = {
            cmd = 'mg',
            quiet = of.q,
            base64 = of.b,
            value = of.v,
            compare_cas = of.C,
            ret = of.ret,
        },
    }
end

local function parse_md(toks, ntok)
    local of, err = preparse(toks, ntok, 3, RET_MD)
    if of == nil then
        return nil, err
    end
    if of.long_opaque then
        return nil, E_OPAQUE
    end
    return {
        op = 'delete',
        key = of.key,
        cas = of.C,
        invalidate = of.I,
        -- md x stores a new item, with the TTL of T or none, as
        -- memcached does (item_alloc(..., of.exptime)); md I keeps the
        -- old TTL without T.
        ttl = of.x and (of.T or 0) or (of.I and of.T or nil),
        clear_value = of.x,
        flags = of.x and (of.F or 0) or nil,
        new_cas = of.E,
        meta = {cmd = 'md', quiet = of.q, base64 = of.b, ret = of.ret},
    }
end

local function parse_ma(toks, ntok)
    local of = preparse(toks, ntok, 3, RET_MA)
    if of == nil then
        -- memcached replaces every flag error of ma with this one.
        return nil, E_MA_FLAG
    end
    local incr = true
    local mode = of.M
    if mode ~= nil then
        if mode == 'D' or mode == '-' then
            incr = false
        elseif mode ~= 'I' and mode ~= '+' then
            return nil, E_MA_MODE
        end
    end
    if of.long_opaque then
        return nil, E_OPAQUE
    end
    return {
        op = 'arith',
        key = of.key,
        incr = incr,
        delta = of.D or 1ULL,
        initial = of.N and (of.J or 0ULL) or nil,
        vivify_ttl = of.N,
        ttl = of.T,
        cas = of.C,
        new_cas = of.E,
        meta = {
            cmd = 'ma',
            quiet = of.q,
            base64 = of.b,
            value = of.v,
            ret = of.ret,
        },
    }
end

local function parse_me(toks, ntok)
    if ntok < 2 then
        return nil, E_FORMAT
    end
    local key = toks[2]
    if #key > MAX_KEY_LENGTH then
        return nil, E_FORMAT
    end
    local base64 = toks[3] == 'b'
    if base64 then
        key = b64decode(key)
        if key == nil then
            return nil, E_FORMAT
        end
    end
    -- A lookup without side effects: peek asks the engine not to hand out
    -- a recache win token either.
    return {
        op = 'get',
        key = key,
        bump_la = false,
        no_lru_bump = true,
        peek = true,
        meta = {cmd = 'me', base64 = base64 or nil},
    }
end

-- The item size as memcached counts it against max_item_size: the same
-- as proto/text.lua item_size() (48 bytes of header, the key and its NUL,
-- 4 bytes of client flags unless 0, the value and its \r\n, the CAS).
local function item_size(nkey, flags, vlen)
    return 48 + nkey + 1 + (flags ~= 0 and 4 or 0) + vlen + 2 + 8
end

-- ms <key> <datalen> <flags>*\r\n<data>\r\n. data_next is the position right
-- after the data block and its \r\n. Once <datalen> is known, an error skips
-- the data block at once, as memcached's conn_swallow does: data_next may be
-- past the end of buf.
local function parse_ms(buf, pos, nl, toks, ntok, opts)
    if ntok < 3 then
        return false, nl + 1, E_FORMAT
    end
    local vlen = parse_i32(toks[3])
    if vlen == nil or vlen < 0 or vlen > MAX_DATA_LENGTH then
        return false, nl + 1, E_FORMAT
    end
    local data_start = nl + 1
    local data_next = data_start + vlen + 2
    local of, err = preparse(toks, ntok, 4, RET_MS)
    if of == nil then
        return false, data_next, err
    end
    local mode = 'set'
    if of.M ~= nil then
        mode = MS_MODES[of.M]
        if mode == nil then
            return false, data_next, E_MS_MODE
        end
    end
    local max_item_size = opts ~= nil and opts.max_item_size
        or DEFAULT_MAX_ITEM_SIZE
    if item_size(#of.key, of.F or 0, vlen) > max_item_size then
        -- As memcached (process_mset_cmd_start): a store that doesn't fit
        -- removes the old value, so no stale data is left, in any mode. The
        -- data block is skipped as it arrives.
        return {
            op = 'delete',
            key = of.key,
            meta = {cmd = 'ms', too_large = true, base64 = of.b},
        },
            data_next
    end
    if of.long_opaque then
        return false, data_next, E_OPAQUE
    end
    if #buf < data_next - 1 then
        return nil, pos, data_next - pos
    end
    if byte(buf, data_next - 2) ~= 13 or byte(buf, data_next - 1) ~= 10 then
        return false, data_next, E_CHUNK
    end
    -- As memcached: a CAS turns set and replace into the cas command, add
    -- ignores the CAS, append and prepend check it. I (invalidate) only
    -- works in the cas command.
    local cas = of.C
    if cas ~= nil then
        if mode == 'replace' then
            mode = 'set'
        elseif mode == 'add' then
            cas = nil
        end
    end
    local append = mode == 'append' or mode == 'prepend'
    return {
        op = 'store',
        key = of.key,
        value = sub(buf, data_start, data_start + vlen - 1),
        flags = of.F or 0,
        exptime = of.T or 0,
        mode = mode,
        cas = cas,
        invalidate = (of.I and cas ~= nil and mode == 'set') or nil,
        vivify_ttl = append and of.N or nil,
        -- The limit of the joined item (engine.lua ops.store).
        max_size = append and max_item_size or nil,
        new_cas = of.E,
        meta = {cmd = 'ms', quiet = of.q, base64 = of.b, ret = of.ret},
    },
        data_next
end

local LINE_PARSERS = {
    mg = parse_mg,
    md = parse_md,
    ma = parse_ma,
    me = parse_me,
}

-- The tokenizer of memcached 1.6.45 (vendor/mcmc _mcmc_tokenize_meta) fails
-- on a flag token that starts with neither a letter (A-z, '[' to '`'
-- included) nor a digit, among the first 23 tokens; the command then gets
-- "bad command line format" (the flag checks would give "invalid flag").
-- first is the index of the first flag token.
local MCMC_MAX_TOKENS = 23

local function bad_flag_chars(toks, ntok, first)
    for i = first, ntok < MCMC_MAX_TOKENS and ntok or MCMC_MAX_TOKENS do
        local c = byte(toks[i], 1)
        if not ((c > 64 and c < 123) or (c >= 48 and c <= 57)) then
            return true
        end
    end
    return false
end

-- parse(buf, pos[, opts]) -> req, next_pos              a complete command
--                         -> nil, pos[, need]           need more data
--                         -> false, next_pos, err_line[, close]
--
-- The conventions are those of proto/text.lua:
-- - need is how many bytes from pos the command takes, when known;
-- - err_line ends with \r\n; meta errors are never hidden by q;
-- - next_pos may be past the end of buf: the bytes up to it are to be
--   skipped as they arrive (the data block of a failed ms);
-- - close = true: close the connection after the reply, if any (more
--   than MAX_LINE_LENGTH bytes without a \n: a silent close, as memcached
--   does).
-- opts.max_item_size: the limit for ms data, 1048576 by default.
function M.parse(buf, pos, opts)
    pos = pos or 1
    local nl = find(buf, '\n', pos, true)
    if nl == nil then
        if #buf - pos + 1 > MAX_LINE_LENGTH then
            -- Closed silently, as memcached does.
            return false, #buf + 1, nil, true
        end
        return nil, pos
    end
    -- memcached's process_request: a line shorter than 4 bytes ("mn\n") is
    -- "bad command line format".
    if nl - pos < 3 then
        return false, nl + 1, E_FORMAT
    end
    -- A longer line whose \n is already buffered is processed, as
    -- memcached's try_read_command_ascii does (a too long key is then "bad
    -- command line format").
    local last = nl - 1
    if byte(buf, last) == 13 then
        last = last - 1
    end
    local toks, ntok = tokenize(sub(buf, pos, last))
    local cmd = toks[1]
    if (cmd == 'ms' or LINE_PARSERS[cmd] ~= nil)
        and bad_flag_chars(toks, ntok, cmd == 'ms' and 4 or 3) then
        -- Before anything else, so an ms data block isn't swallowed.
        return false, nl + 1, E_FORMAT
    end
    if cmd == 'ms' then
        return parse_ms(buf, pos, nl, toks, ntok, opts)
    end
    if cmd == 'mn' then
        return {op = 'mn', meta = {cmd = 'mn'}}, nl + 1
    end
    local parser = LINE_PARSERS[cmd]
    if parser == nil then
        return false, nl + 1, E_UNKNOWN
    end
    if ntok < 2 then
        return false, nl + 1, E_FORMAT
    end
    local req, err = parser(toks, ntok)
    if req == nil then
        return false, nl + 1, err
    end
    return req, nl + 1
end

-- detect(buf, pos) -> true if a meta command starts at pos, false if not,
-- nil if there are too few bytes to tell.
function M.detect(buf, pos)
    pos = pos or 1
    local b1, b2, b3 = byte(buf, pos, pos + 2)
    if b1 == nil then
        return nil
    end
    if b1 ~= 109 then -- 'm'
        return false
    end
    if b2 == nil then
        return nil
    end
    if not META_COMMANDS[char(b1, b2)] then
        return false
    end
    if b3 == nil then
        return nil
    end
    return b3 == 32 or b3 == 13 or b3 == 10
end

--
-- Encoder.
--

local EMPTY = {}

-- An item may be a map (item.key) or an array in the space format order
-- (item[1]); box.tuple works either way.
local function field(item, name, idx)
    local v = item[name]
    if v == nil then
        v = item[idx]
    end
    return v
end

-- Remaining TTL in seconds from an absolute exptime, -1 for never.
local function ttl_str(exptime, now)
    exptime = tonumber(exptime)
    if exptime == nil or exptime == 0 then
        return '-1'
    end
    local ttl = ceil(exptime - now)
    if ttl < 0 then
        ttl = 0
    end
    return format('%d', ttl)
end

-- Remaining TTL from a raw request exptime (memcached rules).
local function raw_ttl_str(raw, now)
    if raw == 0 then
        return '-1'
    elseif raw < 0 then
        return '0'
    elseif raw <= 2592000 then
        return format('%d', raw)
    end
    return ttl_str(raw, now)
end

local function seconds_since(t, now)
    t = tonumber(t)
    if t == nil then
        return 0
    end
    local d = floor(now - t)
    if d < 0 then
        d = 0
    end
    return d
end

local function key_token(meta, key)
    if meta.base64 then
        return ' k' .. b64encode(key) .. ' b'
    end
    return ' k' .. key
end

local function error_line(kind, msg)
    if msg == nil or msg == '' then
        return kind .. '\r\n'
    end
    if sub(msg, 1, #kind) == kind then
        return msg .. '\r\n'
    end
    return kind .. ' ' .. msg .. '\r\n'
end

local function encode_error(resp)
    local status = resp.status
    if status == 'SERVER_ERROR' then
        return error_line('SERVER_ERROR', resp.message)
    elseif status == 'CLIENT_ERROR' then
        return error_line('CLIENT_ERROR', resp.message or NON_NUMERIC)
    end
    return 'SERVER_ERROR unexpected engine status ' .. tostring(status)
        .. '\r\n'
end

-- The O and k flags, the only ones in responses without an item.
local function opaque_and_key(out, req)
    local meta = req.meta
    local ret = meta.ret
    for i = 1, #ret do
        local tok = ret[i]
        local f = sub(tok, 1, 1)
        if f == 'O' then
            out[#out + 1] = ' ' .. tok
        elseif f == 'k' then
            out[#out + 1] = key_token(meta, req.key)
        end
    end
end

local function encode_mg(req, resp, now)
    local meta = req.meta
    local status = resp.status
    if status == 'MISS' then
        if meta.quiet then
            return ''
        end
        local out = {'EN'}
        opaque_and_key(out, req)
        out[#out + 1] = '\r\n'
        return concat(out)
    elseif status ~= 'HIT' then
        return encode_error(resp)
    end
    local item = resp.item or EMPTY
    local value = field(item, 'value', 2) or ''
    local cas = field(item, 'cas', 5)
    local with_value = meta.value
    local ccas = meta.compare_cas
    if with_value and ccas ~= nil and cas ~= nil and cas == ccas then
        with_value = false
    end
    local out = {with_value and ('VA ' .. #value) or 'HD'}
    local ret = meta.ret
    for i = 1, #ret do
        local tok = ret[i]
        local f = sub(tok, 1, 1)
        local s
        if f == 's' then
            s = ' s' .. #value
        elseif f == 't' then
            s = ' t' .. ttl_str(field(item, 'exptime', 4), now)
        elseif f == 'c' then
            s = ' c' .. u64str(cas)
        elseif f == 'f' then
            s = ' f' .. u64str(field(item, 'flags', 3))
        elseif f == 'l' then
            s = ' l' .. seconds_since(field(item, 'last_access', 7), now)
        elseif f == 'h' then
            s = field(item, 'fetched', 8) and ' h1' or ' h0'
        elseif f == 'O' then
            s = ' ' .. tok
        else -- 'k'
            s = key_token(meta, req.key)
        end
        out[#out + 1] = s
    end
    -- Same order as memcached: Z, X, W.
    if resp.already_won then
        out[#out + 1] = ' Z'
    end
    local stale = resp.stale
    if stale == nil then
        stale = field(item, 'stale', 9)
    end
    if stale then
        out[#out + 1] = ' X'
    end
    if resp.won then
        out[#out + 1] = ' W'
    end
    out[#out + 1] = '\r\n'
    if with_value then
        out[#out + 1] = value
        out[#out + 1] = '\r\n'
    end
    return concat(out)
end

local MS_CODES = {
    STORED = 'HD',
    NOT_STORED = 'NS',
    EXISTS = 'EX',
    NOT_FOUND = 'NF',
}

local function encode_ms(req, resp)
    local meta = req.meta
    if meta.too_large then
        -- An error: never hidden by q.
        return E_TOO_LARGE
    end
    local code = MS_CODES[resp.status]
    if code == nil then
        return encode_error(resp)
    end
    local ok = code == 'HD'
    if ok and meta.quiet then
        return ''
    end
    local out = {code}
    local ret = meta.ret
    for i = 1, #ret do
        local tok = ret[i]
        local f = sub(tok, 1, 1)
        local s
        if f == 'O' then
            s = ' ' .. tok
        elseif f == 'k' then
            s = key_token(meta, req.key)
        elseif f == 'c' then
            -- memcached returns c0 unless the item was stored.
            s = ' c' .. (ok and u64str(resp.cas) or '0')
        else -- 's'
            s = ' s' .. ((ok and resp.size) or #req.value)
        end
        out[#out + 1] = s
    end
    out[#out + 1] = '\r\n'
    return concat(out)
end

local MD_CODES = {
    DELETED = 'HD',
    NOT_FOUND = 'NF',
    EXISTS = 'EX',
    NOT_STORED = 'NS',
}

local function encode_md(req, resp)
    local code = MD_CODES[resp.status]
    if code == nil then
        return encode_error(resp)
    end
    if code == 'HD' and req.meta.quiet then
        return ''
    end
    local out = {code}
    opaque_and_key(out, req)
    out[#out + 1] = '\r\n'
    return concat(out)
end

local MA_CODES = {NOT_FOUND = 'NF', NOT_STORED = 'NS', EXISTS = 'EX'}

local function encode_ma(req, resp, now)
    local meta = req.meta
    local status = resp.status
    if status ~= 'STORED' then
        local code = MA_CODES[status]
        if code == nil then
            return encode_error(resp)
        end
        local out = {code}
        opaque_and_key(out, req)
        out[#out + 1] = '\r\n'
        return concat(out)
    end
    -- q hides the success reply, VA included.
    if meta.quiet then
        return ''
    end
    local num = u64str(resp.value)
    local out = {meta.value and ('VA ' .. #num) or 'HD'}
    local ret = meta.ret
    for i = 1, #ret do
        local tok = ret[i]
        local f = sub(tok, 1, 1)
        local s
        if f == 'c' then
            s = ' c' .. u64str(resp.cas)
        elseif f == 't' then
            if resp.exptime ~= nil then
                s = ' t' .. ttl_str(resp.exptime, now)
            elseif req.ttl ~= nil then
                s = ' t' .. raw_ttl_str(req.ttl, now)
            else
                s = ' t-1'
            end
        elseif f == 'O' then
            s = ' ' .. tok
        else -- 'k'
            s = key_token(meta, req.key)
        end
        out[#out + 1] = s
    end
    out[#out + 1] = '\r\n'
    if meta.value then
        out[#out + 1] = num
        out[#out + 1] = '\r\n'
    end
    return concat(out)
end

local function encode_me(req, resp, now)
    if resp == nil or resp.status == 'MISS' then
        return 'EN\r\n'
    elseif resp.status ~= 'HIT' then
        return encode_error(resp)
    end
    local item = resp.item or EMPTY
    local key = req.key
    local value = field(item, 'value', 2) or ''
    local exptime = tonumber(field(item, 'exptime', 4))
    local exp = -1
    if exptime ~= nil and exptime ~= 0 then
        exp = ceil(exptime - now)
    end
    return format(
        'ME %s exp=%d la=%d cas=%s fetch=%s cls=1 size=%d\r\n',
        req.meta.base64 and b64encode(key) or key,
        exp,
        seconds_since(field(item, 'last_access', 7), now),
        u64str(field(item, 'cas', 5)),
        field(item, 'fetched', 8) and 'yes' or 'no',
        #key + #value
    )
end

-- encode(req, resp[, opts]) -> reply string, '' when q hides it.
-- opts.now overrides the clock (fiber.time()) for TTL and last access.
function M.encode(req, resp, opts)
    local cmd = req.meta.cmd
    if cmd == 'mn' then
        return 'MN\r\n'
    end
    local now = opts ~= nil and opts.now or fiber.time()
    if cmd == 'me' then
        return encode_me(req, resp, now)
    end
    if resp == nil then
        return 'SERVER_ERROR no response\r\n'
    end
    if cmd == 'mg' then
        return encode_mg(req, resp, now)
    elseif cmd == 'ms' then
        return encode_ms(req, resp)
    elseif cmd == 'md' then
        return encode_md(req, resp)
    end
    return encode_ma(req, resp, now)
end

return M
