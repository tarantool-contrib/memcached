-- The classic memcached text protocol: parse + encode.
--
-- Pure Lua, no box: it may run in an application thread.
--
-- parse(buf, pos, opts) -> req, next_pos           -- a complete command
--                       -> nil, pos, need          -- need more data; need is
--                                                  -- the number of bytes from
--                                                  -- pos, if known
--                       -> false, next_pos, err_line, close
--                                                  -- protocol error: reply
--                                                  -- err_line (nil = nothing);
--                                                  -- close = close the
--                                                  -- connection after it
-- next_pos may point beyond the end of buf: the bytes up to it are to be
-- skipped as they arrive (a value that is too large is swallowed).
-- opts = {max_item_size = <bytes>}.
--
-- encode(req, resp) -> string
--
-- Requests are engine requests (see DESIGN.md), plus:
-- - noreply: don't reply;
-- - cmd: the command name;
-- - multi: retrieval commands are op = 'retrieve' with an array of engine
--   get requests in multi (with hold, see engine.lua); encode() gets an
--   array of their responses.
-- Front-end requests: op = 'flush' {delay}, 'version', 'verbosity' {level},
-- 'quit', 'stats' {args}; see server.lua for their responses.
-- 'lru_crawler' {sub = 'metadump'|'mgdump', items = <whether the classes
-- include ours, 1>}: the server streams the reply (readview.dump()), so
-- encode() returns ''.

local M = {}

local find = string.find
local sub = string.sub
local byte = string.byte
local match = string.match
local gmatch = string.gmatch
local gsub = string.gsub
local concat = table.concat
local insert = table.insert
local table_new = require('table.new')

M.MAX_KEY_LENGTH = 250
M.DEFAULT_MAX_ITEM_SIZE = 1048576
-- A command line without '\n' longer than this closes the connection, like
-- in memcached; multi-key gets may be longer, up to MAX_GET_LINE.
M.MAX_LINE = 2048
M.MAX_GET_LINE = 1048576
M.MAX_VERBOSITY = 2

local ERROR = 'ERROR\r\n'
local BAD_FORMAT = 'CLIENT_ERROR bad command line format\r\n'
local BAD_CHUNK = 'CLIENT_ERROR bad data chunk\r\n'
local TOO_LARGE = 'SERVER_ERROR object too large for cache\r\n'
local BAD_EXPTIME = 'CLIENT_ERROR invalid exptime argument\r\n'
local BAD_DELTA = 'CLIENT_ERROR invalid numeric delta argument\r\n'
M.NON_NUMERIC = 'CLIENT_ERROR cannot increment or decrement non-numeric value'

-- Numbers ---------------------------------------------------------------
--
-- As memcached 1.6.45 parses them: mcmc_toktou32/_tokto32/_toktou64 for
-- most numbers (digits only, a '-' for the signed one, a length limit), and
-- safe_strtoul/safe_strtol (strtoul/strtol, then a cast) for the flags and
-- the exptime of the storage commands.

local INT_MAX = 2147483647
local UINT32_MAX = 4294967295
local UINT64_MAX_STR = '18446744073709551615'
local INT64_MAX_STR = '9223372036854775807'
local INT64_MIN_STR = '9223372036854775808' -- its magnitude
local TOKTO32_MAX = 11
local TOKTO64_MAX = 22

-- A token of 1 to 9 decimal digits (below 2^31, in the range of every
-- number below) as a number, otherwise nil: the common case of the parsers
-- below, byte by byte, with no pattern (not compiled by the JIT) and no
-- intermediate strings. Each of them gives the same value for it.
local function small_uint(s)
    local len = #s
    if len == 0 or len > 9 then
        return nil
    end
    local v = 0
    for i = 1, len do
        local c = byte(s, i)
        if c < 48 or c > 57 then
            return nil
        end
        v = v * 10 + c - 48
    end
    return v
end

-- mcmc_toktou32: at most 11 digits, at most 2^32-1.
local function parse_u32(s)
    if #s > TOKTO32_MAX or find(s, '[^0-9]') then
        return nil
    end
    local v = tonumber(s) or 0
    if v > UINT32_MAX then
        return nil
    end
    return v
end
M.parse_u32 = parse_u32

-- mcmc_tokto32: at most 11 characters, an optional '-' and digits (no
-- digits: 0), a signed 32-bit value.
local function parse_i32(s)
    local small = small_uint(s)
    if small ~= nil then
        return small
    end
    if #s > TOKTO32_MAX then
        return nil
    end
    local neg = byte(s, 1) == 45
    local digits = neg and sub(s, 2) or s
    if find(digits, '[^0-9]') then
        return nil
    end
    local v = tonumber(digits) or 0
    if neg then
        v = -v
        if v < -INT_MAX - 1 then
            return nil
        end
    elseif v > INT_MAX then
        return nil
    end
    return v
end
M.parse_i32 = parse_i32

-- Leading zeros off ('' for zero).
local function strip_zeros(digits)
    return (gsub(digits, '^0+', ''))
end

-- Whether a decimal without leading zeros is above max.
local function above(digits, max)
    return #digits > #max or (#digits == #max and digits > max)
end

-- mcmc_toktou64: at most 22 digits, at most 2^64-1 -> uint64_t cdata.
local function parse_u64(s)
    if s == '' or #s > TOKTO64_MAX or find(s, '[^0-9]') then
        return nil
    end
    local digits = strip_zeros(s)
    if above(digits, UINT64_MAX_STR) then
        return nil
    end
    return 0ULL + (tonumber64(digits) or 0)
end
M.parse_u64 = parse_u64

-- strtoul/strtol as safe_strtoul/safe_strtol use them: leading whitespace,
-- a sign, decimal digits, then whitespace or the end of the token. Returns
-- the sign and the digits without leading zeros, or nil.
local function strto_parts(s)
    local sign, digits, rest = match(s, '^%s*([+-]?)(%d+)(.*)$')
    if digits == nil or (rest ~= '' and not find(rest, '^%s')) then
        return nil
    end
    return sign, strip_zeros(digits)
end

-- The flags of a storage command: safe_strtoul (an unsigned long: above
-- 2^64-1 is refused, and so is a '-' whose result is negative as a long),
-- then the cast to the 32-bit client_flags_t, which truncates.
local function parse_flags(s)
    local small = small_uint(s)
    if small ~= nil then
        return small
    end
    local sign, digits = strto_parts(s)
    if sign == nil or above(digits, UINT64_MAX_STR) then
        return nil
    end
    local v = 0ULL + (tonumber64(digits) or 0)
    if sign == '-' and v ~= 0 then
        -- strtoul negates: 2^64 - v, negative as a long unless v > 2^63.
        if not above(digits, INT64_MIN_STR) then
            return nil
        end
        v = 0ULL - v
    end
    return tonumber(v % 4294967296ULL)
end
M.parse_flags = parse_flags

-- The exptime of a storage command: safe_strtol (a 64-bit long), then the
-- cast to int32_t, which keeps the low 32 bits.
local function parse_exptime(s)
    local small = small_uint(s)
    if small ~= nil then
        return small
    end
    local sign, digits = strto_parts(s)
    if sign == nil
        or above(digits, sign == '-' and INT64_MIN_STR or INT64_MAX_STR) then
        return nil
    end
    local v = 0ULL + (tonumber64(digits) or 0)
    if sign == '-' then
        v = 0ULL - v
    end
    local low = tonumber(v % 4294967296ULL)
    if low > INT_MAX then
        low = low - 4294967296
    end
    return low
end
M.parse_exptime = parse_exptime

-- Decimal of a Lua number or a 64-bit cdata.
local function u64str(v)
    if type(v) == 'number' then
        return string.format('%d', v)
    end
    local s = tostring(0ULL + v)
    return sub(s, 1, #s - 3)
end
M.u64str = u64str

-- Keys -------------------------------------------------------------------

-- At most 250 bytes. memcached splits the line on spaces only, so a key
-- may hold any other byte, control characters included (but not the
-- line's "\n", and a trailing "\r" ends the line).
local function valid_key(key)
    return #key <= M.MAX_KEY_LENGTH
end
M.valid_key = valid_key

-- The size of an item as memcached counts it against its item size limit
-- (-I, item_size_max; items.c item_make_header and do_item_alloc, with
-- CAS on, as by default): the item header (48 bytes on 64-bit), the key
-- and its NUL, the client flags (4 bytes, none when they are 0), the value
-- and its "\r\n", and the CAS (8 bytes). A store fails with "object too
-- large" when this is above max_item_size (slabs_clsid).
M.ITEM_HEADER = 48
function M.item_size(nkey, flags, vlen)
    return M.ITEM_HEADER + nkey + 1 + (flags ~= 0 and 4 or 0) + vlen + 2 + 8
end
local item_size = M.item_size

-- Parsing ---------------------------------------------------------------
--
-- As memcached 1.6.45's process_request (proto_parser.c) and
-- process_command_ascii (proto_text.c):
-- - a line shorter than 4 bytes, its "\n" included, is "bad command line
--   format";
-- - the command name is the line up to the first space, exact: case
--   matters ("GET" is ERROR). A leading space makes it empty: then only the commands of
--   memcached's second table (here flush_all, verbosity, lru_crawler),
--   looked up by the first token, still work;
-- - tokens are split on spaces only;
-- - too few tokens for a command is "bad command line format", extra
--   tokens are ignored;
-- - only the first 23 tokens count (MCMC_PARSER_MAX_TOKENS - 1); noreply is
--   the last of them. get/gets/gat/gats read all their keys.

local MAX_TOKENS = 23
local BYTE_SPACE = 32

-- The tokens of the line [pos, line_end] of buf into the empty array
-- tokens: its runs of bytes other than a space, as gmatch('[^ ]+') would
-- find them in the line. Byte by byte, with no copy of the line and no
-- gmatch (a closure per line, and not compiled by the JIT). Returns their
-- number.
local function split(tokens, buf, pos, line_end)
    local n = 0
    local i = pos
    while i <= line_end do
        if byte(buf, i) == BYTE_SPACE then
            i = i + 1
        else
            local j = i + 1
            while j <= line_end and byte(buf, j) ~= BYTE_SPACE do
                j = j + 1
            end
            n = n + 1
            tokens[n] = sub(buf, i, j - 1)
            i = j + 1
        end
    end
    return n
end

-- The tokens array of parse_line(), kept between the calls: a call takes
-- it and puts it back cleared, unless it had more than TOKENS_KEEP tokens
-- or the handler raised. parse() doesn't yield, and no handler keeps the
-- array.
local TOKENS_KEEP = 64
local line_tokens = nil

local function put_tokens(tokens, n)
    if n <= TOKENS_KEEP then
        for i = 1, n do
            tokens[i] = nil
        end
        line_tokens = tokens
    end
end

-- An error reply, suppressed by noreply as in memcached.
local function fail(next_pos, noreply, line)
    if noreply then
        return false, next_pos, nil
    end
    return false, next_pos, line
end

-- The commands of memcached's process_request, by exact name, and the
-- ones of its text_cmd_entries, by the first token.
local commands = {}
local others = {}

-- Handlers: (cmd, tokens, n, noreply, buf, pos, next_pos, opts) -> parse()
-- results. tokens has every token, n is the number that counts (at most
-- 23), noreply whether the last of them is "noreply". Most handlers don't
-- need all the arguments.
-- luacheck: push ignore 212

-- set/add/replace/append/prepend <key> <flags> <exptime> <bytes> [noreply]
-- cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]
local function storage(mode, with_cas)
    return function(cmd, tokens, n, noreply, buf, pos, next_pos, opts)
        if n < 5 then
            return false, next_pos, BAD_FORMAT
        end
        -- The length is checked first: noreply doesn't hide its errors,
        -- and the data block isn't swallowed then.
        local vlen = parse_i32(tokens[5])
        if vlen == nil or vlen < 0 or vlen > INT_MAX - 2 then
            return false, next_pos, BAD_FORMAT
        end
        -- The data block is [next_pos, data_end), then "\r\n". From here
        -- on an error swallows it.
        local data_end = next_pos + vlen
        local key = tokens[2]
        local flags = parse_flags(tokens[3])
        local exptime = parse_exptime(tokens[4])
        if not valid_key(key) or flags == nil or exptime == nil then
            return fail(data_end + 2, noreply, BAD_FORMAT)
        end
        local cas
        if with_cas then
            cas = n >= 6 and parse_u64(tokens[6]) or nil
            if cas == nil then
                return fail(data_end + 2, noreply, BAD_FORMAT)
            end
        end
        local max_item_size = opts and opts.max_item_size
            or M.DEFAULT_MAX_ITEM_SIZE
        if item_size(#key, flags, vlen) > max_item_size then
            -- Swallow the data. A failed set removes the old value, so that
            -- no stale data is left (as memcached does).
            if mode == 'set' and not with_cas then
                return {op = 'delete', key = key, cmd = cmd,
                        noreply = noreply, too_large = true},
                    data_end + 2
            end
            return fail(data_end + 2, noreply, TOO_LARGE)
        end
        if #buf < data_end + 1 then
            return nil, pos, data_end + 2 - pos
        end
        if byte(buf, data_end) ~= 13 or byte(buf, data_end + 1) ~= 10 then
            return fail(data_end + 2, noreply, BAD_CHUNK)
        end
        return {
            op = 'store',
            cmd = cmd,
            mode = mode,
            key = key,
            value = sub(buf, next_pos, data_end - 1),
            flags = flags,
            exptime = exptime,
            cas = cas,
            noreply = noreply,
            -- The limit of the joined item (engine.lua ops.store).
            max_size = (mode == 'append' or mode == 'prepend') and
                       max_item_size or nil,
            -- A plain set may take the C data path (engine.lua fast()).
            fast = mode == 'set' and not with_cas and 'set' or nil,
        },
            data_end + 2
    end
end

commands.set = storage('set', false)
commands.add = storage('add', false)
commands.replace = storage('replace', false)
commands.append = storage('append', false)
commands.prepend = storage('prepend', false)
commands.cas = storage('set', true)

-- get/gets <key>*, gat/gats <exptime> <key>*
local function retrieval(with_cas, touch)
    return function(cmd, tokens, n, noreply, buf, pos, next_pos)
        local first = touch and 3 or 2
        if n < first then
            return false, next_pos, BAD_FORMAT
        end
        local ttl
        if touch then
            -- gat doesn't look for noreply.
            noreply = false
            ttl = parse_i32(tokens[2])
            if ttl == nil then
                return false, next_pos, BAD_EXPTIME
            end
        end
        -- A plain get may take the C data path (engine.lua fast()), which
        -- answers a hit with its VALUE block (see encode_retrieve()).
        local fast = not touch and (with_cas and 'gets' or 'get') or nil
        local multi = table_new(#tokens - first + 1, 0)
        for i = first, #tokens do
            local key = tokens[i]
            if not valid_key(key) then
                return fail(next_pos, noreply, BAD_FORMAT)
            end
            multi[i - first + 1] = {
                op = 'get',
                key = key,
                touch_ttl = ttl,
                fast = fast,
                hold = true,
            }
        end
        return {op = 'retrieve', cmd = cmd, with_cas = with_cas,
                multi = multi},
            next_pos
    end
end

commands.get = retrieval(false, false)
commands.gets = retrieval(true, false)
commands.gat = retrieval(false, true)
commands.gats = retrieval(true, true)

-- delete <key> [noreply]; further tokens are ignored.
function commands.delete(cmd, tokens, n, noreply, buf, pos, next_pos)
    if n < 2 then
        return false, next_pos, BAD_FORMAT
    end
    local key = tokens[2]
    if not valid_key(key) then
        return fail(next_pos, noreply, BAD_FORMAT)
    end
    return {op = 'delete', cmd = cmd, key = key, noreply = noreply}, next_pos
end

-- incr/decr <key> <value> [noreply]
local function arith(incr)
    return function(cmd, tokens, n, noreply, buf, pos, next_pos)
        if n < 3 then
            return false, next_pos, BAD_FORMAT
        end
        local key = tokens[2]
        if not valid_key(key) then
            return fail(next_pos, noreply, BAD_FORMAT)
        end
        local delta = parse_u64(tokens[3])
        if delta == nil then
            return fail(next_pos, noreply, BAD_DELTA)
        end
        return {op = 'arith', cmd = cmd, key = key, incr = incr,
                delta = delta, noreply = noreply},
            next_pos
    end
end

commands.incr = arith(true)
commands.decr = arith(false)

-- touch <key> <exptime> [noreply]
function commands.touch(cmd, tokens, n, noreply, buf, pos, next_pos)
    if n < 3 then
        return false, next_pos, BAD_FORMAT
    end
    local key = tokens[2]
    if not valid_key(key) then
        return fail(next_pos, noreply, BAD_FORMAT)
    end
    local ttl = parse_i32(tokens[3])
    if ttl == nil then
        return fail(next_pos, noreply, BAD_EXPTIME)
    end
    return {op = 'get', cmd = cmd, key = key, touch_ttl = ttl,
            touch_only = true, noreply = noreply},
        next_pos
end

function commands.version(cmd, tokens, n, noreply, buf, pos, next_pos)
    return {op = 'version', cmd = cmd}, next_pos
end

function commands.quit(cmd, tokens, n, noreply, buf, pos, next_pos)
    return {op = 'quit', cmd = cmd}, next_pos
end

-- stats [args]; noreply is an argument.
function commands.stats(cmd, tokens, n, noreply, buf, pos, next_pos)
    local args = {}
    for i = 2, n do
        args[i - 1] = tokens[i]
    end
    return {op = 'stats', cmd = cmd, args = args}, next_pos
end

-- flush_all [delay] [noreply]; further tokens are ignored.
function others.flush_all(cmd, tokens, n, noreply, buf, pos, next_pos)
    local delay = 0
    if n ~= (noreply and 2 or 1) then
        delay = parse_i32(tokens[2])
        if delay == nil then
            return fail(next_pos, noreply, BAD_EXPTIME)
        end
    end
    return {op = 'flush', cmd = cmd, delay = delay, noreply = noreply},
        next_pos
end

-- verbosity <level> [noreply]; further tokens are ignored.
function others.verbosity(cmd, tokens, n, noreply, buf, pos, next_pos)
    local level = n >= 2 and parse_u32(tokens[2]) or nil
    if level == nil then
        return fail(next_pos, noreply, BAD_FORMAT)
    end
    return {op = 'verbosity', cmd = cmd,
            level = math.min(level, M.MAX_VERBOSITY), noreply = noreply},
        next_pos
end

-- Slab class ids are 1..63 in memcached; all the items here are in class 1.
M.MAX_CLASS_ID = 63
local BAD_CLASS = 'BADCLASS invalid class id\r\n'

-- lru_crawler metadump|mgdump <classid,classid,...|all|hash>
function others.lru_crawler(cmd, tokens, n, noreply, buf, pos, next_pos)
    local sub = tokens[2]
    if n ~= 3 or (sub ~= 'metadump' and sub ~= 'mgdump') then
        return false, next_pos, ERROR
    end
    local classes = tokens[3]
    local items = classes == 'all' or classes == 'hash'
    if not items then
        for id in gmatch(classes, '[^,]+') do
            local v = parse_u32(id)
            if v == nil or v < 1 or v > M.MAX_CLASS_ID then
                return false, next_pos, BAD_CLASS
            end
            if v == 1 then
                items = true
            end
        end
    end
    return {op = 'lru_crawler', cmd = cmd, sub = sub, classes = classes,
            items = items},
        next_pos
end

-- luacheck: pop

-- A line without '\n' that is too long: memcached keeps reading a "get "
-- or "gets " line (after at most 100 spaces) and closes the connection
-- on anything else (proto_text.c try_read_command_ascii).
local function long_line(buf, pos)
    local len = #buf - pos + 1
    if len <= M.MAX_LINE then
        return false
    end
    if len <= M.MAX_GET_LINE then
        local spaces = #match(buf, '^ *', pos)
        local head = pos + spaces
        if spaces <= 100
            and (find(buf, '^get ', head) or find(buf, '^gets ', head)) then
            return false
        end
    end
    return true
end

-- The fast path of the most common command, "get <key>" or "gets <key>"
-- with one valid key, single spaces: the request, or nil for the general
-- parser. The line is [pos, line_end]. Byte by byte, with no line copy,
-- token table or patterns, so the JIT compiles it.
local function parse_get1(buf, pos, line_end)
    if byte(buf, pos) ~= 103 or byte(buf, pos + 1) ~= 101
        or byte(buf, pos + 2) ~= 116 then
        return nil
    end
    local c = byte(buf, pos + 3)
    local first, with_cas, cmd
    if c == BYTE_SPACE then
        first, with_cas, cmd = pos + 4, false, 'get'
    elseif c == 115 and byte(buf, pos + 4) == BYTE_SPACE then
        first, with_cas, cmd = pos + 5, true, 'gets'
    else
        return nil
    end
    local len = line_end - first + 1
    if len < 1 or len > M.MAX_KEY_LENGTH then
        return nil
    end
    -- A space means more keys (or extra spaces).
    for i = first, line_end do
        if byte(buf, i) == BYTE_SPACE then
            return nil
        end
    end
    return {
        op = 'retrieve',
        cmd = cmd,
        with_cas = with_cas,
        multi = {
            {
                op = 'get',
                key = sub(buf, first, line_end),
                fast = cmd,
                hold = true,
            },
        },
    }
end

-- A complete line [pos, line_end], its '\n' at nl, of at least 4 bytes.
local function parse_line(buf, pos, nl, line_end, opts)
    local tokens = line_tokens or {}
    line_tokens = nil
    local ntokens = split(tokens, buf, pos, line_end)
    local n = ntokens < MAX_TOKENS and ntokens or MAX_TOKENS
    local noreply = n > 0 and tokens[n] == 'noreply'
    -- The command name: the line up to its first space, the first token
    -- ('' after a leading space).
    local handler = commands[byte(buf, pos) == BYTE_SPACE and '' or tokens[1]]
    if handler == nil and n > 0 then
        handler = others[tokens[1]]
    end
    if handler ~= nil then
        local req, next_pos, err, close = handler(
            tokens[1],
            tokens,
            n,
            noreply,
            buf,
            pos,
            nl + 1,
            opts
        )
        put_tokens(tokens, ntokens)
        return req, next_pos, err, close
    end
    -- memcached hangs up on HTTP: an unknown command whose last token
    -- starts with "HTTP/" closes the connection without a reply ("GET /
    -- HTTP/1.1" too: command names are exact).
    local http = n > 1 and sub(tokens[n], 1, 5) == 'HTTP/'
    put_tokens(tokens, ntokens)
    if http then
        return false, nl + 1, nil, true
    end
    return fail(nl + 1, noreply, ERROR)
end

function M.parse(buf, pos, opts)
    local nl = find(buf, '\n', pos, true)
    if nl == nil then
        if long_line(buf, pos) then
            -- Closed silently, as memcached does.
            return false, #buf + 1, nil, true
        end
        return nil, pos
    end
    if nl - pos < 3 then
        return false, nl + 1, BAD_FORMAT
    end
    local line_end = nl - 1
    if byte(buf, line_end) == 13 then
        line_end = line_end - 1
    end
    local req = parse_get1(buf, pos, line_end)
    if req ~= nil then
        return req, nl + 1
    end
    return parse_line(buf, pos, nl, line_end, opts)
end

-- parse() without the get fast path, for the tests.
function M.parse_general(buf, pos, opts)
    local nl = find(buf, '\n', pos, true)
    if nl == nil or nl - pos < 3 then
        return M.parse(buf, pos, opts)
    end
    local line_end = nl - 1
    if byte(buf, line_end) == 13 then
        line_end = line_end - 1
    end
    return parse_line(buf, pos, nl, line_end, opts)
end

-- Encoding --------------------------------------------------------------

local function server_error(resp)
    local message = resp.message or 'unknown error'
    return 'SERVER_ERROR ' .. tostring(message):gsub('[\r\n]+', ' ') .. '\r\n'
end

local function client_error(resp)
    return 'CLIENT_ERROR ' .. tostring(resp.message or 'error') .. '\r\n'
end

local STORE_REPLY = {
    STORED = 'STORED\r\n',
    NOT_STORED = 'NOT_STORED\r\n',
    EXISTS = 'EXISTS\r\n',
    NOT_FOUND = 'NOT_FOUND\r\n',
}

local DELETE_REPLY = {
    DELETED = 'DELETED\r\n',
    NOT_FOUND = 'NOT_FOUND\r\n',
    EXISTS = 'EXISTS\r\n',
}

-- The parts of the reply to a retrieval, VALUE lines and values and END,
-- appended to out after its first n entries; returns the new number of
-- entries. No intermediate strings: table.concat formats Lua numbers
-- itself (integers exactly up to 2^53, and flags are 32-bit). An error
-- response drops the parts of the reply, as memcached drops partial results
-- on errors: the reply is that error.
local function retrieve_parts(req, resps, out, n)
    local n0 = n
    local with_cas = req.with_cas
    for i = 1, #resps do
        local resp = resps[i]
        if type(resp) == 'string' then
            -- A hit on the C data path: the VALUE block, already in the
            -- format of this command (the parser marks the keys of a get
            -- 'get' and of a gets 'gets').
            n = n + 1
            out[n] = resp
        else
            local status = resp.status
            if status == 'SERVER_ERROR' then
                for k = n0 + 2, n do
                    out[k] = nil
                end
                n = n0 + 1
                out[n] = server_error(resp)
                return n
            end
            if status == 'HIT' then
                local item = resp.item
                local value = item.value
                local flags = item.flags
                if type(flags) ~= 'number' then
                    flags = u64str(flags)
                end
                out[n + 1] = 'VALUE '
                out[n + 2] = item.key
                out[n + 3] = ' '
                out[n + 4] = flags
                out[n + 5] = ' '
                out[n + 6] = #value
                n = n + 6
                if with_cas then
                    out[n + 1] = ' '
                    out[n + 2] = u64str(item.cas)
                    n = n + 2
                end
                out[n + 1] = '\r\n'
                out[n + 2] = value
                out[n + 3] = '\r\n'
                n = n + 3
            end
        end
    end
    n = n + 1
    out[n] = 'END\r\n'
    return n
end

-- The parts array of encode_retrieve(), kept between the calls: a call
-- takes it and puts it back cleared (no value stays referenced), unless it
-- had more than OUT_KEEP parts or the call raised. A call doesn't yield, so
-- no other call needs it meanwhile.
local OUT_KEEP = 256
local retrieve_out = nil

local function encode_retrieve(req, resps)
    local out = retrieve_out or {}
    retrieve_out = nil
    local n = retrieve_parts(req, resps, out, 0)
    local res = n == 1 and out[1] or concat(out, '', 1, n)
    if n <= OUT_KEEP then
        for k = 1, n do
            out[k] = nil
        end
        retrieve_out = out
    end
    return res
end

local function stat_value(v)
    local t = type(v)
    if t == 'number' then
        if v == math.floor(v) and v < 2 ^ 63 and v > -2 ^ 63 then
            return string.format('%d', v)
        end
        return string.format('%.6f', v)
    elseif t == 'cdata' then
        return u64str(v)
    elseif t == 'boolean' then
        return v and 'yes' or 'no'
    end
    return tostring(v)
end

local function encode_stats(resp)
    local status = resp.status
    if status == 'RESET' then
        return 'RESET\r\n'
    elseif status == 'OK' then
        return 'OK\r\n'
    elseif status == 'CLIENT_ERROR' then
        return client_error(resp)
    elseif status ~= 'STATS' then
        return ERROR
    end
    local out = {}
    for _, kv in ipairs(resp.stats) do
        insert(out, 'STAT ' .. kv[1] .. ' ' .. stat_value(kv[2]) .. '\r\n')
    end
    insert(out, 'END\r\n')
    return concat(out)
end

-- encode() of a retrieval (op = 'retrieve'), into an array: the parts of
-- the reply appended to out after its first n entries; returns the new
-- number of entries. The server joins the replies of a batch with one
-- concat, so the values are copied once (see server.lua process()).
function M.encode_parts(req, resps, out, n)
    return retrieve_parts(req, resps, out, n)
end

function M.encode(req, resp)
    local op = req.op
    if op == 'retrieve' then
        return encode_retrieve(req, resp)
    end
    if req.noreply or op == 'quit' or op == 'lru_crawler' then
        return ''
    end
    if resp == nil then
        return ERROR
    end
    local status = resp.status
    if status == 'SERVER_ERROR' then
        return server_error(resp)
    end
    if op == 'store' then
        return STORE_REPLY[status] or server_error(resp)
    elseif op == 'get' then
        -- touch
        return status == 'HIT' and 'TOUCHED\r\n' or 'NOT_FOUND\r\n'
    elseif op == 'delete' then
        if req.too_large then
            return TOO_LARGE
        end
        return DELETE_REPLY[status] or server_error(resp)
    elseif op == 'arith' then
        if status == 'STORED' then
            return u64str(resp.value) .. '\r\n'
        elseif status == 'NOT_FOUND' then
            return 'NOT_FOUND\r\n'
        elseif status == 'CLIENT_ERROR' then
            return M.NON_NUMERIC .. '\r\n'
        elseif status == 'EXISTS' then
            return 'EXISTS\r\n'
        end
        return server_error(resp)
    elseif op == 'flush' or op == 'verbosity' then
        return 'OK\r\n'
    elseif op == 'version' then
        return 'VERSION ' .. tostring(resp.version) .. '\r\n'
    elseif op == 'stats' then
        return encode_stats(resp)
    end
    if status == 'CLIENT_ERROR' then
        return client_error(resp)
    end
    return ERROR
end

return M
