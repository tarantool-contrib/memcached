-- tarantool-contrib/memcached behind memcached's memcached-debug command line, for
-- memcached's own t/*.t tests. Started by shim/memcached-debug as
--   tarantool shim.lua <memcached options>
--
-- Options with a contrib equivalent are honoured: -p/--port, -l/--listen,
-- -c/--conn-limit and -I/--max-item-size. Every other memcached option is
-- accepted and ignored (its argument is skipped), so tests that depend on
-- one fail and are listed in memcached_t_expected.lua.
--
-- The only memcached-debug test hook emulated is `debugtime <n>|p|r`
-- (proto_text.c process_debugtime_command): it moves this process's clock by
-- n seconds, or pauses and resumes it. It works by patching fiber.time,
-- through which all contrib code reads the clock, and by wrapping the text
-- parser and encoder of this process only.
--
-- Environment: MCC_SHIM_LOG_LEVEL (default 'warn'), MCC_SHIM_MEMTX_MEMORY
-- (bytes, default 1 GiB), TMPDIR: the box work
-- dir is a temporary directory there, left behind on exit (removing it at
-- shutdown races with the WAL writer); memcached_t_test.lua removes it.

local fio = require('fio')
local fiber = require('fiber')
local log = require('log')

-- memcached's getopt string: the short options that take an argument.
local WITH_ARG = {}
for c in ('apsUmcluPfntDRbBIYeoN'):gmatch('.') do
    WITH_ARG[c] = true
end
local LONG = {
    port = 'p',
    listen = 'l',
    ['conn-limit'] = 'c',
    ['max-item-size'] = 'I',
    ['udp-port'] = 'U',
    ['memory-limit'] = 'm',
    ['unix-socket'] = 's',
    ['unix-mask'] = 'a',
    threads = 't',
    ['extended'] = 'o',
}
local LONG_NO_ARG = {
    ['enable-shutdown'] = true,
    ['enable-ssl'] = true,
    ['disable-evictions'] = true,
    help = true,
    version = true,
}

local function die(fmt, ...)
    io.stderr:write(('memcached-debug shim: ' .. fmt .. '\n'):format(...))
    os.exit(64)
end

-- memcached's -I: a number with an optional k or m suffix.
local function parse_size(s)
    local n, suffix = tostring(s):match('^(%d+)([kKmM]?)$')
    if n == nil then
        die('bad item size %q', s)
    end
    n = tonumber(n)
    if suffix == 'k' or suffix == 'K' then
        n = n * 1024
    elseif suffix == 'm' or suffix == 'M' then
        n = n * 1024 * 1024
    end
    return n
end

local opts = {port = 11211, host = '127.0.0.1'}
local ignored = {}
do
    local i = 1
    while i <= #arg do
        local a = arg[i]
        local name, val
        if a:sub(1, 2) == '--' then
            local long, v = a:match('^%-%-([^=]+)=(.*)$')
            long = long or a:sub(3)
            name = LONG[long]
            if name == nil then
                if not LONG_NO_ARG[long] then
                    die('unknown option %s', a)
                end
                name = long
            elseif v ~= nil then
                val = v
            else
                i = i + 1
                val = arg[i]
            end
        elseif a:sub(1, 1) == '-' and #a >= 2 then
            name = a:sub(2, 2)
            if WITH_ARG[name] then
                if #a > 2 then
                    val = a:sub(3)
                else
                    i = i + 1
                    val = arg[i]
                end
                if val == nil then
                    die('option %s needs an argument', a)
                end
            elseif #a > 2 then
                -- Bundled flags such as -vv: all ignored.
                name = a:sub(2)
            end
        else
            die('unexpected argument %q', a)
        end
        if name == 'p' then
            opts.port = tonumber(val) or die('bad port %q', val)
        elseif name == 'l' then
            opts.host = val
        elseif name == 'c' then
            opts.max_connections = tonumber(val) or die('bad -c %q', val)
        elseif name == 'I' then
            opts.max_item_size = parse_size(val)
        else
            ignored[#ignored + 1] = val and (a .. ' ' .. val) or a
        end
        i = i + 1
    end
end

if #ignored > 0 then
    log.verbose(
        'memcached-debug shim: ignored options: %s',
        table.concat(ignored, ' ')
    )
end

--
-- debugtime: the clock of this process, shifted by delta, or frozen.
--
local real_time = fiber.time
local clock = {delta = 0, paused = nil}
fiber.time = function()
    if clock.paused ~= nil then
        return clock.paused
    end
    return real_time() + clock.delta
end

-- memcached: "debugtime p" pauses, "debugtime r" resumes (the clock then
-- jumps to real time plus the accumulated delta), "debugtime <int64>" adds
-- to the delta; no argument or a bad number answers ERROR.
local function debugtime(arg1)
    if arg1 == nil then
        return 'ERROR\r\n'
    elseif arg1 == 'p' then
        if clock.paused == nil then
            clock.paused = fiber.time()
        end
    elseif arg1 == 'r' then
        clock.paused = nil
    else
        local n = tonumber(arg1:match('^[%+%-]?%d+$') and arg1 or nil)
        if n == nil then
            return 'ERROR\r\n'
        end
        clock.delta = clock.delta + n
        if clock.paused ~= nil then
            clock.paused = clock.paused + n
        end
    end
    return 'OK\r\n'
end

local function wrap_text_protocol()
    local text = require('memcached.proto.text')
    local parse, encode = text.parse, text.encode
    text.parse = function(buf, pos, popts)
        pos = pos or 1
        if buf:find('^debugtime', pos) then
            local nl = buf:find('\n', pos, true)
            if nl == nil then
                return nil, pos
            end
            local line = buf:sub(pos, nl - 1):gsub('\r$', '')
            local cmd, arg1 = line:match('^(%S+) *(%S*)')
            if cmd == 'debugtime' then
                -- Applied at parse time: the tests wait for the reply.
                return {op = 'debugtime',
                        reply = debugtime(arg1 ~= '' and arg1 or nil)},
                    nl + 1
            end
        end
        return parse(buf, pos, popts)
    end
    text.encode = function(req, resp, eopts)
        if req.op == 'debugtime' then
            return req.reply
        end
        return encode(req, resp, eopts)
    end
end

--
-- Box and the instance.
--
local work_dir = fio.tempdir()
local item_size = opts.max_item_size or 1048576
box.cfg {
    work_dir = work_dir,
    listen = box.NULL,
    -- memcached's -m is a cache size with LRU eviction. contrib evicts too,
    -- but its memory is big enough to hold everything a test stores
    -- (getset.t keeps about 263 MB of values under -m 128), so that the
    -- replies don't depend on what it evicts.
    memtx_memory = tonumber(os.getenv('MCC_SHIM_MEMTX_MEMORY'))
        or 1024 * 1024 * 1024,
    -- Let the protocol limit decide, not the tuple size.
    memtx_max_tuple_size = math.max(2 * 1048576, item_size + 65536),
    checkpoint_count = 1,
    log_level = os.getenv('MCC_SHIM_LOG_LEVEL') or 'warn',
}

wrap_text_protocol()

local mcc = require('memcached')
mcc.create('t', {
    listen = opts.host .. ':' .. opts.port,
    max_item_size = opts.max_item_size,
    max_connections = opts.max_connections,
})
