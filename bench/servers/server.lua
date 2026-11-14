-- A Tarantool memcached server under the benchmark.
--
-- Both Tarantool servers (tarantool-contrib/memcached and the C module
-- tarantool/memcached) run through this script with the same box.cfg, so
-- the only difference is the memcached implementation.
--
-- Environment:
--   BENCH_IMPL         mcc (tarantool-contrib/memcached) | cmod (tarantool/memcached)
--                      | mcc-submit (mcc with commit_wait = 'submit')
--                      | mcc-raw | mcc-fake (see below)
--   BENCH_PORT         memcached port, default 11211
--   BENCH_CTRL_PORT    control port (metrics, profiler), default 11299
--   BENCH_WAL_MODE     none | write, default none
--   BENCH_WORK_DIR     default /work
--   BENCH_MEMTX_MB     memtx_memory in MiB, default 1024
--   BENCH_SRC          source root of the implementation, default /src
--   BENCH_GC_PAUSE     collectgarbage('setpause', N) if set (LuaJIT: 200)
--   BENCH_GC_STEPMUL   collectgarbage('setstepmul', N) if set (LuaJIT: 200)
--   BENCH_JITV         a file: log JIT trace events (jit.v) from the start
--   BENCH_MCC_OPTS     mcc: more instance options, a JSON object, e.g.
--                      '{"track_access": false}'
--   BENCH_APP_THREADS  box.cfg.app_threads (one group, 'app'), for
--                      BENCH_MCC_OPTS='{"threads": "app"}'
--
-- Diagnostic modes: tarantool-contrib/memcached's server.lua (transport, parser,
-- encoder) with another backend instead of engine.lua:
--   mcc-raw   one bare box call per request: get = space:get + t[2], t[3];
--             store = space:replace; no transactions, expiration, CAS
--             bookkeeping or last-access writes;
--   mcc-fake  no box at all: every get misses, every store succeeds.
-- mcc - mcc-raw is the cost of the engine's semantics, mcc-raw - mcc-fake
-- the cost of the bare box calls from Lua, mcc-fake the front-end.
--
-- Control port: connect, send one line, read the JSON reply, the server
-- closes the connection. Commands:
--   metrics                         Lua GC and JIT metrics, process CPU
--   jitp_start <mode> <file>        require('jit.p').start(mode, file)
--   jitp_stop
--   sysprof_start <mode> <file>     misc.sysprof.start{mode, path}
--   sysprof_stop                    returns misc.sysprof.report()
--   gc_count                        collectgarbage('count')
--   flush                           delete all the data

local fiber = require('fiber')
local fio = require('fio')
local json = require('json')
local log = require('log')
local socket = require('socket')

local impl = os.getenv('BENCH_IMPL') or 'mcc'
local port = tonumber(os.getenv('BENCH_PORT') or 11211)
local ctrl_port = tonumber(os.getenv('BENCH_CTRL_PORT') or 11299)
local wal_mode = os.getenv('BENCH_WAL_MODE') or 'none'
local work_dir = os.getenv('BENCH_WORK_DIR') or '/work'
local memtx_mb = tonumber(os.getenv('BENCH_MEMTX_MB') or 1024)
local src = os.getenv('BENCH_SRC') or '/src'
local jitv = os.getenv('BENCH_JITV') or ''
if jitv ~= '' then
    require('jit.v').start(jitv)
end
local gc_pause = tonumber(os.getenv('BENCH_GC_PAUSE') or '')
local gc_stepmul = tonumber(os.getenv('BENCH_GC_STEPMUL') or '')

package.path = src .. '/?.lua;' .. src .. '/?/init.lua;' .. package.path
-- tarantool/memcached builds memcached/internal.so in its tree; this module's
-- C data path is in the tree tt package build makes.
package.cpath = src .. '/?.so;'
    .. src .. '/.rocks/lib/tarantool/?.so;'
    .. package.cpath

fio.mkdir(work_dir)
box.cfg {
    work_dir = work_dir,
    wal_mode = wal_mode,
    memtx_memory = memtx_mb * 1024 * 1024,
    checkpoint_interval = 0,
    log_level = 5,
    app_threads = tonumber(os.getenv('BENCH_APP_THREADS') or '') or nil,
}

local inst
local space_name
if impl == 'mcc' or impl == 'mcc-submit' then
    local mcc = require('memcached')
    local opts = {listen = '0.0.0.0:' .. port}
    if impl == 'mcc-submit' then
        opts.commit_wait = 'submit'
    end
    local extra = os.getenv('BENCH_MCC_OPTS') or ''
    if extra ~= '' then
        for k, v in pairs(json.decode(extra)) do
            opts[k] = v
        end
    end
    inst = mcc.create('bench', opts)
    space_name = '__mcc_bench'
elseif impl == 'cmod' then
    local memcached = require('memcached')
    inst = memcached.create(
        'bench',
        '0.0.0.0:' .. port,
        {protocol = 'ascii'}
    )
    space_name = '__mc_bench'
elseif impl == 'mcc-raw' or impl == 'mcc-fake' then
    local schema = require('memcached.schema')
    local server = require('memcached.server')
    space_name = '__mcc_bench'
    local space = schema.create(space_name, 'memory')
    local raw = impl == 'mcc-raw'
    local backend = {}
    function backend.exec(_, reqs)
        local resps = {}
        for i = 1, #reqs do
            local req = reqs[i]
            if req.op == 'get' then
                local t = raw and space:get(req.key) or nil
                if t == nil then
                    resps[i] = {status = 'MISS'}
                else
                    resps[i] = {
                        status = 'HIT',
                        item = {
                            key = req.key,
                            value = t[2],
                            flags = t[3],
                            cas = 1ULL,
                        },
                    }
                end
            elseif req.op == 'store' then
                if raw then
                    space:replace({
                        req.key,
                        req.value,
                        req.flags or 0,
                        0,
                        1,
                        0,
                        0,
                        false,
                        false,
                        0,
                    })
                end
                resps[i] = {status = 'STORED', cas = 1ULL}
            else
                resps[i] = {status = 'SERVER_ERROR', message = 'n/a'}
            end
        end
        return resps
    end
    function backend.flush_all()
        return {status = 'OK'}
    end
    function backend.stats()
        return {}
    end
    inst = server.new({
        name = 'bench',
        listen = '0.0.0.0:' .. port,
        backend = backend,
    })
    inst:start()
else
    error('unknown BENCH_IMPL ' .. impl)
end
_G.bench_instance = inst

if gc_pause ~= nil then
    collectgarbage('setpause', gc_pause)
end
if gc_stepmul ~= nil then
    collectgarbage('setstepmul', gc_stepmul)
end

-- Process CPU time in seconds, total and per thread (name -> seconds).
local function cpu_times()
    local tck = 100
    local function parse(path)
        local f = io.open(path)
        if f == nil then
            return nil
        end
        local s = f:read('*a')
        f:close()
        -- comm may contain spaces: skip to the last ')'.
        local name = s:match('%((.*)%)')
        local rest = s:sub(s:find('%)[^%)]*$') + 2)
        local fields = {}
        for v in rest:gmatch('%S+') do
            fields[#fields + 1] = v
        end
        -- utime and stime are fields 14 and 15, i.e. 12 and 13 after state.
        return name, (tonumber(fields[12]) + tonumber(fields[13])) / tck
    end
    local _, total = parse('/proc/self/stat')
    local threads = {}
    for _, dir in ipairs(fio.glob('/proc/self/task/*')) do
        local name, t = parse(dir .. '/stat')
        if name ~= nil then
            threads[name] = (threads[name] or 0) + t
        end
    end
    return total, threads
end

local function metrics()
    local total, threads = cpu_times()
    return {
        impl = impl,
        lua_mem_kb = collectgarbage('count'),
        misc = misc.getmetrics(),
        cpu = total,
        threads = threads,
        space_len = box.space[space_name] and box.space[space_name]:len(),
        space_bsize = box.space[space_name] and box.space[space_name]:bsize(),
        time = fiber.time(),
    }
end

local commands = {}

function commands.metrics()
    return metrics()
end

function commands.gc_count()
    return {kb = collectgarbage('count')}
end

function commands.jitp_start(mode, file)
    require('jit.p').start(mode or 'f', file)
    return {ok = true}
end

function commands.jitp_stop()
    require('jit.p').stop()
    return {ok = true}
end

function commands.sysprof_start(mode, file)
    local ok, err = misc.sysprof.start({
        mode = mode or 'D',
        interval = 1,
        path = file,
    })
    return {ok = ok, err = err and tostring(err)}
end

function commands.sysprof_stop()
    local ok, err = misc.sysprof.stop()
    local report = misc.sysprof.report()
    return {ok = ok, err = err and tostring(err), report = report}
end

function commands.flush()
    box.space[space_name]:truncate()
    return {ok = true}
end

local function ctrl_handler(s)
    local line = s:read('\n', 10)
    if line == nil then
        s:close()
        return
    end
    local args = {}
    for w in line:gmatch('%S+') do
        args[#args + 1] = w
    end
    local fn = commands[args[1] or '']
    local ok, res
    if fn == nil then
        ok, res = false, 'unknown command'
    else
        ok, res = pcall(fn, args[2], args[3])
    end
    if not ok then
        res = {error = tostring(res)}
    end
    s:write(json.encode(res) .. '\n')
    s:close()
end

assert(socket.tcp_server('127.0.0.1', ctrl_port, ctrl_handler))
log.info(
    'bench: %s on port %d, control port %d, wal_mode %s',
    impl,
    port,
    ctrl_port,
    wal_mode
)
