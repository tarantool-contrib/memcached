-- Microbenchmarks of the tarantool-contrib/memcached layers, without the network.
--
-- They split the per-request cost into parser, encoder, buffer copies,
-- engine and the bare box calls, to see what a C rewrite of each layer
-- could save. Run it pinned to one core:
--
--   docker run --rm --cpuset-cpus 1 -v <tarantool-contrib/memcached>:/src:ro \
--       -e 'LUA_CPATH=/src/.rocks/lib/tarantool/?.so;;' \
--       -v <bench>:/bench:ro --entrypoint "" mcc-dev:3.9 \
--       tarantool /bench/micro.lua
--
-- MICRO_EXPIRE=index measures with the exptime index (see below).
--
-- Every line is: CPU ns per operation (clock.thread()), Lua bytes allocated
-- per operation and new (not interned before) strings per operation
-- (misc.getmetrics() deltas). GC time is included in the CPU time.
--
-- MICRO_WAL_MODE=write (default none) runs it with the WAL: the writes then
-- yield for it, and the CPU time leaves out the WAL thread's.

local buffer = require('buffer')
local clock = require('clock')
local ffi = require('ffi')
local fio = require('fio')
local fiber = require('fiber')

local src = os.getenv('BENCH_SRC') or '/src'
package.path = src .. '/?.lua;' .. src .. '/?/init.lua;' .. package.path

local N = tonumber(os.getenv('MICRO_N') or 300000)

-- The loops don't yield: lift Tarantool 3's fiber slice limit.
fiber.set_max_slice(3600)

-- Results go here, so that the JIT can't drop the work as dead code.
local sink = {}
local function keep(i, v)
    sink[i % 64 + 1] = v
end
local KEYS = 10000

local work = fio.tempdir()
box.cfg {
    work_dir = work,
    wal_mode = os.getenv('MICRO_WAL_MODE') or 'none',
    memtx_memory = 512 * 1024 * 1024,
    log_level = 4,
}

local text = require('memcached.proto.text')
local engine = require('memcached.engine')
local schema = require('memcached.schema')
local server = require('memcached.server')
local backend_local = require('memcached.backend.local')

print(
    ('-- the C data path: %s'):format(
        engine.datapath_available() and 'built' or 'not built'
    )
)

local SPACE = '__mcc_micro'
-- MICRO_EXPIRE=index|scan: the expiration mode of the space (expire.mode;
-- 'index' keeps the exptime index), the default of memory storage if unset.
local space = schema.create(SPACE, 'memory', {
    expire = os.getenv('MICRO_EXPIRE'),
})
print(
    ('-- expire.mode: %s'):format(
        space.index.exptime ~= nil and 'index' or 'scan'
    )
)
local ctx = {space_name = SPACE}

local keys = {}
for i = 1, KEYS do
    keys[i] = 'memtier-' .. i
end
local function key(i)
    return keys[(i * 7919) % KEYS + 1]
end

local v100 = string.rep('x', 100)
local v4k = string.rep('y', 4096)

local results = {}

local function bench(name, fn, n)
    n = n or N
    for i = 1, math.min(n, 20000) do
        fn(i)
    end
    collectgarbage('collect')
    local m0 = misc.getmetrics()
    local t0 = clock.thread()
    for i = 1, n do
        fn(i)
    end
    local t1 = clock.thread()
    local m1 = misc.getmetrics()
    local r = {
        name = name,
        ns = (t1 - t0) / n * 1e9,
        bytes = (m1.gc_allocated - m0.gc_allocated) / n,
        strs = (m1.strhash_miss - m0.strhash_miss) / n,
        aborts = m1.jit_trace_abort - m0.jit_trace_abort,
    }
    table.insert(results, r)
    print(
        ('%-52s %8.0f ns %7.0f B %5.1f str %4d aborts'):format(
            r.name,
            r.ns,
            r.bytes,
            r.strs,
            r.aborts
        )
    )
end

local OPTS = {max_item_size = 1048576}

-- Parser ---------------------------------------------------------------

local get_lines, set100_lines, set4k_lines = {}, {}, {}
for i = 1, KEYS do
    get_lines[i] = 'get ' .. keys[i] .. '\r\n'
    set100_lines[i] = 'set ' .. keys[i] .. ' 0 0 100\r\n' .. v100 .. '\r\n'
    set4k_lines[i] = 'set ' .. keys[i] .. ' 0 0 4096\r\n' .. v4k .. '\r\n'
end
local get10_lines = {}
for i = 1, KEYS do
    local ks = {}
    for j = 0, 9 do
        ks[#ks + 1] = key(i + j)
    end
    get10_lines[i] = 'get ' .. table.concat(ks, ' ') .. '\r\n'
end

print('-- parser (proto/text.lua parse) and the input copy')
bench('parse "get <key>"', function(i)
    keep(i, text.parse(get_lines[i % KEYS + 1], 1, OPTS))
end)
bench('parse "get <10 keys>"', function(i)
    keep(i, text.parse(get10_lines[i % KEYS + 1], 1, OPTS))
end)
bench('parse "set <key> 0 0 100" + data', function(i)
    keep(i, text.parse(set100_lines[i % KEYS + 1], 1, OPTS))
end)
bench('parse "set <key> 0 0 4096" + data', function(i)
    keep(i, text.parse(set4k_lines[i % KEYS + 1], 1, OPTS))
end)

-- conn_step copies the whole input buffer into a Lua string.
local ibuf = buffer.ibuf()
local function fill(s)
    ibuf:reset()
    local p = ibuf:alloc(#s)
    ffi.copy(p, s, #s)
end
-- The first 8 bytes of the buffer change on every iteration, so every copy
-- is a new string, as with real traffic.
local counter = ffi.new('uint64_t[1]')
local function copy_bench(name, s)
    fill(s)
    bench(name, function(i)
        counter[0] = i
        ffi.copy(ibuf.rpos, counter, 8)
        keep(i, ffi.string(ibuf.rpos, ibuf:size()))
    end)
end
copy_bench('ffi.string(ibuf) of a get line (input copy)', get_lines[1])
copy_bench('ffi.string(ibuf) of a 100 B set', set100_lines[1])
copy_bench('ffi.string(ibuf) of a 4 KB set', set4k_lines[1])
local p16 = {}
for i = 1, 16 do
    p16[i] = get_lines[i]
end
copy_bench('ffi.string(ibuf) of 16 pipelined gets', table.concat(p16))

-- Encoder --------------------------------------------------------------

print('-- encoder (proto/text.lua encode)')
local get_req = text.parse(get_lines[1], 1, OPTS)
local get10_req = text.parse(get10_lines[1], 1, OPTS)
local set_req = text.parse(set100_lines[1], 1, OPTS)
-- 256 distinct values per size, so that the responses are not all equal.
local vals100, vals4k = {}, {}
for i = 1, 256 do
    vals100[i] = ('%08d'):format(i) .. v100:sub(9)
    vals4k[i] = ('%08d'):format(i) .. v4k:sub(9)
end
local function hit(i, vals)
    return {
        {
            status = 'HIT',
            item = {
                key = key(i),
                value = vals[i % 256 + 1],
                flags = 0,
                cas = 1ULL,
            },
        },
    }
end
local hits100, hits4k, hits10 = {}, {}, {}
for i = 1, 1024 do
    hits100[i] = hit(i, vals100)
    hits4k[i] = hit(i, vals4k)
    local h = {}
    for j = 1, 10 do
        h[j] = hit(i + j, vals100)[1]
    end
    hits10[i] = h
end
local miss = {{status = 'MISS'}}
bench('encode VALUE 100 B', function(i)
    keep(i, text.encode(get_req, hits100[i % 1024 + 1]))
end)
bench('encode VALUE 4 KB', function(i)
    keep(i, text.encode(get_req, hits4k[i % 1024 + 1]))
end)
bench('encode 10 x VALUE 100 B', function(i)
    keep(i, text.encode(get10_req, hits10[i % 1024 + 1]))
end)
bench('encode END (miss)', function(i)
    keep(i, text.encode(get_req, miss))
end)
bench('encode STORED', function(i)
    keep(i, text.encode(set_req, {status = 'STORED', cas = 1ULL}))
end)

-- Bare box calls ---------------------------------------------------------

print('-- bare box calls (the floor for any Lua implementation)')
local now = fiber.time()
for i = 1, KEYS do
    space:replace({keys[i], v100, 0, 0, i, now, now, true, false, 0})
end
bench('space:get(key)', function(i)
    keep(i, space:get(key(i)))
end)
bench('space:get(key) + read 9 fields', function(i)
    local t = space:get(key(i))
    local a, b, c, d, e, f, g, h, k = t[1],
        t[2], t[3],
        t[4], t[5],
        t[6], t[7],
        t[8], t[9]
    keep(i, {a, b, c, d, e, f, g, h, k})
end)
bench('space:get(key) + t:unpack(1, 9)', function(i)
    local t = space:get(key(i))
    keep(i, {t:unpack(1, 9)})
end)
bench('space:get(key) + t[2] (value as a Lua string)', function(i)
    local t = space:get(key(i))
    keep(i, t[2])
end)
bench('space:replace{10 fields, 100 B}', function(i)
    space:replace({key(i), v100, 0, 0, i, now, now, true, false, 0})
end)
bench('space:update(key, 2 ops)', function(i)
    space:update(key(i), {{'=', 7, now}, {'=', 8, true}})
end)
bench('box.begin() + box.commit()', function()
    box.begin()
    box.commit()
end)
bench('pcall(box.atomic, empty fn)', function()
    pcall(box.atomic, function() end)
end)
bench('box.info.ro', function(i)
    keep(i, box.info.ro)
end)
bench('schema.flush_time() (meta space get)', function(i)
    keep(i, schema.flush_time(SPACE))
end)
bench('fiber.time()', function(i)
    keep(i, fiber.time())
end)

-- Engine -----------------------------------------------------------------

print('-- engine.exec (one request per batch unless noted)')
for i = 1, KEYS do
    engine.exec(ctx, {
        {
            op = 'store',
            key = keys[i],
            value = v100,
            flags = 0,
            exptime = 0,
            mode = 'set',
        },
    })
    engine.exec(ctx, {{op = 'get', key = keys[i]}})
end
bench('engine get hit (no last-access write)', function(i)
    engine.exec(ctx, {{op = 'get', key = key(i)}})
end)
bench('engine get miss', function(i)
    engine.exec(ctx, {{op = 'get', key = 'nokey-' .. (i % KEYS)}})
end)
bench('engine set 100 B', function(i)
    engine.exec(ctx, {
        {
            op = 'store',
            key = key(i),
            value = v100,
            flags = 0,
            exptime = 0,
            mode = 'set',
        },
    })
end)
bench('engine set 100 B + first get (last-access write)', function(i)
    local k = key(i)
    engine.exec(ctx, {
        {
            op = 'store',
            key = k,
            value = v100,
            flags = 0,
            exptime = 0,
            mode = 'set',
        },
    })
    engine.exec(ctx, {{op = 'get', key = k}})
end)
bench('engine set 4 KB', function(i)
    engine.exec(ctx, {
        {
            op = 'store',
            key = key(i),
            value = v4k,
            flags = 0,
            exptime = 0,
            mode = 'set',
        },
    })
end)
bench('engine get hit 4 KB', function(i)
    engine.exec(ctx, {{op = 'get', key = key(i)}})
end)
for i = 1, KEYS do
    engine.exec(ctx, {
        {
            op = 'store',
            key = keys[i],
            value = v100,
            flags = 0,
            exptime = 0,
            mode = 'set',
        },
    })
    engine.exec(ctx, {{op = 'get', key = keys[i]}})
end
local batch16 = {}
bench('engine 16 gets in one batch (per get)', function(i)
    for j = 1, 16 do
        batch16[j] = {op = 'get', key = key(i * 16 + j)}
    end
    engine.exec(ctx, batch16)
end, N / 16
)
results[#results].ns = results[#results].ns / 16
results[#results].bytes = results[#results].bytes / 16
print(
    ('   per get: %.0f ns, %.0f B'):format(
        results[#results].ns,
        results[#results].bytes
    )
)

-- The whole request path without the socket ----------------------------

print('-- server.process (batch + engine + encode), no socket')
local srv = server.new({
    name = 'micro',
    listen = '127.0.0.1:0',
    backend = backend_local.new({
        name = 'micro',
        space_name = SPACE,
    }),
})
-- A connection's per-batch tables, reused as conn_step() does.
local batch = server.new_batch()
local items, protos = {}, {text}
local function process(req)
    items[1] = req
    return srv:process(items, protos, 1, batch)
end
bench('parse + process: get hit', function(i)
    local line = get_lines[i % KEYS + 1]
    local req = text.parse(line, 1, OPTS)
    process(req)
end)
bench('parse + process: set 100 B', function(i)
    local line = set100_lines[i % KEYS + 1]
    local req = text.parse(line, 1, OPTS)
    process(req)
end)
bench('parse + process: get 10 keys', function(i)
    local line = get10_lines[i % KEYS + 1]
    local req = text.parse(line, 1, OPTS)
    process(req)
end, N / 4
)
bench('parse + process: 1:10 mix (per request)', function(i)
    local line
    if i % 11 == 0 then
        line = set100_lines[i % KEYS + 1]
    else
        line = get_lines[i % KEYS + 1]
    end
    local req = text.parse(line, 1, OPTS)
    process(req)
end)

-- The connection loop without the socket --------------------------------

-- server.conn_step() on an input buffer filled with a pipeline: the input
-- handling, the C batch (when available), parsing, process(), the output
-- and a write to a socket stub that takes everything.
print('-- server.conn_step (input buffer -> replies), no socket')
-- The Lua loop's step: the C connection loop needs a socket (below).
local mstate = require('memcached.state')
mstate.cloop_off = true
local sock_stub = {
    syswrite = function(_, _, len)
        return len
    end,
}
local conn = {
    srv = srv,
    sock = sock_stub,
    ibuf = buffer.ibuf(),
    need = 0,
    nread = false,
    skip = 0,
    replied = false,
    closed = false,
    state = 'conn_parse_cmd',
    last_cmd = fiber.clock(),
    -- No socket: the Lua loop only (cloop_off below).
    fd = -1,
}
local function step_bench(name, inputs, per)
    local cin = conn.ibuf
    bench(name, function(i)
        local s = inputs[i % #inputs + 1]
        local p = cin:alloc(#s)
        ffi.copy(p, s, #s)
        server.conn_step(conn)
    end, N / per
    )
    local r = results[#results]
    r.ns = r.ns / per
    r.bytes = r.bytes / per
    print(('   per request: %.0f ns, %.0f B'):format(r.ns, r.bytes))
end
local function pipelines(depth, line)
    local res = {}
    for i = 1, 1024 do
        local p = {}
        for j = 1, depth do
            p[j] = line(i * depth + j)
        end
        res[i] = table.concat(p)
    end
    return res
end
local function mix_line(i)
    if i % 11 == 0 then
        return set100_lines[i % KEYS + 1]
    end
    return get_lines[i % KEYS + 1]
end
step_bench(
    'conn_step: get hit',
    pipelines(1, function(i)
        return get_lines[i % KEYS + 1]
    end),
    1
)
step_bench(
    'conn_step: set 100 B',
    pipelines(1, function(i)
        return set100_lines[i % KEYS + 1]
    end),
    1
)
step_bench('conn_step: 1:10 mix', pipelines(1, mix_line), 1)
step_bench('conn_step: 1:10 mix, pipeline 16', pipelines(16, mix_line), 16)
-- One transaction for the 16 sets on the C path (datapath.c "The group").
step_bench(
    'conn_step: set 100 B, pipeline 16',
    pipelines(16, function(i)
        return set100_lines[i % KEYS + 1]
    end),
    16
)
step_bench(
    'conn_step: get 10 keys',
    pipelines(1, function(i)
        return get10_lines[i % KEYS + 1]
    end),
    1
)
step_bench(
    'conn_step: 16 gets + mn',
    pipelines(1, function(i)
        local p = {}
        for j = 1, 16 do
            p[j] = get_lines[(i * 16 + j) % KEYS + 1]
        end
        return table.concat(p) .. 'mn\r\n'
    end),
    16
)
step_bench(
    'conn_step: mg v f c, pipeline 16 + mn',
    pipelines(1, function(i)
        local p = {}
        for j = 1, 16 do
            p[j] = 'mg ' .. keys[(i * 16 + j) % KEYS + 1] .. ' v f c\r\n'
        end
        return table.concat(p) .. 'mn\r\n'
    end),
    16
)
step_bench(
    'conn_step: ms 100 B',
    pipelines(1, function(i)
        return 'ms ' .. keys[i % KEYS + 1] .. ' 100 T0\r\n' .. v100 .. '\r\n'
    end),
    1
)
-- Counters of their own: the values of the keys are no numbers.
for i = 1, KEYS do
    space:replace({'n' .. i, '0', 0, 0, 1, 0, 0, false, false, 0, false})
end
step_bench(
    'conn_step: incr',
    pipelines(1, function(i)
        return 'incr n' .. (i % KEYS + 1) .. ' 1\r\n'
    end),
    1
)
-- A command left to Lua every 4 commands: the C batch, the Lua one and the
-- C batch again in each batch.
step_bench(
    'conn_step: 12 gets + 4 me, pipeline 16',
    pipelines(1, function(i)
        local p = {}
        for j = 1, 16 do
            local k = keys[(i * 16 + j) % KEYS + 1]
            p[j] = (j % 4 == 0 and 'me ' or 'get ') .. k .. '\r\n'
        end
        return table.concat(p)
    end),
    16
)

mstate.cloop_off = false

-- The connection loop over a socket pair ---------------------------------

-- Connections of the server (server.on_accept()) on socket pairs, each with
-- a client fiber in this thread that sends a command and waits for its
-- reply (pipeline 1): the CPU time per request is that of the client and
-- the server, the reads, writes and fiber switches of both included. The
-- client is the same in every line: the differences are the server's. With
-- the C connection loop (cloop.c), the Lua loop with the C batch
-- (mcc.datapath_loop(false)) and the Lua engine (mcc.datapath(false)).
print('-- connection loop over socket pairs, 16 clients, pipeline 1')
local socket = require('socket')
local function pair_bench(name, line_of, per_conn)
    local clients = {}
    for c = 1, 16 do
        local a, b = socket.socketpair('AF_UNIX', 'SOCK_STREAM', 0)
        a:nonblock(true)
        server.on_accept(srv, b)
        clients[c] = a
    end
    local done = 0
    local function client(c)
        local s = clients[c]
        for i = 1, per_conn do
            local line, term = line_of(i * 16 + c)
            s:syswrite(line)
            local got = ''
            while true do
                local chunk = s:sysread(65536)
                if chunk == nil then
                    s:readable()
                else
                    got = got .. chunk
                    if got:sub(-#term) == term then
                        break
                    end
                end
            end
        end
        done = done + 1
    end
    collectgarbage('collect')
    local m0 = misc.getmetrics()
    local t0 = clock.thread()
    for c = 1, 16 do
        fiber.new(client, c)
    end
    while done < 16 do
        fiber.sleep(0.001)
    end
    local t1 = clock.thread()
    local m1 = misc.getmetrics()
    local n = per_conn * 16
    print(
        ('%-52s %8.0f ns %7.0f B'):format(
            name,
            (t1 - t0) / n * 1e9,
            (m1.gc_allocated - m0.gc_allocated) / n
        )
    )
    for c = 1, 16 do
        clients[c]:close()
    end
    -- The server's ends see the EOF.
    fiber.sleep(0.01)
end
local mcc = require('memcached')
local PAIR_N = math.floor(N / 16 / 2)
local modes = {
    {'C loop', true, true},
    {'Lua loop, C batch', true, false},
    {'Lua engine', false, false},
}
for _, mode in ipairs(modes) do
    mcc.datapath(mode[2])
    mcc.datapath_loop(mode[3])
    local tag = mode[1]
    pair_bench('get hit: ' .. tag, function(i)
        return get_lines[i % KEYS + 1], 'END\r\n'
    end, PAIR_N
    )
    pair_bench('1:10 set:get: ' .. tag, function(i)
        if i % 11 == 0 then
            return set100_lines[i % KEYS + 1], 'STORED\r\n'
        end
        return get_lines[i % KEYS + 1], 'END\r\n'
    end, PAIR_N
    )
    pair_bench('get 10 keys: ' .. tag, function(i)
        return get10_lines[i % KEYS + 1], 'END\r\n'
    end, math.floor(PAIR_N / 4)
    )
end
mcc.datapath(true)
mcc.datapath_loop(true)

print(('-- %d results kept'):format(#sink))
os.exit(0)
