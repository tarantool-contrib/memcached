-- Transport: listening, connection loop, pipelining, dispatch to a backend.
--
-- No TX-only box APIs here: the module may run in an application thread.
--
-- A server object:
--   srv = server.new({name, listen, reuseport, max_connections,
--                     max_item_size, idle_timeout, backend})
--   srv:start()          bind and accept; raises on errors
--   srv:stop()           close the listener and all the connections
--   srv:rebind(listen)   listen on a new URI, keeping the connections
--   srv:release()        stop listening, keeping the connections, until
--                        the next rebind()
--   srv:cfg(opts)        max_connections, max_item_size, idle_timeout,
--                        backend, reuseport
--   srv:info()           counters
-- Methods and the connection loop look the code up in package.loaded on
-- every call / batch, so that a reload of the module takes effect for the
-- existing servers and connections.

local buffer = require('buffer')
local errno = require('errno')
local ffi = require('ffi')
local fiber = require('fiber')
local log = require('log')
local socket = require('socket')
local uri = require('uri')

local M = {}

local SELF = 'memcached.server'
local TEXT = 'memcached.proto.text'
local META = 'memcached.proto.meta'

-- The module version and the memcached version it is compatible with. The
-- version is that of memcached/version.lua, which tt writes when it builds
-- the package (app.manifest.toml); a source tree tt didn't build has none.
local has_version, version_lua = pcall(require, 'memcached.version')
M.VERSION = has_version and version_lua.version or 'scm'
M.MEMCACHED_VERSION = '1.6.45'

M.DEFAULT_MAX_CONNECTIONS = 4096
M.DEFAULT_MAX_ITEM_SIZE = 1048576
-- Close a connection that has waited for a command for longer than this
-- many seconds; 0 = never (memcached's -o idle_timeout).
M.DEFAULT_IDLE_TIMEOUT = 0
-- Read at least this much at once.
M.READ_SIZE = 16384
-- At most this many commands per batch.
M.MAX_BATCH = 512
-- An empty input buffer bigger than this is freed.
M.BUFFER_KEEP = 65536
M.BACKLOG = 1024

M.TOO_MANY_CONNECTIONS = 'ERROR Too many open connections\r\n'

-- Ops executed by a backend; anything else is answered by the front-end.
local ENGINE_OPS = {get = true, store = true, delete = true, arith = true}
-- Front-end ops that must see the effect of the requests before them in the
-- batch: these are executed first.
local BARRIER_OPS = {flush = true, stats = true}
-- Front-end ops whose reply is streamed by M.stream() after the batch
-- (lru_crawler dumps). Such an op ends a batch; like in memcached, it fails
-- if other commands precede it in the batch.
local STREAM_OPS = {lru_crawler = true}
M.PIPELINE_ERROR = 'ERROR cannot pipeline other commands before %s\r\n'

-- Second letters of the meta commands: mg ms md ma mn me.
local META_SECOND = {
    [string.byte('g')] = true,
    [string.byte('s')] = true,
    [string.byte('d')] = true,
    [string.byte('a')] = true,
    [string.byte('n')] = true,
    [string.byte('e')] = true,
}
local BYTE_M = string.byte('m')
local BYTE_SPACE = 32
local BYTE_CR = 13
local BYTE_LF = 10

local byte = string.byte
local concat = table.concat
local fiber_clock = fiber.clock
local iowait = socket.iowait

-- socket.iowait() events: readable.
local COIO_READ = 1

-- Connection states, named as memcached names them ("stats conns").
local CONN_LISTENING = 'conn_listening' -- the listener
local CONN_NEW_CMD = 'conn_new_cmd'     -- accepted, no command yet
local CONN_READ = 'conn_read'           -- waiting for a command
local CONN_NREAD = 'conn_nread'         -- waiting for the value of a command
local CONN_SWALLOW = 'conn_swallow'     -- discarding a too large value
local CONN_PARSE_CMD = 'conn_parse_cmd' -- parsing and executing a batch
local CONN_MWRITE = 'conn_mwrite'       -- writing, the client is not reading

local const_char_ptr = ffi.typeof('const char *')

pcall(ffi.cdef, 'int getpid(void);')

local function current()
    return package.loaded[SELF] or M
end

local mt = {
    __index = function(_, k)
        return current()[k]
    end,
}

-- Protocol modules ------------------------------------------------------

-- Whether proto/meta.lua failed to load. Kept in the module (not in the
-- state), so a reload retries.
local meta_missing = false

local function text_module()
    return package.loaded[TEXT] or require(TEXT)
end

-- proto/meta.lua if it exists, otherwise nil (meta commands then go to the
-- text parser, which answers ERROR).
local function meta_module()
    local mod = package.loaded[META]
    if type(mod) == 'table' then
        return mod
    end
    if meta_missing then
        return nil
    end
    local ok, res = pcall(require, META)
    if ok and type(res) == 'table' then
        return res
    end
    if not ok
        and not tostring(res):find(
            "module '" .. META .. "' not found",
            1,
            true
        ) then
        log.error(
            'memcached: failed to load %s: %s',
            META,
            tostring(res)
        )
    end
    meta_missing = true
    return nil
end
M.meta_module = meta_module

-- Stats -----------------------------------------------------------------

local COUNTERS = {
    'total_connections',
    'rejected_connections',
    'cmd_get',
    'cmd_set',
    'cmd_flush',
    'cmd_touch',
    'cmd_meta',
    'get_hits',
    'get_misses',
    'delete_misses',
    'delete_hits',
    'incr_misses',
    'incr_hits',
    'decr_misses',
    'decr_hits',
    'cas_misses',
    'cas_hits',
    'cas_badval',
    'touch_hits',
    'touch_misses',
    'store_too_large',
    'store_no_memory',
    'bytes_read',
    'bytes_written',
    'total_items',
    -- Connections closed by idle_timeout.
    'idle_kicks',
    -- Error replies by class (ERROR, CLIENT_ERROR, SERVER_ERROR), for the
    -- metrics; "stats" doesn't show them (NOT_STATS).
    'protocol_errors',
    'client_errors',
    'server_errors',
}
M.COUNTERS = COUNTERS

-- Counters that "stats" doesn't list in this order (total_items goes at
-- the end, idle_kicks after auth_errors and only with idle_timeout, as in
-- memcached).
local NOT_STATS = {
    total_items = true,
    idle_kicks = true,
    protocol_errors = true,
    client_errors = true,
    server_errors = true,
}

-- Reply class -> error counter.
local ERROR_COUNTER = {
    ERROR = 'protocol_errors',
    CLIENT_ERROR = 'client_errors',
    SERVER_ERROR = 'server_errors',
}
M.ERROR_COUNTER = ERROR_COUNTER

local function new_stats()
    local stats = {}
    for _, name in ipairs(COUNTERS) do
        stats[name] = 0
    end
    return stats
end

local function inc(stats, name)
    stats[name] = (stats[name] or 0) + 1
end

-- Count an error reply line ("ERROR", "CLIENT_ERROR ...", "SERVER_ERROR
-- ...") by its class.
local function count_error_line(stats, line)
    local name = ERROR_COUNTER[line:match('^[%u_]+')]
    if name ~= nil then
        inc(stats, name)
    end
end

-- Update the counters for an engine request and its response. A string
-- response is a hit on the C data path (engine.lua fast()).
function M.account(stats, req, resp)
    local op = req.op
    local status = type(resp) == 'string' and 'HIT' or resp.status
    local err = ERROR_COUNTER[status]
    if err ~= nil then
        inc(stats, err)
    elseif req.too_large or (req.meta ~= nil and req.meta.too_large) then
        -- Answered "SERVER_ERROR object too large for cache".
        inc(stats, 'server_errors')
    end
    if op == 'get' then
        if req.touch_ttl ~= nil then
            inc(stats, 'cmd_touch')
            inc(stats, status == 'HIT' and 'touch_hits' or 'touch_misses')
        end
        if not req.touch_only then
            inc(stats, 'cmd_get')
            inc(stats, status == 'HIT' and 'get_hits' or 'get_misses')
        end
    elseif op == 'store' then
        inc(stats, 'cmd_set')
        if status == 'STORED' then
            inc(stats, 'total_items')
        elseif resp.no_memory then
            -- "SERVER_ERROR out of memory storing object" (engine.lua).
            inc(stats, 'store_no_memory')
        end
        if req.cas ~= nil
            and (req.mode == nil or req.mode == 'set' or req.mode == 'replace') then
            if status == 'STORED' then
                inc(stats, 'cas_hits')
            elseif status == 'EXISTS' then
                inc(stats, 'cas_badval')
            elseif status == 'NOT_FOUND' then
                inc(stats, 'cas_misses')
            end
        end
    elseif op == 'delete' then
        if req.too_large then
            inc(stats, 'cmd_set')
            inc(stats, 'store_too_large')
        elseif status == 'DELETED' then
            inc(stats, 'delete_hits')
        elseif status == 'NOT_FOUND' then
            inc(stats, 'delete_misses')
        end
    elseif op == 'arith' then
        local decr = req.incr == false
        if status == 'NOT_FOUND' then
            inc(stats, decr and 'decr_misses' or 'incr_misses')
        elseif status == 'STORED' then
            inc(stats, decr and 'decr_hits' or 'incr_hits')
        end
    end
end

function M.version()
    return M.MEMCACHED_VERSION .. '-mcc-' .. M.VERSION
end

local function error_message(err)
    if type(err) == 'cdata' or type(err) == 'table' then
        err = err.message or tostring(err)
    end
    return (tostring(err):gsub('[\r\n]+', ' '))
end

local function backend_stats(srv)
    local backend = srv.backend
    if backend == nil then
        return {}
    end
    local ok, res = pcall(backend.stats, backend)
    if not ok or type(res) ~= 'table' then
        return {}
    end
    return res
end

local function listen_port(srv)
    local lsock = srv.listener
    if lsock == nil then
        return 0
    end
    local name = lsock:name()
    return name and tonumber(name.port) or 0
end

-- The general-purpose stats: an array of {name, value}.
function M.general_stats(srv)
    local now = fiber.time()
    local b = backend_stats(srv)
    -- With application threads, the backend sums the counters, the
    -- connections and the threads up (see threads.lua).
    local st = type(b.counters) == 'table' and b.counters or M.sync_stats(srv)
    local list = {
        {'pid', ffi.C.getpid()},
        {'uptime', math.floor(now - srv.started)},
        {'time', math.floor(now)},
        {'version', M.version()},
        {'pointer_size', ffi.abi('64bit') and 64 or 32},
        {'max_connections', srv.max_connections},
        {'curr_connections', b.curr_connections or srv.nconns},
    }
    for _, name in ipairs(COUNTERS) do
        if not NOT_STATS[name] then
            table.insert(list, {name, st[name] or 0})
        end
    end
    table.insert(list, {'auth_cmds', 0})
    table.insert(list, {'auth_errors', 0})
    if (srv.idle_timeout or 0) > 0 then
        table.insert(list, {'idle_kicks', st.idle_kicks or 0})
    end
    local tail = {
        {'limit_maxbytes', b.limit_maxbytes or 0},
        {'accepting_conns', srv.listener ~= nil and 1 or 0},
        {'threads', b.threads or 1},
        {'bytes', b.bytes or 0},
        {'curr_items', b.curr_items or 0},
        {'total_items', st.total_items or 0},
        -- The eviction counters of the space (evict.lua).
        {'evicted_unfetched', b.evicted_unfetched or 0},
        {'evicted_active', b.evicted_active or 0},
        {'evictions', b.evictions or 0},
        {'reclaimed', b.reclaimed or 0},
        {'direct_reclaims', b.direct_reclaims or 0},
    }
    for _, kv in ipairs(tail) do
        table.insert(list, kv)
    end
    return list
end

function M.settings_stats(srv)
    local b = backend_stats(srv)
    -- memcached's idle_timeout is whole seconds; a fraction is shown as is.
    local idle = srv.idle_timeout or 0
    if idle ~= math.floor(idle) then
        idle = tostring(idle)
    end
    return {
        {'maxbytes', b.limit_maxbytes or 0},
        {'maxconns', srv.max_connections},
        {'tcpport', listen_port(srv)},
        {'verbosity', srv.verbosity},
        {'evictions', b.evict and 'on' or 'off'},
        {'cas_enabled', 'yes'},
        {'item_size_max', srv.parse_opts.max_item_size},
        {'num_threads', b.threads or 1},
        {'binding_protocol', 'ascii'},
        {'idle_timeout', idle},
        {'reuseport', srv.reuseport and 'yes' or 'no'},
    }
end

-- "stats conns" ----------------------------------------------------------

-- An address as memcached shows it: tcp:<ip>:<port>, tcp6:[<ip>]:<port>,
-- unix:<path>, or ?:<AF 0> when there is none (memcached keeps no peer
-- address for a unix socket client).
local function addr_text(name)
    if name == nil then
        return '?:<AF 0>'
    end
    local family = name.family
    if family == 'AF_INET' then
        return ('tcp:%s:%s'):format(name.host, name.port)
    elseif family == 'AF_INET6' then
        return ('tcp6:[%s]:%s'):format(name.host, name.port)
    elseif family == 'AF_UNIX' and name.port ~= nil and #name.port > 1 then
        return 'unix:' .. name.port
    end
    return '?:<AF 0>'
end

-- The listener and the connections of a server for "stats conns": an array
-- of {id, addr, listen_addr?, state, secs}, plain data (it crosses
-- threads). The id is the socket fd, unique in the process, as in
-- memcached. secs is the time since the last batch (since the bind for
-- the listener). Two syscalls per connection (getsockname, getpeername).
function M.conn_list(srv)
    local now = fiber_clock()
    local list = {}
    local lsock = srv.listener
    if lsock ~= nil then
        table.insert(list, {
            id = lsock:fd(),
            addr = addr_text(lsock:name()),
            state = CONN_LISTENING,
            secs = math.floor(now - (srv.listen_clock or now)),
        })
    end
    for _, conn in pairs(srv.conns) do
        local sock = conn.sock
        if not conn.closed and sock ~= nil then
            local name = sock:name()
            local unix = name ~= nil and name.family == 'AF_UNIX'
            table.insert(list, {
                id = conn.fd or sock:fd(),
                addr = unix and '?:<AF 0>' or addr_text(sock:peer()),
                listen_addr = addr_text(name),
                state = conn.state or CONN_PARSE_CMD,
                secs = math.floor(now - (conn.last_cmd or now)),
            })
        end
    end
    return list
end

-- The connections of the whole instance: with application threads, the
-- backend collects those of every thread (backend:conns()); otherwise this
-- server's.
local function all_conns(srv)
    local backend = srv.backend
    if backend ~= nil and backend.conns ~= nil then
        local ok, list = pcall(backend.conns, backend)
        if ok and type(list) == 'table' then
            return list
        end
    end
    return M.conn_list(srv)
end

-- conn_list() entries as the "stats conns" lines, by id:
-- <id>:addr, <id>:listen_addr (not for a listener), <id>:state,
-- <id>:secs_since_last_cmd.
function M.conns_stats(list)
    table.sort(list, function(a, b)
        return a.id < b.id
    end)
    local stats = {}
    for _, c in ipairs(list) do
        local id = tostring(c.id)
        table.insert(stats, {id .. ':addr', c.addr})
        if c.listen_addr ~= nil then
            table.insert(stats, {id .. ':listen_addr', c.listen_addr})
        end
        table.insert(stats, {id .. ':state', c.state})
        table.insert(stats, {id .. ':secs_since_last_cmd', c.secs})
    end
    return stats
end

-- Zero the counters in place: a batch in progress holds the table.
function M.reset_stats(srv)
    local stats = M.sync_stats(srv)
    for name in pairs(stats) do
        stats[name] = 0
    end
end

-- "stats detail on|off|dump" as memcached answers it with no per-prefix
-- stats recorded (detail off, its default; dump_enabled on): on and off
-- are OK, dump is an empty list, anything else is the usage error. The
-- argument may be a prefix of the word (strncmp() over its length), "o"
-- being on. on records nothing here.
local DETAIL_USAGE = 'usage: stats detail on|off|dump'

local function detail_reply(arg)
    if arg ~= nil then
        for _, word in ipairs({'on', 'off', 'dump'}) do
            if word:sub(1, #arg) == arg then
                if word == 'dump' then
                    return {status = 'STATS', stats = {}}
                end
                return {status = 'OK'}
            end
        end
    end
    return {status = 'CLIENT_ERROR', message = DETAIL_USAGE}
end

-- The response to "stats [args]".
function M.stats_reply(srv, args)
    local sub = args and args[1]
    if sub == nil then
        return {status = 'STATS', stats = M.general_stats(srv)}
    elseif sub == 'reset' then
        M.reset_stats(srv)
        -- With application threads: the counters of every thread.
        local backend = srv.backend
        if backend ~= nil and backend.reset_stats ~= nil then
            pcall(backend.reset_stats, backend)
        end
        return {status = 'RESET'}
    elseif sub == 'settings' then
        return {status = 'STATS', stats = M.settings_stats(srv)}
    elseif sub == 'conns' then
        return {status = 'STATS', stats = M.conns_stats(all_conns(srv))}
    elseif sub == 'sizes' then
        -- As memcached without -o track_sizes, its default.
        return {status = 'STATS', stats = {{'sizes_status', 'disabled'}}}
    elseif sub == 'items' or sub == 'slabs' then
        return {status = 'STATS', stats = {}}
    elseif sub == 'detail' then
        return detail_reply(args[2])
    end
    return {status = 'ERROR'}
end

-- Batches ---------------------------------------------------------------

-- A command answered by the front-end.
local function frontend(srv, req)
    local op = req.op
    if op == 'flush' then
        inc(srv.stats, 'cmd_flush')
        local backend = srv.backend
        local ok, res = pcall(backend.flush_all, backend, req.delay or 0)
        if not ok then
            return {status = 'SERVER_ERROR', message = error_message(res)}
        end
        return res or {status = 'OK'}
    elseif op == 'version' then
        return {status = 'VERSION', version = M.version()}
    elseif op == 'verbosity' then
        srv.verbosity = req.level or 0
        return {status = 'OK'}
    elseif op == 'stats' then
        return M.stats_reply(srv, req.args)
    end
    return nil
end

-- The per-batch tables -----------------------------------------------------
--
-- A batch needs a few arrays: the parsed requests and their protocol
-- modules (conn_step), the responses, the pending engine requests with
-- their owners, the response arrays of the multi-key gets and the output
-- parts. A connection keeps them between its batches (conn.batch, see
-- new_batch()) instead of allocating them per batch: it runs one batch at a
-- time, and while its batch yields in the backend the other connections use
-- their own. A batch clears every entry it set when it is done, so that an
-- idle connection keeps no request, response or value alive; an array that
-- had more than SCRATCH_MAX entries is dropped instead (an array part never
-- shrinks).
--
-- The responses themselves are the backend's and are never reused (the
-- engine shares the constant ones). The pending array is passed to
-- backend:exec(): it is kept only when the backend says that it keeps no
-- reference to it after exec() returns (backend.borrows_reqs, see the
-- backends); otherwise every batch has a new one.
M.SCRATCH_MAX = 128
-- The same for the output parts (a reply to a retrieval has several).
M.OUT_KEEP = 512

-- The per-batch tables of a connection (conn.batch when conn is given).
local function new_batch(conn)
    local batch = {
        items = {},
        protos = {},
        resps = {},
        pending = {},
        owners = {},
        subs = {},
        -- The response arrays of the multi-key gets, by their order in the
        -- batch.
        multis = {},
        out = {},
    }
    if conn ~= nil then
        conn.batch = batch
    end
    return batch
end
M.new_batch = new_batch

-- Clear the first n entries of an array; false when it is too big to keep.
local function clear(t, n)
    for i = 1, n do
        t[i] = nil
    end
    return n <= M.SCRATCH_MAX
end

-- Clear an array from index i on.
local function clear_from(t, i)
    while t[i] ~= nil do
        t[i] = nil
        i = i + 1
    end
end

-- conn_step's arrays after a batch of n commands.
local function release_items(batch, n)
    if not clear(batch.items, n) then
        batch.items = {}
    end
    if not clear(batch.protos, n) then
        batch.protos = {}
    end
end

-- Send the first count pending engine requests of a batch to the backend
-- and put the responses in place: resps[owners[k]], or
-- resps[owners[k]][subs[k]] for the keys of a multi-key get. Returns
-- whether the backend borrowed the pending array only (see above), and the
-- items the reads hold at the end, which the local backend returns when
-- the first request carries a held set (engine.exec()).
--
-- A plain function, not a closure in process(): creating a closure per
-- batch (FNEW, UCLO) stops the JIT from compiling process() and the
-- encoders it calls.
local function run_pending(srv, resps, pending, owners, subs, count)
    local stats = srv.stats
    local backend = srv.backend
    local ok, res, held = pcall(backend.exec, backend, pending)
    if not ok or type(res) ~= 'table' then
        held = nil
        local message = ok and 'invalid backend response' or error_message(res)
        res = {}
        for k = 1, count do
            res[k] = {status = 'SERVER_ERROR', message = message}
        end
    end
    local account = M.account
    for k = 1, count do
        local resp = res[k]
            or {
                status = 'SERVER_ERROR',
                message = 'no response',
            }
        account(stats, pending[k], resp)
        local j = subs[k]
        if j then
            resps[owners[k]][j] = resp
        else
            resps[owners[k]] = resp
        end
    end
    return backend.borrows_reqs == true, held
end

-- After run_pending(): the pending array for the next requests of the
-- batch (or the next batch). The backend may keep the one it got (a
-- third-party one): then a new one.
local function next_pending(batch, pending, count, borrowed)
    if count > M.SCRATCH_MAX then
        batch.owners, batch.subs = {}, {}
    end
    if borrowed and clear(pending, count) then
        return pending
    end
    pending = {}
    batch.pending = pending
    return pending
end

-- Execute a batch of parsed commands. items[i] is a request (protos[i]
-- parsed it) or an error line. Returns the output and whether to close.
-- batch is the connection's per-batch tables (see new_batch()); nil: new
-- ones.
--
-- held: the items that the reads of the C batch before these commands
-- hold (engine.lua track_held()), or nil; they go with the first engine
-- request, as in one engine call, unless a barrier comes first (an engine
-- call of its own). With want_held, the third result is the items the reads
-- hold after these commands, for the C batch after them: the first engine
-- request of each engine call carries a held set (a new one when there are
-- none), which engine.exec() fills and returns.
function M.process(srv, items, protos, n, batch, held, want_held)
    local stats = srv.stats
    if batch == nil then
        batch = new_batch()
    end
    local resps = batch.resps
    local multis = batch.multis
    local close = false
    local pending, owners, subs = batch.pending, batch.owners, batch.subs
    local count = 0
    local nmulti = 0

    -- Whether an mn came since the last engine request: memcached writes
    -- the replies out there, so the reads before it no longer hold their
    -- items (engine.lua track_held()).
    local release = false
    -- Whether the next engine request takes the held set.
    local attach = held ~= nil or want_held == true
    for i = 1, n do
        local req = items[i]
        if type(req) == 'table' then
            local multi = req.multi
            local engine_req = multi ~= nil or ENGINE_OPS[req.op]
            if (release or attach) and engine_req then
                local first = multi ~= nil and multi[1] or req
                if first ~= nil then
                    if release then
                        first.release = true
                        release = false
                    end
                    if attach then
                        first.held = held or {}
                        attach = false
                    end
                end
            end
            if req.op == 'mn' then
                release = true
            end
            if multi ~= nil then
                nmulti = nmulti + 1
                local r = multis[nmulti]
                if r == nil then
                    r = {}
                    multis[nmulti] = r
                end
                resps[i] = r
                for j = 1, #multi do
                    count = count + 1
                    pending[count] = multi[j]
                    owners[count] = i
                    subs[count] = j
                end
            elseif ENGINE_OPS[req.op] then
                count = count + 1
                pending[count] = req
                owners[count] = i
                subs[count] = false
            else
                if BARRIER_OPS[req.op] then
                    if count > 0 then
                        local borrowed = run_pending(
                            srv,
                            resps,
                            pending,
                            owners,
                            subs,
                            count
                        )
                        pending = next_pending(batch, pending, count, borrowed)
                        count = 0
                    end
                    -- The engine calls after it start with no held items.
                    held = nil
                    attach = want_held == true
                end
                local resp = frontend(srv, req)
                resps[i] = resp
                if resp ~= nil and ERROR_COUNTER[resp.status] ~= nil then
                    inc(stats, ERROR_COUNTER[resp.status])
                end
                if req.op == 'quit' then
                    close = true
                end
            end
        end
    end
    if count > 0 then
        local borrowed
        borrowed, held = run_pending(srv, resps, pending, owners, subs, count)
        next_pending(batch, pending, count, borrowed)
    end
    if release then
        -- An mn after the last engine request.
        held = nil
    end

    -- The replies, or their parts: a retrieval's go straight into out
    -- (proto.encode_parts), so that its values are copied once, by the
    -- concat of the whole output.
    local out = batch.out
    local nout = 0
    for i = 1, n do
        local req = items[i]
        if type(req) == 'table' then
            local proto = protos[i]
            local parts = req.multi ~= nil and proto.encode_parts or nil
            local ok, res
            if parts ~= nil then
                ok, res = pcall(parts, req, resps[i], out, nout)
                if ok then
                    nout = res
                else
                    -- Drop the parts it may have added.
                    clear_from(out, nout + 1)
                end
            else
                ok, res = pcall(proto.encode, req, resps[i])
                if ok and res ~= nil and res ~= '' then
                    nout = nout + 1
                    out[nout] = res
                end
            end
            if not ok then
                log.error(
                    'memcached: encode failed: %s',
                    tostring(res)
                )
                inc(stats, 'server_errors')
                nout = nout + 1
                out[nout] = 'SERVER_ERROR ' .. error_message(res) .. '\r\n'
            end
        elseif type(req) == 'string' and req ~= '' then
            count_error_line(stats, req)
            if byte(req, #req) ~= BYTE_LF then
                req = req .. '\r\n'
            end
            nout = nout + 1
            out[nout] = req
        end
    end

    -- Nothing of this batch stays referenced.
    if nmulti > M.SCRATCH_MAX then
        batch.multis = {}
    else
        for k = 1, nmulti do
            local r = multis[k]
            if not clear(r, #r) then
                multis[k] = {}
            end
        end
    end
    if not clear(resps, n) then
        batch.resps = {}
    end
    local res
    if nout == 1 then
        -- No copy of a lone reply (a value may be big).
        res = out[1]
    else
        res = concat(out, '', 1, nout)
    end
    if nout > M.OUT_KEEP then
        batch.out = {}
    else
        clear(out, nout)
    end
    return res, close, held
end

-- Connections -----------------------------------------------------------

local function would_block(e)
    return e == errno.EAGAIN or e == errno.EWOULDBLOCK or e == errno.EINTR
end

-- The input buffer (buffer.ibuf) by its pointers: the ibuf methods check
-- their argument's type on every call, and the connection loop calls them
-- several times per request.
local function ibuf_used(ibuf)
    return tonumber(ibuf.wpos - ibuf.rpos)
end

local function ibuf_unused(ibuf)
    return tonumber(ibuf.epos - ibuf.wpos)
end

-- Whether a connection is accepted over TCP: as in memcached, only these
-- time out. One getsockname() per connection, with idle_timeout only.
local function is_tcp(conn)
    local tcp = conn.tcp
    if tcp == nil then
        local name = conn.sock:name()
        tcp = name ~= nil and name.family ~= 'AF_UNIX'
        conn.tcp = tcp
    end
    return tcp
end

-- Wait until the socket of a connection is readable. Returns false when
-- the connection is to be closed.
--
-- While the connection waits for a command (a new one, or the rest of a
-- command line), the wait ends when it has been idle for idle_timeout
-- seconds since its last batch: it is counted in idle_kicks and closed, as
-- in memcached. This is the wait the loop makes once per batch anyway, with
-- a timeout: no syscall and no timer more. srv:cfg() wakes these waits up
-- when idle_timeout changes. Waiting for the value of a command (or a
-- value being discarded), a connection never times out.
local function wait_input(conn)
    if conn.nread or conn.skip > 0 then
        conn.state = conn.skip > 0 and CONN_SWALLOW or CONN_NREAD
        conn.sock:readable()
        conn.state = CONN_PARSE_CMD
        return true
    end
    -- A new connection is in conn_new_cmd until its first input.
    if conn.state ~= CONN_NEW_CMD or ibuf_used(conn.ibuf) > 0 then
        conn.state = CONN_READ
    end
    local srv = conn.srv
    local fd = conn.fd
    if fd == nil then
        -- A connection accepted before a reload of the code.
        fd = conn.sock:fd()
        conn.fd = fd
        conn.last_cmd = fiber_clock()
    end
    while true do
        local idle = srv.idle_timeout or 0
        local events
        if idle > 0 and is_tcp(conn) then
            local left = conn.last_cmd + idle - fiber_clock()
            if left <= 0 then
                inc(srv.stats, 'idle_kicks')
                return false
            end
            events = iowait(fd, COIO_READ, left)
        else
            events = iowait(fd, COIO_READ)
        end
        if conn.closed then
            return false
        end
        if events ~= 0 then
            break
        end
        -- The timeout, or srv:cfg() changed idle_timeout: look again.
        fiber.testcancel()
    end
    conn.state = CONN_PARSE_CMD
    return true
end

-- Read more data into the input buffer. Returns false on EOF or an error.
local function read_more(conn)
    local ibuf = conn.ibuf
    if conn.replied then
        -- A reply has just been written for all the input: the next request
        -- is not here yet, and read() would fail with EAGAIN. Wait first
        -- (one syscall per request fewer).
        conn.replied = false
        if conn.closed or not wait_input(conn) then
            return false
        end
    end
    local want = M.READ_SIZE
    local missing = conn.need - ibuf_used(ibuf)
    if missing > want then
        want = missing
    end
    while true do
        if conn.closed then
            return false
        end
        local sock = conn.sock
        if ibuf_unused(ibuf) < want then
            ibuf:reserve(want)
        end
        local n = sock:sysread(ibuf.wpos, ibuf_unused(ibuf))
        if n == nil then
            if not would_block(sock:errno()) or not wait_input(conn) then
                return false
            end
        elseif n == 0 then
            return false
        else
            ibuf.wpos = ibuf.wpos + n
            local stats = conn.srv.stats
            stats.bytes_read = stats.bytes_read + n
            return true
        end
    end
end

-- Write len bytes from ptr. Returns false on errors.
local function write_ptr(conn, ptr, len)
    local off = 0
    while off < len do
        if conn.closed then
            return false
        end
        local sock = conn.sock
        local n = sock:syswrite(ptr + off, len - off)
        if n == nil then
            if not would_block(sock:errno()) then
                return false
            end
            local st = conn.state
            conn.state = CONN_MWRITE
            sock:writable()
            conn.state = st
        else
            off = off + n
        end
    end
    local stats = conn.srv.stats
    stats.bytes_written = stats.bytes_written + len
    return true
end

-- Write the whole string. Returns false on errors.
local function write_all(conn, data)
    -- Keep the string referenced while its pointer is in use.
    conn.wdata = data
    local ok = write_ptr(conn, ffi.cast(const_char_ptr, data), #data)
    conn.wdata = nil
    return ok
end

-- The output buffer of a connection (buffer.ibuf): the replies of the C
-- batch (see conn_step()). Created on first use: a connection accepted
-- before a reload has none.
local function conn_obuf(conn)
    local obuf = conn.obuf
    if obuf == nil then
        obuf = buffer.ibuf()
        conn.obuf = obuf
    end
    return obuf
end

-- Write the output buffer and the string after it (the replies of the Lua
-- batch, '' for none) with one write, and empty the buffer. Returns false
-- on errors.
local function write_obuf(conn, obuf, tail)
    local len = #tail
    if len > 0 then
        local p = obuf:reserve(len)
        ffi.copy(p, tail, len)
        obuf.wpos = obuf.wpos + len
    end
    local ok = write_ptr(conn, obuf.rpos, ibuf_used(obuf))
    if tonumber(obuf.epos - obuf.buf) > M.BUFFER_KEEP then
        obuf:recycle()
    else
        obuf.rpos = obuf.buf
        obuf.wpos = obuf.buf
    end
    return ok
end

local function consume(conn, count)
    local ibuf = conn.ibuf
    local rpos = ibuf.rpos + count
    if rpos == ibuf.wpos then
        if tonumber(ibuf.epos - ibuf.buf) > M.BUFFER_KEEP then
            ibuf:recycle()
        else
            ibuf.rpos = ibuf.buf
            ibuf.wpos = ibuf.buf
        end
    else
        ibuf.rpos = rpos
    end
end

-- The commands the C batch serves (datapath.c batch()), by their first
-- bytes: their kinds, for the connection loop, which goes back to the C
-- batch at them (conn_step()). The text ones by the name and a space; a
-- meta one by "m", its letter and a space or the end of the line, as
-- parse_batch() takes a meta command. No loop (the JIT compiles the
-- connection loop through it), and memcmp() for the names.
pcall(ffi.cdef, 'int memcmp(const void *s1, const void *s2, size_t n);')
local memcmp = ffi.C.memcmp

local BYTE_A = string.byte('a')
local BYTE_C = string.byte('c')
local BYTE_D = string.byte('d')
local BYTE_E = string.byte('e')
local BYTE_G = string.byte('g')
local BYTE_I = string.byte('i')
local BYTE_N = string.byte('n')
local BYTE_P = string.byte('p')
local BYTE_R = string.byte('r')
local BYTE_S = string.byte('s')
local BYTE_T = string.byte('t')

local K_GET, K_GETS, K_GAT, K_GATS = 1, 2, 3, 4
local K_SET, K_ADD, K_REPLACE, K_APPEND, K_PREPEND, K_CAS = 5, 6, 7, 8, 9, 10
local K_DELETE, K_INCR, K_DECR, K_TOUCH = 11, 12, 13, 14
local K_MG, K_MS, K_MD, K_MA, K_MN = 15, 16, 17, 18, 19

-- Whether the size bytes at p start with name.
local function starts(p, size, name)
    local len = #name
    return size >= len and memcmp(p, name, len) == 0
end

-- The kind of the command at p (size bytes there), 0 when the C batch
-- doesn't serve it.
local function c_kind(p, size)
    if size < 4 then
        if size == 3 and p[0] == BYTE_M and p[1] == BYTE_N
            and (p[2] == BYTE_CR or p[2] == BYTE_LF) then
            return K_MN
        end
        return 0
    end
    local c = p[0]
    if c == BYTE_G then
        if p[2] ~= BYTE_T then
            return 0
        end
        local c2, c3 = p[1], p[3]
        local cas = c3 == BYTE_S
        if cas and (size < 5 or p[4] ~= BYTE_SPACE)
            or not cas and c3 ~= BYTE_SPACE then
            return 0
        end
        if c2 == BYTE_E then
            return cas and K_GETS or K_GET
        elseif c2 == BYTE_A then
            return cas and K_GATS or K_GAT
        end
        return 0
    elseif c == BYTE_M then
        local c3 = p[2]
        if c3 ~= BYTE_SPACE and c3 ~= BYTE_CR and c3 ~= BYTE_LF then
            return 0
        end
        local c2 = p[1]
        if c2 == BYTE_G then
            return K_MG
        elseif c2 == BYTE_S then
            return K_MS
        elseif c2 == BYTE_D then
            return K_MD
        elseif c2 == BYTE_A then
            return K_MA
        elseif c2 == BYTE_N then
            return K_MN
        end
        return 0
    elseif c == BYTE_S then
        return p[1] == BYTE_E and p[2] == BYTE_T
            and p[3] == BYTE_SPACE and K_SET or 0
    elseif c == BYTE_A then
        if starts(p, size, 'add ') then
            return K_ADD
        end
        return starts(p, size, 'append ') and K_APPEND or 0
    elseif c == BYTE_R then
        return starts(p, size, 'replace ') and K_REPLACE or 0
    elseif c == BYTE_P then
        return starts(p, size, 'prepend ') and K_PREPEND or 0
    elseif c == BYTE_C then
        return starts(p, size, 'cas ') and K_CAS or 0
    elseif c == BYTE_D then
        if starts(p, size, 'decr ') then
            return K_DECR
        end
        return starts(p, size, 'delete ') and K_DELETE or 0
    elseif c == BYTE_I then
        return starts(p, size, 'incr ') and K_INCR or 0
    elseif c == BYTE_T then
        return starts(p, size, 'touch ') and K_TOUCH or 0
    end
    return 0
end
M.c_kind = c_kind

-- A Lua batch that may stop for the C batch (conn_step()) copies this many
-- bytes of the input at first rather than all of it, and all of it only
-- when a command doesn't fit.
M.PARSE_CHUNK = 4096

-- Parse up to max commands of the size bytes of the input of a connection,
-- the Lua batch after the before commands of the batch, and consume them.
-- Returns the items (a request or an error line each), the protocol modules
-- that parsed them, their number, whether to close the connection after
-- them, and whether the parsing stopped for the C batch.
--
-- With c_next, the parsing stops before a command that the C batch serves
-- (c_kind()), after one command at least (the first is the one the C batch
-- didn't serve), unless its kind is in refused (those the C batch didn't
-- serve in this batch).
local function parse_batch(conn, size, max, before, c_next, refused)
    local srv = conn.srv
    local ibuf = conn.ibuf
    local base = ibuf.rpos
    local limit = size
    if c_next and size > M.PARSE_CHUNK then
        limit = M.PARSE_CHUNK
    end
    local data = ffi.string(base, limit)
    local text = text_module()
    local meta = meta_module()
    local popts = srv.parse_opts
    local batch = conn.batch or new_batch(conn)
    local items, protos = batch.items, batch.protos
    local n = 0
    local pos = 1
    local close = false
    local more = false
    while pos <= size and n < max do
        if c_next and n > 0 then
            local kind = c_kind(base + pos - 1, size - pos + 1)
            if kind ~= 0 and (refused == nil or not refused[kind]) then
                more = true
                break
            end
        end
        local proto, req, next_pos, err, fatal
        while true do
            proto = text
            if meta ~= nil and byte(data, pos) == BYTE_M then
                local c2, c3 = byte(data, pos + 1, pos + 2)
                if c3 ~= nil and META_SECOND[c2]
                    and (c3 == BYTE_SPACE or c3 == BYTE_CR or c3 == BYTE_LF) then
                    proto = meta
                end
            end
            req, next_pos, err, fatal = proto.parse(data, pos, popts)
            if limit == size or (req ~= nil and not fatal) then
                break
            end
            -- The command may go on after the bytes copied: all of them.
            limit = size
            data = ffi.string(base, size)
        end
        if req == nil then
            local need = tonumber(err)
            -- A known size: the command line is complete, its value is not.
            conn.nread = need ~= nil
            conn.need = need ~= nil and need or size - pos + 2
            break
        end
        if type(next_pos) ~= 'number' or next_pos <= pos then
            log.error('memcached: parser did not advance')
            close = true
            break
        end
        n = n + 1
        protos[n] = proto
        pos = next_pos
        if req == false then
            items[n] = err or false
            if fatal then
                close = true
                break
            end
        else
            items[n] = req
            if proto ~= text then
                inc(srv.stats, 'cmd_meta')
            end
            if req.op == 'quit' then
                break
            end
            if STREAM_OPS[req.op] then
                if n + before > 1 then
                    items[n] = M.PIPELINE_ERROR:format(req.sub or req.op)
                end
                break
            end
        end
    end
    local consumed = pos - 1
    if consumed > size then
        conn.skip = consumed - size
        consumed = size
    end
    consume(conn, consumed)
    return items, protos, n, close, more
end

local function add(stats, name, count)
    stats[name] = (stats[name] or 0) + count
end

local add_more
local inputs_changed

-- The C batch (backend:batch(): the local backend in TX, and the thread
-- backend, which ships the input to TX, backend/thread.lua): it serves the
-- commands it can at the head of the input, up to max, appends their
-- replies to the output buffer and consumes them. held: the items the
-- reads before it hold. Returns the number of commands it served (nil when
-- it can't run here) and the items the reads hold after them (see
-- engine.batch()).
-- The results of the C batch (backend:batch()) after held: consumed,
-- commands, gets, hits, stored, held_out, then the counters of add_more()
-- (nil when none of them moved). Returns what c_batch() does.
local function c_batch_done(
    conn, held, consumed, commands, gets, hits, stored, held_out, ...
)
    if consumed == nil then
        return nil, held
    end
    if commands == 0 then
        return 0, held
    end
    -- The counters as M.account() counts these commands.
    local stats = conn.srv.stats
    if gets > 0 then
        add(stats, 'cmd_get', gets)
        add(stats, 'get_hits', hits)
        add(stats, 'get_misses', gets - hits)
    end
    if stored > 0 then
        add(stats, 'cmd_set', stored)
        add(stats, 'total_items', stored)
    end
    -- None of the others moved (or an older TX, through a thread backend
    -- over a reload, has only those).
    if (...) ~= nil then
        add_more(stats, ...)
    end
    consume(conn, consumed)
    return commands, held_out
end

local function c_batch(conn, backend, obuf, max, held)
    return c_batch_done(
        conn, held,
        backend:batch(
            conn.ibuf,
            obuf,
            max,
            conn.srv.parse_opts.max_item_size,
            held
        )
    )
end

-- The counters of the C batch from unstored on (see c_batch()), as
-- M.account() counts these commands.
function add_more(
    stats, unstored, touches, touch_hits, delete_hits, delete_misses, incr_hits,
    incr_misses, decr_hits, decr_misses, cas_hits, cas_misses, cas_badval, meta
)
    if meta > 0 then
        add(stats, 'cmd_meta', meta)
    end
    if unstored > 0 then
        add(stats, 'cmd_set', unstored)
    end
    if touches > 0 then
        add(stats, 'cmd_touch', touches)
        add(stats, 'touch_hits', touch_hits)
        add(stats, 'touch_misses', touches - touch_hits)
    end
    if delete_hits + delete_misses > 0 then
        add(stats, 'delete_hits', delete_hits)
        add(stats, 'delete_misses', delete_misses)
    end
    if incr_hits + incr_misses + decr_hits + decr_misses > 0 then
        add(stats, 'incr_hits', incr_hits)
        add(stats, 'incr_misses', incr_misses)
        add(stats, 'decr_hits', decr_hits)
        add(stats, 'decr_misses', decr_misses)
    end
    if cas_hits + cas_misses + cas_badval > 0 then
        add(stats, 'cas_hits', cas_hits)
        add(stats, 'cas_misses', cas_misses)
        add(stats, 'cas_badval', cas_badval)
    end
end

-- The counters of the server with those of its C connection loops added
-- (srv.dp_counters, see engine.loop()): the loops count into a C block,
-- which this moves into srv.stats, as memcached adds up the counters of
-- its threads when they are asked for. Every reader of srv.stats calls it
-- first (stats, info(), reset_stats(), the metrics). Returns srv.stats.
-- The counters dp.take() returns: gets, hits, stored, the counters of
-- add_more(), then bytes_read and bytes_written.
local function sync_add(stats, gets, hits, stored, ...)
    if gets > 0 then
        add(stats, 'cmd_get', gets)
        add(stats, 'get_hits', hits)
        add(stats, 'get_misses', gets - hits)
    end
    if stored > 0 then
        add(stats, 'cmd_set', stored)
        add(stats, 'total_items', stored)
    end
    -- add_more() takes the first 13 and leaves the bytes.
    add_more(stats, ...)
    local bytes_read, bytes_written = select(14, ...)
    if bytes_read > 0 then
        add(stats, 'bytes_read', bytes_read)
    end
    if bytes_written > 0 then
        add(stats, 'bytes_written', bytes_written)
    end
end

function M.sync_stats(srv)
    local stats = srv.stats
    local counters = srv.dp_counters
    if counters == nil then
        return stats
    end
    local dp = package.loaded['memcached.datapath']
    if dp == nil or dp.take == nil then
        return stats
    end
    sync_add(stats, dp.take(counters))
    return stats
end

-- Append a string to the output buffer.
local function obuf_append(obuf, s)
    local len = #s
    local p = obuf:reserve(len)
    ffi.copy(p, s, len)
    obuf.wpos = obuf.wpos + len
end

-- A turn of the C batch after a Lua one that serves fewer commands than
-- this ends the turns of the batch: the Lua batch takes the rest of it (a
-- turn costs about as much as a few commands in a Lua batch).
M.REENTRY_MIN = 4

-- The state of a batch of conn_step(), kept per connection (conn.turns):
-- the turns of the C and the Lua batch share it.
local function turns_state(conn)
    local st = conn.turns
    if st == nil then
        st = {
            -- Whether the backend's C batch may run, and whether the Lua
            -- batch stops for it (batch_reentry).
            c_on = false,
            c_next = false,
            -- The commands served so far and the room left.
            done = 0,
            left = 0,
            -- The items the reads hold (see process()).
            held = false,
            -- The kinds the C batch didn't serve (c_kind()), or false.
            refused = false,
            -- The replies of the last Lua batch, not yet in the output
            -- buffer.
            tail = '',
            close = false,
            quit = false,
            -- The last command of the last Lua batch.
            last = false,
            -- Whether the batch has begun (conn.last_cmd).
            started = false,
            -- Whether the C loop has run the C batch of the first turn.
            c_ran = false,
        }
        conn.turns = st
    end
    return st
end

local function start_batch(conn, st)
    if not st.started then
        -- The idle time counts from the last batch, its start and its end.
        st.started = true
        conn.last_cmd = fiber_clock()
        conn.state = CONN_PARSE_CMD
    end
end

-- One turn of a batch: the C batch serves what it can at the head of the
-- input, then the Lua batch parses and runs the commands from there.
-- Returns whether the Lua batch stopped for the C batch, which then takes
-- the next turn.
local function turn(conn, st)
    local srv = conn.srv
    local ibuf = conn.ibuf
    local size = ibuf_used(ibuf)
    if st.c_ran then
        -- The C loop has run the C batch of this turn (conn_step()).
        st.c_ran = false
    elseif st.c_on then
        local kind = c_kind(ibuf.rpos, size)
        local refused = st.refused
        if kind ~= 0 and not (refused and refused[kind]) then
            start_batch(conn, st)
            local obuf = conn_obuf(conn)
            if st.tail ~= '' then
                obuf_append(obuf, st.tail)
                st.tail = ''
            end
            local held = st.held or nil
            local served
            served, held = c_batch(conn, srv.backend, obuf, st.left, held)
            st.held = held or false
            if served == nil then
                st.c_on, st.c_next = false, false
            elseif served > 0 then
                if served < M.REENTRY_MIN and st.done > 0 then
                    -- A short turn after a Lua one costs more than it
                    -- saves: the Lua batch takes the rest.
                    st.c_next = false
                end
                st.done = st.done + served
                st.left = st.left - served
                size = ibuf_used(ibuf)
            else
                if not refused then
                    refused = {}
                    st.refused = refused
                end
                refused[kind] = true
            end
        end
    end
    if size == 0 or st.left == 0 then
        return false
    end
    local items, protos, n, close, more = parse_batch(
        conn,
        size,
        st.left,
        st.done,
        st.c_next,
        st.refused or nil
    )
    st.close = close
    if n == 0 then
        return false
    end
    start_batch(conn, st)
    st.done = st.done + n
    st.left = st.left - n
    local out, quit, held = current().process(
        srv,
        items,
        protos,
        n,
        conn.batch,
        st.held or nil,
        more
    )
    st.held = held or false
    st.quit = quit
    st.last = items[n]
    release_items(conn.batch, n)
    if out ~= '' then
        if st.tail ~= '' then
            obuf_append(conn_obuf(conn), st.tail)
        end
        st.tail = out
    end
    return more and not (close or quit) and ibuf_used(ibuf) > 0
end

-- The C connection loop ---------------------------------------------------

-- The reasons the C loop returns for (memcached/cloop.c).
local LOOP_STOP = 1   -- the C batch stopped before the end of the input
local LOOP_CLOSE = 2  -- end of input, an error: close the connection
local LOOP_WAKE = 3   -- a wait for input ended without input
local LOOP_INPUTS = 4 -- its inputs may have changed: call it again
local LOOP_KEEP = 5   -- a buffer grew past BUFFER_KEEP

-- The C loop leaves a buffer where it is when it empties it: back to its
-- start, or dropped when it grew past BUFFER_KEEP, as consume() and
-- write_obuf() do.
local function rewind(buf)
    if ibuf_used(buf) == 0 then
        if tonumber(buf.epos - buf.buf) > M.BUFFER_KEEP then
            buf:recycle()
        else
            buf.rpos = buf.buf
            buf.wpos = buf.buf
        end
    end
end

-- The inputs changed: the C loops return and take them again (see
-- engine.inputs_changed(); this module doesn't require the engine).
function inputs_changed()
    local st = package.loaded['memcached.state']
    if st ~= nil then
        st.dp_gen = (st.dp_gen or 0) + 1
    end
end

-- Run the C connection loop of the backend (backend:loop(), the local
-- backend: memcached/cloop.c, engine.loop()): it reads, runs the C batch
-- and writes in C for as long as the C batch serves the whole input.
-- Returns nil when it can't run, otherwise the reason it returned for and,
-- for LOOP_STOP, the commands of the batch it served and the items their
-- reads hold.
local function c_loop(conn, backend)
    local srv = conn.srv
    if conn.fd == nil then
        -- A connection accepted before a reload of the code.
        conn.fd = conn.sock:fd()
        conn.last_cmd = fiber_clock()
    end
    local idle = srv.idle_timeout or 0
    if idle > 0 and not is_tcp(conn) then
        idle = 0
    end
    local ibuf = conn.ibuf
    local obuf = conn_obuf(conn)
    local reason, served, held = backend:loop(
        srv,
        conn,
        ibuf,
        obuf,
        idle,
        conn.state == CONN_NEW_CMD,
        M
    )
    if reason == nil then
        return nil
    end
    -- All the input taken and replied to (else the loop has read since).
    conn.replied = reason == LOOP_KEEP and ibuf_used(ibuf) == 0
    rewind(ibuf)
    rewind(obuf)
    return reason, served, held
end

-- What conn_step() returns after the C loop returned for reason (but
-- LOOP_STOP): whether the connection stays open.
local function loop_returned(conn, reason)
    if conn.closed or reason == LOOP_CLOSE then
        return false
    end
    if reason == LOOP_INPUTS or reason == LOOP_KEEP then
        -- The next step calls the loop again.
        return true
    end
    if reason == LOOP_WAKE then
        -- As wait_input() when its wait ends without input.
        local srv = conn.srv
        local idle = srv.idle_timeout or 0
        if idle > 0 and is_tcp(conn)
            and conn.last_cmd + idle - fiber_clock() <= 0 then
            inc(srv.stats, 'idle_kicks')
            return false
        end
        fiber.testcancel()
    end
    return true
end

-- One iteration of a connection: read, parse a batch, execute it, write the
-- replies. Returns false when the connection is to be closed.
--
-- A batch is up to MAX_BATCH commands, and the C batch (when the backend
-- has one) and the Lua one take turns in it (turn()): the C batch serves
-- what it can at the head of the input, the Lua batch parses and runs
-- (process()) the commands from the first one it didn't serve. With a
-- backend whose C batch is a plain call (batch_reentry, the local
-- backend), the Lua batch stops at the next command the C batch serves
-- (parse_batch()), and the C batch goes on from there; a kind of command it
-- didn't serve in this batch stays with Lua. The items the reads hold go
-- from each to the next, as in one engine call (held). The replies of both
-- go out in their order with one write.
function M.conn_step(conn)
    local srv = conn.srv
    local ibuf = conn.ibuf
    if conn.skip > 0 then
        local count = math.min(conn.skip, ibuf_used(ibuf))
        conn.skip = conn.skip - count
        consume(conn, count)
    end
    local size = ibuf_used(ibuf)
    if conn.skip > 0 or size < conn.need then
        return read_more(conn)
    end
    conn.need = 0
    conn.nread = false

    local backend = srv.backend
    -- The C loop (the local backend): it returns for the Lua batch to go
    -- on from the first command it didn't serve (LOOP_STOP), and for
    -- everything else Lua does.
    local reason, served, held
    if backend ~= nil and backend.loop ~= nil then
        reason, served, held = c_loop(conn, backend)
        if reason ~= nil then
            if reason ~= LOOP_STOP then
                return loop_returned(conn, reason)
            end
            size = ibuf_used(ibuf)
        end
    end
    if size == 0 then
        return read_more(conn)
    end

    local st = turns_state(conn)
    st.c_on = backend ~= nil and backend.batch ~= nil
    st.c_next = st.c_on and backend.batch_reentry == true
    st.done = 0
    st.left = M.MAX_BATCH
    st.held = false
    st.refused = false
    st.tail = ''
    st.close = false
    st.quit = false
    st.last = false
    st.started = false
    st.c_ran = false
    if reason == LOOP_STOP then
        -- The first turn's C batch has run in the C loop: as in turn().
        st.c_ran = st.c_on
        st.held = held or false
        local kind = served == 0 and c_kind(ibuf.rpos, size) or 0
        if served > 0 or kind ~= 0 then
            start_batch(conn, st)
        end
        if served > 0 then
            st.done = served
            st.left = st.left - served
        elseif kind ~= 0 then
            st.refused = {[kind] = true}
        end
    end
    -- No loop unless the C batch takes a second turn: a loop that runs once
    -- would be a trace that exits at once on every batch.
    if turn(conn, st) then
        repeat  until not turn(conn, st)
    end
    local close, quit, last, tail = st.close, st.quit, st.last, st.tail
    -- Nothing of this batch stays referenced.
    st.held, st.refused, st.last, st.tail = false, false, false, ''
    st.c_ran = false
    if st.done == 0 then
        if close then
            return false
        end
        return read_more(conn)
    end

    local obuf = conn.obuf
    if obuf ~= nil and ibuf_used(obuf) > 0 then
        if not write_obuf(conn, obuf, tail) then
            return false
        end
    elseif tail ~= '' and not write_all(conn, tail) then
        return false
    end
    conn.last_cmd = fiber_clock()
    -- All the input is consumed, unless a batch limit stopped the parsing.
    conn.replied = ibuf_used(ibuf) == 0
    if type(last) == 'table' and STREAM_OPS[last.op]
        and not current().stream(conn, last) then
        return false
    end
    return not (close or quit or conn.closed)
end

-- Stream the reply of a STREAM_OPS request (see readview.dump()).
-- Returns false when the connection is to be closed.
function M.stream(conn, req)
    local mod = package.loaded['memcached.readview']
        or require('memcached.readview')
    local ok, res = pcall(mod.dump, conn.srv, req, function(data)
        return write_all(conn, data)
    end)
    if not ok then
        log.error(
            'memcached: %s: %s failed: %s',
            conn.srv.name,
            tostring(req.sub or req.op),
            error_message(res)
        )
        inc(conn.srv.stats, 'server_errors')
        return write_all(
            conn,
            'SERVER_ERROR ' .. error_message(res) .. '\r\n'
        )
    end
    return res ~= false
end

-- Release a connection: unregister it and close the socket.
function M.conn_close(conn)
    local srv = conn.srv
    if srv.conns[conn.id] == conn then
        srv.conns[conn.id] = nil
        srv.nconns = srv.nconns - 1
    end
    conn.closed = true
    local sock = conn.sock
    if sock ~= nil then
        pcall(sock.close, sock)
    end
    if conn.ibuf ~= nil then
        conn.ibuf:recycle()
    end
    if conn.obuf ~= nil then
        conn.obuf:recycle()
    end
end

-- The loop of a connection fiber; a plain function, not a closure per
-- connection, so that the JIT can compile the loop.
local function conn_loop(conn)
    while not conn.closed do
        if not current().conn_step(conn) then
            break
        end
    end
end

local function conn_main(conn)
    local ok, err = pcall(conn_loop, conn)
    if not ok and not conn.closed then
        log.warn(
            'memcached: %s: connection %d: %s',
            conn.srv.name,
            conn.id,
            tostring(err)
        )
    end
    current().conn_close(conn)
end

function M.on_accept(srv, client)
    client:nonblock(true)
    local stats = srv.stats
    if srv.nconns >= srv.max_connections then
        inc(stats, 'rejected_connections')
        client:syswrite(M.TOO_MANY_CONNECTIONS)
        client:close()
        return
    end
    pcall(client.setsockopt, client, 'IPPROTO_TCP', 'TCP_NODELAY', true)
    srv.next_conn_id = srv.next_conn_id + 1
    local conn = {
        id = srv.next_conn_id,
        srv = srv,
        sock = client,
        fd = client:fd(),
        ibuf = buffer.ibuf(),
        -- Don't parse again until the buffer has this many bytes.
        need = 0,
        -- need is the end of a value: the command line is complete.
        nread = false,
        -- Bytes of input to discard (a swallowed value).
        skip = 0,
        -- A reply was written and the input buffer is empty: wait for
        -- input before reading.
        replied = false,
        closed = false,
        -- CONN_* (memcached's names), for idle_timeout and "stats conns".
        state = CONN_NEW_CMD,
        -- fiber.clock() of the last batch (or of the accept).
        last_cmd = fiber_clock(),
        -- Whether the connection is over TCP (is_tcp()).
        tcp = nil,
        -- The per-batch tables (new_batch()).
        batch = nil,
        -- The state of a batch of conn_step() (turns_state()).
        turns = nil,
        fiber = nil,
    }
    srv.conns[conn.id] = conn
    srv.nconns = srv.nconns + 1
    inc(stats, 'total_connections')
    conn.fiber = fiber.new(conn_main, conn)
    conn.fiber:name(
        ('mcc/%s/%d'):format(srv.name, conn.id),
        {truncate = true}
    )
end

-- Shut down all the connections without waiting for the clients.
function M.close_connections(srv)
    local conns = srv.conns
    srv.conns = {}
    srv.nconns = 0
    local self_id = fiber.id()
    for _, conn in pairs(conns) do
        conn.closed = true
        local sock = conn.sock
        if sock ~= nil then
            pcall(sock.shutdown, sock, 'RW')
        end
        local f = conn.fiber
        if f ~= nil and f:status() ~= 'dead' and f:id() ~= self_id then
            pcall(f.cancel, f)
        end
    end
end

-- Listening -------------------------------------------------------------

local function parse_listen(listen)
    if type(listen) == 'number' then
        listen = tostring(listen)
    end
    if type(listen) ~= 'string' then
        return nil
    end
    local u = uri.parse(listen)
    if u == nil or u.service == nil then
        return nil
    end
    if u.host == 'unix/' then
        return {family = 'AF_UNIX', host = 'unix/', port = u.service}
    end
    return {host = u.host or '0.0.0.0', port = u.service}
end

function M.check_listen(listen)
    return parse_listen(listen) ~= nil
end

local function cant_bind(listen, err)
    return ("can't bind to %s: %s"):format(tostring(listen), tostring(err))
end

local function bind_one(ai, reuseport)
    local s = socket(ai.family, ai.type, ai.protocol)
    if s == nil then
        return nil, errno.strerror()
    end
    if ai.family ~= 'AF_UNIX' then
        s:setsockopt('SOL_SOCKET', 'SO_REUSEADDR', true)
    end
    if reuseport and not s:setsockopt('SOL_SOCKET', 'SO_REUSEPORT', true) then
        local err = s:error()
        s:close()
        return nil, err
    end
    if not s:bind(ai.host, ai.port) or not s:listen(M.BACKLOG) then
        local err = s:error()
        local e = s:errno()
        s:close()
        return nil, err, e
    end
    s:nonblock(true)
    return s
end

-- A listening socket for a URI, or nil and an error.
function M.bind(listen, reuseport)
    local addr = parse_listen(listen)
    if addr == nil then
        return nil, cant_bind(listen, 'invalid URI')
    end
    if addr.family == 'AF_UNIX' then
        local ai = {
            family = 'AF_UNIX',
            type = 'SOCK_STREAM',
            protocol = 0,
            host = 'unix/',
            port = addr.port,
        }
        local s, err, e = bind_one(ai, false)
        if s == nil and e == errno.EADDRINUSE then
            -- Remove a stale socket file nobody listens on.
            local probe = socket.tcp_connect('unix/', addr.port, 0.1)
            if probe ~= nil then
                probe:close()
            else
                os.remove(addr.port)
                s, err = bind_one(ai, false)
            end
        end
        if s == nil then
            return nil, cant_bind(listen, err)
        end
        return s
    end
    local ais, err = socket.getaddrinfo(addr.host, addr.port, {
        type = 'SOCK_STREAM',
        flags = 'AI_PASSIVE',
    })
    if ais == nil or #ais == 0 then
        return nil, cant_bind(listen, err or 'no address')
    end
    for _, ai in ipairs(ais) do
        local s
        s, err = bind_one(ai, reuseport)
        if s ~= nil then
            return s
        end
    end
    return nil, cant_bind(listen, err)
end

local function close_listener(lsock)
    local name = lsock:name()
    pcall(lsock.close, lsock)
    if name ~= nil and name.family == 'AF_UNIX' and name.port ~= nil then
        os.remove(name.port)
    end
end

local function accept_main(srv, lsock)
    while srv.listener == lsock do
        if not pcall(lsock.readable, lsock) then
            break
        end
        if srv.listener ~= lsock then
            break
        end
        local client = lsock:accept()
        if client ~= nil then
            current().on_accept(srv, client)
        elseif not would_block(lsock:errno())
            and lsock:errno() ~= errno.ECONNABORTED then
            log.error(
                'memcached: %s: accept: %s',
                srv.name,
                lsock:error()
            )
            fiber.sleep(0.1)
        end
    end
end

local function stop_accept(srv)
    local f = srv.accept_fiber
    srv.accept_fiber = nil
    if f ~= nil and f:status() ~= 'dead' and f:id() ~= fiber.id() then
        pcall(f.cancel, f)
    end
end

-- (Re)start the accept loop on the current listener with the current code.
function M.restart_accept(srv)
    stop_accept(srv)
    local lsock = srv.listener
    if lsock == nil then
        return
    end
    local f = fiber.new(function()
        accept_main(srv, lsock)
    end)
    f:name('mcc/accept/' .. srv.name, {truncate = true})
    srv.accept_fiber = f
end

-- Server objects --------------------------------------------------------

function M.new(cfg)
    return setmetatable({
        name = cfg.name or 'memcached',
        -- The configured URI.
        uri = cfg.listen,
        reuseport = cfg.reuseport or false,
        max_connections = cfg.max_connections or M.DEFAULT_MAX_CONNECTIONS,
        parse_opts = {
            max_item_size = cfg.max_item_size or M.DEFAULT_MAX_ITEM_SIZE,
        },
        idle_timeout = cfg.idle_timeout or M.DEFAULT_IDLE_TIMEOUT,
        backend = cfg.backend,
        -- The listening socket, the URI it is bound to and when it was
        -- bound (fiber.clock()).
        listener = nil,
        listen_uri = nil,
        listen_clock = nil,
        accept_fiber = nil,
        conns = {},
        nconns = 0,
        next_conn_id = 0,
        stats = new_stats(),
        verbosity = 0,
        started = fiber.time(),
        running = false,
    }, mt)
end

function M.start(srv)
    if srv.running then
        return srv
    end
    local lsock, err = M.bind(srv.uri, srv.reuseport)
    if lsock == nil then
        error(err, 0)
    end
    srv.listener = lsock
    srv.listen_uri = srv.uri
    srv.listen_clock = fiber_clock()
    srv.running = true
    M.restart_accept(srv)
    return srv
end

function M.stop(srv)
    srv.running = false
    local lsock = srv.listener
    srv.listener = nil
    srv.listen_uri = nil
    stop_accept(srv)
    if lsock ~= nil then
        close_listener(lsock)
    end
    M.close_connections(srv)
    return srv
end

-- Listen on the URI the server was bound to again, after a new one failed.
-- If that fails too, the server stops listening (running = false); its
-- connections are kept.
local function restore(srv)
    local restored, err = M.bind(srv.listen_uri, srv.reuseport)
    if restored ~= nil then
        srv.listener = restored
        M.restart_accept(srv)
        return
    end
    log.error(
        "memcached: %s: can't listen on %s again: %s",
        srv.name,
        tostring(srv.listen_uri),
        err
    )
    srv.running = false
    srv.listen_uri = nil
end

-- Listen on a new URI; the connections are kept. If the new URI can't be
-- bound, the server keeps listening on the old one and an error is raised.
-- After release() it binds the new URI (or the old one again).
function M.rebind(srv, listen)
    if not srv.running then
        srv.uri = listen
        return srv
    end
    local lsock, err = M.bind(listen, srv.reuseport)
    if lsock == nil and srv.listener ~= nil then
        -- The addresses may overlap (say, the same port on another
        -- interface): release the old one and try again.
        local old = srv.listener
        srv.listener = nil
        stop_accept(srv)
        close_listener(old)
        lsock, err = M.bind(listen, srv.reuseport)
    end
    if lsock == nil then
        if srv.listener == nil then
            restore(srv)
        end
        error(err, 0)
    end
    local old = srv.listener
    stop_accept(srv)
    srv.listener = lsock
    srv.listen_uri = listen
    srv.listen_clock = fiber_clock()
    srv.uri = listen
    if old ~= nil then
        close_listener(old)
    end
    M.restart_accept(srv)
    return srv
end

-- Stop listening and keep the connections; the server stays running and
-- the next rebind() binds a URI again. The role releases the addresses
-- that move to another instance, so that the listen URIs of two instances
-- can be swapped in one config.
function M.release(srv)
    local lsock = srv.listener
    if lsock == nil then
        return srv
    end
    srv.listener = nil
    stop_accept(srv)
    close_listener(lsock)
    return srv
end

function M.cfg(srv, opts)
    -- The C connection loops take max_item_size, idle_timeout and the
    -- backend's inputs once per call.
    inputs_changed()
    if opts.max_connections ~= nil then
        srv.max_connections = opts.max_connections
    end
    if opts.max_item_size ~= nil then
        srv.parse_opts = {max_item_size = opts.max_item_size}
    end
    if opts.idle_timeout ~= nil and opts.idle_timeout ~= srv.idle_timeout then
        srv.idle_timeout = opts.idle_timeout
        -- The connections waiting for a command take the new timeout now
        -- (see wait_input()): this state means the fiber is in that wait.
        for _, conn in pairs(srv.conns) do
            local st = conn.state
            local f = conn.fiber
            if (st == CONN_READ or st == CONN_NEW_CMD) and f ~= nil
                and f:status() == 'suspended' then
                f:wakeup()
            end
        end
    end
    if opts.backend ~= nil then
        srv.backend = opts.backend
    end
    if opts.reuseport ~= nil then
        srv.reuseport = opts.reuseport
    end
    if opts.listen ~= nil then
        M.rebind(srv, opts.listen)
    end
    return srv
end

function M.info(srv)
    local info = {}
    local stats = M.sync_stats(srv)
    for _, name in ipairs(COUNTERS) do
        info[name] = stats[name] or 0
    end
    info.curr_connections = srv.nconns
    info.listen = srv.listen_uri
    info.port = listen_port(srv)
    info.running = srv.running
    return info
end

return M
