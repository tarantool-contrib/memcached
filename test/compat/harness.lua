-- Differential test harness: the same request bytes go to a reference
-- memcached and to tarantool-contrib/memcached, and the normalised replies are
-- compared. See test/compat/README.md for the normalisation rules.
--
-- Servers:
--   M.start_servers() -> {ref, ours} or nil, reason (skip the tests then)
--   M.stop_servers(servers)
-- Scenarios: {name, allow = <id>|{<id>...}?, steps = {<step>...}}, see
-- run_step for the step format:
--   M.run(servers, scenario) -> transcript
--   M.compare(transcript[, {record = false}]) -> unexplained mismatches
--   M.check(servers, scenario) raises on unexplained mismatches
--   M.summary() -> the allowlist hits by class, stale tags, stats key sets

local bit = require('bit')
local digest = require('digest')
local errno = require('errno')
local fiber = require('fiber')
local fio = require('fio')
local log = require('log')
local popen = require('popen')
local socket = require('socket')

local env = require('test.helpers.env')
local allowlist = require('test.compat.allowlist')

local M = {}

-- The reference server: a binary name (looked up in PATH) or a path.
M.MEMCACHED = os.getenv('MCC_COMPAT_MEMCACHED') or 'memcached'
-- Seconds to wait for a reply.
M.TIMEOUT = tonumber(os.getenv('MCC_COMPAT_TIMEOUT')) or 3
-- Print every exchange.
M.TRACE = os.getenv('MCC_COMPAT_TRACE') ~= nil
-- Numbers marked as timing-dependent may differ by this much.
M.TOLERANCE = 1
-- A request that can't be found in a CAS placeholder table expands to this:
-- no server hands it out, and it is above every CAS of both servers (so the
-- stale-set path of ms I never takes it for an older CAS).
M.NO_CAS = '18446744073709551615'

-- Every step ends with a request whose reply marks the end of the step's
-- replies; both servers answer requests in order. Not "mn": a step may
-- contain mn itself, and memcached flushes its MN at once. A meta get of a
-- key nobody stores, with a per-step opaque, answers "EN O<token>".
function M.sync_request(ctx)
    ctx.sync_seq = (ctx.sync_seq or 0) + 1
    local token = 'Osync' .. ctx.sync_seq
    return ('mg %ssync %s\r\n'):format(ctx.prefix, token),
        ('EN %s\r\n'):format(token)
end

-- What the run saw, for the summary: allowlist hits and stats key sets.
M.report = {allowed = {}, stats = {}, stale = {}}

--
-- Servers.
--

local function find_exe(name)
    if name:find('/', 1, true) then
        return fio.path.exists(name) and name or nil
    end
    for dir in (os.getenv('PATH') or ''):gmatch('[^:]+') do
        local path = fio.pathjoin(dir, name)
        if fio.path.exists(path) then
            return path
        end
    end
    return nil
end

local function wait_port(port, timeout)
    local deadline = fiber.clock() + timeout
    while fiber.clock() < deadline do
        local s = socket.tcp_connect('127.0.0.1', port, 0.2)
        if s ~= nil then
            s:close()
            return true
        end
        fiber.sleep(0.05)
    end
    return false
end

local function query_version(port)
    local s = socket.tcp_connect('127.0.0.1', port, M.TIMEOUT)
    if s == nil then
        return nil
    end
    s:write('version\r\n')
    local line = s:read('\r\n', M.TIMEOUT)
    s:close()
    return line and line:match('^VERSION (%S+)')
end

-- The reference memcached as a child process: defaults, except the port,
-- no UDP and the loopback address only.
function M.start_memcached()
    local exe = find_exe(M.MEMCACHED)
    if exe == nil then
        return nil, ('reference memcached %q not found: build the image '
                .. 'from test/compat/Dockerfile or set '
                .. 'MCC_COMPAT_MEMCACHED'):format(M.MEMCACHED)
    end
    for _ = 1, 5 do
        local port = env.free_port()
        local ph, err = popen.new(
            {exe, '-p', tostring(port), '-U', '0', '-l', '127.0.0.1'},
            {
                stdin = popen.opts.DEVNULL,
                stdout = popen.opts.DEVNULL,
                stderr = popen.opts.DEVNULL,
                close_fds = true,
            }
        )
        if ph == nil then
            return nil, ('cannot start %s: %s'):format(exe, err)
        end
        if wait_port(port, 5) then
            local version = query_version(port)
            M.reference_name = 'memcached ' .. (version or '?')
            return {
                kind = 'memcached',
                name = M.reference_name,
                exe = exe,
                port = port,
                ph = ph,
                version = version,
            }
        end
        ph:kill()
        ph:wait()
        ph:close()
    end
    return nil, 'reference memcached did not start'
end

local contrib_seq = 0

-- tarantool-contrib/memcached in this process, with its defaults.
function M.start_contrib()
    env.box_cfg()
    -- Let the protocol limit (1 MiB values) decide, as in a deployment
    -- configured for it: init.lua warns when the tuple size doesn't fit.
    M.saved_tuple_size = box.cfg.memtx_max_tuple_size
    if box.cfg.memtx_max_tuple_size < 2 * 1048576 then
        box.cfg {memtx_max_tuple_size = 2 * 1048576}
    end
    local mcc = require('memcached')
    contrib_seq = contrib_seq + 1
    local name = ('compat%d'):format(contrib_seq)
    local port = env.free_port()
    local inst = mcc.create(name, {listen = '127.0.0.1:' .. port})
    if not wait_port(port, 5) then
        inst:destroy()
        return nil, 'tarantool-contrib/memcached did not start'
    end
    return {
        kind = 'contrib',
        name = 'tarantool-contrib/memcached ' .. (query_version(port) or '?'),
        port = port,
        inst = inst,
        space_name = inst:info().space_name or ('__mcc_' .. name),
        version = query_version(port),
    }
end

function M.stop_server(srv)
    if srv == nil then
        return
    end
    if srv.ph ~= nil then
        srv.ph:kill()
        srv.ph:wait()
        srv.ph:close()
        srv.ph = nil
    end
    if srv.inst ~= nil then
        pcall(srv.inst.destroy, srv.inst)
        srv.inst = nil
        pcall(env.drop_spaces, {srv.space_name})
        if box.space._mcc_meta ~= nil then
            pcall(
                box.space._mcc_meta.delete,
                box.space._mcc_meta,
                'flush:' .. srv.space_name
            )
        end
    end
end

-- Both servers, or nil and the reason to skip.
function M.start_servers()
    local ref, err = M.start_memcached()
    if ref == nil then
        return nil, err
    end
    local ours
    ours, err = M.start_contrib()
    if ours == nil then
        M.stop_server(ref)
        return nil, err
    end
    return {ref, ours}
end

function M.stop_servers(servers)
    if servers == nil then
        return
    end
    for _, srv in ipairs(servers) do
        M.stop_server(srv)
    end
    if M.saved_tuple_size ~= nil and type(box.cfg) ~= 'function' then
        pcall(box.cfg, {memtx_max_tuple_size = M.saved_tuple_size})
    end
end

--
-- Connections.
--

local Conn = {}
Conn.__index = Conn

function M.connect(port)
    local s, err = socket.tcp_connect('127.0.0.1', port, M.TIMEOUT)
    if s == nil then
        error(('connect to %d: %s'):format(port, err), 2)
    end
    return setmetatable({sock = s, closed = false}, Conn)
end

-- Returns false if the peer has closed the connection.
function Conn:write(data)
    if self.closed then
        return false
    end
    if data == '' then
        return true
    end
    local n = self.sock:write(data, M.TIMEOUT)
    if n == nil then
        self.closed = true
        return false
    end
    return true
end

local function would_block(e)
    return e == errno.EAGAIN or e == errno.EWOULDBLOCK or e == errno.EINTR
end

-- Read until the data ends with suffix (nil: until idle seconds pass with
-- no data), EOF or the timeout. Returns the data and 'ok', 'closed' or
-- 'timeout'.
function Conn:collect(suffix, timeout, idle)
    local parts, size = {}, 0
    local tail = ''
    local deadline = fiber.clock() + timeout
    while true do
        if suffix ~= nil and tail:sub(-#suffix) == suffix then
            return table.concat(parts), 'ok'
        end
        if self.closed then
            return table.concat(parts), 'closed'
        end
        local left = deadline - fiber.clock()
        if left <= 0 then
            return table.concat(parts), suffix and 'timeout' or 'ok'
        end
        if idle ~= nil and left > idle then
            left = idle
        end
        if not self.sock:readable(left) then
            if idle ~= nil then
                return table.concat(parts), 'ok'
            end
        else
            local data = self.sock:sysread(65536)
            if data == nil then
                if not would_block(self.sock:errno()) then
                    self.closed = true
                end
            elseif data == '' then
                self.closed = true
            else
                size = size + #data
                parts[#parts + 1] = data
                tail = (tail .. data):sub(-64)
            end
        end
    end
end

function Conn:close()
    if self.sock ~= nil then
        self.sock:close()
        self.sock = nil
    end
    self.closed = true
end

--
-- Templates in requests. Expanded per server, so that a CAS placeholder
-- turns into the value this server handed out.
--   {k}          the scenario's key prefix
--   {key:N}      a key of exactly N bytes that starts with the prefix
--   {casN}       the server's CAS value behind placeholder <CN>
--   {lastcas}    the last CAS value the server returned
--   {abs+N}      the unix time at the scenario start plus N seconds
--   {b64:...}    base64 of the expanded text
--   {rep:C:N}    the character C repeated N times
--

local function b64(s)
    return digest.base64_encode(s, {nowrap = true})
end
M.b64 = b64

function M.expand(ctx, s)
    return (s:gsub('%b{}', function(tok)
        local body = tok:sub(2, -2)
        if body == 'k' then
            return ctx.prefix
        elseif body == 'lastcas' then
            return ctx.cas.last or M.NO_CAS
        end
        local n = body:match('^cas(%d+)$')
        if n ~= nil then
            return ctx.cas.list[tonumber(n)] or M.NO_CAS
        end
        local sign, off = body:match('^abs([%+%-])(%d+)$')
        if sign ~= nil then
            local d = tonumber(off)
            return tostring(ctx.base + (sign == '+' and d or -d))
        end
        local inner = body:match('^b64:(.*)$')
        if inner ~= nil then
            return b64(M.expand(ctx, inner))
        end
        local len = body:match('^key:(%d+)$')
        if len ~= nil then
            len = tonumber(len)
            return ctx.prefix .. ('x'):rep(len - #ctx.prefix)
        end
        local c, count = body:match('^rep:(.):(%d+)$')
        if c ~= nil then
            return c:rep(tonumber(count))
        end
        return nil
    end))
end

--
-- Normalisation.
--

local function new_casmap()
    return {map = {}, list = {}, last = nil, literal = {['0'] = true}}
end
M.new_casmap = new_casmap

-- A CAS value -> its placeholder (literal for 0 and client supplied ones).
local function cas_token(cas, v)
    if v ~= '0' then
        cas.last = v
    end
    if cas.literal[v] then
        return v
    end
    local idx = cas.map[v]
    if idx == nil then
        idx = #cas.list + 1
        cas.list[idx] = v
        cas.map[v] = idx
    end
    return '<C' .. idx .. '>'
end

-- Client supplied CAS values (meta E<n>) stay literal in replies.
local function note_literals(cas, req)
    for v in req:gmatch('[ ]E(%d+)') do
        cas.literal[(v:gsub('^0+(%d)', '%1'))] = true
    end
end

local function norm_meta_flags(cas, rest)
    return (rest:gsub('%S+', function(tok)
        local f, arg = tok:sub(1, 1), tok:sub(2)
        if not arg:match('^%d+$') then
            return tok
        end
        if f == 'c' then
            return 'c' .. cas_token(cas, arg)
        elseif f == 't' or f == 'l' then
            return f .. '~' .. arg
        end
        return tok
    end))
end

local META_CODES = {
    HD = true,
    EN = true,
    NF = true,
    NS = true,
    EX = true,
    MN = true,
}

local function norm_line(cas, line)
    local key, flags, len, v = line:match('^VALUE (%S+) (%d+) (%d+) (%d+)$')
    if key ~= nil then
        return ('VALUE %s %s %s %s'):format(key, flags, len, cas_token(cas, v))
    end
    local head, rest = line:match('^(VA %d+)(.*)$')
    if head == nil then
        head, rest = line:match('^(%u%u)(.*)$')
        if head ~= nil and not META_CODES[head] then
            head = nil
        end
        if head ~= nil and rest ~= '' and rest:sub(1, 1) ~= ' ' then
            head = nil
        end
    end
    if head ~= nil then
        return head .. norm_meta_flags(cas, rest)
    end
    local mkey, fields = line:match('^ME (%S+)(.*)$')
    if mkey ~= nil then
        fields = fields:gsub('(%a+)=(%S*)', function(name, val)
            if (name == 'exp' or name == 'la') and val:match('^%d+$') then
                return name .. '=~' .. val
            elseif name == 'cas' and val:match('^%d+$') then
                return 'cas=' .. cas_token(cas, val)
            elseif name == 'cls' or name == 'size' then
                return name .. '=*'
            end
            return name .. '=' .. val
        end)
        return 'ME ' .. mkey .. fields
    end
    if line:match('^VERSION ') then
        return 'VERSION *'
    end
    return line
end

-- Whether a request is a single stats command.
local function stats_request(req)
    return req:match('^stats[^\r\n]*\r?\n$') ~= nil
end

-- Normalise a reply. Stats blocks become '<STATS>' and their names go to
-- stats_names (an array of arrays, one per block).
function M.normalise(cas, raw, req, stats_names)
    note_literals(cas, req)
    local out = {}
    local pos, n = 1, #raw
    local is_stats = stats_request(req)
    local names
    while pos <= n do
        local e = raw:find('\r\n', pos, true)
        if e == nil then
            out[#out + 1] = raw:sub(pos)
            break
        end
        local line = raw:sub(pos, e - 1)
        pos = e + 2
        local stat = line:match('^STAT (%S+)') or line:match('^ITEM (%S+)')
        if stat ~= nil then
            names = names or {}
            names[#names + 1] = stat
        elseif names ~= nil or (is_stats and line == 'END') then
            if line == 'END' then
                out[#out + 1] = '<STATS>\r\n'
                if stats_names ~= nil then
                    stats_names[#stats_names + 1] = names or {}
                end
                names = nil
            else
                -- Not a stats block after all.
                out[#out + 1] = '<STATS?>\r\n'
                names = nil
                out[#out + 1] = norm_line(cas, line) .. '\r\n'
            end
        else
            out[#out + 1] = norm_line(cas, line) .. '\r\n'
            local len = line:match('^VALUE %S+ %d+ (%d+)')
                or line:match('^VA (%d+)')
            if len ~= nil then
                len = tonumber(len)
                out[#out + 1] = raw:sub(pos, pos + len + 1)
                pos = pos + len + 2
            end
        end
    end
    if names ~= nil then
        out[#out + 1] = '<STATS-UNTERMINATED>'
    end
    return table.concat(out)
end

-- Equal, except that numbers after '~' may differ by TOLERANCE.
function M.same(a, b)
    if a == b then
        return true
    end
    local na, nb = {}, {}
    local sa = a:gsub('~(%d+)', function(v)
        na[#na + 1] = tonumber(v)
        return '~'
    end)
    local sb = b:gsub('~(%d+)', function(v)
        nb[#nb + 1] = tonumber(v)
        return '~'
    end)
    if sa ~= sb or #na ~= #nb or #na == 0 then
        return false
    end
    for i = 1, #na do
        if math.abs(na[i] - nb[i]) > M.TOLERANCE then
            return false
        end
    end
    return true
end

-- A readable form of protocol bytes.
function M.show(s, limit)
    limit = limit or 400
    local cut = ''
    if #s > limit then
        cut = ('...(%d bytes)'):format(#s)
        s = s:sub(1, limit)
    end
    s = s:gsub('[%c\128-\255\\]', function(c)
        if c == '\r' then
            return '\\r'
        elseif c == '\n' then
            return '\\n'
        elseif c == '\\' then
            return '\\\\'
        end
        return ('\\x%02x'):format(c:byte())
    end)
    return s .. cut
end

--
-- Running scenarios.
--

local prefix_seq = 0

function M.new_prefix()
    prefix_seq = prefix_seq + 1
    return ('c%d_%04x_'):format(prefix_seq, math.random(0, 0xffff))
end

-- A step: a string, or a table:
--   send = <string|function(ctx)>   request bytes (templates expanded)
--   chunks = {<string>...}           sent as separate writes, gap apart;
--            or function(ctx) -> {<string>...}
--   gap = <seconds>                  between chunks, default 0.05
--   sleep = <seconds>                just wait
--   connect = true                   open a new connection first
--   nosync = true                    don't append the sync no-op: read
--                                    until the connection is idle
--   idle = <seconds>                 for nosync, default 0.5
--   allow = <id> | {<id>...}        known divergences for this step (the
--                                    scenario may have allow too)
--   flaky = true                     the tagged divergence is racy in
--                                    memcached: don't report it as stale
local function step_chunks(ctx, step)
    if type(step) == 'string' then
        return {M.expand(ctx, step)}
    end
    if step.chunks ~= nil then
        local list = step.chunks
        if type(list) == 'function' then
            list = list(ctx)
        end
        local chunks = {}
        for i, c in ipairs(list) do
            chunks[i] = M.expand(ctx, c)
        end
        return chunks
    end
    local send = step.send
    if type(send) == 'function' then
        send = send(ctx)
    end
    return {M.expand(ctx, send)}
end

local function reset(ctx)
    local conn = M.connect(ctx.srv.port)
    conn:write('flush_all\r\n')
    local reply = conn:collect('\r\n', M.TIMEOUT)
    if reply ~= 'OK\r\n' then
        error(('%s: flush_all: %s'):format(ctx.srv.name, M.show(reply)))
    end
    return conn
end

-- Run one step; returns a transcript entry or nil for sleeps.
function M.run_step(ctx, step)
    if type(step) == 'table' and step.sleep ~= nil then
        fiber.sleep(step.sleep)
        return nil
    end
    if type(step) == 'table' and step.connect then
        ctx.conn:close()
        ctx.conn = M.connect(ctx.srv.port)
    end
    local chunks = step_chunks(ctx, step)
    local gap = type(step) == 'table' and step.gap or 0.05
    local conn = ctx.conn
    for i, c in ipairs(chunks) do
        if i > 1 and gap > 0 then
            fiber.sleep(gap)
        end
        conn:write(c)
    end
    local req = table.concat(chunks)
    local raw, status
    if type(step) == 'table' and step.nosync then
        raw, status = conn:collect(nil, M.TIMEOUT, step.idle or 0.5)
    else
        local sync, sync_reply = M.sync_request(ctx)
        conn:write(sync)
        raw, status = conn:collect(sync_reply, M.TIMEOUT)
        if status == 'ok' then
            raw = raw:sub(1, -#sync_reply - 1)
        end
    end
    local names = {}
    local norm = M.normalise(ctx.cas, raw, req, names)
    if status == 'closed' then
        norm = norm .. '<CLOSED>'
    elseif status == 'timeout' then
        norm = norm .. '<TIMEOUT>'
    end
    if M.TRACE then
        log.info('[%s] >> %s', ctx.srv.kind, M.show(req))
        log.info('[%s] << %s', ctx.srv.kind, M.show(raw))
    end
    return {
        req = req,
        raw = raw,
        norm = norm,
        status = status,
        stats = names,
        step = step,
    }
end

-- Run the steps of a scenario on one server.
local function run_on(srv, sc, prefix, base)
    local ctx = {srv = srv, prefix = prefix, base = base, cas = new_casmap()}
    ctx.conn = reset(ctx)
    local entries = {}
    for i, step in ipairs(sc.steps) do
        local e = M.run_step(ctx, step)
        if e ~= nil then
            e.index = i
            entries[#entries + 1] = e
        end
    end
    ctx.conn:close()
    return entries
end

-- Run a scenario on both servers at once (the sleeps overlap) and return
-- {name, ref = entries, ours = entries}.
function M.run(servers, sc)
    local prefix = M.new_prefix()
    local base = math.floor(fiber.time())
    local results, errors = {}, {}
    local fibers = {}
    for i, srv in ipairs(servers) do
        local f = fiber.new(function()
            local ok, res = pcall(run_on, srv, sc, prefix, base)
            if ok then
                results[i] = res
            else
                errors[i] = res
            end
        end)
        f:set_joinable(true)
        fibers[i] = f
    end
    for _, f in ipairs(fibers) do
        f:join()
    end
    for i = 1, #servers do
        if errors[i] ~= nil then
            error(('%s: %s'):format(servers[i].name, tostring(errors[i])), 2)
        end
    end
    return {
        name = sc.name,
        prefix = prefix,
        ref = results[1],
        ours = results[2],
        servers = servers,
        allow = sc.allow,
    }
end

--
-- Comparison.
--

local function sorted_set(list)
    local set, out = {}, {}
    for _, v in ipairs(list) do
        if not set[v] then
            set[v] = true
            out[#out + 1] = v
        end
    end
    table.sort(out)
    return out, set
end

-- Remember which stats names only one side has.
local function note_stats(req, ref_names, our_names)
    for b = 1, math.max(#ref_names, #our_names) do
        local sub = req:match('^stats ?([^\r\n]*)') or '?'
        local rec = M.report.stats[sub]
        if rec == nil then
            rec = {only_ref = {}, only_ours = {}, common = {}}
            M.report.stats[sub] = rec
        end
        local function generic(names)
            local out = {}
            for i, name in ipairs(names or {}) do
                out[i] = name:gsub('^%d+:', 'N:')
            end
            return out
        end
        local rlist, rset = sorted_set(generic(ref_names[b]))
        local olist, oset = sorted_set(generic(our_names[b]))
        for _, name in ipairs(rlist) do
            if oset[name] then
                rec.common[name] = true
            else
                rec.only_ref[name] = true
            end
        end
        for _, name in ipairs(olist) do
            if not rset[name] then
                rec.only_ours[name] = true
            end
        end
    end
end

-- Versions: '1.6.45' -> comparable number.
local function version_num(v)
    local a, b, c = tostring(v or ''):match('^(%d+)%.(%d+)%.(%d+)')
    if a == nil then
        return nil
    end
    return tonumber(a) * 1000000 + tonumber(b) * 1000 + tonumber(c)
end
M.version_num = version_num

-- Whether an allowlist entry applies to the reference version.
function M.applies(a, ref_version)
    if a.ref == nil then
        return true
    end
    local op, v = a.ref:match('^([<>]=?)(.+)$')
    local have, want = version_num(ref_version), version_num(v)
    if have == nil or want == nil then
        return false
    end
    if op == '>' then
        return have > want
    elseif op == '>=' then
        return have >= want
    elseif op == '<' then
        return have < want
    end
    return have <= want
end

-- Whether the fixes of the entries in list (applied in order) make the
-- replies equal.
local function fixed_same(c, list)
    local ref, got = c.ref, c.got
    for _, a in ipairs(list) do
        ref, got = a.fix({req = c.req, ref = ref, got = got, same = M.same})
    end
    return M.same(ref, got)
end

-- The allowlist entries that explain a mismatch, or nil:
-- - an entry the step (or the scenario) is tagged with, whose match (or
--   fix) agrees, if it has one;
-- - else the smallest set of fix entries (tagged or auto) whose fixes
--   together make the replies equal: one step may hit several of them;
-- - else an auto entry whose match agrees.
function M.explain(c)
    c.same = M.same
    local ids = c.step and type(c.step) == 'table' and c.step.allow
        or c.scenario_allow
    if type(ids) == 'string' then
        ids = {ids}
    end
    local tagged = {}
    for _, id in ipairs(ids or {}) do
        tagged[id] = true
    end
    local fixes = {}
    for _, a in ipairs(allowlist) do
        if M.applies(a, c.ref_version) then
            if tagged[a.id] then
                if a.fix ~= nil then
                    if fixed_same(c, {a}) then
                        return {a}
                    end
                elseif a.match == nil or a.match(c) then
                    return {a}
                end
            end
            if a.fix ~= nil and (a.auto or tagged[a.id]) then
                fixes[#fixes + 1] = a
            end
        end
    end
    -- Subsets of the fixes by size; there are only a few of them.
    local n = #fixes
    local best
    for mask = 1, bit.lshift(1, n) - 1 do
        local list = {}
        for i = 1, n do
            if bit.band(mask, bit.lshift(1, i - 1)) ~= 0 then
                list[#list + 1] = fixes[i]
            end
        end
        if (best == nil or #list < #best) and fixed_same(c, list) then
            best = list
        end
    end
    if best ~= nil then
        return best
    end
    for _, a in ipairs(allowlist) do
        if a.auto and a.match ~= nil
            and M.applies(a, c.ref_version) and a.match(c) then
            return {a}
        end
    end
    return nil
end

local function note_allowed(list, name, c)
    for _, a in ipairs(list) do
        local rec = M.report.allowed[a.id]
        if rec == nil then
            rec = {count = 0, example = nil}
            M.report.allowed[a.id] = rec
        end
        rec.count = rec.count + 1
        if rec.example == nil then
            rec.example = {
                scenario = name,
                req = c.req,
                ref = c.ref,
                got = c.got,
            }
        end
    end
end

-- Compare two transcripts; returns the list of unexplained mismatches.
-- opts.record = false: don't add to the report (fuzzer replays).
function M.compare(tr, opts)
    local record = not (opts and opts.record == false)
    local bad = {}
    for i, r in ipairs(tr.ref) do
        local o = tr.ours[i]
        if r.status == 'timeout'
            and not (type(r.step) == 'table' and r.step.expect_timeout) then
            bad[#bad + 1] = {
                index = r.index,
                req = r.req,
                ref = r.norm,
                got = o.norm,
                ref_raw = r.raw,
                got_raw = o.raw,
                note = 'the reference server timed out: '
                    .. 'the scenario is broken',
            }
        elseif not M.same(r.norm, o.norm) then
            local c = {
                index = r.index,
                req = r.req,
                ref = r.norm,
                got = o.norm,
                ref_raw = r.raw,
                got_raw = o.raw,
                ref_status = r.status,
                got_status = o.status,
                step = r.step,
                scenario_allow = tr.allow,
                ref_version = tr.servers and tr.servers[1].version,
            }
            local a = M.explain(c)
            if a ~= nil then
                c.allowed = a
                if record then
                    note_allowed(a, tr.name, c)
                end
            else
                bad[#bad + 1] = c
            end
        elseif record and type(r.step) == 'table'
            and r.step.allow ~= nil and not r.step.flaky then
            local ids = r.step.allow
            ids = type(ids) == 'string' and {ids} or ids
            local version = tr.servers and tr.servers[1].version
            for _, id in ipairs(ids) do
                for _, a in ipairs(allowlist) do
                    if a.id == id and M.applies(a, version) then
                        local stale = M.report.stale[id] or {}
                        M.report.stale[id] = stale
                        stale[#stale + 1] = ('%s:%d'):format(tr.name, r.index)
                    end
                end
            end
        end
        if record then
            note_stats(r.req, r.stats, o.stats)
        end
    end
    return bad
end

function M.format_mismatch(tr, m)
    return table.concat({
        ('%s step %d%s'):format(
            tr.name,
            m.index,
            m.note and (' (' .. m.note .. ')') or ''
        ),
        '  request:   ' .. M.show(m.req),
        '  memcached: ' .. M.show(m.ref),
        '  contrib:   ' .. M.show(m.got),
        '  raw memcached: ' .. M.show(m.ref_raw or ''),
        '  raw contrib:   ' .. M.show(m.got_raw or ''),
    }, '\n')
end

-- Run a scenario and raise if it has unexplained mismatches.
function M.check(servers, sc)
    local tr = M.run(servers, sc)
    local bad = M.compare(tr)
    if #bad > 0 then
        local msgs = {}
        for _, m in ipairs(bad) do
            msgs[#msgs + 1] = M.format_mismatch(tr, m)
        end
        error(
            ('%d mismatch(es), key prefix %s\n%s'):format(
                #bad,
                tr.prefix,
                table.concat(msgs, '\n')
            ),
            0
        )
    end
    return tr
end

local function keys_sorted(t)
    local out = {}
    for k in pairs(t) do
        out[#out + 1] = k
    end
    table.sort(out)
    return out
end

local CLASS_TITLE = {
    a = 'known tarantool-contrib/memcached bugs (class a)',
    b = 'intentional (class b)',
    c = 'memcached-version-specific (class c)',
}

-- The summary of allowlisted divergences and stats key sets.
function M.summary()
    local lines = {
        ('compat summary (reference: %s):'):format(
            M.reference_name or M.MEMCACHED
        ),
    }
    local by_id = {}
    for _, a in ipairs(allowlist) do
        by_id[a.id] = a
    end
    for _, class in ipairs({'a', 'b', 'c'}) do
        local head = false
        for _, id in ipairs(keys_sorted(M.report.allowed)) do
            local a = by_id[id]
            if a.class == class then
                if not head then
                    lines[#lines + 1] = '  ' .. CLASS_TITLE[class] .. ':'
                    head = true
                end
                local rec = M.report.allowed[id]
                lines[#lines + 1] = ('    %-24s x%-3d e.g. %s: %s'):format(
                    id,
                    rec.count,
                    rec.example.scenario,
                    M.show(rec.example.req, 70)
                )
            end
        end
    end
    for _, id in ipairs(keys_sorted(M.report.stale)) do
        local where = M.report.stale[id]
        lines[#lines + 1] = ('  tag %s did not diverge: %s'):format(
            id,
            table.concat(where, ' ', 1, math.min(#where, 6))
                .. (#where > 6 and ' ...' or '')
        )
    end
    for _, sub in ipairs(keys_sorted(M.report.stats)) do
        local rec = M.report.stats[sub]
        lines[#lines + 1] = ('  stats %q: only memcached: %s'):format(
            sub,
            table.concat(keys_sorted(rec.only_ref), ' ')
        )
        lines[#lines + 1] = ('  stats %q: only contrib: %s'):format(
            sub,
            table.concat(keys_sorted(rec.only_ours), ' ')
        )
    end
    return table.concat(lines, '\n')
end

return M
