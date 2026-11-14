-- Child Tarantool processes with application threads, and a pipelined
-- get/set load generator that runs in the application threads of a client
-- process (so the client is not the bottleneck).

local server = require('luatest.server')

local env = require('test.helpers.env')

local M = {}

-- box.cfg{app_threads = N} makes one thread group named 'app'.
M.GROUP = 'app'

-- A started child process with n application threads. opts.setsearchroot
-- = false: don't make luatest's search root the child's one (Tarantool
-- searches modules there before package.path; unset, it is the current
-- directory).
function M.start(alias, n, box_cfg, opts)
    local cfg = {app_threads = n or 0}
    for k, v in pairs(box_cfg or {}) do
        cfg[k] = v
    end
    local srv = server:new({
        alias = alias,
        env = env.lua_env(),
        box_cfg = cfg,
        net_box_credentials = {user = 'admin'},
        setsearchroot = (opts or {}).setsearchroot,
    })
    srv:start()
    return srv
end

-- Create an instance in a child process; returns its status.
function M.create(srv, name, port, opts)
    return srv:exec(function(name_, port_, opts_)
        local mcc = require('memcached')
        opts_.listen = '127.0.0.1:' .. port_
        return mcc.create(name_, opts_).status
    end, {name, port, opts or {}})
end

function M.destroy(srv, name)
    srv:exec(function(name_)
        local mcc = require('memcached')
        if mcc.get(name_) ~= nil then
            mcc.destroy(name_)
        end
    end, {name})
end

-- inst:info() in a child process.
function M.info(srv, name)
    return srv:exec(function(name_)
        return require('memcached').get(name_):info()
    end, {name})
end

-- Evaluate a Lua expression in every application thread of a child
-- process; returns an array of the first results, by thread.
function M.thread_eval(srv, expr, args)
    return srv:exec(function(expr_, args_)
        local res = require('threads').eval('app', expr_, args_)
        local out = {}
        for i, r in ipairs(res) do
            out[i] = r[1]
        end
        return out
    end, {expr, args or {}})
end

-- The load generator: runs in each application thread of the client
-- process. Each of `conns` connections sends pipelines of `depth` commands
-- (sets and gets of `keys` keys, `ratio` gets per set) for `duration`
-- seconds and waits for the replies of each pipeline before the next one.
local LOAD = [[
local opts = ...
local clock = require('clock')
local fiber = require('fiber')
local socket = require('socket')
local value = string.rep('x', opts.value_size)
local ops, rtts, errors = 0, {}, 0
local done = 0
local deadline = clock.monotonic() + opts.duration
local function run(id)
    local s = socket.tcp_connect('127.0.0.1', opts.port, 5)
    if s == nil then
        errors = errors + 1
        done = done + 1
        return
    end
    s:nonblock(true)
    local n = 0
    while clock.monotonic() < deadline do
        local cmds = {}
        local want_end, want_stored = 0, 0
        for i = 1, opts.depth do
            n = n + 1
            local key = ('k%d'):format((n * 7919 + id * 104729) % opts.keys)
            if n % (opts.ratio + 1) == 0 then
                cmds[i] = ('set %s 0 0 %d\r\n%s\r\n'):format(key, #value,
                                                           value)
                want_stored = want_stored + 1
            else
                cmds[i] = ('get %s\r\n'):format(key)
                want_end = want_end + 1
            end
        end
        local data = table.concat(cmds)
        local t0 = clock.monotonic()
        local off = 1
        while off <= #data do
            local w = s:syswrite(data:sub(off))
            if w == nil then
                s:writable(1)
            else
                off = off + w
            end
        end
        -- The reply of a pipeline is small: count its terminators anew
        -- after each read (one may be split between two reads).
        local buf = ''
        while true do
            local chunk = s:sysread(1048576)
            if chunk == nil then
                if not s:readable(5) then
                    errors = errors + 1
                    break
                end
            elseif chunk == '' then
                errors = errors + 1
                break
            else
                buf = buf .. chunk
                local got_end, got_stored = 0, 0
                for _ in buf:gmatch('END\r\n') do
                    got_end = got_end + 1
                end
                for _ in buf:gmatch('STORED\r\n') do
                    got_stored = got_stored + 1
                end
                if got_end >= want_end and got_stored >= want_stored then
                    break
                end
            end
        end
        if errors > 0 then
            break
        end
        ops = ops + opts.depth
        table.insert(rtts, clock.monotonic() - t0)
    end
    s:close()
    done = done + 1
end
for i = 1, opts.conns do
    fiber.create(run, i + 1000 * require('threads').info().thread_id)
end
while done < opts.conns do
    fiber.sleep(0.01)
end
table.sort(rtts)
local sum = 0
for _, v in ipairs(rtts) do
    sum = sum + v
end
local function pct(p)
    if #rtts == 0 then
        return 0
    end
    return rtts[math.max(1, math.ceil(#rtts * p))]
end
return {ops = ops, errors = errors, batches = #rtts,
        avg = #rtts > 0 and sum / #rtts or 0, p50 = pct(0.5),
        p99 = pct(0.99)}
]]

-- Run the load from the threads of a client process (made by M.start).
-- opts: port, conns (per thread), depth, keys, ratio, value_size,
-- duration, target (one client thread: its id; default all).
-- Returns {ops, ops_per_sec, avg/p50/p99 batch round trips in seconds}.
function M.load(client, opts)
    local o = {
        port = opts.port,
        conns = opts.conns or 4,
        depth = opts.depth or 16,
        keys = opts.keys or 10000,
        ratio = opts.ratio or 9,
        value_size = opts.value_size or 32,
        duration = opts.duration or 3,
    }
    local res = client:exec(function(code, o_, target)
        local clock = require('clock')
        local t0 = clock.monotonic()
        local list = require('threads').eval(
            'app',
            code,
            {o_},
            {target = target}
        )
        return {list = list, elapsed = clock.monotonic() - t0}
    end, {LOAD, o, opts.target})
    local total = {ops = 0, errors = 0, avg = 0, p50 = 0, p99 = 0}
    local n = 0
    for _, r in ipairs(res.list) do
        local x = r[1]
        total.ops = total.ops + x.ops
        total.errors = total.errors + x.errors
        total.avg = total.avg + x.avg
        total.p50 = math.max(total.p50, x.p50)
        total.p99 = math.max(total.p99, x.p99)
        n = n + 1
    end
    total.avg = n > 0 and total.avg / n or 0
    total.ops_per_sec = total.ops / o.duration
    total.elapsed = res.elapsed
    return total
end

return M
