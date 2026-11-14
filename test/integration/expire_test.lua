-- The expiration sweeper: how it stops. Tarantool cancels every fiber on
-- shutdown; that must not be logged as a sweeper failure (it was, at every
-- normal shutdown: "sweeper of 't' failed: fiber is cancelled"). The tests
-- run in a child process to read its log.

local fio = require('fio')
local fiber = require('fiber')
local t = require('luatest')
local server = require('luatest.server')

local env = require('test.helpers.env')

local g = t.group('expire')

g.before_each(function(cg)
    cg.server = server:new({alias = 'mcc_expire', env = env.lua_env()})
    cg.server:start()
    cg.port = env.free_port()
    -- An instance with a sweeper, which has reached its wait.
    local status = cg.server:exec(function(port)
        local fiber_ = require('fiber')
        local mcc = require('memcached')
        mcc.create('t', {listen = '127.0.0.1:' .. port})
        local rec = require('memcached.state').sweepers.t
        for _ = 1, 100 do
            if rec.fiber:status() == 'suspended' then
                break
            end
            fiber_.sleep(0.01)
        end
        return rec.fiber:status()
    end, {cg.port})
    t.assert_equals(status, 'suspended')
end)

g.after_each(function(cg)
    cg.server:drop()
end)

-- A cancel that is not expire.stop(), as on shutdown, is silent.
g.test_cancel_is_silent = function(cg)
    local res = cg.server:exec(function()
        local fiber_ = require('fiber')
        local state = require('memcached.state')
        local rec = state.sweepers.t
        rec.fiber:cancel()
        for _ = 1, 100 do
            if rec.fiber:status() == 'dead' then
                break
            end
            fiber_.sleep(0.01)
        end
        return {rec.stopped, rec.fiber:status(), state.sweepers.t == nil}
    end)
    t.assert_equals(res, {false, 'dead', true})
    t.assert_equals(cg.server:grep_log('sweeper of'), nil)
end

-- Other errors are still logged, and the sweeper goes on.
g.test_errors_are_logged = function(cg)
    local calls = cg.server:exec(function()
        local fiber_ = require('fiber')
        local expire = require('memcached.expire')
        local step = expire.step
        local n = 0
        expire.step = function()
            n = n + 1
            if n == 1 then
                error('boom', 0)
            end
            return 0.01
        end
        expire.start('e', '__mcc_t')
        for _ = 1, 300 do
            if n >= 2 then
                break
            end
            fiber_.sleep(0.01)
        end
        expire.step = step
        expire.stop('e')
        return n
    end)
    t.assert_ge(calls, 2)
    t.assert_not_equals(cg.server:grep_log("sweeper of 'e': boom"), nil)
    t.assert_equals(cg.server:grep_log("sweeper of 'e' failed"), nil)
end

-- A normal shutdown logs no sweeper error.
g.test_shutdown_is_silent = function(cg)
    local log_file = cg.server.log_file
    cg.server:stop()
    -- The log goes through a pipe: wait for the end of the shutdown.
    local function read()
        local f = assert(fio.open(log_file, {'O_RDONLY'}))
        local data = f:read()
        f:close()
        return data
    end
    t.helpers.retrying({timeout = 10}, function()
        t.assert_str_contains(read(), 'tx_binary: stopped')
    end)
    fiber.sleep(0.2)
    local data = read()
    t.assert_not_str_contains(data, 'sweeper of')
    t.assert_not_str_contains(data, 'fiber is cancelled')
end
