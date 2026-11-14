-- A minimal memcached text protocol client over a Tarantool socket.

local socket = require('socket')
local t = require('luatest')

local M = {}

M.TIMEOUT = 2

local Client = {}
Client.__index = Client

function M.connect(port, host)
    local s, err = socket.tcp_connect(host or '127.0.0.1', port, M.TIMEOUT)
    if s == nil then
        return nil, err
    end
    return setmetatable({sock = s}, Client)
end

function M.must_connect(port, host)
    local c, err = M.connect(port, host)
    t.assert(c, ('connect to %s: %s'):format(port, err))
    return c
end

function M.is_listening(port, host)
    local s = socket.tcp_connect(host or '127.0.0.1', port, 0.5)
    if s == nil then
        return false
    end
    s:close()
    return true
end

function Client:send(data)
    assert(self.sock:write(data))
end

-- One line, with the "\r\n"; nil on timeout or EOF.
function Client:line(timeout)
    return self.sock:read('\r\n', timeout or M.TIMEOUT)
end

-- Exactly n bytes.
function Client:read(n, timeout)
    return self.sock:read(n, timeout or M.TIMEOUT)
end

-- Read up to the terminator, inclusive.
function Client:read_until(terminator, timeout)
    return self.sock:read(terminator, timeout or M.TIMEOUT)
end

-- Send a command and read the reply: one line, or up to the terminator.
function Client:call(data, terminator)
    self:send(data)
    return self:read_until(terminator or '\r\n')
end

-- Read a retrieval reply: the items by VALUE lines, up to END. Returns the
-- raw reply and an array of {key, flags, value, cas}.
function Client:read_values()
    local raw = {}
    local items = {}
    while true do
        local line = self:line()
        if line == nil then
            return nil
        end
        table.insert(raw, line)
        if line == 'END\r\n' or not line:find('^VALUE ') then
            break
        end
        local key, flags, len, cas = line:match(
            '^VALUE (%S+) (%d+) (%d+) ?(%d*)\r\n$'
        )
        local data = self:read(tonumber(len) + 2)
        table.insert(raw, data)
        table.insert(items, {
            key = key,
            flags = tonumber(flags),
            value = data:sub(1, -3),
            cas = cas ~= '' and cas or nil,
        })
    end
    return table.concat(raw), items
end

-- The value of a key or nil; also the flags and the CAS (a string).
function Client:get(key, cmd)
    self:send(('%s %s\r\n'):format(cmd or 'gets', key))
    local _, items = self:read_values()
    local item = items and items[1]
    if item == nil then
        return nil
    end
    return item.value, item.flags, item.cas
end

function Client:set(key, value, opts)
    opts = opts or {}
    return self:call(
        ('%s %s %d %d %d\r\n%s\r\n'):format(
            opts.cmd or 'set',
            key,
            opts.flags or 0,
            opts.exptime or 0,
            #value,
            value
        )
    )
end

-- The connection is closed by the server: a read gets EOF.
function Client:is_closed(timeout)
    local data = self.sock:read(1, timeout or M.TIMEOUT)
    return data == ''
end

function Client:close()
    self.sock:close()
end

-- One-shot helpers.
function M.set(port, key, value)
    local c = M.must_connect(port)
    local reply = c:set(key, value)
    c:close()
    return reply
end

function M.get(port, key)
    local c = M.must_connect(port)
    local reply = c:call(('get %s\r\n'):format(key), 'END\r\n')
    c:close()
    return reply
end

return M
