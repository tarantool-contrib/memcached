-- server.lua with fake backends: SO_REUSEPORT, the per-batch lookup of the
-- code (what hot reload relies on), backend errors.

local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local g = t.group('server')

local SERVER = 'memcached.server'
local TEXT = 'memcached.proto.text'

local server

-- A backend that answers every get with a fixed value.
local function fake_backend(value)
    local backend = {calls = 0, batches = {}}
    function backend.exec(self, reqs)
        self.calls = self.calls + 1
        table.insert(self.batches, #reqs)
        local resps = {}
        for i, req in ipairs(reqs) do
            if req.op == 'get' then
                resps[i] = {
                    status = 'HIT',
                    item = {
                        key = req.key,
                        value = value,
                        flags = 0,
                        cas = 1ULL,
                    },
                }
            else
                resps[i] = {status = 'STORED', cas = 1ULL}
            end
        end
        return resps
    end
    function backend.flush_all(_, delay)
        backend.flushed = delay
        return {status = 'OK'}
    end
    function backend.stats()
        return {curr_items = 7, bytes = 8}
    end
    return backend
end

g.before_all(function(cg)
    env.box_cfg()
    server = require(SERVER)
    cg.port = env.free_port()
end)

g.after_each(function(cg)
    package.loaded[SERVER] = server
    package.loaded[TEXT] = cg.text or package.loaded[TEXT]
    for _, srv in ipairs(cg.servers or {}) do
        srv:stop()
    end
    cg.servers = nil
end)

local function start(cg, opts)
    opts.name = opts.name or 'fake'
    opts.listen = opts.listen or '127.0.0.1:' .. cg.port
    local srv = server.new(opts)
    srv:start()
    cg.servers = cg.servers or {}
    table.insert(cg.servers, srv)
    return srv
end

g.test_reuseport = function(cg)
    start(cg, {backend = fake_backend('a'), reuseport = true})
    -- Without SO_REUSEPORT the port is busy.
    local srv = server.new({
        name = 'x',
        listen = '127.0.0.1:' .. cg.port,
        backend = fake_backend('b'),
    })
    t.assert_error_msg_contains(
        "can't bind to 127.0.0.1:" .. cg.port,
        srv.start,
        srv
    )
    -- With it, both listen. macOS doesn't spread the connections over the
    -- sockets of a port (README.md, Requirements): one server gets them all.
    start(cg, {backend = fake_backend('b'), reuseport = true})
    if jit.os == 'OSX' then
        local c = client.must_connect(cg.port)
        t.assert_not_equals(c:get('k', 'get'), nil)
        c:close()
        return
    end
    local seen = {}
    t.helpers.retrying({timeout = 10}, function()
        local c = client.must_connect(cg.port)
        seen[c:get('k', 'get')] = true
        c:close()
        t.assert(seen.a and seen.b, 'both servers accept connections')
    end)
end

g.test_batches = function(cg)
    local backend = fake_backend('v')
    start(cg, {backend = backend})
    local c = client.must_connect(cg.port)
    -- One read: one batch, flush_all splits it, version doesn't reach the
    -- backend.
    c:send(
        'get a b\r\nset x 0 0 1\r\n1\r\nflush_all 5\r\nget c\r\n'
            .. 'version\r\nget d\r\n'
    )
    t.assert_equals(
        c:read_values(),
        'VALUE a 0 1\r\nv\r\nVALUE b 0 1\r\nv\r\nEND\r\n'
    )
    t.assert_equals(c:line(), 'STORED\r\n')
    t.assert_equals(c:line(), 'OK\r\n')
    t.assert_equals(c:read_values(), 'VALUE c 0 1\r\nv\r\nEND\r\n')
    t.assert_str_matches(c:line(), 'VERSION .*\r\n')
    t.assert_equals(c:read_values(), 'VALUE d 0 1\r\nv\r\nEND\r\n')
    t.assert_equals(backend.flushed, 5)
    t.assert_equals(backend.batches, {3, 2})
    c:send('stats\r\n')
    local stats = c:read_until('END\r\n')
    t.assert_str_contains(stats, 'STAT curr_items 7\r\n')
    t.assert_str_contains(stats, 'STAT bytes 8\r\n')
    t.assert_str_contains(stats, 'STAT cmd_flush 1\r\n')
    c:close()
end

g.test_backend_lookup_per_batch = function(cg)
    local srv = start(cg, {backend = fake_backend('old')})
    local c = client.must_connect(cg.port)
    t.assert_equals(c:get('k', 'get'), 'old')
    -- A new backend object is used by the existing connection.
    srv:cfg({backend = fake_backend('new')})
    t.assert_equals(c:get('k', 'get'), 'new')
    c:close()
end

g.test_code_lookup_per_batch = function(cg)
    start(cg, {backend = fake_backend('v')})
    local c = client.must_connect(cg.port)
    t.assert_str_matches(c:call('version\r\n'), 'VERSION 1%.6.*\r\n')
    -- A new text protocol module (as after a reload) is used by the
    -- existing connection on its next batch.
    cg.text = package.loaded[TEXT]
    local new_text = setmetatable({
        encode = function(req, resp)
            if req.op == 'version' then
                return 'VERSION reloaded\r\n'
            end
            return cg.text.encode(req, resp)
        end,
    }, {__index = cg.text})
    package.loaded[TEXT] = new_text
    t.assert_equals(c:call('version\r\n'), 'VERSION reloaded\r\n')
    package.loaded[TEXT] = cg.text
    -- And so is a new server module.
    package.loaded[SERVER] = setmetatable({
        process = function(srv, items, protos, n)
            local out, close = server.process(srv, items, protos, n)
            return out:upper(), close
        end,
    }, {__index = server})
    t.assert_equals(c:get('k', 'get'), 'V')
    package.loaded[SERVER] = server
    t.assert_equals(c:get('k', 'get'), 'v')
    c:close()
end

g.test_backend_errors = function(cg)
    local backend = fake_backend('v')
    backend.exec = function()
        error('backend is down')
    end
    backend.flush_all = function()
        error({message = 'flush\nfailed'})
    end
    local srv = start(cg, {backend = backend})
    local c = client.must_connect(cg.port)
    c:send('get a\r\nset b 0 0 1\r\nx\r\nflush_all\r\n')
    t.assert_str_matches(c:line(), 'SERVER_ERROR .*backend is down\r\n')
    t.assert_str_matches(c:line(), 'SERVER_ERROR .*backend is down\r\n')
    t.assert_equals(c:line(), 'SERVER_ERROR flush failed\r\n')
    -- The connection survives.
    srv:cfg({backend = fake_backend('ok')})
    t.assert_equals(c:get('a', 'get'), 'ok')
    c:close()
end

-- release() stops listening and keeps the connections; the next rebind()
-- binds a URI again, or the old one when the new one is taken.
g.test_release = function(cg)
    local srv = start(cg, {backend = fake_backend('v')})
    local c = client.must_connect(cg.port)
    t.assert_equals(c:get('a', 'get'), 'v')
    srv:release()
    t.assert_not(client.is_listening(cg.port))
    t.assert(srv.running)
    t.assert_equals(srv:info().port, 0)
    t.assert_equals(c:get('a', 'get'), 'v', 'the connection stays')
    -- Another server takes the address meanwhile.
    local other = start(cg, {name = 'other', backend = fake_backend('o')})
    t.assert_equals(client.get(cg.port, 'a'), 'VALUE a 0 1\r\no\r\nEND\r\n')
    -- The new URI is taken, and so is the old one: the server stops
    -- listening (running = false) and keeps the connections.
    local port2 = env.free_port()
    local busy = start(cg, {
        name = 'busy',
        listen = '127.0.0.1:' .. port2,
        backend = fake_backend('b'),
    })
    t.assert_error_msg_contains(
        "can't bind to 127.0.0.1:" .. port2,
        srv.rebind,
        srv,
        '127.0.0.1:' .. port2
    )
    t.assert_equals(srv.running, false)
    t.assert_equals(c:get('a', 'get'), 'v')
    busy:stop()
    other:stop()
    -- Running again, released, then a free URI.
    srv:start()
    srv:release()
    srv:rebind('127.0.0.1:' .. port2)
    t.assert_equals(srv:info().port, port2)
    t.assert_equals(client.get(port2, 'a'), 'VALUE a 0 1\r\nv\r\nEND\r\n')
    t.assert_not(client.is_listening(cg.port))
    -- Released, and the new URI is taken: back to the old one.
    srv:release()
    start(cg, {name = 'other', backend = fake_backend('o')})
    t.assert_error_msg_contains(
        "can't bind to 127.0.0.1:" .. cg.port,
        srv.rebind,
        srv,
        '127.0.0.1:' .. cg.port
    )
    t.assert(srv.running)
    t.assert_equals(srv:info().port, port2)
    t.assert_equals(c:get('a', 'get'), 'v')
    c:close()
end
