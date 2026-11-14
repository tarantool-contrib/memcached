-- The front-end in an application thread: a batch is one
-- threads.call('tx', 'mcc.exec', {name, reqs}) to the TX thread, after the
-- raw batch (M.batch()) that ships the input bytes of the classic get, gets
-- and set at its head to TX and gets their replies back: through a
-- shared-memory channel (memcached.ipc), or in one threads.call without
-- it.
--
-- No TX-only box APIs here: the module runs in an application thread (it
-- also works in TX, where threads.call('tx', ...) is a direct call).
--
-- Backend contract (see DESIGN.md):
--   backend:exec(reqs) -> resps
--   backend:flush_all(delay) -> resp
--   backend:stats() -> table; with threads it also has the counters
--                      aggregated over the threads (see threads.lua)
--   backend:reset_stats()     resets the counters in every thread
--   backend:conns()           the connections of every thread ("stats
--                             conns")
--   backend:close()
--   backend:batch(input, output, max_commands, max_item_size)
--                             the raw batch, as backend/local.lua's batch()
-- The TX functions are exported by memcached.threads.

local ffi = require('ffi')
local fiber = require('fiber')
local msgpack = require('msgpack')
local threads = require('threads')

local SELF = 'memcached.backend.thread'

local M = {}

-- Methods are looked up in the current module on every call, so existing
-- backend objects use the new code after a reload.
local mt = {
    __index = function(_, k)
        return (package.loaded[SELF] or M)[k]
    end,
}

-- cfg: the instance options plus name.
function M.new(cfg)
    return setmetatable({
        name = cfg.name,
        space_name = cfg.space_name,
        -- The raw batches TX served and their commands, and those of
        -- them that went through the channel (for the tests).
        raw_batches = 0,
        raw_commands = 0,
        ipc_batches = 0,
        -- The channel (memcached.ipc) to TX, once TX serves it.
        ipc = nil,
        -- Whether a fiber is opening the channel.
        ipc_opening = false,
        -- Don't try to open it before this fiber.clock().
        ipc_retry = 0,
        -- close() was called.
        closed = false,
    }, mt)
end

-- Call a function exported in TX; returns its first result.
local function call(func, args)
    local res = threads.call('tx', func, args)
    return res[1][1]
end
M.call = call

function M.exec(self, reqs)
    return call('mcc.exec', {self.name, reqs})
end

-- exec() keeps no reference to the reqs array after it returns (the call
-- copies it to TX, or in TX passes it to a backend that borrows it too):
-- the server reuses it (server.lua "The per-batch tables").
M.borrows_reqs = true

-- The raw batch ---------------------------------------------------------
--
-- server.lua conn_step() calls backend:batch() when the input starts with
-- g or s. With the C data path in TX, the input bytes go to TX as one
-- string, TX runs the C batch on them (engine.batch() through
-- threads.batch()) and returns its replies as one string with the
-- counters and the held keys, as backend/local.lua's batch() returns them:
-- no request or response tables cross the threads. The rest of the input
-- is parsed here and goes to TX with exec() as before.

-- At most this many bytes of input go to TX at once, unless the first
-- command needs more (a set with a big value).
M.RAW_MAX = 65536
-- After TX answered that it can't run the C batch for this instance (no
-- data path, switched off, not memtx, a commit wait mode, snapshot reads,
-- an older TX), or after a set it didn't serve, don't try again for this
-- many seconds.
M.RAW_RETRY = 1
-- Appended to a truncated input: the C batch then sees that more input
-- follows its last command, and returns the keys its reads hold. It is not
-- a newline, so no command ends in it.
local MORE = '\0'

local BYTE_E = string.byte('e')
local BYTE_G = string.byte('g')
local BYTE_S = string.byte('s')
local BYTE_T = string.byte('t')
local BYTE_SPACE = 32
local BYTE_LF = 10

pcall(ffi.cdef, 'void *memchr(const void *s, int c, size_t n);')

-- proto/text.lua item_size() less the key, the flags and the value.
local ITEM_OVERHEAD = 48 + 1 + 2 + 8

local fiber_clock = fiber.clock
local char_ptr = ffi.typeof('char *')

-- The channel --------------------------------------------------------------
--
-- Each backend object (one per instance per thread) opens a channel of
-- memcached.ipc (memcached/ipc.c) on its first raw batch and asks TX to
-- serve it (mcc.ipc_open, threads.lua "The IPC"); meanwhile and when that
-- fails, the raw batches go by threads.call. A request of a connection
-- takes a slot of the channel and waits for its reply there: the bytes,
-- appended to the output buffer, and the values engine.batch() returned in
-- TX. A receiver fiber wakes the connections whose replies came. Without
-- the module (a tree tt didn't build), with M.IPC false, when TX can't
-- serve channels (another ABI, an older TX), when no slot is free and for a
-- request over IPC_MAX bytes, the raw batch uses threads.call. close()
-- closes the channel: the requests in it are still served, TX lets it go
-- after them.

-- Whether to use the channel.
M.IPC = true
M.IPC_MODULE = 'memcached.ipc'
-- The requests in flight in a channel, at most: a connection has one.
M.IPC_SLOTS = 1024
-- After TX couldn't serve a channel, or let it go, don't open one again
-- for this many seconds.
M.IPC_RETRY = 5
-- The receiver checks the channel at least this often (seconds).
M.IPC_WAIT = 1
-- A request over this many bytes goes by threads.call.
M.IPC_MAX = 4 * 1048576

-- The statuses of ipc call() without a reply (memcached/ipc.c).
local IPC_UNAVAILABLE = 3
local IPC_CANCELLED = 4
local IPC_ABI = 1
-- ipc.c info().tx when TX let the channel go.
local IPC_TX_GONE = 2

local decode = msgpack.decode

local function current()
    return package.loaded[SELF] or M
end

-- The receiver fiber of a channel: until it is closed and no connection
-- waits. If it is cancelled, the channel is closed (the waiting connections
-- still get their replies: they look for them every second).
local function receiver(ch)
    while true do
        local ok, more = pcall(ch.receive, ch, current().IPC_WAIT)
        if not ok or not more or not pcall(fiber.testcancel) then
            break
        end
    end
    ch:close()
end

-- Open the channel; on errors, try again after IPC_RETRY. Yields.
local function ipc_open(self)
    local ok, ipc = pcall(require, M.IPC_MODULE)
    if not ok or type(ipc) ~= 'table' or ipc.ABI ~= IPC_ABI then
        self.ipc_retry = fiber_clock() + M.IPC_RETRY
        return
    end
    local ch = ipc.new(M.IPC_SLOTS)
    if ch == nil then
        self.ipc_retry = fiber_clock() + M.IPC_RETRY
        return
    end
    local f = fiber.new(receiver, ch)
    f:name('mcc/ipc-rx/' .. tostring(self.name), {truncate = true})
    self.ipc_opening = true
    local res
    ok, res = pcall(
        threads.call,
        'tx',
        'mcc.ipc_open',
        {self.name, ch:id(), ipc.ABI}
    )
    self.ipc_opening = false
    if ok and res[1] ~= nil and res[1][1] == true and not self.closed then
        self.ipc = ch
        return
    end
    ch:close()
    self.ipc_retry = fiber_clock() + M.IPC_RETRY
    if not ok then
        fiber.testcancel()
    end
end

-- The bytes to send of the input [p, p + size), 0 for none: the input must
-- start with a complete "get ", "gets " or "set " command, which the C
-- batch is likely to serve (a set only when sets is true).
local function raw_size(p, size, sets, max_item_size)
    local first = p[0]
    local name
    if first == BYTE_G then
        if size < 5 or p[1] ~= BYTE_E or p[2] ~= BYTE_T then
            return 0
        end
        if p[3] ~= BYTE_SPACE and (p[3] ~= BYTE_S or p[4] ~= BYTE_SPACE) then
            return 0
        end
        name = 'get'
    elseif first == BYTE_S then
        if not sets or size < 5
            or p[1] ~= BYTE_E or p[2] ~= BYTE_T
            or p[3] ~= BYTE_SPACE then
            return 0
        end
        name = 'set'
    else
        return 0
    end
    local limit = size < M.RAW_MAX and size or M.RAW_MAX
    local nl = ffi.C.memchr(p, BYTE_LF, limit)
    if nl == nil then
        return 0
    end
    local line_len = tonumber(ffi.cast(char_ptr, nl) - p) + 1
    if name == 'set' then
        -- set <key> <flags> <exptime> <bytes>: within max_item_size
        -- (proto/text.lua item_size()), and its data block is here.
        local line = ffi.string(p, line_len)
        local key, flags, bytes = line:match('^set ([^ ]+) (%d+) %d+ (%d+)')
        bytes = tonumber(bytes)
        if bytes == nil
            or ITEM_OVERHEAD + #key + (tonumber(flags) ~= 0 and 4 or 0) + bytes
                > max_item_size then
            return 0
        end
        local need = line_len + bytes + 2
        if need > size then
            return 0
        end
        if need > limit then
            limit = need
        end
    end
    return limit
end

-- The raw batch done: count it.
local function raw_served(self, now, p, consumed, commands, ...)
    if commands == 0 then
        if p[0] == BYTE_S then
            -- A set it doesn't serve: on a read-only instance and with
            -- memtx MVCC it serves none.
            self.raw_sets_off = now + M.RAW_RETRY
        end
        return 0, 0
    end
    self.raw_batches = (self.raw_batches or 0) + 1
    self.raw_commands = (self.raw_commands or 0) + commands
    return consumed, commands, ...
end

-- The raw batch by threads.call.
local function call_batch(
    self, now, p, n, size, output, max_commands, max_item_size
)
    local data = ffi.string(p, n)
    if n < size then
        data = data .. MORE
    end
    local ok, res = pcall(
        threads.call,
        'tx',
        'mcc.batch',
        {self.name, data, max_commands, max_item_size}
    )
    if not ok then
        -- A connection closed while it waited is cancelled.
        fiber.testcancel()
        self.raw_off = now + M.RAW_RETRY
        return nil
    end
    res = res[1] or {}
    local reply = res[1]
    if reply == nil then
        self.raw_off = now + M.RAW_RETRY
        return nil
    end
    local len = #reply
    if len > 0 and res[3] ~= 0 then
        local wp = output:reserve(len)
        ffi.copy(wp, reply, len)
        output.wpos = output.wpos + len
    end
    return raw_served(self, now, p, unpack(res, 2, table.maxn(res)))
end

-- The reply of a request of the channel: the values of engine.batch(), or
-- nil and the status.
local function ipc_done(
    self, now, p, n, size, output, max_commands, max_item_size, consumed, ...
)
    if consumed ~= nil then
        if (...) > 0 then
            self.ipc_batches = (self.ipc_batches or 0) + 1
        end
        return raw_served(self, now, p, consumed, ...)
    end
    local status = ...
    if status == IPC_UNAVAILABLE then
        -- Not run in TX. If TX let the channel go, drop it.
        local ch = self.ipc
        if ch ~= nil then
            local info = ch:info()
            if info.tx == nil or info.tx == IPC_TX_GONE or info.closed then
                self.ipc = nil
                ch:close()
                self.ipc_retry = now + M.IPC_RETRY
            end
        end
        return call_batch(
            self,
            now,
            p,
            n,
            size,
            output,
            max_commands,
            max_item_size
        )
    end
    if status == IPC_CANCELLED then
        fiber.testcancel()
    end
    -- TX can't run the C batch, or it raised.
    self.raw_off = now + M.RAW_RETRY
    return nil
end

-- See backend/local.lua batch() and engine.batch(): returns nil when the
-- raw batch is not used, otherwise the results of engine.batch(),
--   consumed, commands, gets, hits, stored, held, <counters>
-- No held items in: the server doesn't come back to this batch after the
-- commands it leaves to Lua (a call to TX each time).
function M.batch(self, input, output, max_commands, max_item_size)
    local now = fiber_clock()
    local off = self.raw_off
    if off ~= nil then
        if now < off then
            return nil
        end
        self.raw_off = nil
    end
    local sets = true
    off = self.raw_sets_off
    if off ~= nil then
        if now < off then
            sets = false
        else
            self.raw_sets_off = nil
        end
    end
    local p = input.rpos
    local size = tonumber(input.wpos - p)
    local n = raw_size(p, size, sets, max_item_size)
    if n == 0 then
        return nil
    end
    local ch = self.ipc
    if ch == nil and M.IPC and not self.ipc_opening and not self.closed
        and now >= (self.ipc_retry or 0) then
        -- The input stays: only this fiber reads into it.
        ipc_open(self)
        ch = self.ipc
    end
    if ch ~= nil and n <= M.IPC_MAX then
        return ipc_done(
            self, now, p, n, size, output, max_commands, max_item_size,
            ch:call(
                input,
                n,
                n < size,
                max_commands,
                max_item_size,
                output,
                decode
            )
        )
    end
    return call_batch(
        self,
        now,
        p,
        n,
        size,
        output,
        max_commands,
        max_item_size
    )
end

function M.flush_all(self, delay)
    return call('mcc.flush_all', {self.name, delay})
end

function M.stats(self)
    return call('mcc.stats', {self.name})
end

function M.reset_stats(self)
    call('mcc.reset_stats', {self.name})
end

function M.conns(self)
    return call('mcc.conns', {self.name})
end

-- No more batches through the channel; those in flight get their replies.
function M.close(self)
    self.closed = true
    local ch = self.ipc
    self.ipc = nil
    if ch ~= nil then
        ch:close()
    end
end

return M
