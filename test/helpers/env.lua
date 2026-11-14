-- Test environment: package paths, an in-process box, free ports.

local fio = require('fio')
local socket = require('socket')

local M = {}

-- The repository root: test/helpers/env.lua -> ../..
M.root = fio.abspath(
    fio.pathjoin(
        fio.dirname(debug.getinfo(1, 'S').source:sub(2)),
        '..',
        '..'
    )
)

-- The tree tt builds (tt test, tt package build, app.manifest.toml): the
-- dev dependencies (luatest, vshard, checks) and the rock of the package,
-- whose C data path and memcached/version.lua the tests use. The rock's
-- copies of the Lua files come after the source tree's in every path below,
-- so the tests run the sources.
M.rocks = M.root .. '/.rocks'
M.datapath_so = M.rocks .. '/lib/tarantool/memcached/datapath.so'

-- A datapath.so or ipc.so in the source tree (an old in-tree build) would
-- be found in the search root ahead of the one tt builds.
for _, so in ipairs({'datapath.so', 'ipc.so'}) do
    local stale = M.root .. '/memcached/' .. so
    assert(
        not fio.path.exists(stale),
        stale .. ' is not built by tt and ' .. 'would shadow ' .. M.rocks
            .. '/lib/tarantool/memcached/' .. so .. ': remove it'
    )
end

-- box.cfg{work_dir = ...} changes the current directory, so relative
-- package paths stop working (the server loads proto/meta.lua lazily): use
-- absolute ones.
local LUA_PATH = M.root .. '/?.lua;' .. M.root .. '/?/init.lua;' .. M.rocks
    .. '/share/tarantool/?.lua;' .. M.rocks .. '/share/tarantool/?/init.lua;'
local LUA_CPATH = M.rocks .. '/lib/tarantool/?.so;'
if not package.path:find(LUA_PATH, 1, true) then
    package.path = LUA_PATH .. package.path
end
if not package.cpath:find(LUA_CPATH, 1, true) then
    package.cpath = LUA_CPATH .. package.cpath
end

-- Environment for child Tarantool processes.
function M.lua_env()
    return {
        LUA_PATH = LUA_PATH .. (os.getenv('LUA_PATH') or ';'),
        LUA_CPATH = LUA_CPATH .. (os.getenv('LUA_CPATH') or ';;'),
        -- The data path switch (memcached/state.lua datapath_off).
        MEMCACHED_DATAPATH = os.getenv('MEMCACHED_DATAPATH'),
    }
end

-- Configure box in this process once. luatest configures the log.
function M.box_cfg()
    if type(box.cfg) == 'function' then
        M.workdir = fio.tempdir()
        box.cfg {
            work_dir = M.workdir,
            memtx_memory = 256 * 1024 * 1024,
            checkpoint_count = 1,
        }
    end
    if box.info.ro then
        box.cfg {read_only = false}
    end
end

-- A TCP port nobody listens on (as far as we can tell).
function M.free_port()
    local s = socket('AF_INET', 'SOCK_STREAM', 'tcp')
    assert(s:bind('127.0.0.1', 0))
    local port = s:name().port
    s:close()
    return port
end

-- Drop the spaces created by a test.
function M.drop_spaces(names)
    for _, name in ipairs(names) do
        if box.space[name] ~= nil then
            box.space[name]:drop()
        end
    end
end

return M
