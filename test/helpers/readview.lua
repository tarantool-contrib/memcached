-- Read view helpers for child servers (see test/helpers/threads.lua): the
-- read view an instance uses in TX and in the application threads, the read
-- views open in each thread, fibers by name.

local t = require('luatest')

local th = require('test.helpers.threads')

local M = {}

-- The id of the snapshot read view of an instance in TX, or nil.
function M.current_id(srv, name)
    return srv:exec(function(name_)
        local rv = package.loaded['memcached.readview']
        return rv ~= nil and rv.current_id(name_) or nil
    end, {name})
end

-- The same in every application thread: an array by thread (false = none).
function M.thread_ids(srv, name)
    return th.thread_eval(
        srv,
        [[
        local rv = package.loaded['memcached.readview']
        local id = rv ~= nil and rv.current_id(...) or nil
        return id or false
    ]],
        {name}
    )
end

-- Wait until TX uses a read view other than `after` (any, if nil); with
-- opts.threads, until every thread uses it as well. Returns its id.
function M.wait_id(srv, name, after, opts)
    opts = opts or {}
    local id
    t.helpers.retrying({timeout = opts.timeout or 10}, function()
        id = M.current_id(srv, name)
        t.assert_not_equals(id, nil, 'TX has a read view')
        t.assert_not_equals(id, after, 'TX has a new read view')
        if opts.threads then
            for _, tid in ipairs(M.thread_ids(srv, name)) do
                t.assert_equals(tid, id, 'every thread uses it')
            end
        end
    end)
    return id
end

-- Make TX refresh the snapshot read view of an instance now.
function M.refresh(srv, name)
    srv:exec(function(name_)
        require('memcached.readview').invalidate(name_)
    end, {name})
end

-- The read views named `rv_name` in TX: an array of their statuses.
function M.views(srv, rv_name)
    return srv:exec(function(rv_name_)
        local res = {}
        for _, rv in ipairs(box.read_view.list()) do
            if rv.name == rv_name_ then
                table.insert(res, rv.status)
            end
        end
        return res
    end, {rv_name})
end

-- The number of open read views named `rv_name` in each thread.
function M.thread_views(srv, rv_name)
    return th.thread_eval(
        srv,
        [[
        local n = 0
        for _, rv in ipairs(box.read_view.list()) do
            if rv.name == ... and rv.status == 'open' then
                n = n + 1
            end
        end
        return n
    ]],
        {rv_name}
    )
end

-- The names of the live TX fibers that match a Lua pattern.
function M.fibers(srv, pattern)
    return srv:exec(function(pattern_)
        local res = {}
        for _, f in pairs(require('fiber').info()) do
            if tostring(f.name):find(pattern_) then
                table.insert(res, f.name)
            end
        end
        table.sort(res)
        return res
    end, {pattern})
end

return M
