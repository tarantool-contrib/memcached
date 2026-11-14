-- The instance option commit_wait (DESIGN.md "Commit wait"): when the
-- commit of a write returns. 'complete' (the default) after the WAL write,
-- 'submit' and 'none' before it, so that the writes of a pipelined batch go
-- to the WAL together.
--
-- The box of the test process has wal_mode = 'write'. Every test runs with
-- the C data path on and off (twice the Lua engine when
-- the data path is not built, env.datapath_so). memtx MVCC and a failing WAL need
-- child processes.

local fiber = require('fiber')
local fio = require('fio')
local t = require('luatest')
local server = require('luatest.server')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local g = t.group('commit_wait')

local MEMTX = 'mcc_cw_memtx'
local VINYL = 'mcc_cw_vinyl'
local NAME = 'cw'
local MODES = {'complete', 'submit', 'none'}
local QUEUE_FULL = {
    status = 'SERVER_ERROR',
    message = 'The WAL queue is full',
}

local mcc
local engine
local schema
local state

g.before_all(function(cg)
    env.box_cfg()
    t.assert_equals(box.cfg.wal_mode, 'write')
    mcc = require('memcached')
    engine = require('memcached.engine')
    schema = require('memcached.schema')
    state = require('memcached.state')
    cg.datapath = not state.datapath_off
    cg.queue = box.cfg.wal_queue_max_size
    cg.port = env.free_port()
end)

g.before_each(function()
    for _, s in ipairs({{MEMTX, 'memory'}, {VINYL, 'disk'}}) do
        schema.ensure(s[1], s[2])
        box.space[s[1]]:truncate()
        box.space._mcc_meta:delete(schema.flush_key(s[1]))
    end
end)

g.after_each(function(cg)
    box.cfg {wal_queue_max_size = cg.queue}
    mcc.datapath(cg.datapath)
    if mcc.get(NAME) ~= nil then
        mcc.destroy(NAME)
    end
    env.drop_spaces({'__mcc_' .. NAME})
end)

g.after_all(function()
    env.drop_spaces({MEMTX, VINYL})
end)

local function uri(port)
    return '127.0.0.1:' .. port
end

-- Engine requests; fast = the mark of the text parser (the C data path).
local function set(key, value, fast)
    return {
        op = 'store',
        mode = 'set',
        key = key,
        value = value,
        flags = 0,
        exptime = 0,
        fast = fast and 'set' or nil,
    }
end

local function get(key, fast)
    return {op = 'get', key = key, fast = fast and 'get' or nil}
end

local function append(key, value)
    return {
        op = 'store',
        mode = 'append',
        key = key,
        value = value,
        flags = 0,
        exptime = 0,
    }
end

-- The value of a get response: a HIT of the Lua engine or the VALUE block
-- of the C data path; nil for a miss.
local function value(resp)
    if type(resp) == 'string' then
        return (resp:match('^VALUE %S+ %d+ %d+\r\n(.*)\r\n$'))
    end
    return resp.status == 'HIT' and resp.item.value or nil
end

-- This instance's component of the vclock: it moves when a WAL write is
-- done.
local function lsn()
    return box.info.vclock[box.info.id] or 0
end

-- Context switches of a fiber so far.
local function csw(f)
    return fiber.info({backtrace = false})[(f or fiber.self()):id()].csw
end

-- fn(what) with the C data path on and off.
local function each_datapath(fn)
    for _, on in ipairs({true, false}) do
        mcc.datapath(on)
        fn(mcc.datapath().enabled and 'C' or 'Lua')
    end
end

g.test_options = function(cg)
    t.assert_equals(mcc.DEFAULTS.commit_wait, 'complete')
    local listen = uri(cg.port)
    t.assert_error_msg_contains(
        "Option 'commit_wait' must be a string, got boolean",
        mcc.check,
        NAME,
        {listen = listen, commit_wait = true}
    )
    t.assert_error_msg_contains(
        "Bad value for option 'commit_wait'",
        mcc.check,
        NAME,
        {listen = listen, commit_wait = 'async'}
    )
    -- With vshard the writes run on the storages: it is set there.
    t.assert_error_msg_contains(
        "Option 'commit_wait' = 'submit' is not supported by the 'vshard' "
            .. "backend: set it on the storages, storage.cfg({spaces = "
            .. "{[<space>] = {commit_wait = 'submit'}}})",
        mcc.check,
        NAME,
        {listen = listen, backend = 'vshard', commit_wait = 'submit'}
    )

    local inst = mcc.create(NAME, {listen = listen})
    t.assert_equals(inst.opts.commit_wait, 'complete')
    t.assert_equals(inst.backend.ctx.commit_wait, 'complete')
    t.assert_error_msg_contains(
        "Bad value for option 'commit_wait'",
        inst.cfg,
        inst,
        {commit_wait = 'async'}
    )
    t.assert_error_msg_contains(
        "is not supported by the 'vshard' backend",
        inst.cfg,
        inst,
        {
            backend = 'vshard',
            commit_wait = 'none',
        }
    )
    t.assert_equals(inst.opts.commit_wait, 'complete')
    -- A change swaps the backend, whose ctx has the mode.
    local backend = inst.backend
    inst:cfg({commit_wait = 'submit'})
    t.assert_not_equals(inst.backend, backend)
    t.assert_equals(inst.backend.ctx.commit_wait, 'submit')
    backend = inst.backend
    inst:cfg({commit_wait = 'submit'})
    t.assert_is(inst.backend, backend)
end

-- Every mode on memtx and vinyl: the replies, a connection's reads of its
-- own writes in the same batch and in the next one, and the data after
-- the WAL writes.
g.test_modes = function()
    each_datapath(function(dp)
        for _, space in ipairs({MEMTX, VINYL}) do
            for _, mode in ipairs(MODES) do
                local what = ('%s, %s, %s'):format(space, mode, dp)
                box.space[space]:truncate()
                local ctx = {space_name = space, commit_wait = mode}
                local r = engine.exec(ctx, {
                    set('a', 'v1', true),
                    get('a', true),
                    append('a', '+x'),
                    get('a', true),
                    set('b', 'w', true),
                    {op = 'delete', key = 'b'},
                    get('b', true),
                })
                t.assert_equals(r[1].status, 'STORED', what)
                t.assert_equals(value(r[2]), 'v1', what)
                t.assert_equals(r[3].status, 'STORED', what)
                t.assert_equals(value(r[4]), 'v1+x', what)
                t.assert_equals(r[5].status, 'STORED', what)
                t.assert_equals(r[6].status, 'DELETED', what)
                t.assert_equals(r[7].status, 'MISS', what)
                r = engine.exec(ctx, {get('a', true), get('b', true)})
                t.assert_equals(value(r[1]), 'v1+x', what)
                t.assert_equals(r[2].status, 'MISS', what)
                -- After the WAL writes: what a confirmed read sees.
                box.ctl.wal_sync()
                local a = box.space[space]:get('a')
                t.assert_equals(a.value, 'v1+x', what)
                -- The last-access write of the get after the append.
                t.assert_equals(a.fetched, true, what)
                t.assert_equals(box.space[space]:get('b'), nil, what)
            end
        end
    end)
end

-- A pipelined batch of sets. With 'submit' and 'none' the batch doesn't
-- yield and nothing is written yet when exec() returns; the WAL writes
-- come after it, together. With 'complete' every set waits for its own.
g.test_group_commit = function()
    local n = 64
    each_datapath(function(dp)
        for _, mode in ipairs(MODES) do
            local what = ('%s, %s'):format(mode, dp)
            local batch = {}
            for i = 1, n do
                batch[i] = set('k' .. i, mode .. i, true)
            end
            box.ctl.wal_sync()
            local lsn0, csw0 = lsn(), csw()
            local r = engine.exec(
                {space_name = MEMTX, commit_wait = mode},
                batch
            )
            local lsn1, csw1 = lsn(), csw()
            for i = 1, n do
                t.assert_equals(r[i].status, 'STORED', what)
            end
            if mode == 'complete' then
                t.assert_ge(csw1 - csw0, n, what)
                t.assert_equals(lsn1 - lsn0, n, what)
            else
                t.assert_equals(csw1 - csw0, 0, what)
                t.assert_equals(lsn1, lsn0, what)
                box.ctl.wal_sync()
                t.assert_equals(lsn() - lsn0, n, what)
            end
            t.assert_equals(
                box.space[MEMTX]:get('k' .. n).value,
                mode .. n,
                what
            )
        end
    end)
end

-- The same through an instance and a socket, switched with cfg(): the
-- connection's fiber yields for every set with 'complete' on the Lua
-- engine, for every group of sets on the C batch (one transaction for the
-- sets of an input it serves, datapath.c "The group"), and with 'submit'
-- only to wait for the socket.
g.test_pipeline = function(cg)
    local n = 64
    local inst = mcc.create(NAME, {listen = uri(cg.port)})
    local c = client.must_connect(cg.port)
    t.assert_equals(c:set('warm', 'x'), 'STORED\r\n')
    local conn_fiber
    for _, conn in pairs(inst.server.conns) do
        conn_fiber = conn.fiber
    end
    local function pipeline(tag)
        local cmds = {}
        for i = 1, n do
            cmds[i] = ('set %s%d 0 0 %d\r\n%s\r\n'):format(tag, i, #tag, tag)
        end
        box.ctl.wal_sync()
        local csw0 = csw(conn_fiber)
        c:send(table.concat(cmds))
        for _ = 1, n do
            t.assert_equals(c:line(), 'STORED\r\n', tag)
        end
        return csw(conn_fiber) - csw0
    end
    each_datapath(function(dp)
        inst:cfg({commit_wait = 'complete'})
        local yields = pipeline('complete-' .. dp)
        if dp == 'C' then
            t.assert_lt(yields, n / 4)
        else
            t.assert_ge(yields, n)
        end
        inst:cfg({commit_wait = 'submit'})
        t.assert_lt(pipeline('submit-' .. dp), n / 4)
        t.assert_equals(c:get('submit-' .. dp .. n, 'get'), 'submit-' .. dp)
    end)
    c:close()
end

-- 'none' with a full WAL queue. wal_queue_max_size = 1 makes the queue full
-- as soon as one write is in it, and nothing leaves it before the batch
-- ends, since 'none' never yields: the next writes fail at once, and a get
-- answers without its last-access write. 'submit' waits for room instead.
g.test_none_queue_full = function(cg)
    each_datapath(function(dp)
        box.space[MEMTX]:truncate()
        box.ctl.wal_sync()
        box.cfg {wal_queue_max_size = 1}
        local r = engine.exec({space_name = MEMTX, commit_wait = 'none'}, {
            set('a', '1', true),
            set('b', '2', true),
            append('a', '+'),
            get('a', true),
        })
        box.cfg {wal_queue_max_size = cg.queue}
        t.assert_equals(r[1].status, 'STORED', dp)
        t.assert_equals(r[2], QUEUE_FULL, dp)
        t.assert_equals(r[3], QUEUE_FULL, dp)
        t.assert_equals(value(r[4]), '1', dp)
        box.ctl.wal_sync()
        t.assert_equals(box.space[MEMTX]:get('a').value, '1', dp)
        t.assert_equals(box.space[MEMTX]:get('a').fetched, false, dp)
        t.assert_equals(box.space[MEMTX]:get('b'), nil, dp)

        box.cfg {wal_queue_max_size = 1}
        local csw0 = csw()
        r = engine.exec({space_name = MEMTX, commit_wait = 'submit'}, {
            set('a', 'x', true),
            set('b', 'y', true),
            set('c', 'z', true),
        })
        local yields = csw() - csw0
        box.cfg {wal_queue_max_size = cg.queue}
        for i = 1, 3 do
            t.assert_equals(r[i].status, 'STORED', dp)
        end
        t.assert_ge(yields, 2, dp)
    end)

    -- Through a socket.
    mcc.create(NAME, {listen = uri(cg.port), commit_wait = 'none'})
    local c = client.must_connect(cg.port)
    box.ctl.wal_sync()
    box.cfg {wal_queue_max_size = 1}
    c:send('set x 0 0 1\r\n1\r\nset y 0 0 1\r\n2\r\n')
    local replies = {c:line(), c:line()}
    box.cfg {wal_queue_max_size = cg.queue}
    t.assert_equals(replies, {
        'STORED\r\n',
        'SERVER_ERROR The WAL queue is full\r\n',
    })
    c:close()
end

-- With memtx MVCC (a child: memtx_use_mvcc_engine can't be changed at
-- runtime) and on vinyl, a connection reads its own writes too: the
-- engine's transactions read the submitted ones (read-committed), which a
-- best-effort autocommit read doesn't see until the WAL write.
local m = t.group('commit_wait_mvcc')

m.before_all(function(cg)
    cg.server = server:new({
        alias = 'mcc_cw_mvcc',
        env = env.lua_env(),
        box_cfg = {memtx_use_mvcc_engine = true},
    })
    cg.server:start()
end)

m.after_all(function(cg)
    cg.server:drop()
end)

m.test_read_your_writes = function(cg)
    local results = cg.server:exec(function()
        local mcc = require('memcached')
        local engine = require('memcached.engine')
        local schema = require('memcached.schema')
        local function set(key, value, fast)
            return {
                op = 'store',
                mode = 'set',
                key = key,
                value = value,
                flags = 0,
                exptime = 0,
                fast = fast and 'set' or nil,
            }
        end
        local function append(key, value)
            return {
                op = 'store',
                mode = 'append',
                key = key,
                value = value,
                flags = 0,
                exptime = 0,
            }
        end
        local function get(key)
            return {op = 'get', key = key, fast = 'get'}
        end
        -- The value of a hit (C: a VALUE block), otherwise the status.
        local function reply(resp)
            if type(resp) == 'string' then
                return (resp:match('^VALUE %S+ %d+ %d+\r\n(.*)\r\n$'))
            end
            return resp.status == 'HIT' and resp.item.value or resp.status
        end
        local out = {mvcc = box.cfg.memtx_use_mvcc_engine}
        for _, on in ipairs({true, false}) do
            mcc.datapath(on)
            for _, sp in ipairs({
                {'mcc_cw_m', 'memory'},
                {'mcc_cw_v', 'disk'},
            }) do
                local space = schema.ensure(sp[1], sp[2])
                for _, mode in ipairs({'complete', 'submit', 'none'}) do
                    space:truncate()
                    engine.exec({space_name = sp[1]}, {set('a', 'old')})
                    local ctx = {space_name = sp[1], commit_wait = mode}
                    local r1 = engine.exec(ctx, {
                        set('a', 'v1', true),
                        get('a'),
                        append('a', '+x'),
                        get('a'),
                    })
                    -- An application's read with the default isolation.
                    local other = space:get('a').value
                    local r2 = engine.exec(ctx, {get('a'), append('a', '+y')})
                    box.ctl.wal_sync()
                    table.insert(out, {
                        what = ('%s, %s, %s'):format(
                            sp[2],
                            mode,
                            mcc.datapath().enabled and 'C' or 'Lua'
                        ),
                        mode = mode,
                        replies = {
                            reply(r1[1]),
                            reply(r1[2]),
                            reply(r1[3]),
                            reply(r1[4]),
                            reply(r2[1]),
                            reply(r2[2]),
                        },
                        other = other,
                        final = space:get('a').value,
                    })
                end
            end
        end
        -- A get that may win a token runs in a transaction there: with a
        -- wait mode it doesn't wait either, and the next get sees the
        -- prepared win.
        local space = schema.ensure('mcc_cw_m', 'memory')
        space:truncate()
        local ctx = {space_name = 'mcc_cw_m', commit_wait = 'submit'}
        engine.exec(ctx, {
            set('w', 'x'),
            {
                op = 'delete',
                key = 'w',
                invalidate = true,
            },
        })
        box.ctl.wal_sync()
        local fiber = require('fiber')
        local csw0 = fiber.info({backtrace = false})[fiber.id()].csw
        local wins = engine.exec(ctx, {
            {op = 'get', key = 'w', win_token = true},
            {op = 'get', key = 'w', win_token = true},
        })
        out.win = {
            yields = fiber.info({backtrace = false})[fiber.id()].csw - csw0,
            won = wins[1].won,
            already_won = wins[2].already_won,
            other = space:get('w').token,
        }
        box.ctl.wal_sync()
        out.win.token = space:get('w').token
        return out
    end)
    t.assert_equals(results.mvcc, true)
    t.assert_equals(results.win, {
        yields = 0,
        won = true,
        already_won = true,
        other = false,
        token = true,
    })
    for _, r in ipairs(results) do
        t.assert_equals(r.replies, {
            'STORED',
            'v1',
            'STORED',
            'v1+x',
            'v1+x',
            'STORED',
        }, r.what
        )
        t.assert_equals(r.final, 'v1+x+y', r.what)
        -- Before its WAL write the change is invisible to the others with
        -- 'submit' and 'none'; with 'complete' it is written already.
        t.assert_equals(
            r.other,
            r.mode == 'complete' and 'v1+x' or 'old',
            r.what
        )
    end
end

-- Whether this process can create a file in a read-only directory (root).
local function ignores_dir_modes()
    local dir = fio.tempdir()
    fio.chmod(dir, tonumber('555', 8))
    local path = fio.pathjoin(dir, 'probe')
    local fh = fio.open(path, {'O_CREAT', 'O_WRONLY'}, tonumber('644', 8))
    if fh ~= nil then
        fh:close()
        fio.unlink(path)
    end
    fio.chmod(dir, tonumber('755', 8))
    fio.rmtree(dir)
    return fh ~= nil
end

-- A WAL write that fails after the reply: Tarantool rolls the transactions
-- back, and the client was told STORED. The child writes a new xlog file
-- for every batch (wal_max_size = 2), so a read-only WAL directory fails
-- the next write.
local w = t.group('commit_wait_wal_failure')

w.before_all(function(cg)
    cg.skip = ignores_dir_modes()
    if cg.skip then
        return
    end
    cg.server = server:new({
        alias = 'mcc_cw_wal',
        env = env.lua_env(),
        box_cfg = {wal_max_size = 2},
    })
    cg.server:start()
end)

w.after_all(function(cg)
    if cg.server ~= nil then
        cg.server:drop()
    end
end)

w.test_rollback_after_reply = function(cg)
    t.skip_if(cg.skip, 'running as root: a read-only directory is writable')
    local results = cg.server:exec(function()
        local fiber = require('fiber')
        local fio = require('fio')
        local mcc = require('memcached')
        local engine = require('memcached.engine')
        local schema = require('memcached.schema')
        local function set(key, value)
            return {
                op = 'store',
                mode = 'set',
                key = key,
                value = value,
                flags = 0,
                exptime = 0,
                fast = 'set',
            }
        end
        local function get(key)
            return {op = 'get', key = key, fast = 'get'}
        end
        -- The value of a hit (C: a VALUE block), otherwise the status.
        local function reply(resp)
            if type(resp) == 'string' then
                return (resp:match('^VALUE %S+ %d+ %d+\r\n(.*)\r\n$'))
            end
            return resp.status == 'HIT' and resp.item.value or resp.status
        end
        local dir = fio.abspath(box.cfg.wal_dir)
        local space = schema.ensure('mcc_cw', 'memory')
        local ctx = {space_name = 'mcc_cw', commit_wait = 'submit'}
        local out = {}
        for _, on in ipairs({true, false}) do
            mcc.datapath(on)
            space:truncate()
            engine.exec({space_name = 'mcc_cw'}, {set('a', 'old')})
            local rollbacks = box.stat().ROLLBACK.total
            fio.chmod(dir, tonumber('555', 8))
            local acked = engine.exec(ctx, {
                set('a', 'new'),
                get('a'),
                set('b', 'two'),
            })
            -- The three transactions (the get recorded its access) are
            -- rolled back once the WAL write has failed.
            local deadline = fiber.clock() + 10
            while box.stat().ROLLBACK.total < rollbacks + 3
                and fiber.clock() < deadline do
                fiber.sleep(0.01)
            end
            local after = engine.exec(ctx, {get('a'), get('b')})
            local complete = engine.exec(
                {space_name = 'mcc_cw'},
                {set('c', 'x')}
            )
            fio.chmod(dir, tonumber('755', 8))
            local again = engine.exec(ctx, {set('d', 'y')})
            box.ctl.wal_sync()
            table.insert(out, {
                what = mcc.datapath().enabled and 'C' or 'Lua',
                acked = {reply(acked[1]), reply(acked[2]), reply(acked[3])},
                rollbacks = box.stat().ROLLBACK.total - rollbacks,
                after = {reply(after[1]), reply(after[2])},
                complete = complete[1],
                again = reply(again[1]),
                c = space:get('c') ~= nil,
                d = space:get('d') and space:get('d').value,
            })
        end
        return out
    end)
    for _, r in ipairs(results) do
        t.assert_equals(r.acked, {'STORED', 'new', 'STORED'}, r.what)
        t.assert_ge(r.rollbacks, 3, r.what)
        -- The old state is back.
        t.assert_equals(r.after, {'old', 'MISS'}, r.what)
        -- A write that waits for the WAL learns of the failure.
        t.assert_equals(r.complete, {
            status = 'SERVER_ERROR',
            message = 'Failed to write to disk',
        }, r.what
        )
        t.assert_equals(r.c, false, r.what)
        t.assert_equals(r.again, 'STORED', r.what)
        t.assert_equals(r.d, 'y', r.what)
    end
end
