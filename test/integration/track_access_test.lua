-- The instance option track_access: whether gets record the last access
-- and the fetched flag (meta h/l, me). Default true; false: gets never
-- write the item.

local fiber = require('fiber')
local t = require('luatest')

local env = require('test.helpers.env')
local client = require('test.helpers.client')

local g = t.group('track_access')

local SPACE = 'mcc_track_access_test'
local NAMES = {'track', 'notrack'}

local engine
local schema
local mcc

g.before_all(function(cg)
    env.box_cfg()
    engine = require('memcached.engine')
    schema = require('memcached.schema')
    mcc = require('memcached')
    cg.port = env.free_port()
end)

g.before_each(function()
    schema.ensure(SPACE, 'memory')
    box.space[SPACE]:truncate()
    box.space._mcc_meta:delete(schema.flush_key(SPACE))
end)

g.after_each(function()
    for _, name in ipairs(NAMES) do
        if mcc.get(name) ~= nil then
            mcc.destroy(name)
        end
    end
    local spaces = {}
    for _, name in ipairs(NAMES) do
        table.insert(spaces, '__mcc_' .. name)
    end
    env.drop_spaces(spaces)
end)

g.after_all(function()
    env.drop_spaces({SPACE})
    box.space._mcc_meta:delete(schema.flush_key(SPACE))
end)

local TRACK = {space_name = SPACE}
local NO_TRACK = {space_name = SPACE, track_access = false}

local function one(ctx, req)
    return engine.exec(ctx, {req})[1]
end

local function set(ctx, key, value)
    return one(ctx, {
        op = 'store',
        mode = 'set',
        key = key,
        value = value,
        flags = 0,
        exptime = 0,
    })
end

-- Count the writes to the data space while fn runs.
local function writes(fn)
    local n = 0
    local function count()
        n = n + 1
    end
    local space = box.space[SPACE]
    space:on_replace(count)
    local ok, err = pcall(fn)
    space:on_replace(nil, count)
    t.assert(ok, err)
    return n
end

g.test_engine_default_tracks = function()
    set(TRACK, 'k', 'v')
    -- The first get after a store writes the item once; the next ones
    -- (within LA_UPDATE_INTERVAL) don't.
    t.assert_equals(
        writes(function()
            local resp = one(TRACK, {op = 'get', key = 'k'})
            t.assert_equals(resp.status, 'HIT')
            t.assert_equals(resp.item.fetched, false)
        end),
        1
    )
    t.assert_equals(box.space[SPACE]:get('k').fetched, true)
    t.assert_equals(
        writes(function()
            t.assert_equals(
                one(TRACK, {op = 'get', key = 'k'}).item.fetched,
                true
            )
        end),
        0
    )
end

g.test_engine_no_track = function()
    set(NO_TRACK, 'k', 'v')
    local stored = box.space[SPACE]:get('k')
    fiber.sleep(0.01)
    -- No get writes, a classic one or a meta one with h/l semantics.
    t.assert_equals(
        writes(function()
            for _ = 1, 3 do
                local resp = one(NO_TRACK, {op = 'get', key = 'k'})
                t.assert_equals(resp.status, 'HIT')
                t.assert_equals(resp.item.fetched, false)
                t.assert_equals(resp.item.last_access, stored.last_access)
            end
            local resp = one(NO_TRACK, {
                op = 'get',
                key = 'k',
                win_token = true,
                bump_la = true,
            })
            t.assert_equals(resp.item.fetched, false)
            t.assert_equals(
                engine.exec(NO_TRACK, {
                    {op = 'get', key = 'k'},
                    {op = 'get', key = 'nope'},
                })[2],
                {status = 'MISS'}
            )
        end),
        0
    )
    local tuple = box.space[SPACE]:get('k')
    t.assert_equals(tuple.fetched, false)
    t.assert_equals(tuple.last_access, stored.last_access)
    t.assert_equals(tuple.cas, stored.cas)

    -- A touch still sets the TTL, and only that.
    t.assert_equals(
        writes(function()
            t.assert_equals(
                one(NO_TRACK, {
                    op = 'get',
                    key = 'k',
                    touch_ttl = 100,
                }).status,
                'HIT'
            )
        end),
        1
    )
    tuple = box.space[SPACE]:get('k')
    t.assert_almost_equals(tuple.exptime, fiber.time() + 100, 1)
    t.assert_equals(tuple.fetched, false)
    t.assert_equals(tuple.last_access, stored.last_access)

    -- A vivified stub is stored, not fetched.
    local resp = one(NO_TRACK, {
        op = 'get',
        key = 'viv',
        vivify_ttl = 100,
        win_token = true,
    })
    t.assert_equals(resp.won, true)
    t.assert_equals(box.space[SPACE]:get('viv').fetched, false)

    -- A dead item is still deleted on access (the sweeper would do it
    -- anyway): the only write a get makes with track_access = false.
    box.space[SPACE]:replace({
        'dead',
        'v',
        0,
        fiber.time() - 10,
        1,
        fiber.time() - 20,
        fiber.time() - 20,
        false,
        false,
        0,
    })
    t.assert_equals(
        writes(function()
            t.assert_equals(
                one(NO_TRACK, {op = 'get', key = 'dead'}),
                {status = 'MISS'}
            )
        end),
        1
    )
    t.assert_equals(box.space[SPACE]:get('dead'), nil)

    -- The same items are tracked again by a context that tracks.
    t.assert_equals(one(TRACK, {op = 'get', key = 'k'}).item.fetched, false)
    t.assert_equals(box.space[SPACE]:get('k').fetched, true)
end

-- A plain memtx get writes in autocommit mode (no box.atomic): a failing
-- write still doesn't fail the read.
g.test_engine_failed_bump = function()
    set(TRACK, 'k', 'v')
    local space = box.space[SPACE]
    local function fail()
        error('no writes', 0)
    end
    space:before_replace(fail)
    local ok, resp = pcall(one, TRACK, {op = 'get', key = 'k'})
    space:before_replace(nil, fail)
    t.assert(ok, resp)
    t.assert_equals(resp.status, 'HIT')
    t.assert_equals(space:get('k').fetched, false)
end

local function uri(port)
    return '127.0.0.1:' .. port
end

-- The h flag of "mg <key> h".
local function mg_h(c, key)
    local line = c:call(('mg %s h\r\n'):format(key))
    return line:match(' h(%d)')
end

g.test_instance = function(cg)
    t.assert_equals(mcc.DEFAULTS.track_access, true)
    t.assert_error_msg_contains(
        "Option 'track_access' must be a boolean, got string",
        mcc.create,
        'notrack',
        {listen = uri(cg.port), track_access = 'no'}
    )
    -- With vshard the gets run on the storages: it is set there.
    t.assert_error_msg_contains(
        "Option 'track_access' = false is not supported by the 'vshard' "
            .. "backend: set it on the storages",
        mcc.check,
        'notrack',
        {listen = uri(cg.port), backend = 'vshard', track_access = false}
    )

    local inst = mcc.create('notrack', {
        listen = uri(cg.port),
        track_access = false,
    })
    t.assert_equals(inst.opts.track_access, false)
    local space = box.space.__mcc_notrack
    local c = client.must_connect(cg.port)
    t.assert_equals(c:set('a', 'x'), 'STORED\r\n')
    t.assert_equals(c:get('a', 'get'), 'x')
    t.assert_equals(c:get('a', 'gets'), 'x')
    t.assert_equals(mg_h(c, 'a'), '0')
    t.assert_equals(mg_h(c, 'a'), '0')
    t.assert_str_contains(c:call('me a\r\n'), ' fetch=no ')
    t.assert_equals(space:get('a').fetched, false)

    -- Switched on at runtime: the connection stays, the next get is the
    -- first fetch.
    inst:cfg({track_access = true})
    t.assert_equals(mg_h(c, 'a'), '0')
    t.assert_equals(mg_h(c, 'a'), '1')
    t.assert_equals(space:get('a').fetched, true)
    t.assert_str_contains(c:call('me a\r\n'), ' fetch=yes ')

    -- And off again.
    inst:cfg({track_access = false})
    t.assert_equals(c:set('a', 'y'), 'STORED\r\n')
    t.assert_equals(c:get('a', 'get'), 'y')
    t.assert_equals(mg_h(c, 'a'), '0')
    t.assert_equals(space:get('a').fetched, false)
    c:close()
end

g.test_instance_default = function(cg)
    mcc.create('track', {listen = uri(cg.port)})
    local c = client.must_connect(cg.port)
    t.assert_equals(c:set('a', 'x'), 'STORED\r\n')
    t.assert_equals(c:get('a', 'get'), 'x')
    t.assert_equals(mg_h(c, 'a'), '1')
    t.assert_equals(box.space.__mcc_track:get('a').fetched, true)
    c:close()
end
