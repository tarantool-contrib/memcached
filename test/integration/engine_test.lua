-- engine.exec() directly: the semantics the protocols are built on,
-- including the meta-only fields.

local fiber = require('fiber')
local t = require('luatest')
local server = require('luatest.server')

local env = require('test.helpers.env')

local g = t.group('engine')

local SPACE = 'mcc_engine_test'
local CTX = {space_name = SPACE}

local engine
local schema
local state

g.before_all(function()
    env.box_cfg()
    engine = require('memcached.engine')
    schema = require('memcached.schema')
    state = require('memcached.state')
end)

g.before_each(function()
    schema.ensure(SPACE, 'memory')
    box.space[SPACE]:truncate()
    box.space._mcc_meta:delete(schema.flush_key(SPACE))
    state.tokens[SPACE] = nil
end)

g.after_all(function()
    env.drop_spaces({SPACE})
    box.space._mcc_meta:delete(schema.flush_key(SPACE))
end)

local function exec(...)
    return engine.exec(CTX, {...})
end

local function one(req)
    return engine.exec(CTX, {req})[1]
end

local function set(key, value, extra)
    local req = {
        op = 'store',
        mode = 'set',
        key = key,
        value = value,
        flags = 0,
        exptime = 0,
    }
    for k, v in pairs(extra or {}) do
        req[k] = v
    end
    return one(req)
end

local function get(key, extra)
    local req = {op = 'get', key = key}
    for k, v in pairs(extra or {}) do
        req[k] = v
    end
    return one(req)
end

-- The fields of format v2, before the token field.
local function v2_format()
    local format = {}
    for i = 1, schema.BUCKET_ID do
        format[i] = schema.FORMAT[i]
    end
    return format
end

g.test_schema = function()
    local space = box.space[SPACE]
    t.assert_equals(space.engine, 'memtx')
    t.assert_equals(space.index.primary.type, 'HASH')
    -- expire.mode = 'scan', the default of memory: no exptime index.
    t.assert_equals(space.index.exptime, nil)
    schema.ensure(SPACE, 'memory', {expire = 'index'})
    t.assert_equals(space.index.exptime.type, 'TREE')
    t.assert_equals(space.index.exptime.unique, false)
    t.assert_equals(space.index.exptime.parts[1].fieldno, schema.EXPTIME)
    schema.ensure(SPACE, 'memory', {expire = 'scan'})
    t.assert_equals(space.index.exptime, nil)
    t.assert_equals(space.index.bucket_id, nil)
    local names = {}
    for _, f in ipairs(space:format()) do
        table.insert(names, f.name)
    end
    t.assert_equals(names, {
        'key',
        'value',
        'flags',
        'exptime',
        'cas',
        'created',
        'last_access',
        'fetched',
        'stale',
        'bucket_id',
        'token',
    })
    t.assert_equals(space:format()[schema.TOKEN].is_nullable, true)
    t.assert_not_equals(box.space._mcc_meta, nil)
    -- vshard storage mode adds the bucket_id index.
    schema.ensure(SPACE, 'memory', {vshard = true})
    t.assert_not_equals(space.index.bucket_id, nil)
    space.index.bucket_id:drop()
    -- A space with another format is refused.
    box.schema.space.create('mcc_engine_bad', {
        format = {
            {name = 'key', type = 'string'},
            {name = 'expire', type = 'number'},
        },
    })
    t.assert_error_msg_contains(
        'incompatible format',
        schema.ensure,
        'mcc_engine_bad',
        'memory'
    )
    box.space.mcc_engine_bad:drop()
    -- So is one with another field after the v2 ones.
    local format = v2_format()
    format[schema.TOKEN] = {name = 'other', type = 'any'}
    box.schema.space.create('mcc_engine_bad', {format = format})
    t.assert_error_msg_contains(
        "field 11 must be 'token'",
        schema.ensure,
        'mcc_engine_bad',
        'memory'
    )
    box.space.mcc_engine_bad:drop()
end

-- A space of format v2 (10 fields, as made by the code before the token
-- field) is upgraded in place: its tuples keep 10 fields and are read as
-- not won; the next writes add the field.
g.test_format_v2_upgrade = function()
    for _, storage in ipairs({'memory', 'disk'}) do
        local name = 'mcc_engine_v2_' .. storage
        local space = box.schema.space.create(name, {
            engine = schema.engine(storage),
            format = v2_format(),
        })
        space:create_index('primary', {
            type = storage == 'memory' and 'HASH' or 'TREE',
            parts = {{schema.KEY, 'string'}},
        })
        space:create_index('exptime', {
            unique = false,
            parts = {{schema.EXPTIME, 'number'}},
        })
        local now = fiber.time()
        local cas = 1000
        for _, key in ipairs({'old', 'stale', 'r', 'n'}) do
            cas = cas + 1
            space:replace({
                key,
                key == 'n' and '5' or 'v',
                0,
                key == 'r' and now + 10 or 0,
                cas,
                now,
                now,
                false,
                key == 'stale',
                0,
            })
        end
        t.assert_equals(#space:get('old'), 10)
        t.assert_equals(schema.ensure(name, storage).id, space.id)
        t.assert_equals(space:format()[schema.TOKEN].name, 'token', storage)
        -- Old tuples are as they were, and read as not won.
        t.assert_equals(#space:get('old'), 10, storage)
        local ctx = {space_name = name}
        local function run(req)
            return engine.exec(ctx, {req})[1]
        end
        local resp = run({op = 'get', key = 'stale', win_token = true})
        t.assert_equals({resp.won, resp.already_won}, {true, nil}, storage)
        t.assert_equals(space:get('stale')[schema.TOKEN], true, storage)
        resp = run({op = 'get', key = 'stale', win_token = true})
        t.assert_equals({resp.won, resp.already_won}, {nil, true}, storage)
        resp = run({
            op = 'get',
            key = 'r',
            win_token = true,
            recache_ttl = 30,
        })
        t.assert_equals(resp.won, true, storage)
        t.assert_equals(space:get('r')[schema.TOKEN], true, storage)
        -- The writes of a new version add the field, false.
        run({op = 'arith', key = 'n', incr = true, delta = 1ULL})
        t.assert_equals(space:get('n')[schema.TOKEN], false, storage)
        run({
            op = 'store',
            mode = 'set',
            key = 'old',
            value = 'x',
            exptime = 0,
        })
        t.assert_equals(space:get('old')[schema.TOKEN], false, storage)
        -- Again: nothing to do.
        schema.ensure(name, storage)
        t.assert_equals(#space:format(), #schema.FORMAT, storage)
        space:drop()
    end
end

g.test_tuple = function()
    local resp = set('k', 'v', {flags = 7, exptime = 100})
    t.assert_equals(resp.status, 'STORED')
    t.assert_equals(type(resp.cas), 'cdata')
    t.assert_equals(resp.size, 1)
    local tuple = box.space[SPACE]:get('k')
    t.assert_equals(tuple.value, 'v')
    t.assert_equals(tuple.flags, 7)
    t.assert_almost_equals(tuple.exptime, fiber.time() + 100, 1)
    t.assert_equals(engine.u64(tuple.cas), resp.cas)
    t.assert_almost_equals(tuple.created, fiber.time(), 1)
    t.assert_almost_equals(tuple.last_access, fiber.time(), 1)
    t.assert_equals(tuple.fetched, false)
    t.assert_equals(tuple.stale, false)
    t.assert_equals(tuple.bucket_id, 0)
    -- bucket_id comes from the context.
    engine.exec(
        {space_name = SPACE, bucket_id = 42},
        {{op = 'store', key = 'b', value = '', exptime = 0}}
    )
    t.assert_equals(box.space[SPACE]:get('b').bucket_id, 42)
end

g.test_get_item = function()
    local stored = set('k', 'v', {flags = 3})
    local resp = get('k')
    t.assert_equals(resp.status, 'HIT')
    local item = resp.item
    t.assert_equals(item.key, 'k')
    t.assert_equals(item.value, 'v')
    t.assert_equals(item.flags, 3)
    t.assert_equals(item.exptime, 0)
    t.assert_equals(item.cas, stored.cas)
    t.assert_equals(item.stale, false)
    -- fetched and last_access are as before the access.
    t.assert_equals(item.fetched, false)
    t.assert_equals(get('k').item.fetched, true)
    -- no_lru_bump / bump_la = false don't mark the item.
    set('k', 'v')
    t.assert_equals(get('k', {no_lru_bump = true}).item.fetched, false)
    t.assert_equals(get('k', {bump_la = false}).item.fetched, false)
    t.assert_equals(get('k').item.fetched, false)
    t.assert_equals(get('nope'), {status = 'MISS'})
end

g.test_touch = function()
    local stored = set('k', 'v', {exptime = 100})
    local resp = get('k', {touch_ttl = 1000})
    t.assert_almost_equals(resp.item.exptime, fiber.time() + 1000, 1)
    t.assert_almost_equals(
        box.space[SPACE]:get('k').exptime,
        fiber.time() + 1000,
        1
    )
    -- touch doesn't change the CAS.
    t.assert_equals(resp.item.cas, stored.cas)
    get('k', {touch_ttl = 0})
    t.assert_equals(box.space[SPACE]:get('k').exptime, 0)
    -- An absolute time.
    local when = math.floor(fiber.time()) + 5000
    get('k', {touch_ttl = when})
    t.assert_equals(box.space[SPACE]:get('k').exptime, when)
    -- Negative: expires at once.
    t.assert_equals(get('k', {touch_ttl = -1}).status, 'HIT')
    t.assert_equals(get('k').status, 'MISS')
    t.assert_equals(box.space[SPACE]:get('k'), nil)
end

g.test_cas_rules = function()
    local a = set('k', '1')
    local b = set('k', '2')
    t.assert_gt(b.cas, a.cas)
    -- The counter is per process, CAS values never repeat.
    local c = set('other', '1')
    t.assert_gt(c.cas, b.cas)
    -- set with a CAS: cas semantics.
    t.assert_equals(set('k', '3', {cas = a.cas}).status, 'EXISTS')
    t.assert_equals(set('k', '3', {cas = 0ULL}).status, 'EXISTS')
    local d = set('k', '3', {cas = b.cas})
    t.assert_equals(d.status, 'STORED')
    t.assert_equals(set('none', '3', {cas = b.cas}).status, 'NOT_FOUND')
    -- replace with a CAS is the same.
    t.assert_equals(
        set('k', '4', {mode = 'replace', cas = b.cas}).status,
        'EXISTS'
    )
    t.assert_equals(
        set('k', '4', {mode = 'replace', cas = d.cas}).status,
        'STORED'
    )
    t.assert_equals(
        set('none', '4', {mode = 'replace', cas = d.cas}).status,
        'NOT_FOUND'
    )
    -- add ignores it.
    t.assert_equals(
        set('k', '5', {mode = 'add', cas = 1ULL}).status,
        'NOT_STORED'
    )
    -- append and prepend check it.
    local cur = get('k').item.cas
    t.assert_equals(
        set('k', 'x', {mode = 'append', cas = cur + 1}).status,
        'EXISTS'
    )
    t.assert_equals(
        set('k', 'x', {mode = 'append', cas = cur}).status,
        'STORED'
    )
    t.assert_equals(get('k').item.value, '4x')
    -- The new CAS is bigger than the old one even if the old one is ahead
    -- of the counter (say, it came from another node).
    local big = 18446744073709551000ULL
    box.space[SPACE]:update('k', {{'=', 'cas', big}})
    t.assert_equals(set('k', 'y').cas, big + 1)
    -- A client's CAS (meta E).
    t.assert_equals(set('k', 'z', {new_cas = 777ULL}).cas, 777ULL)
    t.assert_equals(get('k').item.cas, 777ULL)
end

g.test_invalidate = function()
    -- An older CAS with invalidate: stored, but stale; TTL kept.
    local a = set('k', '1', {exptime = 100})
    local b = set('k', '2', {exptime = 200})
    local resp = set('k', '3', {
        cas = a.cas,
        invalidate = true,
        exptime = 5,
    })
    t.assert_equals(resp.status, 'STORED')
    local hit = get('k')
    t.assert_equals(hit.item.value, '3')
    t.assert_equals(hit.item.stale, true)
    t.assert_equals(hit.stale, true)
    t.assert_almost_equals(hit.item.exptime, fiber.time() + 200, 1)
    -- A newer CAS with invalidate is EXISTS.
    t.assert_equals(
        set('k', '4', {cas = b.cas + 1000000, invalidate = true})
            .status,
        'EXISTS'
    )

    -- delete with invalidate marks stale, bumps the CAS and sets the TTL.
    local c = set('d', 'v')
    resp = one({op = 'delete', key = 'd', invalidate = true, ttl = 30})
    t.assert_equals(resp.status, 'DELETED')
    t.assert_gt(resp.cas, c.cas)
    hit = get('d', {no_lru_bump = true})
    t.assert_equals(hit.item.stale, true)
    t.assert_equals(hit.item.cas, resp.cas)
    t.assert_almost_equals(hit.item.exptime, fiber.time() + 30, 1)
    -- delete with a CAS.
    t.assert_equals(
        one({op = 'delete', key = 'd', cas = c.cas}).status,
        'EXISTS'
    )
    t.assert_equals(
        one({op = 'delete', key = 'd', cas = resp.cas}).status,
        'DELETED'
    )
    t.assert_equals(one({op = 'delete', key = 'd'}).status, 'NOT_FOUND')
    -- clear_value (meta x) keeps the item with an empty value.
    set('x', 'value', {flags = 5})
    resp = one({op = 'delete', key = 'x', clear_value = true, flags = 9})
    t.assert_equals(resp.status, 'DELETED')
    hit = get('x')
    t.assert_equals(hit.item.value, '')
    t.assert_equals(hit.item.flags, 9)
    t.assert_equals(hit.item.stale, false)
end

-- A get as meta mg makes it: it may take win tokens.
local function mg(key, extra)
    local req = {win_token = true}
    for k, v in pairs(extra or {}) do
        req[k] = v
    end
    return get(key, req)
end

g.test_win_tokens = function()
    -- Vivify on a miss: a stub item, the caller wins.
    local resp = mg('v', {vivify_ttl = 30})
    t.assert_equals(resp.status, 'HIT')
    t.assert_equals(resp.won, true)
    t.assert_equals(resp.vivified, true)
    t.assert_equals(resp.item.value, '')
    t.assert_almost_equals(resp.item.exptime, fiber.time() + 30, 1)
    -- The others see that someone has won.
    resp = mg('v', {vivify_ttl = 30})
    t.assert_equals(resp.won, nil)
    t.assert_equals(resp.already_won, true)
    -- A new value clears the token.
    set('v', 'data')
    resp = mg('v', {vivify_ttl = 30})
    t.assert_equals(resp.won, nil)
    t.assert_equals(resp.already_won, nil)

    -- Recache: the remaining TTL is below the token.
    set('r', 'data', {exptime = 10})
    t.assert_equals(mg('r', {recache_ttl = 5}).won, nil)
    resp = mg('r', {recache_ttl = 30})
    t.assert_equals(resp.won, true)
    resp = mg('r', {recache_ttl = 30})
    t.assert_equals(resp.won, nil)
    t.assert_equals(resp.already_won, true)
    -- No TTL: never wins.
    set('r', 'data')
    t.assert_equals(mg('r', {recache_ttl = 30}).won, nil)

    -- A stale item: the first fetch wins, then Z.
    set('s', 'data')
    one({op = 'delete', key = 's', invalidate = true})
    -- peek has no side effects: it doesn't take the token.
    resp = get('s', {peek = true})
    t.assert_equals(resp.status, 'HIT')
    t.assert_equals(resp.stale, true)
    t.assert_equals(resp.won, nil)
    resp = mg('s')
    t.assert_equals(resp.stale, true)
    t.assert_equals(resp.won, true)
    t.assert_equals(resp.already_won, nil)
    resp = mg('s')
    t.assert_equals(resp.won, nil)
    t.assert_equals(resp.already_won, true)
    resp = get('s', {peek = true})
    t.assert_equals(resp.already_won, true)
    -- delete with invalidate again: the token is cleared.
    one({op = 'delete', key = 's', invalidate = true})
    t.assert_equals(mg('s').won, true)
end

-- The win token is a field of the item: true once the token is out for
-- its current version, false for every new version (but those memcached
-- keeps ITEM_TOKEN_SENT for).
g.test_token_field = function()
    local space = box.space[SPACE]
    local function token(key)
        return space:get(key)[schema.TOKEN]
    end
    set('s', 'v', {exptime = 100})
    t.assert_equals(token('s'), false)
    one({op = 'delete', key = 's', invalidate = true})
    t.assert_equals(token('s'), false)
    t.assert_equals(mg('s').won, true)
    t.assert_equals(token('s'), true)
    -- A touch and the last access keep it.
    mg('s', {touch_ttl = 200})
    t.assert_equals(token('s'), true)
    t.assert_equals(mg('s').already_won, true)
    -- A stale store (ms I with an older CAS) keeps it.
    local cas = get('s').item.cas
    t.assert_equals(
        set('s', 'w', {cas = cas - 1, invalidate = true}).status,
        'STORED'
    )
    t.assert_equals(token('s'), true)
    t.assert_equals(mg('s').already_won, true)
    -- md I gives a new version.
    one({op = 'delete', key = 's', invalidate = true})
    t.assert_equals(token('s'), false)
    t.assert_equals(mg('s').won, true)
    -- So do md x (with the same CAS), an arith that writes a new item (a
    -- longer number) and a store; an arith in place keeps it.
    one({op = 'delete', key = 's', clear_value = true})
    t.assert_equals(token('s'), false)
    one({op = 'delete', key = 's', invalidate = true})
    mg('s')
    t.assert_equals(token('s'), true)
    space:update('s', {{'=', schema.VALUE, '1'}})
    one({op = 'arith', key = 's', incr = true, delta = 1ULL})
    t.assert_equals(token('s'), true, 'in place')
    space:update('s', {{'=', schema.VALUE, '9'}})
    one({op = 'arith', key = 's', incr = true, delta = 1ULL})
    t.assert_equals(token('s'), false, 'a new item')
    one({op = 'delete', key = 's', invalidate = true})
    mg('s')
    t.assert_equals(token('s'), true)
    set('s', 'v')
    t.assert_equals(token('s'), false)
    -- A vivified stub has it.
    mg('v', {vivify_ttl = 30})
    t.assert_equals(token('v'), true)
    -- A failed request rolls it back with the rest: a touch that fails
    -- after the win.
    set('f', 'v', {exptime = 100})
    one({op = 'delete', key = 'f', invalidate = true})
    local function fail(old, new)
        if old ~= nil and new ~= nil
            and old[schema.KEY] == 'f' and new[schema.EXPTIME] == 0 then
            error('no touch')
        end
    end
    space:before_replace(fail)
    local resp = mg('f', {touch_ttl = 0})
    space:before_replace(nil, fail)
    t.assert_equals(resp.status, 'SERVER_ERROR')
    t.assert_equals(token('f'), false)
    t.assert_equals(mg('f').won, true)
end

-- A read-only instance can't write the token field: its wins are kept in
-- memory, for the version they were taken for, and still count once the
-- instance is writable.
g.test_read_only_tokens = function()
    local space = box.space[SPACE]
    set('s', 'v')
    one({op = 'delete', key = 's', invalidate = true})
    set('gone', 'v')
    one({op = 'delete', key = 'gone', invalidate = true})
    local function read_only(on)
        box.cfg {read_only = on}
        -- The box.status watcher of the engine.
        fiber.sleep(0.01)
    end
    read_only(true)
    local ok, err = pcall(function()
        local resp = mg('s')
        t.assert_equals({resp.won, resp.already_won}, {true, nil})
        t.assert_equals(space:get('s')[schema.TOKEN], false)
        t.assert_not_equals(state.tokens[SPACE].s, nil)
        resp = mg('s')
        t.assert_equals({resp.won, resp.already_won}, {nil, true})
        t.assert_equals(get('s', {peek = true}).already_won, true)
        t.assert_equals(mg('gone').won, true)
        -- A win that needs a write fails as it did.
        t.assert_equals(
            mg('nope', {vivify_ttl = 30}).status,
            'SERVER_ERROR'
        )
    end)
    read_only(false)
    t.assert(ok, err)
    -- Writable again: the tokens taken while read-only still count.
    t.assert_equals(mg('s').already_won, true)
    t.assert_equals(space:get('s')[schema.TOKEN], false)
    -- A new version with the same CAS (md x, then md I E<the same CAS>)
    -- is not won: the entry is dropped when seen.
    local cas = get('s').item.cas
    one({op = 'delete', key = 's', clear_value = true})
    one({op = 'delete', key = 's', invalidate = true, new_cas = cas})
    t.assert_equals(get('s').item.cas, cas)
    t.assert_equals(mg('s').won, true)
    t.assert_equals(state.tokens[SPACE].s, nil)
    t.assert_equals(space:get('s')[schema.TOKEN], true)
    -- An entry of an item that is gone is dropped on the next lookup.
    one({op = 'delete', key = 'gone'})
    t.assert_not_equals(state.tokens[SPACE].gone, nil)
    t.assert_equals(get('gone').status, 'MISS')
    t.assert_equals(state.tokens[SPACE].gone, nil)
end

-- A token kept in memory by the code before the token field (a CAS) still
-- counts after a hot reload, for that CAS.
g.test_reloaded_memory_token = function()
    set('s', 'v')
    one({op = 'delete', key = 's', invalidate = true})
    state.tokens[SPACE] = {s = get('s').item.cas}
    t.assert_equals(mg('s').already_won, true)
    one({op = 'delete', key = 's', invalidate = true})
    t.assert_equals(mg('s').won, true)
    t.assert_equals(state.tokens[SPACE].s, nil)
end

-- A win needs its write: a get whose token write fails is answered without
-- the win (and without its other optional writes).
g.test_failed_win = function()
    local space = box.space[SPACE]
    set('s', 'v')
    one({op = 'delete', key = 's', invalidate = true})
    local function fail(old, new)
        if old ~= nil and new ~= nil and new[schema.TOKEN] == true then
            error('no token')
        end
    end
    space:before_replace(fail)
    local resp = mg('s')
    space:before_replace(nil, fail)
    t.assert_equals(resp.status, 'HIT')
    t.assert_equals(resp.stale, true)
    t.assert_equals({resp.won, resp.already_won}, {nil, nil})
    t.assert_equals(space:get('s')[schema.TOKEN], false)
    t.assert_equals((state.tokens[SPACE] or {}).s, nil)
    -- The next one wins.
    t.assert_equals(mg('s').won, true)
end

-- Regression (compat classic_read_takes_token): a classic read (get, gets,
-- gat, gats, touch: no win_token) of a stale item doesn't take its win
-- token, so the next mg still wins. memcached's process_get_cmd never sets
-- ITEM_TOKEN_SENT.
g.test_classic_read_keeps_token = function()
    set('s', 'data', {exptime = 100})
    one({op = 'delete', key = 's', invalidate = true})
    for _, extra in ipairs({
        {},
        {touch_ttl = 100},
        {touch_ttl = 100, touch_only = true},
    }) do
        local resp = get('s', extra)
        t.assert_equals(resp.status, 'HIT')
        t.assert_equals(resp.won, nil)
        t.assert_equals(resp.already_won, nil)
    end
    -- Not a recache either.
    set('r', 'data', {exptime = 10})
    t.assert_equals(get('r', {recache_ttl = 30}).won, nil)
    t.assert_equals(mg('r', {recache_ttl = 30}).won, true)
    local resp = mg('s')
    t.assert_equals(resp.stale, true)
    t.assert_equals(resp.won, true)
    -- After the mg, classic reads still don't report or take anything.
    t.assert_equals(get('s').already_won, nil)
    t.assert_equals(mg('s').already_won, true)
end

g.test_peek = function()
    set('p', 'data', {exptime = 100})
    local before = box.space[SPACE]:get('p')
    local resp = get('p', {
        peek = true,
        touch_ttl = 5,
        vivify_ttl = 5,
        recache_ttl = 1000,
    })
    t.assert_equals(resp.status, 'HIT')
    t.assert_equals(resp.won, nil)
    t.assert_equals(box.space[SPACE]:get('p'), before)
    t.assert_equals(
        get('none', {peek = true, vivify_ttl = 5}),
        {status = 'MISS'}
    )
    t.assert_equals(box.space[SPACE]:get('none'), nil)
    -- A dead item is not deleted by a peek.
    set('dead', 'x', {exptime = -1})
    box.space[SPACE]:insert({'dead2', 'x', 0, 1, 1, 1, 1, false, false, 0})
    t.assert_equals(get('dead2', {peek = true}), {status = 'MISS'})
    t.assert_not_equals(box.space[SPACE]:get('dead2'), nil)
    t.assert_equals(get('dead2'), {status = 'MISS'})
    t.assert_equals(box.space[SPACE]:get('dead2'), nil)
end

g.test_append_vivify = function()
    t.assert_equals(set('a', 'x', {mode = 'append'}).status, 'NOT_STORED')
    local resp = set('a', 'x', {mode = 'append', vivify_ttl = 50, flags = 3})
    t.assert_equals(resp.status, 'STORED')
    local item = get('a').item
    t.assert_equals(item.value, 'x')
    t.assert_almost_equals(item.exptime, fiber.time() + 50, 1)
    resp = set('a', 'yz', {mode = 'prepend', vivify_ttl = 1})
    t.assert_equals(resp.size, 3)
    t.assert_equals(get('a').item.value, 'yzx')
end

g.test_arith = function()
    t.assert_equals(
        one({op = 'arith', key = 'n', incr = true, delta = 1ULL}),
        {status = 'NOT_FOUND'}
    )
    -- Vivify with an initial value: the delta isn't applied.
    local resp = one({
        op = 'arith',
        key = 'n',
        incr = true,
        delta = 5ULL,
        initial = 10ULL,
        vivify_ttl = 100,
    })
    t.assert_equals(resp.status, 'STORED')
    t.assert_equals(resp.value, 10ULL)
    t.assert_almost_equals(resp.exptime, fiber.time() + 100, 1)
    t.assert_equals(get('n').item.value, '10')
    resp = one({op = 'arith', key = 'n', incr = false, delta = 3ULL})
    t.assert_equals(resp.value, 7ULL)
    t.assert_equals(type(resp.value), 'cdata')
    t.assert_almost_equals(resp.exptime, fiber.time() + 100, 1)
    -- With a CAS and a new TTL.
    t.assert_equals(
        one({
            op = 'arith',
            key = 'n',
            delta = 1ULL,
            cas = resp.cas + 1,
        }).status,
        'EXISTS'
    )
    resp = one({
        op = 'arith',
        key = 'n',
        delta = 1ULL,
        cas = resp.cas,
        ttl = 0,
    })
    t.assert_equals(resp.status, 'STORED')
    t.assert_equals(resp.value, 8ULL)
    t.assert_equals(resp.exptime, 0)
    -- Defaults: incr by 1.
    t.assert_equals(one({op = 'arith', key = 'n'}).value, 9ULL)
    -- Wrap around and stop at 0.
    set('n', '18446744073709551615')
    t.assert_equals(
        one({
            op = 'arith',
            key = 'n',
            incr = true,
            delta = 2ULL,
        }).value,
        1ULL
    )
    t.assert_equals(
        one({
            op = 'arith',
            key = 'n',
            incr = false,
            delta = 18446744073709551615ULL,
        }).value,
        0ULL
    )
    -- Surrounding spaces are fine, as in memcached.
    set('n', ' 12 ')
    t.assert_equals(
        one({op = 'arith', key = 'n', delta = 1ULL}).value,
        13ULL
    )
    for _, value in ipairs({
        '',
        'abc',
        '-1',
        '1.5',
        '0x10',
        '18446744073709551616',
    }) do
        set('n', value)
        t.assert_equals(one({op = 'arith', key = 'n', delta = 1ULL}), {
            status = 'CLIENT_ERROR',
            message = 'cannot increment or decrement non-numeric value',
        }, value
        )
    end
end

g.test_flush = function()
    set('a', '1')
    local resps = exec(
        {op = 'store', key = 'b', value = '2', exptime = 0},
        {op = 'flush', delay = 0},
        {op = 'store', key = 'c', value = '3', exptime = 0},
        {op = 'get', key = 'a'},
        {op = 'get', key = 'b'},
        {op = 'get', key = 'c'}
    )
    t.assert_equals(resps[2], {status = 'OK'})
    t.assert_equals(resps[4].status, 'MISS')
    t.assert_equals(resps[5].status, 'MISS')
    t.assert_equals(resps[6].status, 'HIT')
    -- Persisted in the meta space.
    local flush = box.space._mcc_meta:get(schema.flush_key(SPACE))[2]
    t.assert_almost_equals(flush, fiber.time(), 1)
    t.assert_equals(schema.flush_time(SPACE), flush)
    -- A delayed flush: items stored before it, even after the command, die
    -- when it comes.
    local when = fiber.time() + 1
    t.assert_equals(
        one({op = 'flush', delay = math.ceil(when)}).status,
        'OK'
    )
    set('d', '4')
    t.assert_equals(get('d').status, 'HIT')
    t.helpers.retrying({timeout = 4}, function()
        t.assert_equals(get('d').status, 'MISS')
    end)
    t.assert_equals(get('c').status, 'MISS')
end

g.test_errors = function()
    -- A failed commit fails only its request.
    local space = box.space[SPACE]
    set('a', '1')
    local function trigger(_, new)
        if new ~= nil and new[1] == 'bad' then
            error('no bad keys', 0)
        end
    end
    space:before_replace(trigger)
    local resps = exec(
        {op = 'store', key = 'bad', value = 'x', exptime = 0},
        {op = 'store', key = 'good', value = 'x', exptime = 0},
        {op = 'bogus', key = 'a'}
    )
    space:before_replace(nil, trigger)
    t.assert_equals(resps[1], {
        status = 'SERVER_ERROR',
        message = 'no bad keys',
    })
    t.assert_equals(resps[2].status, 'STORED')
    t.assert_equals(resps[3], {
        status = 'SERVER_ERROR',
        message = "unknown operation 'bogus'",
    })
    -- Malformed requests.
    t.assert_equals(
        exec(
            {op = 'get'},
            {op = 'get', key = 1},
            {op = 'get', key = 'good'}
        )[3].status,
        'HIT'
    )
    t.assert_equals(
        one({op = 'store', value = 'x'}),
        {status = 'SERVER_ERROR', message = 'bad request: no key'}
    )
    -- A missing space.
    t.assert_equals(engine.exec(
        {space_name = 'nope'},
        {{op = 'get', key = 'a'}}
    ), {
        {
            status = 'SERVER_ERROR',
            message = "Space 'nope' does not exist",
        },
    })
    -- Read-only: reads work without their optional writes.
    box.cfg {read_only = true}
    local ok, err = pcall(function()
        local resp = get('a')
        t.assert_equals(resp.status, 'HIT')
        t.assert_equals(resp.item.fetched, false)
        t.assert_equals(set('a', '2').status, 'SERVER_ERROR')
        t.assert_equals(get('a', {touch_ttl = 10}).status, 'SERVER_ERROR')
        t.assert_equals(
            one({op = 'flush', delay = 0}).status,
            'SERVER_ERROR'
        )
    end)
    box.cfg {read_only = false}
    t.assert(ok, err)
    t.assert_equals(get('a').item.value, '1')
end

g.test_times = function()
    local now = 1000000000
    t.assert_equals(engine.abs_time(0, now), 0)
    t.assert_equals(engine.abs_time(-5, now), -1)
    t.assert_equals(engine.abs_time(10, now), now + 10)
    t.assert_equals(engine.abs_time(2592000, now), now + 2592000)
    t.assert_equals(engine.abs_time(2592001, now), 2592001)
    t.assert_equals(engine.flush_time(-1, now) >= now, true)
    t.assert_equals(engine.flush_time(10, now), now + 10)
    -- Stamps strictly increase within one event loop iteration.
    local a = engine.stamp()
    local b = engine.stamp()
    t.assert_gt(b, a)
end

--
-- Regressions found by the differential compat suite (test/compat): each
-- test names its former allowlist entry.
--

-- vivify_fetched: the mg that vivifies counts as the first fetch
-- (memcached's delayed do_item_bump), unless it has u.
g.test_vivify_fetched = function()
    local resp = mg('v', {vivify_ttl = 30})
    t.assert_equals(resp.won, true)
    t.assert_equals(resp.item.fetched, false, 'as before this access')
    t.assert_equals(box.space[SPACE]:get('v').fetched, true)
    t.assert_equals(mg('v').item.fetched, true)
    -- With u (no bump) the stub stays unfetched.
    resp = mg('u', {vivify_ttl = 30, bump_la = false, no_lru_bump = true})
    t.assert_equals(resp.won, true)
    t.assert_equals(box.space[SPACE]:get('u').fetched, false)
end

-- vivify_negative_ttl: mg N-1 (or an absolute time in the past) still
-- vivifies and wins; the stub expires at once.
g.test_vivify_negative_ttl = function()
    for _, ttl in ipairs({-1, engine.MAX_RELATIVE_TIME + 1}) do
        local resp = mg('n', {vivify_ttl = ttl})
        t.assert_equals(resp.status, 'HIT', ttl)
        t.assert_equals(resp.won, true, ttl)
        t.assert_equals(resp.item.value, '', ttl)
        t.assert_equals(box.space[SPACE]:get('n'), nil, ttl)
        t.assert_equals(get('n').status, 'MISS', ttl)
    end
end

-- md_x_new_item, md_x_cas: md x stores a new empty item with the old CAS.
g.test_delete_clear_value_new_item = function()
    set('x', 'value', {exptime = 100})
    get('x')
    one({op = 'delete', key = 'x', invalidate = true})
    local before = box.space[SPACE]:get('x')
    t.assert_equals({before.fetched, before.stale}, {true, true})
    fiber.sleep(0.01)
    local resp = one({op = 'delete', key = 'x', clear_value = true, ttl = 100})
    t.assert_equals(resp.status, 'DELETED')
    local after = box.space[SPACE]:get('x')
    t.assert_equals(after.value, '')
    t.assert_equals(after.fetched, false)
    t.assert_equals(after.stale, false)
    t.assert_gt(after.last_access, before.last_access)
    -- The CAS is kept (memcached passes ITEM_get_cas(it) to do_store_item).
    t.assert_equals(engine.u64(after.cas), engine.u64(before.cas))
    t.assert_equals(resp.cas, engine.u64(before.cas))
    -- The win token is gone with the old item: the next mg doesn't win.
    resp = mg('x')
    t.assert_equals({resp.won, resp.already_won, resp.stale}, {nil, nil, nil})

    -- md x I: the new item is stale, with a new CAS.
    local stored = set('y', 'value')
    t.assert_equals(
        one({
            op = 'delete',
            key = 'y',
            clear_value = true,
            invalidate = true,
        }).status,
        'DELETED'
    )
    after = box.space[SPACE]:get('y')
    t.assert_equals(after.stale, true)
    t.assert_gt(engine.u64(after.cas), stored.cas)
    t.assert_equals(mg('y').won, true)
    -- E sets the CAS in both cases.
    one({op = 'delete', key = 'y', clear_value = true, new_cas = 77ULL})
    t.assert_equals(engine.u64(box.space[SPACE]:get('y').cas), 77ULL)
    -- A plain delete of the tombstone with the kept CAS works.
    set('z', 'v')
    local cas = get('z').item.cas
    one({op = 'delete', key = 'z', clear_value = true})
    t.assert_equals(
        one({op = 'delete', key = 'z', cas = cas}).status,
        'DELETED'
    )
end

-- cas_zero_means_none: a CAS of 0 is no CAS (do_store_item CAS_NONE,
-- do_add_delta *cas != 0).
g.test_cas_zero = function()
    set('a', 'x')
    t.assert_equals(
        set('a', 'y', {mode = 'append', cas = 0ULL}).status,
        'STORED'
    )
    t.assert_equals(
        set('a', 'z', {mode = 'prepend', cas = 0ULL}).status,
        'STORED'
    )
    t.assert_equals(get('a').item.value, 'zxy')
    -- The cas command fails with it, even with invalidate.
    t.assert_equals(set('a', 'n', {cas = 0ULL}).status, 'EXISTS')
    t.assert_equals(
        set('a', 'n', {cas = 0ULL, invalidate = true}).status,
        'EXISTS'
    )
    t.assert_equals(set('missing', 'n', {cas = 0ULL}).status, 'NOT_FOUND')
    -- Even if the item's CAS is 0 (E0).
    set('e', 'x', {new_cas = 0ULL})
    t.assert_equals(set('e', 'n', {cas = 0ULL}).status, 'EXISTS')
    -- Arithmetic ignores it.
    set('n', '1')
    local resp = one({op = 'arith', key = 'n', delta = 1ULL, cas = 0ULL})
    t.assert_equals({resp.status, resp.value}, {'STORED', 2ULL})
    -- A non-zero CAS is still checked.
    t.assert_equals(
        set('a', 'y', {mode = 'append', cas = 1ULL}).status,
        'EXISTS'
    )
    t.assert_equals(
        one({op = 'arith', key = 'n', cas = 1ULL}).status,
        'EXISTS'
    )
end

-- arith_empty_cas_order: an empty value is refused before the CAS check.
g.test_arith_empty_before_cas = function()
    set('n', '')
    t.assert_equals(one({
        op = 'arith',
        key = 'n',
        delta = 1ULL,
        cas = 18446744073709551615ULL,
    }), {
        status = 'CLIENT_ERROR',
        message = 'cannot increment or decrement non-numeric value',
    })
    -- A non-numeric non-empty value: the CAS is checked first.
    set('n', 'abc')
    t.assert_equals(
        one({
            op = 'arith',
            key = 'n',
            delta = 1ULL,
            cas = 18446744073709551615ULL,
        }).status,
        'EXISTS'
    )
end

-- arith_strtoull_value: stored numbers are read as safe_strtoull() does.
g.test_arith_strtoull = function()
    local cases = {
        {'12 ab', 13ULL},
        {'+5', 6ULL},
        {' \t12', 13ULL},
        {'7\tx', 8ULL},
        {'7\0junk', 8ULL},
        {'12\r\n', 13ULL},
        {'-0', 1ULL},
        {'007', 8ULL},
        {'18446744073709551615', 0ULL},
        -- A '-' wraps; the result must not be negative as an int64.
        {'-9223372036854775809', 9223372036854775808ULL},
        {'-1', nil},
        {'-9223372036854775808', nil},
        {'12ab', nil},
        {'+ 5', nil},
        {'--5', nil},
        {'', nil},
        {' ', nil},
        {'x1', nil},
        {'1.5', nil},
        {'0x10', nil},
        {'18446744073709551616', nil},
    }
    for _, case in ipairs(cases) do
        local value, want = case[1], case[2]
        set('n', value)
        local resp = one({op = 'arith', key = 'n', delta = 1ULL})
        if want == nil then
            t.assert_equals(resp.status, 'CLIENT_ERROR', value)
        else
            t.assert_equals({resp.status, resp.value}, {'STORED', want}, value)
        end
    end
end

-- arith_item_flags: in place (do_add_delta, res + 2 <= it->nbytes) the item
-- keeps fetched, stale and the win token and its last access is bumped as
-- do_item_update() does; a new item (a longer number, or held by a read of
-- the batch) has none of the three and is accessed now.
g.test_arith_item_flags = function()
    local space = box.space[SPACE]
    local function mark(key, la)
        space:update(key, {
            {'=', 'fetched', true},
            {'=', 'stale', true},
            {'=', 'token', true},
            {'=', 'last_access', la},
        })
    end
    local function flags(key)
        local tu = space:get(key)
        return {tu.value, tu.fetched, tu.stale, tu.token}
    end
    local now = fiber.time()
    local old = now - 1000
    local recent = now - 10

    -- In place: the same length, shorter (padded).
    set('n', '10')
    mark('n', old)
    one({op = 'arith', key = 'n', delta = 1ULL})
    t.assert_equals(flags('n'), {'11', true, true, true})
    t.assert_ge(space:get('n').last_access, now)
    mark('n', recent)
    one({op = 'arith', key = 'n', incr = false, delta = 2ULL})
    t.assert_equals(flags('n'), {'9 ', true, true, true})
    t.assert_equals(space:get('n').last_access, recent, 'within the interval')

    -- A longer number is a new item.
    set('m', '9')
    mark('m', old)
    one({op = 'arith', key = 'm', delta = 1ULL})
    t.assert_equals(flags('m'), {'10', false, false, false})
    t.assert_ge(space:get('m').last_access, now)

    -- Held by a read earlier in the batch: a new item, not padded.
    set('h', '10')
    mark('h', old)
    exec(
        {op = 'get', key = 'h', hold = true},
        {op = 'arith', key = 'h', incr = false, delta = 1ULL}
    )
    t.assert_equals(flags('h'), {'9', false, false, false})

    -- track_access = false: no last access bump in place.
    set('n', '10')
    mark('n', old)
    engine.exec(
        {space_name = SPACE, track_access = false},
        {{op = 'arith', key = 'n', delta = 1ULL}}
    )
    t.assert_equals(flags('n'), {'11', true, true, true})
    t.assert_equals(space:get('n').last_access, old)
end

--
-- memtx MVCC, in a child server: an autocommit read doesn't see a prepared
-- change, so the gets that may win keep their transaction.
--

local m = t.group('engine_mvcc')

m.before_all(function(cg)
    cg.server = server:new({
        alias = 'mcc_mvcc',
        env = env.lua_env(),
        box_cfg = {memtx_use_mvcc_engine = true},
    })
    cg.server:start()
end)

m.after_all(function(cg)
    cg.server:drop()
end)

-- Two gets of a stale item during the WAL write of the first one's win:
-- one wins, the other is aborted by the conflict and read again without a
-- win.
m.test_concurrent_wins = function(cg)
    local res = cg.server:exec(function()
        local fiber_ = require('fiber')
        local engine_ = require('memcached.engine')
        require('memcached.schema').ensure('__mcc_mvcc', 'memory')
        local ctx = {space_name = '__mcc_mvcc'}
        engine_.exec(ctx, {
            {
                op = 'store',
                mode = 'set',
                key = 's',
                value = 'v',
                exptime = 0,
            },
            {op = 'delete', key = 's', invalidate = true},
        })
        local resps, fibers = {}, {}
        for i = 1, 2 do
            fibers[i] = fiber_.new(function()
                resps[i] = engine_.exec(ctx, {
                    {
                        op = 'get',
                        key = 's',
                        win_token = true,
                    },
                })[1]
            end)
            fibers[i]:set_joinable(true)
        end
        for i = 1, 2 do
            fibers[i]:join()
        end
        local won = 0
        for i = 1, 2 do
            if resps[i].won then
                won = won + 1
            end
        end
        local again = engine_.exec(ctx, {
            {
                op = 'get',
                key = 's',
                win_token = true,
            },
        })[1]
        return {
            won,
            resps[1].status,
            resps[2].status,
            again.won,
            again.already_won,
        }
    end)
    t.assert_equals(res, {1, 'HIT', 'HIT', nil, true})
end
