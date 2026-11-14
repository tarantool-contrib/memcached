-- mcc.tune_gc(): the explicit GC knob. memcached itself never
-- changes the collector.

local t = require('luatest')

local env = require('test.helpers.env')

local g = t.group('tune_gc')

local mcc

g.before_all(function(cg)
    env.box_cfg()
    mcc = require('memcached')
    -- collectgarbage('setpause') returns the previous value: read and
    -- restore.
    cg.pause = collectgarbage('setpause', 200)
    collectgarbage('setpause', cg.pause)
    cg.stepmul = collectgarbage('setstepmul', 200)
    collectgarbage('setstepmul', cg.stepmul)
end)

g.after_each(function(cg)
    collectgarbage('setpause', cg.pause)
    collectgarbage('setstepmul', cg.stepmul)
    if mcc.get('gc') ~= nil then
        mcc.destroy('gc')
    end
    env.drop_spaces({'__mcc_gc'})
end)

g.test_tune = function(cg)
    t.assert_equals(mcc.tune_gc({}), {})
    t.assert_equals(mcc.tune_gc({pause = 400}), {pause = cg.pause})
    t.assert_equals(collectgarbage('setpause', 400), 400)
    t.assert_equals(
        mcc.tune_gc({pause = 300, stepmul = 500}),
        {pause = 400, stepmul = cg.stepmul}
    )
    -- Restore with the returned values.
    t.assert_equals(
        mcc.tune_gc({pause = 400, stepmul = cg.stepmul}),
        {pause = 300, stepmul = 500}
    )
    t.assert_equals(collectgarbage('setstepmul', cg.stepmul), cg.stepmul)
end

g.test_validation = function()
    t.assert_error_msg_contains('Usage: mcc.tune_gc', mcc.tune_gc, 400)
    t.assert_error_msg_contains(
        "Unknown GC parameter 'step'",
        mcc.tune_gc,
        {step = 400}
    )
    for _, bad in ipairs({99, 1001, 400.5, '400'}) do
        t.assert_error_msg_contains(
            "GC parameter 'pause' must be an integer in 100..1000",
            mcc.tune_gc,
            {pause = bad}
        )
    end
    t.assert_error_msg_contains(
        "GC parameter 'stepmul' must be an integer in 100..10000",
        mcc.tune_gc,
        {stepmul = 10001}
    )
end

-- Creating an instance leaves the collector alone.
g.test_instance_keeps_gc = function(cg)
    local port = env.free_port()
    collectgarbage('setpause', 123)
    collectgarbage('setstepmul', 321)
    mcc.create('gc', {listen = '127.0.0.1:' .. port})
    t.assert_equals(collectgarbage('setpause', cg.pause), 123)
    t.assert_equals(collectgarbage('setstepmul', cg.stepmul), 321)
end
