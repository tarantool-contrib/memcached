-- Layer 1: hand-written scenarios, run against a reference memcached and
-- tarantool-contrib/memcached; the normalised replies must match (see README.md).

local t = require('luatest')

local harness = require('test.compat.harness')

local g = t.group('compat_scenarios')

local FILES = {'smoke', 'classic', 'meta', 'errors', 'pipeline', 'ttl'}

g.before_all(function(cg)
    harness.report = {allowed = {}, stats = {}, stale = {}}
    cg.servers, cg.skip = harness.start_servers()
end)

g.after_all(function(cg)
    harness.stop_servers(cg.servers)
    print(harness.summary())
end)

for _, file in ipairs(FILES) do
    for _, sc in ipairs(require('test.compat.scenarios.' .. file)) do
        g[('test_%s_%s'):format(file, sc.name)] = function(cg)
            t.skip_if(cg.servers == nil, cg.skip)
            harness.check(cg.servers, sc)
        end
    end
end
