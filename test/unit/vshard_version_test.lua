-- backend/vshard.lua: the check of the vshard version it loads.

local t = require('luatest')

local backend = require('memcached.backend.vshard')

local g = t.group('vshard_version')

g.test_supported = function()
    for _, v in ipairs({'1.0.0', '1.0.7', '1.3.0', '1.0.0-12-g1234567'}) do
        t.assert_equals(backend.version_warning(v), nil, v)
    end
end

g.test_unsupported = function()
    for _, v in ipairs({'0.1.27', '0.9.9', '2.0.0', '10.1.0', 'x', ''}) do
        local warning = backend.version_warning(v)
        t.assert_str_contains(
            warning,
            'vshard ' .. v .. ' is not a supported version (>= 1.0.0, < 2.0.0',
            false,
            v
        )
    end
    t.assert_str_contains(
        backend.version_warning(nil, 'vshard-ee'),
        'vshard-ee (unknown version) is not a supported'
    )
end

-- The vshard the tests run on.
g.test_installed = function()
    local ok, consts = pcall(require, 'vshard.consts')
    t.skip_if(not ok, 'no vshard')
    t.assert_equals(backend.version_warning(consts.VERSION), nil)
end
