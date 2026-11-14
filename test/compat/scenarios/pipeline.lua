-- Layer 1: pipelining (many commands in one write) and commands split
-- across writes at awkward places.

local harness = require('test.compat.harness')

-- Every byte of s (templates expanded) as a separate chunk.
local function bytes(s)
    return function(ctx)
        local e = harness.expand(ctx, s)
        local out = {}
        for i = 1, #e do
            out[i] = e:sub(i, i)
        end
        return out
    end
end

return {
    {
        name = 'many_in_one_write',
        steps = {
            'set {k}a 0 0 1\r\n1\r\nset {k}b 0 0 1\r\n2\r\nget {k}a {k}b\r\n'
                .. 'delete {k}a\r\nget {k}a\r\nincr {k}b 5\r\nmg {k}b v\r\n'
                .. 'ms {k}c 2 T0\r\nhi\r\nmd {k}c q\r\nmg {k}c v\r\nmn\r\n'
                .. 'add {k}b 0 0 1\r\nx\r\nappend {k}b 0 0 1\r\nx\r\nget {k}b\r\n',
        },
    },
    {
        name = 'pipeline_errors_in_between',
        steps = {
            'set {k}a 0 0 1\r\nx\r\nbogus\r\nget {k}a\r\n'
                .. 'set {k}b 0 0 3\r\nabcde\r\nget {k}b\r\nmg {k}a v v\r\nmg {k}a v\r\n',
        },
    },
    {
        name = 'pipeline_noreply_quiet',
        steps = {
            'set {k}a 0 0 1 noreply\r\na\r\nset {k}b 0 0 1 noreply\r\nb\r\n'
                .. 'mg {k}a v q\r\nmg {k}missing v q\r\nmg {k}b v q k\r\nmn\r\n'
                .. 'ms {k}c 1 q\r\nc\r\nmd {k}missing q\r\nmd {k}c q\r\nmn\r\n'
                .. 'ma {k}missing q\r\nma {k}n q v\r\nmg {k}n v\r\n',
            {send = 'ma {k}n N0 q\r\nma {k}n q v\r\nmg {k}n v\r\n'},
        },
    },
    {
        name = 'pipeline_get_error',
        steps = {
            {
                send = 'set {k}a 0 0 1\r\nx\r\nget {k}a\r\nmd {k}b\r\n'
                    .. 'get {key:251}\r\nget {k}a\r\n',
                allow = 'get_error_drops_pipeline',
            },
        },
    },
    {
        name = 'pipeline_200_sets',
        steps = {
            {
                send = function(ctx)
                    local out = {}
                    for i = 1, 200 do
                        local v = ('value-%d'):format(i)
                        out[#out + 1] = ('set %sk%d %d 0 %d\r\n%s\r\n'):format(
                            ctx.prefix,
                            i,
                            i,
                            #v,
                            v
                        )
                    end
                    return table.concat(out)
                end,
            },
            {
                send = function(ctx)
                    local keys = {}
                    for i = 1, 200, 7 do
                        keys[#keys + 1] = ('%sk%d'):format(ctx.prefix, i)
                    end
                    return 'get ' .. table.concat(keys, ' ') .. '\r\n'
                end,
            },
        },
    },
    {
        name = 'pipeline_after_too_large',
        steps = {
            'set {k}a 0 0 1\r\nx\r\n',
            {
                send = function()
                    return ('set {k}big 0 0 1048577\r\n%s\r\nget {k}a\r\n'):format(
                        ('v'):rep(1048577)
                    )
                end,
            },
            {
                send = function()
                    return ('ms {k}big 1048577\r\n%s\r\nmg {k}a v\r\n'):format(
                        ('v'):rep(1048577)
                    )
                end,
            },
        },
    },
    {
        name = 'split_command_name',
        steps = {
            'set {k}a 0 0 1\r\nx\r\n',
            {chunks = {'ge', 't {k}a\r\n'}},
            {chunks = {'g', 'et', 's {k}a\r\n'}},
            {chunks = {'m', 'g {k}a v\r\n'}},
            {chunks = {'mg', ' {k}a v\r\n'}},
            {chunks = {'set', ' {k}b 0 0 1\r\ny\r\n'}},
        },
    },
    {
        name = 'split_key',
        steps = {
            'set {k}abc 0 0 1\r\nx\r\n',
            {chunks = {'get {k}a', 'bc\r\n'}},
            {chunks = {'mg {k}ab', 'c v\r\n'}},
            {chunks = {'set {k}a', 'bc 0 0 1\r\nz\r\n'}},
            'get {k}abc\r\n',
        },
    },
    {
        name = 'split_crlf',
        steps = {
            'set {k}a 0 0 1\r\nx\r\n',
            {chunks = {'get {k}a\r', '\n'}},
            {chunks = {'mg {k}a v\r', '\n'}},
            {chunks = {'set {k}b 0 0 1\r', '\ny\r\n'}},
            {chunks = {'set {k}c 0 0 1\r\nz\r', '\n'}},
            {chunks = {'ms {k}d 1\r\nw\r', '\n'}},
            'get {k}b {k}c {k}d\r\n',
        },
    },
    {
        name = 'split_data_block',
        steps = {
            {chunks = {'set {k}a 0 0 10\r\n01234', '56789\r\n'}},
            {chunks = {'set {k}b 0 0 10\r\n', '0123456789', '\r\n'}},
            {chunks = {'ms {k}c 10 T0\r\n0', '12345678', '9\r\n'}},
            {chunks = {'set {k}d 0 0 3\r\nab', 'cde\r\n'}},
            'get {k}a {k}b {k}c {k}d\r\n',
        },
    },
    {
        name = 'byte_by_byte',
        steps = {
            {
                chunks = bytes('set {k}a 0 0 2\r\nhi\r\nget {k}a\r\n'),
                gap = 0.005,
            },
            {chunks = bytes('ms {k}b 2 c\r\nyo\r\nmg {k}b v\r\n'), gap = 0.005},
        },
    },
    {
        name = 'split_large_value',
        steps = {
            {
                chunks = {
                    'set {k}a 0 0 200000\r\n',
                    ('a'):rep(70000),
                    ('b'):rep(70000),
                    ('c'):rep(60000) .. '\r\nget {k}a\r\n',
                },
            },
        },
    },
}
