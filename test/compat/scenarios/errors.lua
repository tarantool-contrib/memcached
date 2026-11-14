-- Layer 1: malformed input: bad command lines, bad data chunks, too large
-- values, key limits, long lines and unknown commands.
local MiB = 1048576

-- A storage command with a value of n bytes.
local function set_of(cmd, n)
    return function()
        return ('%s {k}big 0 0 %d\r\n%s\r\n'):format(cmd, n, ('v'):rep(n))
    end
end

local function ms_of(n)
    return function()
        return ('ms {k}big %d\r\n%s\r\n'):format(n, ('v'):rep(n))
    end
end

-- The largest value of a 20-byte key with flags 0 in memcached's 1 MiB
-- items: 48 bytes of header, the key and its NUL, the value and its \r\n,
-- the CAS.
local MAX_VALUE_20 = MiB - 48 - 21 - 2 - 8

-- A storage command on a 20-byte key with a value of n bytes.
local function set20_of(cmd, n, flags)
    return function()
        return ('%s {key:20} %d 0 %d\r\n%s\r\n'):format(
            cmd,
            flags or 0,
            n,
            ('v'):rep(n)
        )
    end
end

return {
    {
        name = 'unknown_commands',
        steps = {
            'bogus\r\n',
            'bogus with args\r\n',
            'gett {k}a\r\n',
            'sets {k}a 0 0 1\r\n',
            'mx {k}a\r\n',
            {send = 'm\r\n'},
            'mgx {k}a\r\n',
            'get{k}a\r\n',
            'lru_crawler bogus\r\n',
            {send = 'GET {k}a\r\n'},
            {send = 'Set {k}a 0 0 1\r\nx\r\n'},
        },
    },
    {
        name = 'empty_lines',
        steps = {
            {send = '\r\n'},
            {send = '\n'},
            {send = ' \r\n'},
            '   \r\n',
            {send = ' get {k}a\r\n'},
            {send = 'get\r\n'},
            {send = 'gets\r\n'},
            {send = 'gets \r\n'},
        },
    },
    {
        name = 'bare_newlines',
        steps = {
            'set {k}a 0 0 1\nx\r\n',
            'get {k}a\n',
            'mg {k}a v\n',
            'ms {k}b 1\nx\r\n',
            'mg {k}b v\r\n',
            'get {k}a {k}b\n',
        },
    },
    {
        name = 'bad_data_chunk',
        steps = {
            'set {k}a 0 0 3\r\nabcde\r\n',
            'get {k}a\r\n',
            'set {k}a 0 0 1\r\nx\r\n',
            'set {k}a 0 0 3\r\nabcd\r\n',
            'get {k}a\r\n',
            'set {k}a 0 0 3\r\nab\r\n\r\n',
            'get {k}a\r\n',
            'ms {k}a 3\r\nabcde\r\n',
            'ms {k}a 3 q\r\nabcd\r\n',
            'get {k}a\r\n',
            'append {k}a 0 0 1\r\nxyz\r\n',
            'get {k}a\r\n',
        },
    },
    {
        name = 'too_large_set',
        steps = {
            'set {k}big 0 0 1\r\nx\r\n',
            {send = set_of('set', MiB + 1)},
            'get {k}big\r\n',
            'set {k}big 0 0 1\r\nx\r\n',
            {send = set_of('set', 2 * MiB)},
            'get {k}big\r\n',
            'set {k}big 0 0 1\r\nx\r\n',
            {send = set_of('set', MiB + 1)},
            'get {k}big\r\n',
        },
    },
    {
        name = 'too_large_other_modes',
        steps = {
            'set {k}big 0 0 1\r\nx\r\n',
            {send = set_of('add', MiB + 1)},
            {send = set_of('replace', MiB + 1)},
            {send = set_of('append', MiB + 1)},
            {send = set_of('prepend', MiB + 1)},
            {
                send = function()
                    return ('cas {k}big 0 0 %d 1\r\n%s\r\n'):format(
                        MiB + 1,
                        ('v'):rep(MiB + 1)
                    )
                end,
            },
            'get {k}big\r\n',
            {
                send = function()
                    return ('set {k}big 0 0 %d noreply\r\n%s\r\n'):format(
                        MiB + 1,
                        ('v'):rep(MiB + 1)
                    )
                end,
            },
            'get {k}big\r\n',
        },
    },
    -- The joined item of an append/prepend over the limit: NOT_STORED/NS,
    -- the old item stays (do_store_item).
    {
        name = 'too_large_after_append',
        steps = {
            {send = set_of('set', MiB - 2000)},
            {send = set_of('append', 3000)},
            {send = set_of('prepend', 3000)},
            {
                send = function()
                    return ('ms {k}big 3000 MA\r\n%s\r\n'):format(
                        ('a'):rep(3000)
                    )
                end,
            },
            {
                send = function()
                    return ('ms {k}big 3000 MP\r\n%s\r\n'):format(
                        ('a'):rep(3000)
                    )
                end,
            },
            'mg {k}big s\r\n',
            {send = set_of('append', 500)},
            'mg {k}big s\r\n',
        },
    },
    {
        name = 'too_large_ms',
        steps = {
            'set {k}big 0 0 1\r\nx\r\n',
            {send = ms_of(MiB + 1)},
            'get {k}big\r\n',
            'set {k}big 0 0 1\r\nx\r\n',
            {
                send = function()
                    return ('ms {k}big %d MA q\r\n%s\r\n'):format(
                        MiB + 1,
                        ('v'):rep(MiB + 1)
                    )
                end,
            },
            'get {k}big\r\n',
        },
    },
    {
        name = 'size_boundary',
        steps = {
            {send = set_of('set', MiB - 1024)},
            'mg {k}big s\r\n',
            {send = set_of('set', MiB)},
            'mg {k}big s\r\n',
            {send = ms_of(MiB)},
            'mg {k}big s\r\n',
            -- The exact limit: the item size counts the key and the flags.
            {send = set20_of('set', MAX_VALUE_20)},
            'mg {key:20} s f\r\n',
            {send = set20_of('set', MAX_VALUE_20 + 1)},
            'mg {key:20} s f\r\n',
            {send = set20_of('set', MAX_VALUE_20, 1)},
            'mg {key:20} s f\r\n',
            {send = set20_of('set', MAX_VALUE_20 - 4, 1)},
            'mg {key:20} s f\r\n',
            {
                send = function()
                    local n = MAX_VALUE_20 + 1
                    return ('ms {key:20} %d\r\n%s\r\n'):format(n, ('v'):rep(n))
                end,
            },
            'mg {key:20} s f\r\n',
        },
    },
    {
        name = 'too_large_announced_only',
        steps = {
            -- The data block is swallowed as it arrives.
            {
                chunks = {
                    'set {k}big 0 0 2000000\r\n',
                    ('v'):rep(1000000),
                    ('v'):rep(1000000) .. '\r\n',
                },
            },
            'get {k}big\r\n',
            {send = 'set {k}big 0 0 2147483646\r\nx\r\n'},
        },
    },
    {
        name = 'key_length',
        steps = {
            'set {key:250} 0 0 1\r\nx\r\n',
            'get {key:250}\r\n',
            'touch {key:250} 10\r\n',
            'incr {key:250} 1\r\n',
            'delete {key:250}\r\n',
            {send = 'set {key:251} 0 0 1\r\nx\r\n'},
            'get {key:251}\r\n',
            'get {k}a {key:251}\r\n',
            'set {k}a 0 0 1\r\nx\r\n',
            'get {k}a {key:251}\r\n',
            'gets {key:251} {k}a\r\n',
            'touch {key:251} 10\r\n',
            'incr {key:251} 1\r\n',
            'delete {key:251}\r\n',
            'gat 10 {key:251}\r\n',
        },
    },
    {
        name = 'key_control_chars',
        steps = {
            {send = 'set {k}a\1b 0 0 1\r\nx\r\n'},
            {send = 'get {k}a\1b\r\n'},
            {send = 'set {k}a\127b 0 0 1\r\nx\r\n'},
            {send = 'get {k}a\127b\r\n'},
            {send = 'get {k}a\tb\r\n'},
            {send = 'mg {k}a\1b v\r\n'},
            {send = 'ms {k}a\1b 1\r\nx\r\n'},
            {send = 'md {k}a\1b\r\n'},
            'set {k}u\195\169 0 0 1\r\nx\r\n',
            'get {k}u\195\169\r\n',
        },
    },
    {
        name = 'long_line_no_newline',
        steps = {
            {send = function() return ('x'):rep(3000) end, nosync = true},
            {connect = true, send = 'get {k}a\r\n'},
            {
                send = function() return 'get ' .. ('k'):rep(3000) end,
                nosync = true,
                idle = 0.3,
            },
            'mn\r\n',
        },
    },
    {
        name = 'long_lines_with_newline',
        steps = {
            {
                send = function()
                    return 'set ' .. ('k'):rep(3000) .. ' 0 0 1\r\nx\r\n'
                end,
            },
            {send = function() return 'mg ' .. ('k'):rep(3000) .. ' v\r\n' end},
            {
                connect = true,
                send = function()
                    return 'bogus ' .. ('x'):rep(3000) .. '\r\n'
                end,
            },
        },
    },
    {
        name = 'long_multiget',
        steps = {
            'set {k}a 0 0 1\r\nx\r\n',
            {
                send = function(ctx)
                    local keys = {}
                    for i = 1, 400 do
                        keys[i] = ('%sk%03d'):format(ctx.prefix, i)
                    end
                    keys[#keys + 1] = ctx.prefix .. 'a'
                    return 'get ' .. table.concat(keys, ' ') .. '\r\n'
                end,
            },
        },
    },
    {
        name = 'http_close',
        steps = {
            {send = 'foo / HTTP/1.1\r\n'},
            {connect = true, send = 'bogus HTTP/1.0\r\n'},
            {connect = true, send = 'get {k}a HTTP/1.0\r\n'},
            {connect = true, send = 'GET / HTTP/1.1\r\n'},
            {connect = true, send = 'version\r\n'},
        },
    },
    {
        name = 'too_many_get_keys_tokens',
        steps = {
            'set {k}a 0 0 1\r\nx\r\n',
            {
                send = function(ctx)
                    local keys = {}
                    for i = 1, 300 do
                        keys[i] = ctx.prefix .. 'a'
                    end
                    return 'get ' .. table.concat(keys, ' ') .. '\r\n'
                end,
            },
        },
    },
}
