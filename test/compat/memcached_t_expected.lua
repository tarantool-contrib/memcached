-- Expected failures of memcached's t/*.t against tarantool-contrib/memcached (see
-- memcached_t_test.lua). Per file, failures is a list of
--   {test = <n> | tests = {<n>...} | range = {<from>, <to>} |
--    ranges = {{<from>, <to>}...}, class = 'a' | 'b' | 'c', reason = <text>}
-- Classes:
--   a  a bug in tarantool-contrib/memcached (to be fixed; reproducer in the reason);
--   b  intentional (see allowlist.lua) or not applicable: the test uses a
--      memcached-only option or internals.
-- Checked against the reference binary with MCC_COMPAT_T_REFERENCE:
-- /opt/memcached-src/memcached-debug (1.6.45) passes every file.
return {
    ['flush-all'] = {
        failures = {
            {
                tests = {25, 26},
                class = 'b',
                reason = 'test uses a memcached-only option: -F (disable '
                    .. 'flush_all) has no contrib equivalent, the shim ignores '
                    .. 'it, so flush_all answers OK and flushes foo',
            },
        },
    },
}
