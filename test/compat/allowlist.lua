-- Known divergences between memcached 1.6.45 and tarantool-contrib/memcached.
--
-- A mismatch passes only if an entry explains it:
-- - a scenario step (or a whole scenario) tagged with allow = <id>, when the
--   entry applies to this reference version and its match, if any, agrees;
-- - or an entry with auto = true whose match agrees (used for untagged
--   steps and by the fuzzer).
-- Anything else that differs is a failure.
--
-- Fields:
--   id, reason      what differs and why it is allowed
--   class           'a' contrib bug (known, not fixed yet: the suite stays
--                   green and the summary lists it), 'b' intentional,
--                   'c' specific to a memcached version (none: contrib
--                   follows 1.6.45, the only reference)
--   ref             reference versions the entry applies to, e.g. '>1.6.45'
--                   or '<=1.6.45'; nil = every version
--   source          where memcached 1.6.45 does it
--   repro           class 'a': a minimal reproducer
--   match(c)        whether the entry explains a whole step: c.req (request
--                   bytes), c.ref / c.got (normalised replies), c.ref_status
--                   / c.got_status ('ok', 'closed', 'timeout'), c.same(a, b)
--                   (the harness comparator)
--   fix(c) -> ref, got   rewrites the replies to undo this divergence only;
--                   fixes compose, so one step may hit several entries
--   auto            match/fix may explain untagged steps (and the fuzzer)

local BAD_FORMAT = 'CLIENT_ERROR bad command line format\r\n'

local function lines(s)
    local out = {}
    for line in s:gmatch('[^\n]*\n') do
        out[#out + 1] = line
    end
    local tail = s:match('[^\n]*$')
    if tail ~= '' then
        out[#out + 1] = tail
    end
    return out
end

-- A number padded with spaces (do_add_delta in place) in a VALUE or VA
-- block, written without its padding.
local function unpad(s)
    s = s:gsub(
        '(VALUE %S+ %d+ )(%d+)([^\r\n]*)\r\n(%d+) +\r\n',
        function(head, _, rest, num)
            return head .. #num .. rest .. '\r\n' .. num .. '\r\n'
        end
    )
    s = s:gsub('VA (%d+)([^\r\n]*)\r\n(%d+) +\r\n', function(len, rest, num)
        rest = rest:gsub(' s' .. len .. '%f[^%d]', ' s' .. #num)
        return 'VA ' .. #num .. rest .. '\r\n' .. num .. '\r\n'
    end)
    return s
end

return {
    --
    -- (b) intentional.
    --
    {
        id = 'key_binary_per_request',
        class = 'b',
        reason = 'mg/me k: the key is returned in base64 only when b is in '
            .. 'the same request; memcached remembers it per item '
            .. '(ITEM_KEY_BINARY, set by ms b only)',
        source = 'proto_parser.c process_mget_cmd META_KEY(...ITEM_KEY_BINARY)',
    },
    {
        id = 'opaque_too_long_first',
        class = 'b',
        reason = 'an opaque longer than 32 bytes rejects mg/ma before '
            .. 'executing; memcached may apply T/N first',
        source = 'proto_parser.c process_mget_cmd (O checked in the reply loop)',
    },
    {
        id = 't_after_T',
        class = 'b',
        reason = 'mg/ma t reports the TTL after T regardless of the flag '
            .. 'order; memcached reports it as of the t position',
        source = 'proto_parser.c process_mget_cmd reply loop',
    },
    {
        id = 'R_after_T',
        class = 'b',
        reason = 'mg R compares the TTL the item had before this request, '
            .. 'whatever the flag order; memcached applies T first when it '
            .. 'comes first (mg k T10 R30 on an item without TTL wins there)',
        source = 'proto_parser.c process_mget_cmd reply loop (case T, case R)',
    },
    {
        id = 'me_fields',
        class = 'b',
        reason = 'me cls=1 and size=#key+#value; normalised away (cls, size)',
        source = 'proto_text.c process_meta_command',
    },
    {
        id = 'ma_T_lost',
        class = 'b',
        reason = 'ma T and E always apply to the stored item; memcached '
            .. 'loses both when it writes a new item (the number grows in '
            .. 'length, or another reference holds the item, such as an '
            .. 'earlier read of the batch): they go to the replaced item, so '
            .. 'the new one keeps the old TTL and gets a fresh CAS while '
            .. 'the reply shows the new TTL/CAS',
        source = 'memcached.c do_add_delta (*it_ret = the replaced item) + '
            .. 'proto_parser.c process_marithmetic_cmd (case T, '
            .. 'ITEM_set_cas(it, of.cas_id_in))',
    },
    {
        id = 'ttl_underflow',
        class = 'b',
        reason = 'mg t of an item touched into the past (T-1) is 0; memcached '
            .. 'prints the unsigned wrap of a negative TTL (t4294967234)',
        source = 'proto_parser.c process_mget_cmd itoa_u32(it->exptime - '
            .. 'current_time)',
    },
    {
        id = 'arith_pad_race',
        class = 'b',
        reason = 'memcached race, not replicated: incr/decr/ma update a '
            .. 'shortened number in place (padded with spaces) only when '
            .. 'nothing else references the item; the LRU maintainer and '
            .. 'crawler threads take references to the items they '
            .. 'inspect, so now and then memcached writes a new item of '
            .. 'the exact length instead, which also drops the fetched '
            .. 'flag, the stale marker and the win token. Contrib pads (and '
            .. 'keeps the three) unless a read earlier in the batch holds '
            .. 'the item. The fix undoes the padding only: the fuzzer '
            .. 'checks no flags of numbers, and a scenario that does, after '
            .. 'an update in place, fails if the race hits it',
        source = 'memcached.c do_add_delta (it->refcount == 2), items.c '
            .. 'lru_pull_tail (refcount_incr)',
        auto = true,
        fix = function(c)
            return c.ref, unpad(c.got)
        end,
    },
    {
        id = 'get_error_drops_pipeline',
        class = 'b',
        reason = 'a get that fails (key too long) answers its CLIENT_ERROR '
            .. 'and keeps the replies of the commands before it; '
            .. 'memcached culls every reply of the read batch not sent '
            .. 'yet',
        source = 'proto_text.c process_get_command_err -> memcached.c '
            .. 'conn_release_items ("Cull any unsent responses")',
    },
    {
        id = 'stats_detail',
        class = 'b',
        reason = 'stats detail on|off answer OK as in memcached, but no '
            .. 'per-prefix stats are recorded, so stats detail dump is '
            .. 'always an empty END; memcached (detail on) counts the '
            .. 'classic get/set/delete of keys with a ":" by prefix '
            .. '(PREFIX <p> get <n> hit <n> set <n> del <n>)',
        source = 'proto_text.c process_stats_detail, stats_prefix.c',
    },

    {
        id = 'get_error_crlf',
        class = 'b',
        reason = 'memcached bug, not replicated: a too long key in '
            .. 'get/gets/gat ends the CLIENT_ERROR line with an extra "\\r\\n"',
        source = 'proto_text.c process_get_command_err (copies a line that '
            .. 'already ends in \\r\\n, then out_string adds one)',
        auto = true,
        fix = function(c)
            local r = lines(c.ref)
            local out = {}
            for i, line in ipairs(r) do
                if not (line == '\r\n' and r[i - 1] == BAD_FORMAT) then
                    out[#out + 1] = line
                end
            end
            return table.concat(out), c.got
        end,
    },
    {
        id = 'me_b_ignored',
        class = 'b',
        reason = 'memcached bug, not replicated: me <key> b ignores b '
            .. 'unless a 4th token follows (ntokens >= 4), so the base64 '
            .. 'text is looked up as is',
        source = 'proto_text.c process_meta_command',
    },

    --
    -- (a) tarantool-contrib/memcached bugs: known, reported, not fixed yet (the compat
    -- suite never changes product code). Remove an entry when it is fixed:
    -- the summary reports tags that no longer diverge.
    --
}
