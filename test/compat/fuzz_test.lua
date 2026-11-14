-- Layer 2: a differential fuzzer. Random command sequences, generated from a
-- seeded PRNG, go to memcached and tarantool-contrib/memcached through the harness;
-- the normalised replies must match or be explained by the allowlist (its
-- auto entries: fuzz steps carry no tags). An unexplained mismatch is shrunk
-- with ddmin, by replaying candidate sequences on a fresh state, and the
-- test fails with the seed and the minimal reproducer.
--
--   MCC_COMPAT_FUZZ_ITERS    steps per seed (default 300: the CI run);
--                            e.g. 20000 for a long run
--   MCC_COMPAT_FUZZ_SEED     seeds, comma separated (default 1,2,3)
--   MCC_COMPAT_FUZZ_EPISODE  steps per episode (a fresh key prefix and a
--                            flush_all), default 40
--   MCC_COMPAT_FUZZ_SHRINK   replay budget for shrinking, default 400
--
-- A step is 1-4 complete commands sent in one write. The keys are few, so
-- commands collide: four text keys, two numeric keys and two binary keys
-- that are only used base64-encoded (meta b). The generator stays away from
-- inputs that are known to differ (see allowlist.lua): short opaques, no
-- t together with T or with a negative N
-- (ttl_underflow), and a few per-key rules listed at KINDS. It covers the
-- fixed former class (a) entries: C0 (cas_zero_means_none), N-1
-- (vivify_negative_ttl), h/me after a vivify or an md x (vivify_fetched,
-- md_x_new_item), md x without E (md_x_cas) and ma C on empty or text values
-- (arith_empty_cas_order).

local bit = require('bit')
local t = require('luatest')

local harness = require('test.compat.harness')

local g = t.group('compat_fuzz')

local function env_number(name, default)
    local v = tonumber(os.getenv(name))
    return v ~= nil and v or default
end

local ITERS = env_number('MCC_COMPAT_FUZZ_ITERS', 300)
local EPISODE = env_number('MCC_COMPAT_FUZZ_EPISODE', 40)
local SHRINK_BUDGET = env_number('MCC_COMPAT_FUZZ_SHRINK', 400)
local SEEDS = {}
for s in (os.getenv('MCC_COMPAT_FUZZ_SEED') or '1,2,3'):gmatch('%d+') do
    SEEDS[#SEEDS + 1] = tonumber(s)
end

--
-- PRNG: xorshift32.
--

local Rng = {}
Rng.__index = Rng

local function new_rng(seed)
    local x = bit.tobit(seed * 2654435761 + 0x9e3779b9)
    if x == 0 then
        x = 0x6d2b79f5
    end
    local r = setmetatable({x = x}, Rng)
    for _ = 1, 16 do
        r:next()
    end
    return r
end

function Rng:next()
    local x = self.x
    x = bit.bxor(x, bit.lshift(x, 13))
    x = bit.bxor(x, bit.rshift(x, 17))
    x = bit.bxor(x, bit.lshift(x, 5))
    self.x = x
    return x
end

-- An integer in [lo, hi].
function Rng:int(lo, hi)
    return lo + self:next() % (hi - lo + 1)
end

function Rng:pick(list)
    return list[self:int(1, #list)]
end

function Rng:chance(p)
    return self:int(0, 9999) < p * 10000
end

function Rng:shuffle(list)
    for i = #list, 2, -1 do
        local j = self:int(1, i)
        list[i], list[j] = list[j], list[i]
    end
    return list
end

-- A weighted choice: {{weight, value}, ...}.
function Rng:weighted(choices)
    local total = 0
    for _, c in ipairs(choices) do
        total = total + c[1]
    end
    local x = self:int(1, total)
    for _, c in ipairs(choices) do
        x = x - c[1]
        if x <= 0 then
            return c[2]
        end
    end
    return choices[#choices][2]
end

--
-- Keys. Each kind has rules that keep known divergences out:
--   tv  text values; may be vivified (mg N, ms MA/MP N)
--   tf  text values; never vivified
--   text kinds (tv, tf, bt): h, md x and md I allowed
--   num numbers only; no h/me/R/N on mg and no md I (arith_pad_race: the
--       race decides the fetched flag, the win token and the stale
--       marker too),
--       no ma T (ma_T_lost)
--   bt  base64 only (b), text values; may be vivified; no k
--       (key_binary_per_request), no me (me_b_ignored), no ma
--   bn  base64 only (b), numbers; the num and bt rules
--

local KINDS = {
    tv = {'{k}t1', '{k}t2'},
    tf = {'{k}t3', '{k}t4'},
    num = {'{k}n1', '{k}n2'},
    bt = {'{b64:{k}\1bt}'},
    bn = {'{b64:{k}\2bn}'},
}

local PLAIN = {'tv', 'tv', 'tf', 'tf', 'num', 'num'}
local META = {'tv', 'tf', 'num', 'bt', 'bn'}

local function numeric(kind)
    return kind == 'num' or kind == 'bn'
end

local function binary(kind)
    return kind == 'bt' or kind == 'bn'
end

local function key_of(r, kind)
    return r:pick(KINDS[kind])
end

local LETTERS = 'abcdefghijklmnopqrstuvwxyz'

local function text_value(r)
    local n = r:pick({0, 1, 2, 3, 5, 8})
    local out = {}
    for i = 1, n do
        local c = r:int(1, #LETTERS)
        out[i] = LETTERS:sub(c, c)
    end
    return table.concat(out)
end

local BIG = '18446744073709551615'

local function num_value(r)
    return r:pick({
        '0',
        '1',
        '5',
        '9',
        '10',
        '99',
        '100',
        '007',
        tostring(r:int(0, 100000)),
        '18446744073709551614',
        BIG,
    })
end

local function value_of(r, kind)
    return numeric(kind) and num_value(r) or text_value(r)
end

local function ttl(r)
    return r:weighted({{5, 0}, {3, 1000}, {1, -1}})
end

-- Meta F: 32 bits (above, F is refused).
local function client_flags(r)
    return r:pick({0, 0, 0, 1, 42, 4294967295})
end

-- Classic flags: above 2^32-1, memcached truncates them (strtoul, then a
-- cast to 32 bits).
local function classic_flags(r)
    return r:chance(0.1)
        and r:pick({
            '4294967296',
            '4294967338',
            '18446744073709551615',
        })
        or tostring(client_flags(r))
end

local function opaque(r)
    return 'O' .. r:pick({'1', 'x', 'op', 'abc123', 'Zz9'})
end

-- A CAS to compare with: one this server handed out, one above every CAS of
-- both servers, or 0, which means "no CAS" in some commands (append,
-- prepend, arith, add) and fails others (cas, ms set) as in memcached.
local function cas_ref(r)
    return r:weighted({
        {5, '{lastcas}'},
        {3, '{cas' .. r:int(1, 6) .. '}'},
        {1, harness.NO_CAS},
        {1, '0'},
    })
end

-- An autovivify TTL (N). -1: the item is created expired, and the caller
-- still wins (mg) or gets the initial value (ma).
local function vivify_ttl(r)
    return r:weighted({{4, 0}, {4, 1000}, {1, -1}})
end

-- Client-supplied CAS values (E): literal in both servers' replies, and
-- far from both servers' own counters.
local function explicit_cas(r)
    return tostring(1000000000000 + r:int(0, 999999) * 1000 + r:int(0, 999))
end

local function noreply(r)
    return r:chance(0.15) and ' noreply' or ''
end

-- ctx: {mode = {E = bool, I = bool}}

local function add(f, r, p, flag)
    if r:chance(p) then
        f[#f + 1] = flag
    end
end

-- Output-only flags in random order after the ones whose position matters
-- (N before t: memcached reports t as of its position).
local function meta_line(cmd, key, head, tail, r)
    local f = {}
    for _, v in ipairs(head) do
        f[#f + 1] = v
    end
    for _, v in ipairs(r:shuffle(tail)) do
        f[#f + 1] = v
    end
    if #f == 0 then
        return ('%s %s'):format(cmd, key)
    end
    return ('%s %s %s'):format(cmd, key, table.concat(f, ' '))
end

local gen = {}

function gen.storage(r)
    local kind = r:pick(PLAIN)
    local key = key_of(r, kind)
    local cmd = r:pick({
        'set',
        'set',
        'add',
        'replace',
        'append',
        'prepend',
        'cas',
    })
    local value = value_of(r, kind)
    local cas = cmd == 'cas' and (' ' .. cas_ref(r)) or ''
    return ('%s %s %s %d %d%s%s\r\n%s\r\n'):format(
        cmd,
        key,
        classic_flags(r),
        ttl(r),
        #value,
        cas,
        noreply(r),
        value
    )
end

function gen.retrieval(r)
    local keys = {}
    for i = 1, r:int(1, 3) do
        keys[i] = key_of(r, r:pick(PLAIN))
    end
    local cmd = r:pick({'get', 'get', 'gets', 'gat', 'gats'})
    if cmd == 'gat' or cmd == 'gats' then
        return ('%s %d %s\r\n'):format(cmd, ttl(r), table.concat(keys, ' '))
    end
    return ('%s %s\r\n'):format(cmd, table.concat(keys, ' '))
end

function gen.arith(r)
    local kind = r:weighted({{4, 'num'}, {1, 'tv'}, {1, 'tf'}})
    local cmd = r:pick({'incr', 'decr'})
    local delta = r:pick({'0', '1', '1', '7', '100', BIG})
    return ('%s %s %s%s\r\n'):format(
        cmd,
        key_of(r, kind),
        delta,
        noreply(r)
    )
end

function gen.touch(r)
    return ('touch %s %d%s\r\n'):format(
        key_of(r, r:pick(PLAIN)),
        ttl(r),
        noreply(r)
    )
end

function gen.delete(r)
    local key = key_of(r, r:pick(PLAIN))
    return ('delete %s%s%s\r\n'):format(
        key,
        r:chance(0.2) and ' 0' or '',
        noreply(r)
    )
end

function gen.mg(r, ctx)
    local kind = r:pick(META)
    local key = key_of(r, kind)
    local head, tail = {}, {}
    if binary(kind) then
        head[#head + 1] = 'b'
    end
    -- Not N and T together: memcached applies them in flag order, contrib
    -- vivifies with N's TTL.
    local touch = r:chance(0.15)
    -- No t after T (t_after_T) or a negative N (ttl_underflow).
    local no_t = touch
    if not touch and (kind == 'tv' or kind == 'bt') and r:chance(0.25) then
        local n = vivify_ttl(r)
        head[#head + 1] = 'N' .. n
        no_t = n < 0
        if ctx.mode.E and r:chance(0.3) then
            head[#head + 1] = 'E' .. explicit_cas(r)
        end
    end
    if touch then
        head[#head + 1] = 'T' .. ttl(r)
    end
    -- Not R after T either (R_after_T).
    if not touch and not numeric(kind) and r:chance(0.15) then
        head[#head + 1] = 'R' .. r:pick({0, 30, 5000})
    end
    if r:chance(0.08) then
        head[#head + 1] = 'C' .. cas_ref(r)
    end
    add(tail, r, 0.6, 'v')
    add(tail, r, 0.3, 's')
    add(tail, r, 0.3, 'f')
    add(tail, r, 0.3, 'c')
    if not binary(kind) then
        add(tail, r, 0.25, 'k')
    end
    add(tail, r, 0.2, opaque(r))
    add(tail, r, 0.15, 'q')
    if not numeric(kind) then
        add(tail, r, 0.3, 'h')
    end
    add(tail, r, 0.2, 'l')
    add(tail, r, 0.1, 'u')
    if not no_t then
        add(tail, r, 0.3, 't')
    end
    return meta_line('mg', key, head, tail, r) .. '\r\n'
end

function gen.ms(r, ctx)
    local kind = r:pick(META)
    local key = key_of(r, kind)
    local value = value_of(r, kind)
    local head, tail = {}, {}
    if binary(kind) then
        head[#head + 1] = 'b'
    end
    local mode = r:pick({'', '', 'S', 'E', 'R', 'A', 'P'})
    if mode ~= '' then
        head[#head + 1] = 'M' .. mode
    end
    if (mode == 'A' or mode == 'P') and (kind == 'tv' or kind == 'bt')
        and r:chance(0.3) then
        head[#head + 1] = 'N' .. vivify_ttl(r)
    end
    add(head, r, 0.4, 'T' .. ttl(r))
    add(head, r, 0.3, 'F' .. client_flags(r))
    if r:chance(0.2) then
        head[#head + 1] = 'C' .. cas_ref(r)
        -- Stale numbers: arith_pad_race.
        if ctx.mode.I and not numeric(kind) then
            add(head, r, 0.4, 'I')
        end
    end
    if ctx.mode.E then
        add(head, r, 0.15, 'E' .. explicit_cas(r))
    end
    add(tail, r, 0.3, 'c')
    add(tail, r, 0.2, 'k')
    add(tail, r, 0.2, opaque(r))
    add(tail, r, 0.15, 'q')
    add(tail, r, 0.2, 's')
    -- ms <key> <datalen> <flags>*
    local line = meta_line('ms', key .. ' ' .. #value, head, tail, r)
    return line .. '\r\n' .. value .. '\r\n'
end

function gen.md(r, ctx)
    local kind = r:pick(META)
    local key = key_of(r, kind)
    local head, tail = {}, {}
    if binary(kind) then
        head[#head + 1] = 'b'
    end
    add(head, r, 0.15, 'C' .. cas_ref(r))
    local clear = not numeric(kind) and r:chance(0.2)
    if clear then
        head[#head + 1] = 'x'
        add(head, r, 0.6, 'T' .. r:pick({0, 1000}))
        if ctx.mode.E then
            add(head, r, 0.3, 'E' .. explicit_cas(r))
        end
        add(head, r, 0.3, 'F' .. client_flags(r))
    end
    -- Stale numbers: arith_pad_race.
    if not numeric(kind) and r:chance(0.25) then
        head[#head + 1] = 'I'
        if not clear then
            add(head, r, 0.3, 'T' .. ttl(r))
        end
        if ctx.mode.E and not clear then
            add(head, r, 0.3, 'E' .. explicit_cas(r))
        end
    end
    add(tail, r, 0.2, 'q')
    add(tail, r, 0.2, 'k')
    add(tail, r, 0.2, opaque(r))
    return meta_line('md', key, head, tail, r) .. '\r\n'
end

function gen.ma(r)
    local kind = r:weighted({{4, 'num'}, {2, 'bn'}, {1, 'tv'}, {1, 'tf'}})
    local key = key_of(r, kind)
    local head, tail = {}, {}
    if binary(kind) then
        head[#head + 1] = 'b'
    end
    local vivify = numeric(kind) and r:chance(0.3)
    local no_t = false
    if vivify then
        local n = vivify_ttl(r)
        head[#head + 1] = 'N' .. n
        -- t after a negative N: ttl_underflow.
        no_t = n < 0
        add(head, r, 0.5, 'J' .. r:pick({'0', '5', BIG}))
    end
    local mode = r:chance(0.3) and r:pick({'I', '+', 'D', '-'}) or nil
    add(head, r, 0.4, 'D' .. r:pick({'1', '3', '100', BIG}))
    if mode ~= nil then
        head[#head + 1] = 'M' .. mode
    end
    -- On text keys too: an empty or text value is refused before the CAS.
    add(head, r, 0.15, 'C' .. cas_ref(r))
    -- No E: memcached loses it when the number grows (ma_T_lost).
    add(tail, r, 0.5, 'v')
    add(tail, r, 0.3, 'c')
    if not no_t then
        add(tail, r, 0.3, 't')
    end
    if not binary(kind) then
        add(tail, r, 0.2, 'k')
    end
    add(tail, r, 0.2, opaque(r))
    add(tail, r, 0.15, 'q')
    return meta_line('ma', key, head, tail, r) .. '\r\n'
end

-- Text keys only: me of a number shows fetch= (arith_pad_race), and bt
-- would need b (me_b_ignored).
function gen.me(r)
    return ('me %s\r\n'):format(key_of(r, r:pick({'tv', 'tf'})))
end

function gen.misc(r)
    return r:weighted({
        {4, 'mn\r\n'},
        {2, 'version\r\n'},
        {1, 'flush_all\r\n'},
        {1, 'flush_all noreply\r\n'},
        {1, 'verbosity 1\r\n'},
    })
end

-- Malformed commands that memcached answers like contrib, or whose
-- differences have auto fixes in the allowlist.
function gen.malformed(r)
    local key = key_of(r, r:pick(PLAIN))
    return r:pick({
        'bogus ' .. key .. '\r\n',
        'mg ' .. key .. ' v v\r\n',
        'mg ' .. key .. ' Tabc\r\n',
        'incr ' .. key .. ' abc\r\n',
        'touch ' .. key .. ' abc\r\n',
        'gat abc ' .. key .. '\r\n',
        'ma ' .. key .. ' Dx\r\n',
        'ms ' .. key .. ' 2 MX\r\nab\r\n',
        'set ' .. key .. ' 0 0 3\r\nabcd\r\n',
        'set ' .. key .. ' abc 0 1\r\nx\r\n',
        'get {key:251}\r\n',
        -- Command names are case-sensitive: ERROR.
        'GET ' .. key .. '\r\n',
        'Gets ' .. key .. '\r\n',
        'SET ' .. key .. ' 0 0 1\r\nxy\r\n',
        'DELETE ' .. key .. ' noreply\r\n',
        'INCR ' .. key .. ' 1\r\n',
        'Flush_all\r\n',
    })
end

local WEIGHTS = {
    {22, 'storage'},
    {16, 'retrieval'},
    {6, 'arith'},
    {3, 'touch'},
    {3, 'delete'},
    {15, 'mg'},
    {13, 'ms'},
    {6, 'md'},
    {8, 'ma'},
    {1, 'me'},
    {2, 'misc'},
}

local function gen_command(r, ctx)
    return gen[r:weighted(WEIGHTS)](r, ctx)
end

-- An episode: a list of steps, each a list of commands. A malformed
-- command is a step of its own: the fixes in the allowlist align reply
-- lines one divergence at a time, and a failing get
-- culls memcached's unsent replies of its batch (get_error_drops_pipeline).
local function gen_episode(r, n, ctx)
    local steps = {}
    for i = 1, n do
        if r:chance(0.04) then
            steps[i] = {gen.malformed(r)}
        else
            local cmds = {}
            for j = 1, r:int(1, 4) do
                cmds[j] = gen_command(r, ctx)
            end
            steps[i] = cmds
        end
    end
    return steps
end

--
-- Running and shrinking.
--

local function scenario(name, steps)
    local sc = {name = name, steps = {}}
    for i, cmds in ipairs(steps) do
        sc.steps[i] = table.concat(cmds)
    end
    return sc
end

-- Replay on a fresh state; returns the unexplained mismatches.
local function replay(servers, steps)
    local tr = harness.run(servers, scenario('fuzz replay', steps))
    return harness.compare(tr, {record = false}), tr
end

-- ddmin, complement removal at increasing granularity: the result fails
-- and no single element can be removed (within the budget).
local function ddmin(items, fails, budget)
    local n = 2
    while #items >= 2 and budget.left > 0 do
        local size = math.ceil(#items / n)
        local reduced = false
        for i = 0, n - 1 do
            local lo, hi = i * size + 1, math.min((i + 1) * size, #items)
            if lo > #items or budget.left <= 0 then
                break
            end
            local rest = {}
            for j = 1, #items do
                if j < lo or j > hi then
                    rest[#rest + 1] = items[j]
                end
            end
            if #rest > 0 and fails(rest) then
                items = rest
                n = math.max(n - 1, 2)
                reduced = true
                break
            end
        end
        if not reduced then
            if n >= #items then
                break
            end
            n = math.min(#items, n * 2)
        end
    end
    return items
end

local function shrink(servers, steps)
    local budget = {left = SHRINK_BUDGET}
    local function fails(candidate)
        budget.left = budget.left - 1
        return #replay(servers, candidate) > 0
    end
    if not fails(steps) then
        return steps,
            'did not reproduce on a fresh state (state or timing '
                .. 'dependent): showing the original prefix'
    end
    steps = ddmin(steps, fails, budget)
    -- Then one command per step, if the pipelining doesn't matter.
    local flat = {}
    for _, cmds in ipairs(steps) do
        for _, c in ipairs(cmds) do
            flat[#flat + 1] = {c}
        end
    end
    if #flat > #steps and budget.left > 0 and fails(flat) then
        steps = ddmin(flat, fails, budget)
    end
    return steps, budget.left <= 0 and 'the shrink budget ran out' or nil
end

local function show_steps(steps)
    local out = {}
    for i, cmds in ipairs(steps) do
        out[i] = ('  %2d: %s'):format(i, harness.show(table.concat(cmds), 300))
    end
    return table.concat(out, '\n')
end

local function run_seed(servers, seed)
    local r = new_rng(seed)
    local done, episode, commands = 0, 0, 0
    while done < ITERS do
        episode = episode + 1
        local n = math.min(EPISODE, ITERS - done)
        -- E and I (ms stale sets) don't mix: E literals sit between the two
        -- servers' CAS ranges, so "older than" differs (see explicit_cas).
        local with_e = r:chance(0.5)
        local ctx = {mode = {E = with_e, I = not with_e}}
        local steps = gen_episode(r, n, ctx)
        for _, cmds in ipairs(steps) do
            commands = commands + #cmds
        end
        done = done + n
        local name = ('fuzz seed %d episode %d'):format(seed, episode)
        local tr = harness.run(servers, scenario(name, steps))
        local bad = harness.compare(tr)
        if #bad > 0 then
            local prefix = {}
            for i = 1, bad[1].index do
                prefix[i] = steps[i]
            end
            local min, note = shrink(servers, prefix)
            local mism, rtr = replay(servers, min)
            local details = {}
            for _, m in ipairs(mism) do
                details[#details + 1] = harness.format_mismatch(rtr, m)
            end
            if #details == 0 then
                details[1] = harness.format_mismatch(tr, bad[1])
            end
            error(
                ('fuzz mismatch: seed %d, episode %d, step %d '
                    .. '(reproduce: MCC_COMPAT_FUZZ_SEED=%d '
                    .. 'MCC_COMPAT_FUZZ_ITERS=%d)%s\nminimal reproducer '
                    .. '(%d steps, templates as sent):\n%s\n%s'):format(
                        seed,
                        episode,
                        bad[1].index,
                        seed,
                        done,
                        note and ('; ' .. note) or '',
                        #min,
                        show_steps(min),
                        table.concat(details, '\n')
                    ),
                0
            )
        end
    end
    return {episodes = episode, steps = done, commands = commands}
end

g.before_all(function(cg)
    harness.report = {allowed = {}, stats = {}, stale = {}}
    cg.servers, cg.skip = harness.start_servers()
    cg.stats = {}
end)

g.after_all(function(cg)
    harness.stop_servers(cg.servers)
    for _, line in ipairs(cg.stats) do
        print(line)
    end
    print((harness.summary():gsub('^compat summary', 'compat fuzz summary')))
end)

for _, seed in ipairs(SEEDS) do
    g['test_seed_' .. seed] = function(cg)
        t.skip_if(cg.servers == nil, cg.skip)
        local res = run_seed(cg.servers, seed)
        cg.stats[#cg.stats + 1] = ('fuzz seed %d: %d steps, %d commands, '
            .. '%d episodes'):format(
                seed,
                res.steps,
                res.commands,
                res.episodes
            )
    end
end

-- The PRNG and the shrinker, without servers.
g.test_rng_deterministic = function()
    local a, b = new_rng(42), new_rng(42)
    for _ = 1, 100 do
        t.assert_equals(a:next(), b:next())
    end
    local r = new_rng(7)
    for _ = 1, 1000 do
        local v = r:int(3, 5)
        t.assert(v >= 3 and v <= 5)
    end
end

g.test_ddmin = function()
    local items = {}
    for i = 1, 40 do
        items[i] = i
    end
    -- Fails when both 7 and 31 are present.
    local function fails(list)
        local has7, has31 = false, false
        for _, v in ipairs(list) do
            has7 = has7 or v == 7
            has31 = has31 or v == 31
        end
        return has7 and has31
    end
    t.assert_equals(ddmin(items, fails, {left = 1000}), {7, 31})
end
