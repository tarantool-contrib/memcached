-- Layer 3: memcached's own t/*.t tests, run against tarantool-contrib/memcached
-- through the memcached-debug shim (test/compat/shim/).
--
-- A temporary build directory gets a copy of memcached's t/, its timedrun
-- helper and the shim, then `prove -v --merge t/<file>.t` runs there for
-- each file in FILES. The TAP output is parsed, and the failures must be
-- exactly the ones listed in memcached_t_expected.lua: a new failure fails
-- the test; an expected failure that passes is only reported.
--
-- Environment:
--   MCC_COMPAT_MEMCACHED_SRC  memcached source tree with t/ and timedrun
--                             (default /opt/memcached-src, from the image)
--   MCC_COMPAT_T_FILES        space-separated subset of FILES to run
--   MCC_COMPAT_T_REFERENCE    path of a real memcached binary to run the
--                             same files against instead of the shim, for
--                             classifying failures (e.g.
--                             /opt/memcached-src/memcached-debug); the
--                             results are reported, not checked
--   MCC_COMPAT_T_VERBOSE      print the whole TAP output of each file
--   MCC_COMPAT_T_TIMEOUT      seconds per file (default 900)

local fio = require('fio')
local fiber = require('fiber')
local popen = require('popen')
local t = require('luatest')

local env = require('test.helpers.env')
local expected = require('test.compat.memcached_t_expected')

local g = t.group('compat_memcached_t')

local SRC = os.getenv('MCC_COMPAT_MEMCACHED_SRC') or '/opt/memcached-src'
local REFERENCE = os.getenv('MCC_COMPAT_T_REFERENCE')
local VERBOSE = os.getenv('MCC_COMPAT_T_VERBOSE') ~= nil
local TIMEOUT = tonumber(os.getenv('MCC_COMPAT_T_TIMEOUT')) or 900

-- Text protocol tests. Skipped on purpose: binary protocol, extstore,
-- proxy, TLS, SASL, slab/LRU/memory internals and command line validation
-- (item_size_max.t, dash-M.t, ...), which have no contrib counterpart.
local FILES = {
    'getset',
    'cas',
    'incrdecr',
    'noreply',
    'flush-all',
    'expirations',
    'flags',
    'multiversioning',
    'metaget',
    'touch',
    'getandtouch',
    'bogus-commands',
    'malicious-commands',
    'issue_3',
    'issue_70',
    'issue_104',
    'issue_108',
    'issue_152',
    'issue_68',
    'issue_192',
}

local function find_exe(name)
    for dir in (os.getenv('PATH') or ''):gmatch('[^:]+') do
        local path = fio.pathjoin(dir, name)
        if fio.path.exists(path) then
            return path
        end
    end
    return nil
end

local function shell_quote(s)
    return "'" .. tostring(s):gsub("'", "'\\''") .. "'"
end

-- Run a shell command; returns its stdout and stderr merged, and whether
-- it finished in time.
local function run(cmd, extra_env, timeout)
    local envs = os.environ() -- luacheck: ignore 143 (a Tarantool extension)
    for k, v in pairs(extra_env or {}) do
        envs[k] = v
    end
    local ph = assert(
        popen.new({'/bin/sh', '-c', cmd .. ' 2>&1'}, {
            stdin = popen.opts.DEVNULL,
            stdout = popen.opts.PIPE,
            stderr = popen.opts.DEVNULL,
            env = envs,
            setsid = true,
            group_signal = true,
            close_fds = true,
        })
    )
    local parts = {}
    local deadline = fiber.clock() + timeout
    local in_time = true
    while true do
        local left = deadline - fiber.clock()
        if left <= 0 then
            in_time = false
            break
        end
        local chunk = ph:read({timeout = math.min(left, 5)})
        if chunk == '' then
            break
        elseif chunk ~= nil then
            parts[#parts + 1] = chunk
        end
        -- nil: a read timeout, keep waiting until the deadline.
    end
    if not in_time then
        ph:kill()
    end
    ph:wait()
    ph:close()
    return table.concat(parts), in_time
end

-- Parse prove -v --merge output into
-- {plan, skip_all, results = {[n] = {ok, skip, desc, diag}}, notes, max}.
-- A subtest prints its own indented TAP before the parent's result line:
-- that goes into the parent's diag when the parent fails.
local function parse_tap(out)
    local res = {results = {}, notes = {}, max = 0}
    local cur
    local sub = {}
    for line in (out .. '\n'):gmatch('([^\n]*)\n') do
        line = line:gsub('\r$', '')
        -- prove prints "t/x.t .. " before the first TAP line.
        line = line:gsub('^t/[%w_%-]+%.t %.%.+ ?', '')
        local plan, skip = line:match(
            '^1%.%.(%d+)%s*#%s*[Ss][Kk][Ii][Pp]%s*(.*)$'
        )
        if plan == nil then
            plan = line:match('^1%.%.(%d+)%s*$')
        end
        local notok, num, rest = line:match('^(n?o?t? ?)ok (%d+)(.*)$')
        if plan ~= nil then
            res.plan = tonumber(plan)
            if skip ~= nil and res.plan == 0 then
                res.skip_all = skip ~= '' and skip or 'skipped'
            end
            cur = nil
        elseif num ~= nil and (notok == '' or notok == 'not ') then
            num = tonumber(num)
            local desc = rest:gsub('^%s*%-?%s*', '')
            local directive = desc:match('#%s*(%a+)')
            directive = directive and directive:upper()
            local ok = notok == '' or directive == 'TODO'
            local entry = {
                ok = ok,
                desc = desc,
                diag = {},
                skip = directive == 'SKIP',
            }
            if not ok then
                for _, l in ipairs(sub) do
                    if not l:match('^%s+ok ') then
                        table.insert(entry.diag, l)
                    end
                end
            end
            sub = {}
            res.results[num] = entry
            if num > res.max then
                res.max = num
            end
            cur = (not ok) and entry or nil
        elseif line:match('^%s+%S') then
            sub[#sub + 1] = line
        elseif line:match('^#') then
            local text = line:gsub('^#%s?', '')
            if cur ~= nil then
                table.insert(cur.diag, text)
            elseif text:match('Looks like') then
                table.insert(res.notes, text)
            end
        elseif line ~= '' and line ~= 'ok' then
            -- Perl dies, server logs and prove's summary.
            table.insert(res.notes, line)
            cur = nil
        end
    end
    return res
end

-- Expected failures of a file: test number -> entry.
local function expected_map(file)
    local map = {}
    local spec = expected[file]
    if spec == nil then
        return map
    end
    for _, e in ipairs(spec.failures or {}) do
        local list = {}
        if e.test ~= nil then
            list[#list + 1] = e.test
        end
        for _, n in ipairs(e.tests or {}) do
            list[#list + 1] = n
        end
        for _, r in ipairs(e.ranges or {e.range}) do
            for n = r[1], r[2] do
                list[#list + 1] = n
            end
        end
        for _, n in ipairs(list) do
            map[n] = e
        end
    end
    return map
end

local report = {}

local function summarize(file, tap, in_time)
    local total = tap.plan or tap.max
    if total < tap.max then
        total = tap.max
    end
    local failed, passed, skipped = {}, 0, 0
    for n = 1, total do
        local r = tap.results[n]
        if r == nil then
            failed[#failed + 1] = {num = n, desc = '<not run>', diag = {}}
        elseif not r.ok then
            failed[#failed + 1] = {num = n, desc = r.desc, diag = r.diag}
        elseif r.skip then
            skipped = skipped + 1
        else
            passed = passed + 1
        end
    end
    local rec = {
        file = file,
        total = total,
        passed = passed,
        skipped = skipped,
        failed = failed,
        in_time = in_time,
        skip_all = tap.skip_all,
        notes = tap.notes,
        unexpected = {},
        fixed = {},
        expected = 0,
    }
    local map = expected_map(file)
    local seen = {}
    for _, f in ipairs(failed) do
        local e = map[f.num]
        seen[f.num] = true
        if e == nil then
            rec.unexpected[#rec.unexpected + 1] = f
        else
            rec.expected = rec.expected + 1
            f.entry = e
        end
    end
    for n in pairs(map) do
        if not seen[n] and n <= total then
            rec.fixed[#rec.fixed + 1] = n
        end
    end
    table.sort(rec.fixed)
    report[#report + 1] = rec
    return rec
end

local function format_failure(f)
    local lines = {('    not ok %d - %s'):format(f.num, f.desc)}
    for i, d in ipairs(f.diag) do
        if i > 12 then
            lines[#lines + 1] = '      ...'
            break
        end
        lines[#lines + 1] = '      # ' .. d
    end
    return table.concat(lines, '\n')
end

g.before_all(function(cg)
    if not fio.path.is_dir(fio.pathjoin(SRC, 't'))
        or not fio.path.exists(fio.pathjoin(SRC, 'timedrun')) then
        cg.skip = ('%s/t or %s/timedrun is missing: build the image from '
            .. 'test/compat/Dockerfile'):format(SRC, SRC)
        return
    end
    if find_exe('perl') == nil or find_exe('prove') == nil then
        cg.skip = 'perl or prove is missing'
        return
    end
    local tarantool = arg[-1]
    if tarantool == nil or not tarantool:find('/', 1, true) then
        tarantool = find_exe(tarantool or 'tarantool') or 'tarantool'
    end
    local dir = fio.tempdir()
    cg.builddir = dir
    local out = run(
        ('cp -r %s %s && ln -s %s %s && mkdir %s'):format(
            shell_quote(fio.pathjoin(SRC, 't')),
            shell_quote(dir .. '/t'),
            shell_quote(fio.pathjoin(SRC, 'timedrun')),
            shell_quote(dir .. '/timedrun'),
            shell_quote(dir .. '/tmp')
        ),
        nil,
        60
    )
    -- The memcached-debug in the build directory: the shim, or for a
    -- reference run a wrapper around a real memcached.
    local name, subs = 'memcached-debug', {['@MCC_ROOT@'] = env.root}
    if REFERENCE ~= nil then
        name, subs = 'memcached-ref', {['@REF@'] = shell_quote(REFERENCE)}
    end
    local fh = assert(
        io.open(
            fio.pathjoin(
                env.root,
                'test',
                'compat',
                'shim',
                name
            ),
            'r'
        )
    )
    local text = fh:read('*a')
    fh:close()
    for from, to in pairs(subs) do
        text = text:gsub(from, function() return to end)
    end
    local exe = dir .. '/memcached-debug'
    fh = assert(io.open(exe, 'w'))
    fh:write(text)
    fh:close()
    assert(fio.chmod(exe, tonumber('755', 8)))
    assert(fio.path.exists(dir .. '/t/lib/MemcachedTest.pm'), out)
    cg.env = {
        MCC_ROOT = env.root,
        MCC_TARANTOOL = tarantool,
        TMPDIR = dir .. '/tmp',
    }
end)

g.after_all(function(cg)
    if cg.builddir ~= nil then
        fio.rmtree(cg.builddir)
    end
    local lines = {
        ('memcached t/*.t against %s:'):format(
            REFERENCE or 'tarantool-contrib/memcached (shim)'
        ),
    }
    lines[#lines + 1] = ('  %-20s %13s %7s %8s %10s  %s'):format(
        'file',
        'passed',
        'failed',
        'skipped',
        'unexpected',
        'note'
    )
    local rank = {}
    for i, file in ipairs(FILES) do
        rank[file] = i
    end
    table.sort(report, function(a, b) return rank[a.file] < rank[b.file] end)
    local sum_pass, sum_total = 0, 0
    for _, rec in ipairs(report) do
        local note = rec.skip_all and ('skip: ' .. rec.skip_all)
            or (not rec.in_time and 'TIMEOUT')
            or ''
        if #rec.fixed > 0 then
            note = note
                .. (' expected failures passed: %s'):format(
                    table.concat(rec.fixed, ',')
                )
        end
        lines[#lines + 1] = ('  %-20s %6d/%-6d %7d %8d %10d  %s'):format(
            rec.file,
            rec.passed,
            rec.total,
            #rec.failed,
            rec.skipped,
            #rec.unexpected,
            note
        )
        sum_pass = sum_pass + rec.passed
        sum_total = sum_total + rec.total
    end
    lines[#lines + 1] = ('  total: %d/%d passed'):format(sum_pass, sum_total)
    print(table.concat(lines, '\n'))
end)

local selected = {}
for name in (os.getenv('MCC_COMPAT_T_FILES') or ''):gmatch('%S+') do
    selected[name:gsub('%.t$', '')] = true
end

for _, file in ipairs(FILES) do
    if next(selected) == nil or selected[file] then
        g['test_' .. file:gsub('[^%w]', '_')] = function(cg)
            t.skip_if(cg.skip ~= nil, cg.skip)
            local cmd = ('cd %s && exec prove -v --merge t/%s.t'):format(
                shell_quote(cg.builddir),
                file
            )
            local out, in_time = run(cmd, cg.env, TIMEOUT)
            if VERBOSE then
                print(out)
            end
            local tap = parse_tap(out)
            local rec = summarize(file, tap, in_time)
            local msg = {
                ('%s.t: %d/%d passed, %d failed (%d expected)'):format(
                    file,
                    rec.passed,
                    rec.total,
                    #rec.failed,
                    rec.expected
                ),
            }
            -- The expected failures, one line per allowlist entry.
            local groups, order = {}, {}
            for _, f in ipairs(rec.failed) do
                if f.entry ~= nil then
                    local grp = groups[f.entry]
                    if grp == nil then
                        grp = {}
                        groups[f.entry] = grp
                        order[#order + 1] = f.entry
                    end
                    grp[#grp + 1] = f.num
                end
            end
            for _, e in ipairs(order) do
                local nums = groups[e]
                local which = #nums == 1 and tostring(nums[1])
                    or ('%d..%d (%d)'):format(
                        nums[1],
                        nums[#nums],
                        #nums
                    )
                msg[#msg + 1] = ('  expected not ok %s (%s): %s'):format(
                    which,
                    e.class,
                    e.reason
                )
            end
            if #rec.fixed > 0 then
                msg[#msg + 1] = '  expected failures that passed: '
                    .. table.concat(rec.fixed, ', ')
            end
            if #rec.unexpected > 0 then
                msg[#msg + 1] = '  unexpected failures:'
                for i, f in ipairs(rec.unexpected) do
                    if i > 40 then
                        msg[#msg + 1] = ('    ... and %d more'):format(
                            #rec.unexpected - 40
                        )
                        break
                    end
                    msg[#msg + 1] = format_failure(f)
                end
                for i, n in ipairs(rec.notes) do
                    if i > 20 then
                        break
                    end
                    msg[#msg + 1] = '  | ' .. n
                end
            end
            if REFERENCE ~= nil or #rec.unexpected == 0 or not in_time then
                print(table.concat(msg, '\n'))
            end
            if REFERENCE ~= nil then
                return
            end
            t.assert(
                in_time,
                ('%s.t timed out after %d s'):format(
                    file,
                    TIMEOUT
                )
            )
            t.assert_equals(#rec.unexpected, 0, table.concat(msg, '\n'))
        end
    end
end
