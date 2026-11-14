#!/usr/bin/env python3
"""Build patched copies of a tarantool-contrib/memcached tree, one per Lua-level fix.

usage: python3 -I bench/variants/make.py <src tree> <dst dir> [variant...]

Each variant is <dst dir>/<name>, a copy of <src tree> with the patches of
the fix applied. 'all' applies every fix. The patches are textual: a patch
whose old text is not found (the code changed) makes the variant fail
loudly instead of silently measuring the unpatched code.

The patches are prototypes for measuring, not production code: some of them
drop a feature (nola) or assume a single instance without flushes (nobatch);
see FIXES. GC tuning needs no patch: run.sh with BENCH_GC_PAUSE.
"""

import os
import shutil
import sys

ENGINE = 'memcached/engine.lua'
SERVER = 'memcached/server.lua'
TEXT = 'memcached/proto/text.lua'

# name -> (description, [(file, old, new)])
FIXES = {}

FIXES['nola'] = ('no last-access/fetched write on the first get after a '
                 'store (meta h/l bookkeeping off)', [
    (ENGINE,
     "    if req.bump_la ~= false and not req.no_lru_bump and not env.ro and\n",
     "    if false and req.bump_la ~= false and not req.no_lru_bump and\n"
     "       not env.ro and\n"),
])

FIXES['notxn'] = ('plain gets run without box.atomic (each write they may '
                  'do is a single autocommit statement)', [
    (ENGINE,
     "    local ok, res = pcall(box.atomic, fn, env, req)\n",
     "    local ok, res\n"
     "    if optional_writes(req) then\n"
     "        ok, res = pcall(fn, env, req)\n"
     "    else\n"
     "        ok, res = pcall(box.atomic, fn, env, req)\n"
     "    end\n"),
])

FIXES['nobatch'] = ('no _mcc_meta get (flush time) and box.info.ro per '
                    'batch: the upper bound of caching them', [
    (ENGINE, "        flush = schema.flush_time(space_name),\n",
     "        flush = nil,\n"),
    (ENGINE, "        ro = box.info.ro,\n", "        ro = false,\n"),
])

_RUN_PENDING_OLD = """    local function run_pending()
        local count = #pending
        if count == 0 then
            return
        end
"""
_RUN_PENDING_END = """        pending, owners, subs = {}, {}, {}
    end

"""


def _noclosure(text):
    """Hoist run_pending out of process(): FNEW/UCLO abort the traces."""
    start = text.index(_RUN_PENDING_OLD)
    end = text.index(_RUN_PENDING_END, start) + len(_RUN_PENDING_END)
    body = text[start + len(_RUN_PENDING_OLD):end - len(_RUN_PENDING_END)]
    body = '\n'.join(line[4:] if line.startswith('    ') else line
                     for line in body.split('\n'))
    func = ("local function run_pending(srv, stats, resps, pending, owners,\n"
            "                           subs)\n"
            "    local count = #pending\n"
            "    if count == 0 then\n"
            "        return pending, owners, subs\n"
            "    end\n" + body + "    return {}, {}, {}\nend\n\n")
    text = text[:start] + text[end:]
    call = ("pending, owners, subs = run_pending(srv, stats, resps, "
            "pending,\n{indent}    owners, subs)")
    text = text.replace("                    run_pending()\n",
                        "                    " + call.format(
                            indent=' ' * 20) + "\n", 1)
    text = text.replace("    run_pending()\n",
                        "    " + call.format(indent=' ' * 4) + "\n", 1)
    marker = "-- Execute a batch of parsed commands."
    text = text.replace(marker, func + marker, 1)
    return text


FIXES['noclosure'] = ('process() without the per-batch run_pending closure '
                      '(lets the JIT compile process and the encoders)', [
    (SERVER, _noclosure, None),
])

FIXES['ibuf'] = ('raw ibuf pointer arithmetic instead of ibuf:size(), '
                 ':unused(), :capacity(), :alloc(), :reset() in the '
                 'connection loop', [
    (SERVER, "local concat = table.concat\n",
     "local concat = table.concat\n\n"
     "local function ibuf_used(b) return tonumber(b.wpos - b.rpos) end\n"
     "local function ibuf_unused(b) return tonumber(b.epos - b.wpos) end\n"
     "local function ibuf_capacity(b) return tonumber(b.epos - b.buf) end\n"),
    (SERVER, "    local missing = conn.need - ibuf:size()\n",
     "    local missing = conn.need - ibuf_used(ibuf)\n"),
    (SERVER, "        local n = sock:sysread(ibuf.wpos, ibuf:unused())\n",
     "        local n = sock:sysread(ibuf.wpos, ibuf_unused(ibuf))\n"),
    (SERVER, "            ibuf:alloc(n)\n", "            ibuf.wpos = ibuf.wpos + n\n"),
    (SERVER, "    if ibuf:size() == 0 then\n        if ibuf:capacity() > M.BUFFER_KEEP then\n",
     "    if ibuf_used(ibuf) == 0 then\n        if ibuf_capacity(ibuf) > M.BUFFER_KEEP then\n"),
    (SERVER, "            ibuf:reset()\n",
     "            ibuf.rpos = ibuf.buf\n            ibuf.wpos = ibuf.buf\n"),
    (SERVER, "        local count = math.min(conn.skip, ibuf:size())\n",
     "        local count = math.min(conn.skip, ibuf_used(ibuf))\n"),
    (SERVER, "    local size = ibuf:size()\n", "    local size = ibuf_used(ibuf)\n"),
])

FIXES['waitfirst'] = ('after a reply drained the input, wait for '
                      'readability before read(): no EAGAIN read per '
                      'request', [
    (SERVER, "    local missing = conn.need - ",
     "    if conn.wrote then\n"
     "        conn.wrote = false\n"
     "        if not conn.closed then\n"
     "            conn.sock:readable()\n"
     "        end\n"
     "    end\n"
     "    local missing = conn.need - "),
    (SERVER, "    if out ~= '' and not write_all(conn, out) then\n"
             "        return false\n"
             "    end\n",
     "    if out ~= '' and not write_all(conn, out) then\n"
     "        return false\n"
     "    end\n"
     "    conn.wrote = true\n"),
])

FIXES['parse'] = ('text parser fast path for "get <key>": no line copy, '
                  'split, token table or lower()', [
    (TEXT, "    local tokens, n = split(sub(buf, pos, line_end))\n",
     "    -- Fast path: \"get <key>\".\n"
     "    if byte(buf, pos) == 103 and byte(buf, pos + 1) == 101 and\n"
     "       byte(buf, pos + 2) == 116 and byte(buf, pos + 3) == 32 then\n"
     "        local sp = find(buf, ' ', pos + 4, true)\n"
     "        if sp == nil or sp > line_end then\n"
     "            local key = sub(buf, pos + 4, line_end)\n"
     "            if key ~= '' and valid_key(key) then\n"
     "                return {op = 'retrieve', cmd = 'get', with_cas = false,\n"
     "                        multi = {{op = 'get', key = key}}}, nl + 1\n"
     "            end\n"
     "        end\n"
     "    end\n"
     "    local tokens, n = split(sub(buf, pos, line_end))\n"),
])

_ENC_OLD = """local function encode_retrieve(req, resps)
    local out = {}
    for _, resp in ipairs(resps) do
        if resp.status == 'SERVER_ERROR' then
            -- Partial results are dropped, as memcached does on errors.
            return server_error(resp)
        end
        if resp.status == 'HIT' then
            local item = resp.item
            local value = item.value
            local line = 'VALUE ' .. item.key .. ' ' .. u64str(item.flags) ..
                         ' ' .. #value
            if req.with_cas then
                line = line .. ' ' .. u64str(item.cas)
            end
            insert(out, line .. '\\r\\n')
            insert(out, value)
            insert(out, '\\r\\n')
        end
    end
    insert(out, 'END\\r\\n')
    return concat(out)
end
"""
_ENC_NEW = """local function encode_retrieve(req, resps)
    local out = {}
    local n = 0
    local with_cas = req.with_cas
    for i = 1, #resps do
        local resp = resps[i]
        local status = resp.status
        if status == 'SERVER_ERROR' then
            -- Partial results are dropped, as memcached does on errors.
            return server_error(resp)
        end
        if status == 'HIT' then
            local item = resp.item
            local value = item.value
            out[n + 1] = 'VALUE '
            out[n + 2] = item.key
            out[n + 3] = ' '
            out[n + 4] = u64str(item.flags)
            out[n + 5] = ' '
            out[n + 6] = #value
            n = n + 6
            if with_cas then
                out[n + 1] = ' '
                out[n + 2] = u64str(item.cas)
                n = n + 2
            end
            out[n + 1] = '\\r\\n'
            out[n + 2] = value
            out[n + 3] = '\\r\\n'
            n = n + 3
        end
    end
    out[n + 1] = 'END\\r\\n'
    return concat(out)
end
"""

FIXES['enc'] = ('encode_retrieve with one table.concat and no '
                'intermediate strings', [
    (TEXT, _ENC_OLD, _ENC_NEW),
])

ORDER = ['nola', 'notxn', 'nobatch', 'noclosure', 'ibuf', 'waitfirst',
         'parse', 'enc']


def apply(tree, name):
    for path, old, new in FIXES[name][1]:
        p = os.path.join(tree, path)
        with open(p) as f:
            text = f.read()
        if callable(old):
            patched = old(text)
        else:
            if old not in text:
                raise SystemExit('%s: patch for %s does not apply: %r' % (
                    name, path, old[:60]))
            patched = text.replace(old, new, 1)
        if patched == text:
            raise SystemExit('%s: patch for %s changed nothing' % (name, path))
        with open(p, 'w') as f:
            f.write(patched)


def build(src, dst, name):
    tree = os.path.join(dst, name)
    if os.path.exists(tree):
        shutil.rmtree(tree)
    shutil.copytree(src, tree, symlinks=True,
                    ignore=shutil.ignore_patterns('.git'))
    for fix in (ORDER if name == 'all' else [name]):
        apply(tree, fix)
    return tree


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        for name in ORDER:
            print('  %-10s %s' % (name, FIXES[name][0]))
        sys.exit(1)
    src, dst = sys.argv[1], sys.argv[2]
    names = sys.argv[3:] or ORDER + ['all']
    os.makedirs(dst, exist_ok=True)
    for name in names:
        print(build(src, dst, name))


if __name__ == '__main__':
    main()
