#!/usr/bin/env python3
"""Group a profile.sh run into layers.

usage: python3 -I bench/jitp_categories.py <profile.sh output dir> [src]

src is the tarantool-contrib/memcached tree that was profiled: jit.p names anonymous
and some local functions by their definition line ("text.lua:430"), and the
script looks the names up there.

Reads:
- prof/jitp-v*.txt: jit.p VM states;
- prof/jitp-F*.txt: jit.p functions ("NN%  file:function"), grouped into
  layers by RULES below;
- sysprof.report.json: misc.sysprof samples per VM state;
- plain.{before,after}.json, plain.json, plain.cpu: misc.getmetrics()
  deltas and the throughput of the run without a profiler.

jit.p charges the time spent in C (box, syscalls, the event loop) to the Lua
function that runs next: the caller of a C function, or the function that
resumes after a yield. So "box API" includes the C work of box, and "socket
I/O" includes the syscalls and the event loop.
"""

import glob
import json
import os
import re
import sys

# (file, function regex, layer). The first match wins. Files are base names
# as jit.p prints them; Tarantool's built-in modules have the same base
# names as their sources (box/lua/schema.lua -> schema.lua).
RULES = [
    ('text.lua', r'^(encode|encode_retrieve|encode_stats|server_error|'
                 r'client_error|stat_value|u64str)$', 'encoder (text)'),
    ('text.lua', r'.*', 'parser (text)'),
    ('meta.lua', r'^encode', 'encoder (meta)'),
    ('meta.lua', r'.*', 'parser (meta)'),
    ('server.lua', r'^(read_more|write_all|would_block)$',
     'socket I/O, event loop (Lua side + C)'),
    ('server.lua', r'^(consume)$', 'input buffer (ibuf, copies)'),
    ('server.lua', r'^(conn_step)$',
     'conn_step: input copy + parse loop'),
    ('server.lua', r'^(process|run_pending|account|inc|frontend)$',
     'batching, dispatch, stats'),
    ('server.lua', r'.*', 'transport, other'),
    ('local.lua', r'.*', 'batching, dispatch, stats'),
    ('server.lua', r'^(conn_main|M\.conn_step)$',
     'conn_step: input copy + parse loop'),
    ('engine.lua', r'^(exec|M\.exec|tokens_of)$', 'engine: per-batch setup'),
    ('engine.lua', r'^(run|optional_writes)$',
     'engine: per-request txn (pcall + box.atomic)'),
    ('engine.lua', r'.*', 'engine: command logic'),
    ('schema.lua', r'^(flush_time|flush_key)$', 'engine: per-batch setup'),
    ('schema.lua', r'^(atomic|atomic_tail|begin|commit|rollback)$',
     'box txn (begin/commit)'),
    ('schema.lua', r'.*', 'box API: space/index calls (Lua + C)'),
    ('tuple.lua', r'.*', 'box API: tuple field access'),
    ('msgpackffi.lua', r'.*', 'box API: space/index calls (Lua + C)'),
    ('socket.lua', r'^(syswrite|sysread)$',
     'socket syscalls (read/write, kernel TCP)'),
    ('socket.lua', r'.*', 'socket I/O, event loop (Lua side + C)'),
    ('fiber.lua', r'.*', 'socket I/O, event loop (Lua side + C)'),
    ('utils.lua', r'.*', 'box API: space/index calls (Lua + C)'),
    ('buffer.lua', r'.*', 'input buffer (ibuf, copies)'),
    ('init.lua', r'.*', 'transport, other'),
]


def parse_jitp(path):
    """[(percent, label)] from a one-level jit.p profile."""
    rows = []
    with open(path) as f:
        for line in f:
            m = re.match(r'^\s*(\d+)%\s+(.*\S)\s*$', line)
            if m:
                rows.append((int(m.group(1)), m.group(2)))
    return rows


def parse_jitp_two_level(path):
    """{primary: (percent, [(percent, label)])} from a two-level profile."""
    res = {}
    cur = None
    with open(path) as f:
        for line in f:
            m = re.match(r'^\s*(\d+)%\s+(.*\S)\s*$', line)
            if m and '-- ' not in line:
                cur = m.group(2)
                res[cur] = (int(m.group(1)), [])
                continue
            m = re.match(r'^\s+--\s+(\d+)%\s+(.*\S)\s*$', line)
            if m and cur is not None:
                res[cur][1].append((int(m.group(1)), m.group(2)))
    return res


SOURCES = {}


def load_sources(src):
    """base name -> lines, for the Lua files of the profiled tree."""
    for d, _, files in os.walk(src):
        for f in files:
            if f.endswith('.lua') and '/test' not in d:
                with open(os.path.join(d, f)) as fh:
                    SOURCES.setdefault(f, fh.read().splitlines())


def resolve(file, func):
    """A function name for "file:<line>" labels, if the source is known."""
    if not func.isdigit() or file not in SOURCES:
        return func
    lines = SOURCES[file]
    n = int(func)
    if n < 1 or n > len(lines):
        return func
    line = lines[n - 1]
    m = re.search(r'function\s+([\w.:]+)\s*\(', line)
    if m:
        name = m.group(1)
        return name.split('.')[-1] if name.startswith('ops.') else name
    m = re.search(r'(\w+)\s*=\s*function', line)
    if m:
        return m.group(1)
    # An anonymous function: name it after the enclosing named function.
    for k in range(n - 2, -1, -1):
        m = re.match(r'(?:local\s+)?function\s+([\w.:]+)', lines[k])
        if m:
            return m.group(1) + '<closure>'
    return func


def label_name(label):
    if ':' in label:
        file, func = label.rsplit(':', 1)
    else:
        file, func = label, ''
    file = os.path.basename(file.lstrip('@'))
    return file, resolve(file, func)


def layer_of(label):
    file, func = label_name(label)
    func = re.sub(r'^M\.', '', func)
    for rfile, rfunc, layer in RULES:
        if file == rfile and re.search(rfunc, func):
            return layer
    if label.startswith('[') or file == '':
        return 'C / builtins without a Lua frame'
    return 'other: ' + file


def load(path):
    with open(path) as f:
        return json.load(f)


def main():
    root = sys.argv[1]
    prof = os.path.join(root, 'prof')
    if len(sys.argv) > 2:
        load_sources(sys.argv[2])
    us_per_req = None

    plain = os.path.join(root, 'plain.json')
    if os.path.exists(plain):
        stats = load(plain)['ALL STATS']
        ops = stats['Totals']['Ops/sec']
        count = stats['Totals']['Count']
        secs = stats['Runtime']['Total duration'] / 1000.0
        with open(os.path.join(root, 'plain.cpu')) as f:
            c0, c1 = map(float, f.read().split())
        cpu = (c1 - c0) / secs
        print('## Run without a profiler')
        print()
        us_per_req = cpu / ops * 1e6
        print('- %.0f ops/s, server CPU %.2f cores, %.1f us of server CPU '
              'per request' % (ops, cpu, us_per_req))
        b = load(os.path.join(root, 'plain.before.json'))
        a = load(os.path.join(root, 'plain.after.json'))
        mb, ma = b['misc'], a['misc']

        def d(k):
            return ma[k] - mb[k]
        steps = sum(d(k) for k in ma if k.startswith('gc_steps_'))
        print('- Lua allocation: %.0f bytes/request, %.1f MB/s' % (
            d('gc_allocated') / count, d('gc_allocated') / secs / 1e6))
        print('- strings: %.1f new + %.1f found interned per request' % (
            d('strhash_miss') / count, d('strhash_hit') / count))
        print('- GC: %d full cycles (%.1f/s), %.1f incremental steps per '
              '1000 requests, %.0f finalizer steps per 1000 requests' % (
                  d('gc_steps_atomic'), d('gc_steps_atomic') / secs,
                  steps / count * 1000,
                  d('gc_steps_finalize') / count * 1000))
        print('- Lua heap: %.1f MB before, %.1f MB after; tables %+d, '
              'cdata %+d' % (b['lua_mem_kb'] / 1024, a['lua_mem_kb'] / 1024,
                             d('gc_tabnum'), d('gc_cdatanum')))
        print('- JIT: %d trace aborts, %d new traces, %.3f snapshot '
              'restores (trace exits) per request' % (
                  d('jit_trace_abort'), d('jit_trace_num'),
                  d('jit_snap_restore') / count))
        print()

    sp = os.path.join(root, 'sysprof.report.json')
    if os.path.exists(sp):
        rep = load(sp).get('report') or {}
        vm = rep.get('vmstate') or {}
        total = rep.get('samples') or sum(vm.values()) or 1
        print('## misc.sysprof VM states (%d samples)' % total)
        print()
        print('| state | share |')
        print('|---|---:|')
        for k, v in sorted(vm.items(), key=lambda kv: -kv[1]):
            if v:
                print('| %s | %.1f%% |' % (k, v * 100.0 / total))
        print()

    for path in sorted(glob.glob(os.path.join(prof, 'jitp-v*.txt'))):
        if 'F' in os.path.basename(path):
            continue
        print('## jit.p VM states (%s)' % os.path.basename(path))
        print()
        print('| state | share |')
        print('|---|---:|')
        for pct, label in parse_jitp(path):
            print('| %s | %d%% |' % (label, pct))
        print()

    for path in sorted(glob.glob(os.path.join(prof, 'jitp-F*.txt'))):
        rows = parse_jitp(path)
        layers = {}
        for pct, label in rows:
            layer = layer_of(label)
            layers.setdefault(layer, [0, []])
            layers[layer][0] += pct
            layers[layer][1].append((pct, label))
        total = sum(p for p, _ in rows) or 1
        print('## jit.p layers (%s)' % os.path.basename(path))
        print()
        print('Shares are of all the samples (%d%% listed); us/request = '
              'share x server CPU per request of the plain run.' % total)
        print()
        print('| layer | share | us/request | top functions |')
        print('|---|---:|---:|---|')
        for layer, (pct, funcs) in sorted(layers.items(),
                                          key=lambda kv: -kv[1][0]):
            top = ', '.join('%s %d%%' % (':'.join(label_name(lbl)), p)
                            for p, lbl in sorted(funcs, reverse=True)[:4]
                            if p > 0)
            us = '%.1f' % (pct / 100.0 * us_per_req) if us_per_req else '-'
            print('| %s | %d%% | %s | %s |' % (layer, pct, us, top))
        print()

    for path in sorted(glob.glob(os.path.join(prof, 'jitp-vF*.txt'))):
        tl = parse_jitp_two_level(path)
        print('## jit.p functions per VM state (%s)' % os.path.basename(path))
        print()
        for state, (pct, funcs) in sorted(tl.items(), key=lambda kv:
                                          -kv[1][0]):
            top = ', '.join('%s %d%%' % (':'.join(label_name(lbl)), p)
                            for p, lbl in funcs[:6])
            print('- **%s %d%%**: %s' % (state, pct, top))
        print()


if __name__ == '__main__':
    main()
