#!/usr/bin/env python3
"""Summarize bench/run.sh results as Markdown tables.

usage: python3 -I bench/summarize.py <results dir>

For every (wal mode, server, workload) it prints the median over the
repetitions of: total ops/s, p50 and p99 latency of all the requests, the
server CPU use, and for the Tarantool servers the Lua allocation and string
interning per request (misc.getmetrics() deltas).
"""

import glob
import json
import os
import re
import statistics
import sys

WORKLOADS = {
    'w1': '1:10, 100 B, pipeline 1',
    'w2': '1:10, 100 B, pipeline 16',
    'w3': '1:10, 4 KB, pipeline 1',
    'w4': 'get misses, pipeline 1',
    'w5': '1:10, 10-key get, pipeline 1',
    'w6': 'sets only, 100 B, pipeline 16',
}
SERVERS = ['mcc', 'mcc-submit', 'mcc-raw', 'mcc-fake', 'cmod', 'memcached1',
           'memcached4']
SERVER_NAMES = {
    'mcc': 'tarantool-contrib/memcached (Lua)',
    'mcc-submit': 'tarantool-contrib/memcached, commit_wait submit',
    'mcc-raw': 'mcc front-end + bare box calls',
    'mcc-fake': 'mcc front-end, no box',
    'cmod': 'tarantool/memcached (C)',
    'memcached1': 'memcached -t 1',
    'memcached4': 'memcached -t 4',
}


def load(path):
    with open(path) as f:
        return json.load(f)


def client_cpu(log_path):
    """User + system CPU seconds of memtier from bash `times`."""
    with open(log_path) as f:
        lines = [ln for ln in f.read().splitlines() if ln.strip()]
    total = 0.0
    for t in re.findall(r'(\d+)m([\d.]+)s', lines[-1]):
        total += int(t[0]) * 60 + float(t[1])
    return total


def run_stats(prefix):
    res = {}
    stats = load(prefix + '.json')['ALL STATS']
    totals = stats['Totals']
    runtime = stats['Runtime']['Total duration'] / 1000.0
    res['ops'] = totals['Ops/sec']
    res['count'] = totals['Count']
    pct = totals.get('Percentile Latencies', {})
    res['p50'] = pct.get('p50.00')
    res['p99'] = pct.get('p99.00')
    res['keys'] = stats['Gets']['Hits/sec'] + stats['Gets']['Misses/sec'] + \
        stats['Sets']['Ops/sec']
    with open(prefix + '.cpu') as f:
        c0, c1 = map(float, f.read().split())
    res['srv_cpu'] = (c1 - c0) / runtime
    if os.path.exists(prefix + '.io'):
        with open(prefix + '.io') as f:
            r0, w0, r1, w1 = map(float, f.read().split())
        res['sys_per_op'] = (r1 - r0 + w1 - w0) / res['count']
    try:
        res['cli_cpu'] = client_cpu(prefix + '.log') / runtime
    except (OSError, IndexError):
        res['cli_cpu'] = None
    if os.path.exists(prefix + '.after.json'):
        a = load(prefix + '.after.json')
        b = load(prefix + '.before.json')
        ma, mb = a['misc'], b['misc']
        n = res['count']
        res['alloc_per_op'] = (ma['gc_allocated'] - mb['gc_allocated']) / n
        res['strmiss_per_op'] = (ma['strhash_miss'] - mb['strhash_miss']) / n
        res['strhit_per_op'] = (ma['strhash_hit'] - mb['strhash_hit']) / n
        steps = 0
        for k in ma:
            if k.startswith('gc_steps_'):
                steps += ma[k] - mb[k]
        res['gc_steps_per_kop'] = steps / n * 1000
        res['gc_cycles'] = ma['gc_steps_atomic'] - mb['gc_steps_atomic']
        res['lua_mem_growth_kb'] = a['lua_mem_kb'] - b['lua_mem_kb']
        res['trace_abort'] = ma['jit_trace_abort'] - mb['jit_trace_abort']
        res['snap_restore_per_op'] = \
            (ma['jit_snap_restore'] - mb['jit_snap_restore']) / n
    return res


def median(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    return statistics.median(values)


def spread(values):
    values = [v for v in values if v is not None]
    if len(values) < 2:
        return 0.0
    m = statistics.median(values)
    return (max(values) - min(values)) / m * 100 if m else 0.0


def fmt(v, spec):
    return '-' if v is None else format(v, spec)


def main():
    root = sys.argv[1]
    print('# Results: ' + root)
    env = os.path.join(root, 'env.txt')
    if os.path.exists(env):
        print()
        print('```')
        with open(env) as f:
            print(f.read().rstrip())
        print('```')
    for wal in sorted(os.listdir(root)):
        wdir = os.path.join(root, wal)
        if not os.path.isdir(wdir):
            continue
        print()
        print('## wal_mode = %s' % wal)
        print()
        print('Median of the repetitions. Latency in ms over all requests. '
              'Spread = (max - min) / median of ops/s. CPU = cores used '
              'by the server / the client; server us/op = server CPU time '
              'per request.')
        print()
        print('| workload | server | ops/s | spread | p50 ms | p99 ms | '
              'server CPU | server us/op | client CPU | rd+wr syscalls/op | '
              'Lua bytes/op | new strings/op | GC cycles |')
        print('|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|'
              '---:|')
        for wl in WORKLOADS:
            for server in SERVERS:
                d = os.path.join(wdir, server, wl)
                reps = sorted(glob.glob(os.path.join(d, 'rep*.json')))
                reps = [r for r in reps if re.search(r'rep\d+\.json$', r)]
                if not reps:
                    continue
                runs = []
                for r in reps:
                    try:
                        runs.append(run_stats(r[:-len('.json')]))
                    except (OSError, ValueError, KeyError):
                        # A repetition in progress or a failed one.
                        pass
                if not runs:
                    continue

                def m(k):
                    return median([r.get(k) for r in runs])
                ops = m('ops')
                name = SERVER_NAMES.get(server, server)
                ops_s = fmt(ops, ',.0f')
                if wl == 'w5':
                    ops_s += ' (%s keys/s)' % fmt(m('keys'), ',.0f')
                us = m('srv_cpu') / ops * 1e6 if ops else None
                print('| %s %s | %s | %s | %s%% | %s | %s | %s | %s | %s | '
                      '%s | %s | %s | %s |' % (
                          wl, WORKLOADS[wl], name, ops_s,
                          fmt(spread([r['ops'] for r in runs]), '.1f'),
                          fmt(m('p50'), '.3f'), fmt(m('p99'), '.3f'),
                          fmt(m('srv_cpu'), '.2f'), fmt(us, '.1f'),
                          fmt(m('cli_cpu'), '.2f'),
                          fmt(m('sys_per_op'), '.2f'),
                          fmt(m('alloc_per_op'), ',.0f'),
                          fmt(m('strmiss_per_op'), '.1f'),
                          fmt(m('gc_cycles'), '.0f')))


if __name__ == '__main__':
    main()
