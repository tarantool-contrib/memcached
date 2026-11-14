#!/usr/bin/env python3
"""Compare the variants measured by bench/variants/run.sh.

usage: python3 -I bench/variants/compare.py <variants OUT dir>

Per workload and variant: median ops/s over the repetitions, the change
against 'base', server CPU per request, Lua bytes per request, p50/p99.
"""

import glob
import os
import statistics
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(
    __file__))))

import summarize  # noqa: E402

ORDER = ['base', 'nola', 'notxn', 'nobatch', 'noclosure', 'ibuf',
         'waitfirst', 'parse', 'enc', 'gc400', 'all', 'all+gc400']


def stats(root, variant, wl):
    d = os.path.join(root, variant, 'none', 'mcc', wl)
    runs = []
    for p in sorted(glob.glob(os.path.join(d, 'rep*.json'))):
        try:
            runs.append(summarize.run_stats(p[:-len('.json')]))
        except (OSError, ValueError, KeyError):
            pass
    if not runs:
        return None

    def med(k):
        vals = [r[k] for r in runs if r.get(k) is not None]
        return statistics.median(vals) if vals else None
    res = {k: med(k) for k in ('ops', 'srv_cpu', 'p50', 'p99',
                               'alloc_per_op', 'sys_per_op')}
    res['n'] = len(runs)
    res['all_ops'] = [r['ops'] for r in runs]
    return res


def main():
    root = sys.argv[1]
    variants = [v for v in ORDER if os.path.isdir(os.path.join(root, v))]
    variants += sorted(v for v in os.listdir(root)
                       if os.path.isdir(os.path.join(root, v)) and
                       v not in variants)
    wls = set()
    for v in variants:
        wls.update(os.path.basename(p) for p in
                   glob.glob(os.path.join(root, v, 'none', 'mcc', 'w*')))
    for wl in sorted(wls):
        base = stats(root, 'base', wl)
        print('## %s %s' % (wl, summarize.WORKLOADS.get(wl, '')))
        print()
        print('| variant | ops/s | vs base | runs (ops/s) | us/op | '
              'Lua B/op | rd+wr syscalls/op | p50 ms | p99 ms |')
        print('|---|---:|---:|---|---:|---:|---:|---:|---:|')
        for v in variants:
            s = stats(root, v, wl)
            if s is None:
                continue
            delta = '-'
            if base and v != 'base' and base['ops']:
                delta = '%+.1f%%' % ((s['ops'] / base['ops'] - 1) * 100)
            us = s['srv_cpu'] / s['ops'] * 1e6 if s['ops'] else None
            print('| %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
                v, summarize.fmt(s['ops'], ',.0f'), delta,
                ' '.join('%.0f' % o for o in s['all_ops']),
                summarize.fmt(us, '.1f'),
                summarize.fmt(s['alloc_per_op'], ',.0f'),
                summarize.fmt(s['sys_per_op'], '.2f'),
                summarize.fmt(s['p50'], '.3f'),
                summarize.fmt(s['p99'], '.3f')))
        print()


if __name__ == '__main__':
    main()
