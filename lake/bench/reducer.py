#!/usr/bin/env python3
"""reducer.py - Hadoop Streaming reducer (and combiner) for the Step 29 benchmark.

Input: mapper lines sorted by (yyyymm, route). Sums requests, weight and high-confidence
counts per key. Its output has the same layout as its input, so it also works as the
combiner (partial sums on the map side, less data to shuffle).
"""
import sys

current, sums = None, [0, 0, 0]
for line in sys.stdin:
    f = line.rstrip("\n").split("\t")
    if len(f) != 5:
        continue
    key = (f[0], f[1])
    if key != current:
        if current is not None:
            sys.stdout.write("%s\t%s\t%d\t%d\t%d\n" % (current[0], current[1], sums[0], sums[1], sums[2]))
        current, sums = key, [0, 0, 0]
    sums[0] += int(f[2]); sums[1] += int(f[3]); sums[2] += int(f[4])
if current is not None:
    sys.stdout.write("%s\t%s\t%d\t%d\t%d\n" % (current[0], current[1], sums[0], sums[1], sums[2]))
