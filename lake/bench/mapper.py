#!/usr/bin/env python3
"""mapper.py - Hadoop Streaming mapper for the Step 29 benchmark (Python 3.4, stdlib only).

Input: one request per line, tab-separated, as written by BenchPrepare.scala:
  request_key  yyyymm  origin  dest  cargo_type  weight_kg  confidence
Output: one partial result per line; the first two fields are the key (the job sets
stream.num.map.output.key.fields=2):
  yyyymm  route  requests  weight_kg  high_confidence
"""
import sys

for line in sys.stdin:
    f = line.rstrip("\n").split("\t")
    if len(f) != 7:
        continue                                   # a broken line is skipped, not fatal
    key, yyyymm, origin, dest, cargo, weight, conf = f
    try:
        kg = int(weight or 0)
    except ValueError:
        kg = 0
    sys.stdout.write("%s\t%s-%s\t1\t%d\t%d\n" % (yyyymm, origin, dest, kg, 1 if conf == "high" else 0))
