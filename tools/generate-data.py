#!/usr/bin/env python

from __future__ import print_function

import sys
import random
import numpy

def make_ranges(total, parts):
    for i in xrange(parts):
        yield total / parts + (1 if i < total % parts else 0)

NUM_POINTS = 10000 if len(sys.argv) < 2 else int(sys.argv[1])
NUM_CLUSTERS = 3 if len(sys.argv) < 3 else int(sys.argv[2])

print("generating %d points in %d clusters" % (NUM_POINTS, NUM_CLUSTERS), file=sys.stderr)

for cluster_id, cluster_size in enumerate(make_ranges(NUM_POINTS, NUM_CLUSTERS)):
    print("%d/%d" % (cluster_id, NUM_CLUSTERS), file=sys.stderr)

    cluster_variance = random.random() * 2.0 + 0.1
    samples = numpy.random.normal(0.0, cluster_variance, (cluster_size, 2))
    samples[:,0] += random.random() * 10.0
    samples[:,1] += random.random() * 10.0

    for sample in samples:
        print('%d %f %f' % ((cluster_id,) + tuple(sample)))

print("done", file=sys.stderr)
