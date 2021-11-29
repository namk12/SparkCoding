#!/usr/bin/env python
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
parkViolation = sc.textFile(sys.argv[1], 1)

parkViolation = parkViolation.mapPartitions(lambda x: reader(x))

def func(x):
    if x == 'NY':
        return ('NY',1)
    else:
        return ('Other',1)

violation_code_park = parkViolation.filter(lambda line: len(line)>1).zipWithIndex().filter(lambda kv: kv[1] > 0).keys().map( lambda l: l[16] )

res = violation_code_park.map(lambda x: func(x)).reduceByKey(lambda x,y:x+y)

output = res.map(lambda r:"\t".join([str(c) for c in r]))
output.saveAsTextFile("task4.out")
sc.stop()

