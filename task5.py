#!/usr/bin/env python
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
parkViolation = sc.textFile(sys.argv[1], 1)

parkViolation = parkViolation.mapPartitions(lambda x: reader(x))

violation_code_park = parkViolation.filter(lambda line: len(line)>1).zipWithIndex().filter(lambda kv: kv[1] > 0).keys().map( lambda l: (l[20],1) )

res = violation_code_park.reduceByKey(lambda x,y:x+y).takeOrdered(1,key = lambda x: -x[1])
res = sc.parallelize(res)

output = res.map(lambda r:"\t".join([str(c) for c in r]))
output.saveAsTextFile("task5.out")
sc.stop()

