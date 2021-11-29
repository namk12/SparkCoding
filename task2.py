#!/usr/bin/env python
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


sc = SparkContext()
open_vio = sc.textFile(sys.argv[1], 1)
open_vio = open_vio.mapPartitions(lambda x: reader(x))

open_violations = open_vio.filter(lambda line: len(line)>1).zipWithIndex().filter(lambda kv: kv[1] > 0).keys().map(lambda line: (line[7],1))
open_violations = open_violations.reduceByKey(add).sortByKey(True)

output = open_violations.map(lambda r:"\t".join([str(c) for c in r]))
output.saveAsTextFile("task2.out")
sc.stop()
