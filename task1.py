
#!/usr/bin/env python
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
parkViolation = sc.textFile(sys.argv[1], 1)
openViolation = sc.textFile(sys.argv[2],1)

parkViolation = parkViolation.mapPartitions(lambda x: reader(x))
openViolation = openViolation.mapPartitions(lambda x: reader(x))

park_vio = parkViolation.filter(lambda line: len(line)>1).map(lambda line: (line[0], str(line[3]) + ', ' + str(line[16]) + ', ' + str(line[2]) + ', ' + str(line[1])))
open_vio = 	openViolation.filter(lambda line: len(line)>1).map(lambda line: (line[0], ""))

result=park_vio.subtractByKey(open_vio).sortByKey(True)

output = result.map(lambda r:"\t".join([str(c) for c in r]))
output.saveAsTextFile("task1.out")
sc.stop()
