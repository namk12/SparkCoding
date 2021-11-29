#!/usr/bin/env python
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()
parkViolation = sc.textFile(sys.argv[1], 1)

parkViolation = parkViolation.mapPartitions(lambda x: reader(x))

violation_code_park = parkViolation.filter(lambda line: len(line)>1).zipWithIndex().filter(lambda kv: kv[1] > 0).keys().map( lambda l: (l[3],l[1]) ).sortByKey().groupByKey().map(lambda x: (x[0],list(x[1])))

def fnc(x):
    var = ['2016-03-05','2016-03-06','2016-03-12','2016-03-13','2016-03-19','2016-03-20','2016-03-26','2016-03-27']
    weekend = 0
    weekday = 0
    for i in range(0,len(x)):
        if x[i] in var:
            weekend+=1
        else:
            weekday+=1
    
    weekday = float(weekday/23.00)
    weekend = float(weekend/8.00)
    
    return weekend, weekday

res = violation_code_park.map(lambda x:(x[0],fnc(x[1])))
output = res.map(lambda x: "%s\t%.2f, %.2f" %(x[0],x[1][0], x[1][1]))

output.saveAsTextFile("task7.out")
sc.stop()

