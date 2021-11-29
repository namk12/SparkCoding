import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
open_vio = sc.textFile(sys.argv[1],1)
open_vio = open_vio.mapPartitions(lambda x:reader(x)).zipWithIndex().filter(lambda kv: kv[1]>0).keys()
amount = open_vio.map(lambda x: (x[5],float(x[12])))
sum = amount.reduceByKey(lambda x,y:x+y)
pre = open_vio.map(lambda x:(x[5],1))
count = pre.reduceByKey(lambda x,y:x+y)
countsum = sum.join(count)
avg = countsum.map(lambda x: (x[0],x[1][0],float(x[1][0]/x[1][1])))
avg.map(lambda x: x[0]+ '\t' + "%.2f" %x[1] + ', ' + "%.2f" %x[2]).saveAsTextFile("task3.out")
sc.stop()
