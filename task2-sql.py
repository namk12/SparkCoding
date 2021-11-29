#!/usr/bin/env python
import sys
from pyspark import SparkContext
from pyspark.sql import HiveContext,SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("HW2_task2") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


open_vio = spark.read.format("csv").options(header="true",inferschema="true").load(sys.argv[1])
open_vio.createOrReplaceTempView("open_vio")

result = spark.sql('SELECT violation, count(violation) as violation_count FROM open_vio GROUP BY violation ORDER BY violation')
result = result.select(format_string('%s\t%d',result.violation,result.violation_count)).write.save("task2-sql.out",format="text")
