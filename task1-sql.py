#!/usr/bin/env python
import sys
from pyspark import SparkContext
from pyspark.sql import HiveContext,SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("HW2_task1") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


park_vio = spark.read.format("csv").options(header="true",inferschema="true").load(sys.argv[1])
park_vio.createOrReplaceTempView("park_vio")

open_vio=spark.read.format("csv").options(header="true",inferschema="true").load(sys.argv[2])
open_vio.createOrReplaceTempView("open_vio")


result = spark.sql('SELECT summons_number, violation_county, registration_state, violation_code, issue_date FROM park_vio WHERE summons_number IN (SELECT summons_number FROM park_vio MINUS SELECT summons_number FROM open_vio)')
result = result.select(format_string('%d\t%s, %s, %d, %s',result.summons_number,result.violation_county,result.registration_state,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")
