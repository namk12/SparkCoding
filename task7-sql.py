import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task6-sql").config("spark.some.config.option", "some-value").getOrCreate()
park_vio = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
park_vio.createOrReplaceTempView("park_vio")

x = spark.sql('SELECT violation_county, SUM(CASE WHEN SUBSTRING(issue_date, 1, 7) == "2016-03" AND SUBSTRING(issue_date, 9, 2) IN ("05", "06", "12", "13", "19", "20", "26", "27") THEN 1 ELSE 0 END)/8 AS weekend_avg, SUM(CASE WHEN SUBSTRING(issue_date, 1, 7) == "2016-03" AND SUBSTRING(issue_date, 9, 2) NOT IN ("05", "06", "12", "13", "19", "20", "26", "27") THEN 1 ELSE 0 END)/23 AS weekday_avg FROM park_vio GROUP BY violation_county')

x.select(format_string('%s\t%.2f, %.2f', x.violation_county, x.weekend_avg, x.weekday_avg)).write.save("task7-sql.out", format = "text")
