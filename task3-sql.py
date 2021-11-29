import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3-sql").config("spark.some.config.option", "some-value").getOrCreate()
ov = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])

ov.createOrReplaceTempView("ov")

result = spark.sql("SELECT precinct, SUM(CAST(amount_due AS FLOAT)) AS total, AVG(CAST(amount_due AS FLOAT)) AS average FROM ov GROUP BY precinct")
result.createOrReplaceTempView("result")
result.select(format_string('%s\t%.2f, %.2f', result.precinct, result.total, result.average)).write.save("task3-sql.out", format = "text")
