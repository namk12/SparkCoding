import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task6-sql").config("spark.some.config.option", "some-value").getOrCreate()
park_vio = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
park_vio.createOrReplaceTempView("park_vio")

result = spark.sql('SELECT vehicle_make , COUNT(*) C1 FROM park_vio GROUP BY vehicle_make ORDER BY C1 DESC limit 10')

result.select(format_string('%s\t%d', result.vehicle_make, result.C1)).write.save("task6-sql.out", format = "text")
