import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task4-sql").config("spark.some.config.option", "some-value").getOrCreate()
park_vio = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
park_vio.createOrReplaceTempView("park_vio")

result = spark.sql('SELECT CASE WHEN registration_state == "NY" THEN "NY" ELSE "Other" END AS reg_state, COUNT(*) AS c1 FROM park_vio GROUP BY 1')

result.select(format_string('%s\t%d', result.reg_state, result.c1)).write.save("task4-sql.out", format = "text")
