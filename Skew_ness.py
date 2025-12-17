from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("Skew").getOrCreate()
# spark.conf.set("spark.sql.adaptive.enabled", "false")
#
# # 10 million rows with SAME key
# big = spark.range(0, 10000000).withColumn("id", lit(1))
#
# # small table
# small = spark.range(0, 10).withColumn("id", lit(1))
#
# df = big.join(small, "id")
#
# print(df.count())


# for fun in spark.catalog.listFunctions():
#     print(fun)
print(spark.catalog.currentDatabase())
print()

for table in spark.catalog.listTables():
    print(table)
print(1)
import time
time.sleep(500)
