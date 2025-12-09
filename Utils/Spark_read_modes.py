from pyspark.sql import functions as F
from pyspark.sql.window import Window
from Utils.common import create_spark_session

spark = create_spark_session("Duplicate")
df = spark.createDataFrame([
    (1, "A"),
    (2, "B"),
    (2, "C"),
    (3, "D")
], ["id", "value"])

df.show()
# # w = Window.partitionBy("id")
# #
# # df_no_duplicates = df.withColumn("cnt", F.count("*").over(w)) \
# #                      # .filter("cnt = 1") \
# #                      # .drop("cnt")
# #
# # df_no_duplicates.show()
# df=df.dropDuplicates(["id"])
# df.show()

df.write.mode("append").partitionBy("id").csv(r"E:\Data Engineering\Spark\Spark - Depth\data\id")
df.write.mode("append").partitionBy("value").csv(r"E:\Data Engineering\Spark\Spark - Depth\data\val")