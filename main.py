from pyspark import StorageLevel
from pyspark.sql.functions import *

import Utils.common
from Utils.common import check_file_exist,check_extension,create_spark_session,read_csv_file,write_to_csv_file

app_name="spark_depth-UI-explore"
file_path=r"D:\Data Engineering\AWS Data Engineering\airlines_flights_data.csv"
write_path=r"D:\Data Engineering\AWS Data Engineering\Spark\par"
spark=create_spark_session(app_name)
file=check_file_exist(file_path)
exn=check_extension(file_path)
if file & exn:
    print("File exist")

else:
    print("File not exist or extension is not correct")


df=read_csv_file(spark,file_path)
# df.cache()
# print("no of columns before appending : -",df.count())
# print("No of partitions in df is : -",df.rdd.getNumPartitions())
# print("No of Executors in spark is : -",spark.sparkContext.getConf().get("spark.executor.instances"))
# print("No of Executor cores in spark is :-",spark.sparkContext.getConf().get("spark.executor.cores"))


# df.withColumn("partitionID",spark_partition_id()).groupBy("partitionID").count().show()
# df_all=df
# for i in range(40):
#     df_all=df_all.union(df)
# #
# #
# write_to_csv_file(write_path,df_all,mode="overwrite")
#
# df_append=read_csv_file(spark,write_path)
# print("no of columns after appending : -",df_append.count())

# df_union=df.union(df)
# df_union.cache()
# df_union.unpersist()
# df.persist(StorageLevel.MEMORY_ONLY)
# df.collect()
# print("no of columns after appending : -",df_union.count())
#
# schema = ["id"]
# data = [("1",)]*1000000
# brd = spark.createDataFrame(data, schema)
# #
# # brd.show()
# print("Union Count")
# # df_union.withColumn("partitionID",spark_partition_id()).groupBy("partitionID").count().show()
# brdf=df.join(brd,df["index"]==brd["id"],"fullouter")
# # # print("brdf count")
# brdf.groupBy("id").count().show()
# brdf.withColumn("partitionID",spark_partition_id()).groupBy("partitionID").count().show()
(df.withColumn("flight_source",concat_ws("-",col("flight"),col("airline")))\
    .withColumn("Fli",concat(col("source_city"),lit("-"),col("airline"))).\
    fillna({
    "source_city":"Bangalore",
    "index":"1",
})
 # .withColumn("Fli",when(col("Fli").isNull(),concat(col("airline"),lit("-"),col("airline"))).otherwise(col("Fli")))
 .withColumn("Fli",coalesce(col("source_city"),col("airline")))
 .show(10,truncate=False))
import  time
time.sleep(500)
spark.stop()