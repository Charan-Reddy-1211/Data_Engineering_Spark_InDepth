

from Utils.common import  create_spark_session
from pyspark.sql.functions import *
app_name='skew'

spark=create_spark_session(app_name)

schema = ["id"]
data = [("1",)]*1000000
data2=[("2",)]*10
one_data = spark.createDataFrame(data, schema)

two_data = spark.createDataFrame(data2, schema)

union_df=one_data.union(two_data)
print(union_df.columns)
union_df=union_df.withColumn("partitionID",spark_partition_id())
union_df_grouped = union_df.groupBy("id","partitionID").count()
union_df_grouped.show()

import  time
time.sleep(500)
spark.stop()