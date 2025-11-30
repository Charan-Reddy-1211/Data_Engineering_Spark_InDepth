import os
from pyspark.sql import SparkSession

def create_spark_session(app_name):
    spark = (SparkSession.builder.master("local-cluster[1,4,1024]").config("spark.executor.instance",1)
             .config("spark.sql.files.maxPartitionBytes",268435456).config("spark.shuffle.spill", "false").appName(app_name).getOrCreate())
    return spark

def check_file_exist(file_path):
    if os.path.isfile(file_path):
        return True
    else:
        print(f"{file_path} not exist")
        return False

def check_extension(file_path):
    if file_path.endswith(".csv"):
        return True
    else:
        return False

def read_csv_file(spark,file_path):
    df=spark.read.option("delimiter",",").option("header","true").csv(file_path)
    print(df.columns)
    return df


def write_to_csv_file(file_path,df,mode="append"):
    df.coalesce(1).write.mode(mode).csv(file_path,header="true")
    return print(f"{file_path} written")