from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

spark = SparkSession.builder \
    .appName("S3Example") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6") \
    .getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
hadoop_conf.set("fs.s3a.endpoint", S3_ENDPOINT_URL)
hadoop_conf.set("fs.s3a.connection.timeout", "60000")
hadoop_conf.set("fs.s3a.connection.maximum", "100")
hadoop_conf.set("fs.s3a.threads.max", "256") 


df = spark.read.csv("/Users/pulastya/Downloads/employee.csv", header=True, inferSchema=True)

output_path = f"s3a://{S3_BUCKET_NAME}/output/partitioned_data/"

df.write.mode("overwrite") \
        .partitionBy("department") \
        .parquet(output_path)




spark.stop()