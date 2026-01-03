from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

df = spark.read.csv("/Users/pulastya/Downloads/employee.csv", header=True, inferSchema=True)

window_spec = Window.partitionBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = df.withColumn("count", F.count("id").over(window_spec)).filter(F.col("count") == 1).drop("count")

df.show()

print(type(df))
spark.stop()