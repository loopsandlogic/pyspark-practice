from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.appName("Flatten Json").getOrCreate()

df = spark.read.option("multiline", True).json("/Users/pulastya/Downloads/orders.json")
df_flat = df.select("id"
                    , "name"
                    , col("contact.email").alias("email")
                    , col("contact.phone").alias("phone")
                    , "orders"
).withColumn("order"
             , explode("orders")
            ).drop("orders").select("id"
                              , "name"
                              , "email"
                              , "phone"
                              , col("order.order_id").alias("order_id")
                              , col("order.amount").alias("amount")
)
df_flat.show(truncate=False)
spark.stop()