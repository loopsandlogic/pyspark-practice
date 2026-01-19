from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType

spark = SparkSession.builder.appName("same-trans").getOrCreate()

def int_10_inc(df):
    for col in [field.name for field in df.schema.fields if isinstance(field.dataType, IntegerType)]:
        upd_val = F.col(col) + (F.col(col) * 10 / 100)
        df = df.withColumn(col, upd_val)
    return df

def str_trim_space(df):
    for col in [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]:
        df = df.withColumns({
            col: F.trim(F.col(col))
            })
    return df

# Define schema
schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("score", IntegerType(), nullable=True)
])

# Example DataFrame
df = spark.createDataFrame([
    (1, " Alice ", "  Chicago  ", 100),
    (2, " Bob ", "  NYC  ", 215),
    (3, " Charlie ", "  Richmond   ", 300)
], schema=schema)

df.show()


df = int_10_inc(str_trim_space(df))

df.show()

spark.stop()