from pyspark.sql import SparkSession

# Create Spark session
spark = (
    SparkSession.builder
    .appName("ConsoleToFileStream")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read stream from console
# Spark will wait for you to type input lines in terminal
stream_df = (
    spark.readStream
    .format("socket")      # or "text" for files
    .option("host", "localhost")
    .option("port", 9999)  # we'll send data here
    .load()
)

# Write stream to file (sink)
file_output = (
    stream_df.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", "/Users/pulastya/Downloads")   # your local folder
    .option("checkpointLocation", "/Users/pulastya/Downloads")
    .start()
)


if file_output.awaitTermination(timeout=100):  # wait up to 5 minutes
    print("Stream stopped gracefully.")
else:
    print("Timeout reached. Stopping stream manually.")
    file_output.stop()

spark.stop()
print("Spark session stopped")
