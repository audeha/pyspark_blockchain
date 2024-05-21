from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, LongType


# Set localhost socket parameters from ther server
localhost = "127.0.0.1"
local_port = 8776

# Create Spark session
spark = SparkSession.builder.appName("Blockchain Stream Reader").getOrCreate()

# Create streaming DataFrame from local socket
# delimiter added on server side
df = (
    spark.readStream.format("socket")
    .option("host", localhost)
    .option("port", local_port)
    .option("delimiter", "/n")
    .option("includeTimestamp", True)
    .load()
)

'''# Print stream to terminal
# truncate option is important to avoid not seeing anything in the terminal
query = (
    df.writeStream.outputMode("append")
    .option("truncate", False)
    .format("console")
    .start()
    .awaitTermination()
)'''

# Cleans input data
schema = StructType([
    StructField("sequence", StringType()),
    StructField("addr", StringType()),
    StructField("value", StringType()),
    StructField("time", StringType()),
    StructField("inputs", ArrayType(
        StructType([
            StructField("sequence", StringType()),
            StructField("prev_out", StructType([
                StructField("spent", BooleanType()),
                StructField("tx_index", LongType()),
                StructField("type", LongType()),
                StructField("addr", StringType()),
                StructField("value", StringType()),
                StructField("n", LongType()),
                StructField("script", StringType())
            ])),
            StructField("script", StringType())
        ])
    )),
    StructField("hash", StringType()),
    StructField("out", ArrayType(
        StructType([
            StructField("spent", BooleanType()),
            StructField("tx_index", LongType()),
            StructField("type", LongType()),
            StructField("addr", StringType()),
            StructField("value", StringType()),
            StructField("n", LongType()),
            StructField("script", StringType())
        ])
    ))
])

from pyspark.sql.functions import from_unixtime

def with_normalized_values(df, schema):
    parsed_df = (
        df.withColumn("json_data", from_json(col("value").cast("string"), schema))
        .withColumn("sequence", col("json_data.sequence"))
        .withColumn("addr", col("json_data.addr"))
        .withColumn("json_value", col("json_data.value"))
        .withColumn("time", col("json_data.time"))
        .drop("json_data")
    )

    # Converts Satoshi value into Euros
    # Assuming the conversion rate is 0.00000001
    parsed_df = parsed_df.withColumn("value_in_eur", col("json_value").cast("double") * 0.00000001)

    # Returns time in a readable format
    parsed_df = parsed_df.withColumn("human_time", from_unixtime(col("time").cast("double")))

    return parsed_df

def perform_available_now_update(parsed_df, schema):
    checkpointPath = "checkpoint_dir/tmp_block_checkpoint"
    path = "output_dir/tmp_blocks"
    return parsed_df.transform(lambda df: with_normalized_values(df, schema)).writeStream.trigger(
        availableNow=True
    ).format("parquet").option("checkpointLocation", checkpointPath).start(path)


parsed_df = with_normalized_values(df, schema)
perform_available_now_update(parsed_df, schema)

query = perform_available_now_update(parsed_df, schema)

