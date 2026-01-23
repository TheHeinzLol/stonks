import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, lit, map_entries, struct, to_json, udf
from pyspark.sql.types import DoubleType, FloatType, IntegerType, MapType, StringType, StructField, StructType, TimestampType

# set connection to s3
# Below, spark.jars and spark.jars.ivy are needed to avoid some errors of stating environment variables I made earlier. 
spark = SparkSession.builder \
    .appName("List MinIO Files") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.jars", 
               f"{os.environ.get('SPARK_HOME', '/opt/spark')}/jars/hadoop-aws-3.3.4.jar,"
               f"{os.environ.get('SPARK_HOME', '/opt/spark')}/jars/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.jars.ivy", f"{os.environ.get('SPARK_HOME', '/opt/spark')}/.ivy2") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# get file from s3
s3_file_path = "s3a://airflow.learn/2026-01-13 08:37:19.193383+00:00"
binary_df = spark.read.format("binaryFile").load(s3_file_path)

test_text = '{"bars":{"LIN":{"c":443.65,"h":443.705,"l":443.55,"n":115,"o":443.62,"t":"2026-01-12T20:59:00Z","v":3160,"vw":443.647292},"COST":{"c":943.35,"h":943.59,"l":943.11,"n":133,"o":943.28,"t":"2026-01-12T20:59:00Z","v":3793,"vw":943.258824},"LLY":{"c":1081.015,"h":1081.24,"l":1080.79,"n":102,"o":1080.79,"t":"2026-01-12T20:59:00Z","v":2708,"vw":1080.940634},"CNC":{"c":46.39,"h":46.43,"l":46.39,"n":128,"o":46.43,"t":"2026-01-12T20:59:00Z","v":5152,"vw":46.415255}}}'

json_df = binary_df.select(
    col("path"),
    col("content").cast(StringType()).alias("json_str")
)
json_df = json_df.withColumn("json_str", lit(test_text))

bars_schema = StructType([
    StructField("c", DoubleType()),  # close
    StructField("h", DoubleType()),  # high
    StructField("l", DoubleType()),  # low
    StructField("n", IntegerType()), # n
    StructField("o", DoubleType()),  # open
    StructField("t", StringType()),  # time
    StructField("v", IntegerType()), # volume
    StructField("vw", DoubleType())  # vwap
])

# Parse the JSON string
parsed_df = json_df.select(
    from_json(col("json_str"), StructType([
        StructField("bars", MapType(StringType(), bars_schema))
    ])).alias("parsed_json")
)

# Extract and explode the bars data
result_df = parsed_df.select(
    explode(col("parsed_json.bars")).alias("ticker", "bar_data")
).select(
    col("ticker").alias("ticker"),
    col("bar_data.c").alias("close"),
    col("bar_data.h").alias("high"),
    col("bar_data.l").alias("low"),
    col("bar_data.n").alias("n"),
    col("bar_data.o").alias("open"),
    col("bar_data.t").alias("time"),
    col("bar_data.v").alias("volume"),
    col("bar_data.vw").alias("vwap")
)

# Show the result
result_df.show(truncate=False)

# original
# try:
#     # Try UTF-8 decoding
#     json_string = binary_data.decode('utf-8')
#     print(json_string)
#     #json_string = spark.read.text(json_string)
# except UnicodeDecodeError:
#     print("Not valid UTF-8, might be compressed or encoded differently")

# bar_schema = StructType([
#     StructField("ticker", StringType(), False), 
#     StructField("close", FloatType(), True),
#     StructField("high", FloatType(), True),
#     StructField("low", FloatType(), True),
#     StructField("n", IntegerType(), True),    # number of trades
#     StructField("open", FloatType(), True),
#     StructField("time", TimestampType(), True),
#     StructField("volume", IntegerType(), True),
#     StructField("vwap", FloatType(), True)      # volume weighted price
# ])

