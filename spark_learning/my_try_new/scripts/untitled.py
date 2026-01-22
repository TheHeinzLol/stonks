from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType, MapType

spark = SparkSession.builder \
    .appName("List MinIO Files") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
#test
s3_file_path = "s3a://airflow.learn/2026-01-13 08:37:19.193383+00:00"
bronze_df = spark.read.format("binaryFile").load(s3_file_path)
binary_data = bronze_df.select("content").collect()[0][0]

try:
    # Try UTF-8 decoding
    json_string = binary_data.decode('utf-8')
    print("\nDecoded as UTF-8 (first 1000 chars):")
    print(json_string[:1000])
except UnicodeDecodeError:
    print("Not valid UTF-8, might be compressed or encoded differently")

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
