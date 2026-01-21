from pyspark.sql import SparkSession

# Create Spark session with MinIO (S3A) configuration
spark = SparkSession.builder \
    .appName("List MinIO Files") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# List files in the bucket using Hadoop FileSystem API
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
    "s3a://airflow.learning/"
)

status = fs.listStatus(path)

print("Files in bucket airflow.learning:")
for file_status in status:
    print(
        f"- {file_status.getPath().toString()} "
        f"(size={file_status.getLen()} bytes)"
    )

spark.stop()
