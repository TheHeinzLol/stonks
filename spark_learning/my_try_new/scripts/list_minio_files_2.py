
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("List MinIO Files") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

sc = spark.sparkContext
jvm = sc._jvm

# IMPORTANT: use URI-based FileSystem lookup
uri = jvm.java.net.URI("s3a://airflow.learn/")
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)

path = jvm.org.apache.hadoop.fs.Path("s3a://airflow.learn/")
status = fs.listStatus(path)

print("Files in bucket airflow.learn:")
for f in status:
    print(f"- {f.getPath().toString()} (size={f.getLen()} bytes)")

spark.stop()
