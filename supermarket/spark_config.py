from pyspark.sql import SparkSession
import os

def get_spark_session():
    spark = SparkSession.builder \
        .appName("Iceberg-MinIO") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1") \
        .config("spark.sql.catalog.supermarket_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.supermarket_catalog.type", "hadoop") \
        .config("spark.sql.catalog.supermarket_catalog.warehouse", "s3a://bucket-09082024-supermarket-sales") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    return spark
