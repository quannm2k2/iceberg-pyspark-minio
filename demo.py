from pyspark.sql import SparkSession

# Cấu hình Spark với Iceberg và MinIO
spark = SparkSession.builder \
    .appName("Iceberg-MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1") \
    .config("spark.sql.catalog.iceberg_demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg_demo.type", "hadoop") \
    .config("spark.sql.catalog.iceberg_demo.warehouse", "s3a://demo-bucket-2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# # Tạo bảng với định dạng Parquet và schema được quản lý bằng Avro
# spark.sql("""
#     CREATE TABLE iceberg_demo.db1.my_table (
#         id INT,
#         name STRING,
#         age INT
#     )
#     USING iceberg
# """)

# Thêm dữ liệu vào bảng Iceberg
spark.sql("""
    INSERT INTO iceberg_demo.db1.my_table VALUES
    (1, 'John', 25),
    (2, 'Jane', 30),
    (3, 'Bob', 22)
""")

# Truy vấn dữ liệu từ bảng Iceberg
spark.sql("SELECT * FROM iceberg_demo.db1.my_table").show()

# Xem lịch sử phiên bản
spark.sql("SELECT * FROM iceberg_demo.db1.my_table.history").show()

# # Quay lại một thời điểm cụ thể dựa trên snapshot ID hoặc timestamp
# spark.sql("""
#     SELECT * FROM iceberg_demo.db1.my_table 
#     TIMESTAMP AS OF '2024-09-08 10:46:38.17667762923195685'
# """).show()

