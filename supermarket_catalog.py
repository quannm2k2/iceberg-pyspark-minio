from pyspark.sql import SparkSession

# Tạo SparkSession và cấu hình kết nối tới MinIO, Iceberg
spark = SparkSession.builder \
    .appName("Iceberg-MinIO-Project") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1") \
    .config("spark.sql.catalog.supermarket_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.supermarket_catalog.type", "hadoop") \
    .config("spark.sql.catalog.supermarket_catalog.warehouse", "s3a://bucket-09082024-supermarket-sales") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Đọc tệp CSV từ MinIO
df = spark.read.csv("s3a://bucket-09082024-supermarket-sales/data-csv/supermarket_sales.csv", header=True, inferSchema=True)

# Tạo bảng Iceberg với partition
spark.sql("""
    CREATE TABLE supermarket_catalog.db1.supermarket_table (
        `Invoice ID` STRING,
        `Branch` STRING,
        `City` STRING,
        `Customer type` STRING,
        `Gender` STRING,
        `Product line` STRING,
        `Unit price` DOUBLE,
        `Quantity` INT,
        `Tax 5%` DOUBLE,
        `Total` DOUBLE,
        `Date` STRING,
        `Time` STRING,
        `Payment` STRING,
        `cogs` DOUBLE,
        `gross margin percentage` DOUBLE,
        `gross income` DOUBLE,
        `Rating` DOUBLE
    )
    USING iceberg
    PARTITIONED BY (`City`, `Product line`)
""")

# Chèn dữ liệu từ dataframe vào bảng Iceberg
df.writeTo("supermarket_catalog.db1.supermarket_table").append()

# Truy vấn dữ liệu từ bảng Iceberg
spark.sql("SELECT * FROM supermarket_catalog.db1.supermarket_table").show()

# Xem lịch sử version control của bảng Iceberg
spark.sql("SELECT * FROM supermarket_catalog.db1.supermarket_table.history").show()