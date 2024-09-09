from spark_config import get_spark_session

# Get SparkSession
spark = get_spark_session()

# Read CSV file from MinIO
df = spark.read.csv("s3a://bucket-09082024-supermarket-sales/data-csv/supermarket_sales.csv", header=True, inferSchema=True)

# Insert data from dataframe into Iceberg table
df.writeTo("supermarket_catalog.db1.supermarket_table").append()