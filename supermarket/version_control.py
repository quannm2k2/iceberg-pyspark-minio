from spark_config import get_spark_session

# Get SparkSession
spark = get_spark_session()

# Query the change history of a table
spark.sql("SELECT * FROM supermarket_catalog.db1.supermarket_table.history").show(truncate=False)