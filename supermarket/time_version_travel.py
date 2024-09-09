from spark_config import get_spark_session

# Get SparkSession
spark = get_spark_session()

# Query Time travel by Snapshot ID
spark.sql("""
    SELECT * FROM supermarket_catalog.db1.supermarket_table VERSION AS OF '6018300659428562180' LIMIT 10
""").show(truncate=False)

# Query Time travel by Timestamp
spark.sql("""
    SELECT * FROM supermarket_catalog.db1.supermarket_table TIMESTAMP AS OF '2024-09-09 10:28:37.922' LIMIT 10
""").show(truncate=False)