from spark_config import get_spark_session

# Get SparkSession
spark = get_spark_session()

# Delete data in the table
spark.sql("""
    DELETE FROM supermarket_catalog.db1.supermarket_table
    WHERE `Invoice ID` = 'INV-6011'
""")
