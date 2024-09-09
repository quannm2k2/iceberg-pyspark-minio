from spark_config import get_spark_session

# Get SparkSession
spark = get_spark_session()

# Update data in the table
spark.sql("""
    UPDATE supermarket_catalog.db1.supermarket_table
    SET City = 'Hanoi'
    WHERE `Invoice ID` = 'INV-6010'
""")

spark.sql("""
    SELECT * FROM supermarket_catalog.db1.supermarket_table
    WHERE City = 'Hanoi'
""").show()