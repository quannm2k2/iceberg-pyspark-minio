from spark_config import get_spark_session

# Get SparkSession
spark = get_spark_session()

# Create a table
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
