from spark_config import get_spark_session

# Get SparkSession
spark = get_spark_session()

# Add new data
spark.sql("""
    INSERT INTO supermarket_catalog.db1.supermarket_table VALUES
    ('INV-6010', 'A', 'New York', 'Member', 'Male', 'Health and beauty', 50.0, 10, 25.0, 525.0, '2024/09/01', '12:30', 'Credit card', 500.0, 5.0, 25.0, 7.0),
    ('INV-6011', 'B', 'Los Angeles', 'Normal', 'Female', 'Electronic accessories', 100.0, 5, 25.0, 525.0, '2024/09/02', '14:00', 'Ewallet', 500.0, 5.0, 25.0, 8.0)
""")
