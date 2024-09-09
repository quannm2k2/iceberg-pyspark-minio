from spark_config import get_spark_session

# Get SparkSession
spark = get_spark_session()

# Truy vấn với điều kiện (các bản ghi thuộc thành phố New York và thuộc dòng sản phẩm 'Health and beauty')
spark.sql("""
    SELECT * FROM supermarket_catalog.db1.supermarket_table
    WHERE City = 'New York' AND `Product line` = 'Health and beauty'
""").show()

# Tìm tất cả đơn hàng từ các khách hàng 'Member' có tổng tiền lớn hơn 500 và thanh toán bằng 'Ewallet' hoặc 'Credit card'
spark.sql("""
    SELECT *
    FROM supermarket_catalog.db1.supermarket_table
    WHERE `Customer type` = 'Member' 
    AND Total > 500
    AND (Payment = 'Ewallet' OR Payment = 'Credit card')
""").show()
