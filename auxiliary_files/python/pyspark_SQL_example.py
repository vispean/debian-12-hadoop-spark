from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Define schemas
product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", FloatType(), True)
])

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True)
])

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("date_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("discount", FloatType(), True),
    StructField("payment_method", StringType(), True)
])

def main():
    spark = SparkSession.builder \
        .appName("Mock Data SQL Analysis") \
        .getOrCreate()

    products_df = spark.read.csv("/mnt/output/star/dim_product.csv", header=False, schema=product_schema)
    customers_df = spark.read.csv("/mnt/output/star/dim_customer.csv", header=False, schema=customer_schema)
    sales_df = spark.read.csv("/mnt/output/star/fact_sales.csv", header=False, schema=sales_schema)

    # Create temporary views for SQL queries
    products_df.createOrReplaceTempView("dim_product")
    customers_df.createOrReplaceTempView("dim_customer")
    sales_df.createOrReplaceTempView("fact_sales")

    total_products = spark.sql("SELECT COUNT(*) AS total_products FROM dim_product")
    total_products.show()

    avg_price = spark.sql("SELECT AVG(price) AS average_price FROM dim_product")
    avg_price.show()

    total_customers = spark.sql("SELECT COUNT(*) AS total_customers FROM dim_customer")
    total_customers.show()

    total_sales = spark.sql("SELECT SUM(total_amount) AS total_sales FROM fact_sales")
    total_sales.show()

    sales_by_product = spark.sql("""
        SELECT p.name, SUM(s.total_amount) AS total_sales
        FROM fact_sales s
        JOIN dim_product p ON s.product_id = p.product_id
        GROUP BY p.name
        ORDER BY total_sales DESC
        LIMIT 10
    """)
    sales_by_product.show()

    total_sales.write.csv("/home/vagrant/total_sales.csv", header=True)

    spark.stop()

if __name__ == "__main__":
    main()