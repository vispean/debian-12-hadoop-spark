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
        .appName("Mock Data Analysis") \
        .getOrCreate()

    products_df = spark.read.csv("/mnt/output/star/dim_product.csv", header=False, schema=product_schema)
    customers_df = spark.read.csv("/mnt/output/star/dim_customer.csv", header=False, schema=customer_schema)
    sales_df = spark.read.csv("/mnt/output/star/fact_sales.csv", header=False, schema=sales_schema)

    total_products = products_df.count()
    print(f"Total Products: {total_products}")

    avg_price = products_df.agg({"price": "avg"}).collect()[0][0]
    print(f"Average Price: {avg_price:.2f}")

    total_customers = customers_df.count()
    print(f"Total Customers: {total_customers}")

    total_sales = sales_df.agg({"total_amount": "sum"}).collect()[0][0]
    print(f"Total Sales Amount: {total_sales:.2f}")

    sales_by_product = sales_df.groupBy("product_id").agg({"total_amount": "sum"}).orderBy("sum(total_amount)", ascending=False)
    sales_by_product_df = sales_by_product.join(products_df, "product_id").select("name", "sum(total_amount)")
    print("Sales by Product:")
    sales_by_product_df.show(10)

    sales_by_product_df.write.csv("/home/vagrant/output/sales_by_product.csv", header=True)

    spark.stop()

if __name__ == "__main__":
    main()