from pyspark.sql.functions import sum, avg, min, max, count, date_format, year, month
from pyspark.sql.functions import year, quarter, month, sum, avg


class GoldAggregations:

#   # Read in the necessary tables
#   user = "shivam_panicker"
#   sales_df = spark.read.table(f"{user}_silverdb.silver_sales")
#   products_df = spark.read.table(f"{user}_silverdb.silver_products")
#   customers_df = spark.read.table(f"{user}_silverdb.silver_customers")
#   orders_df = spark.read.table(f"{user}_silverdb.silver_orders")



  # Query 1: Total number of orders

  def total_num_orders(spark, orders):
    return spark.sql(f"SELECT COUNT(*) as total_orders FROM {orders}")

  # Query 2: Total sales amount in USD
  def total_sales_amount_in_usd(spark, sales):
    return spark.sql(f"SELECT SUM(sale_amount) as total_sales FROM {sales} WHERE currency = 'USD'")

  # Query 3: Top 10 best selling products by sales amount
  def top_10_best_selling_products(spark, sales, products):
    return spark.sql(f"""
      SELECT p.product_id, p.product_category, SUM(s.sale_amount) as total_sales
      FROM {products} p
      JOIN {sales} s ON p.product_id = s.product_id
      GROUP BY p.product_id, p.product_category
      ORDER BY total_sales DESC
      LIMIT 10
  """)

  # Query 4: Number of customers by state
  def num_customers_by_state(spark, customers):
    return spark.sql(f"""
      SELECT state, COUNT(DISTINCT customer_id) as total_customers
      FROM {customers}
      GROUP BY state
  """)

  # Query 5: Average sales amount by month
  def avg_sales_by_month(spark, sales):
    return spark.sql(f"""
      SELECT 
          YEAR(sale_date) as year,
          MONTH(sale_date) as month,
          AVG(sale_amount) as avg_sales
      FROM {sales}
      GROUP BY year, month
  """)

#   query1 = total_num_orders()
#   query2 = total_sales_amount_in_usd()
#   query3 = top_10_best_selling_products()
#   query4 = num_customers_by_state()
#   query5 = avg_sales_by_month()


#   # write the query results to Delta Lake in Gold layer
#   query1.write.format("delta").mode("overwrite").save("/mnt/gold/total_orders")
#   query2.write.format("delta").mode("overwrite").save("/mnt/gold/total_sales")
#   query3.write.format("delta").mode("overwrite").save("/mnt/gold/best_selling_products")
#   query4.write.format("delta").mode("overwrite").save("/mnt/gold/customers_by_state")
#   query5.write.format("delta").mode("overwrite").save("/mnt/gold/avg_sales_by_month")