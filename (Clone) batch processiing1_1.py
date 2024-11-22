# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

orders_path='dbfs:/mnt/globalmart/ecom_data/orders.csv'
orders_df = spark.read.option("header","true").option("inferSchema","true").csv(orders_path)

# COMMAND ----------

customers_path='dbfs:/mnt/globalmart/ecom_data/customers.csv'
customers_df = spark.read.option("header","true").option("inferSchema","true").csv(customers_path)

# COMMAND ----------

products_path='dbfs:/mnt/globalmart/ecom_data/products.csv'
products_df = spark.read.option("header","true").option("inferSchema","true").csv(products_path)

# COMMAND ----------

orders_items_path='dbfs:/mnt/globalmart/ecom_data/orders_items.csv'
orders_items_df = spark.read.option("header","true").option("inferSchema","true").csv(orders_items_path)

# COMMAND ----------

orders_df.createOrReplaceGlobalTempView("orders")
customers_df.createOrReplaceGlobalTempView("customers")
products_df.createOrReplaceGlobalTempView("products")
orders_items_df.createOrReplaceGlobalTempView("ord_items")

# COMMAND ----------

# MAGIC %md
# MAGIC **Query to identify top 10 customers by total spend**

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC   select
# MAGIC   c.CustomerID,
# MAGIC   concat(c.FirstName, ' ',c.LastName) as name,
# MAGIC   (oi.Quantity * CAST(REPLACE(REPLACE(p.Actual_Price,'₹',''),',','') AS INT)) AS amt
# MAGIC   from global_temp.products p
# MAGIC   join global_temp.ord_items oi on p.Product_ID = oi.ProductID
# MAGIC   join global_temp.orders o on o.OrderID = oi.OrderID
# MAGIC   join global_temp.customers c on o.CustomerID = c.CustomerID
# MAGIC )
# MAGIC  
# MAGIC select
# MAGIC CustomerID,
# MAGIC name,
# MAGIC sum(amt) as price
# MAGIC from cte
# MAGIC group by CustomerID, name
# MAGIC order by sum(amt) desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC **Query to determine the most popular shipping tier for orders**

# COMMAND ----------

shipping_tier_path='dbfs:/mnt/globalmart/ecom_data/shipping_tier.csv'
shipping_tier_df = spark.read.option("header","true").option("inferSchema","true").csv(shipping_tier_path)

# COMMAND ----------

shipping_tier_df.createOrReplaceGlobalTempView("shipping_tier")

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC   select
# MAGIC   o.OrderID,
# MAGIC   s.ShippingTierID,
# MAGIC   s.TierName,
# MAGIC   count(s.TierName) as cnt
# MAGIC   from global_temp.orders o
# MAGIC   join global_temp.shipping_tier s on o.ShippingTierID = s.ShippingTierID
# MAGIC   group by o.OrderID, s.ShippingTierID, s.TierName
# MAGIC )
# MAGIC select
# MAGIC TierName,
# MAGIC sum(cnt) as ct
# MAGIC from cte
# MAGIC group by TierName
# MAGIC order by sum(cnt) desc limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Query to find Running Total of Sales for Each Product**

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte1 as (
# MAGIC   select 
# MAGIC   o.OrderID,
# MAGIC   p.Product_ID,
# MAGIC   Od.Quantity,
# MAGIC   p.Actual_price,
# MAGIC   (Od.Quantity * CAST(REPLACE(REPLACE(p.Actual_Price,'₹',''),',','') AS INT)) AS sales,
# MAGIC   o.OrderDate
# MAGIC   from global_temp.orders o
# MAGIC   join global_temp.ord_items Od on o.OrderID = Od.OrderID
# MAGIC   join global_temp.products p on Od.ProductID = p.Product_ID
# MAGIC   order by 2,6
# MAGIC )
# MAGIC select 
# MAGIC OrderID,
# MAGIC Product_ID,
# MAGIC sales,
# MAGIC sum(sales)over(partition by Product_ID order by OrderDate) as rolling_sum
# MAGIC from cte1

# COMMAND ----------

# MAGIC %md
# MAGIC **Query to create a summary table at a Customer level**

# COMMAND ----------

returns_path = 'dbfs:/mnt/globalmart/ecom_data/returns.csv'
returns_df = spark.read.option("header","true").option("inferSchema","true").csv(returns_path)

# COMMAND ----------

returns_df.createOrReplaceGlobalTempView("returns")

# COMMAND ----------

# MAGIC %sql
# MAGIC with order_dates as (
# MAGIC   select
# MAGIC   o.CustomerID,
# MAGIC   o.OrderDate,
# MAGIC   lead(o.OrderDate) over (partition by o.CustomerID order by o.OrderDate) as next_order_date
# MAGIC   from global_temp.orders o
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   concat(c.FirstName, ' ',c.LastName) as customer_name,
# MAGIC   count(distinct o.OrderID) as tot_orders,
# MAGIC   count(distinct r.OrderId) as tot_returns,
# MAGIC   sum(cast(replace(replace(p.Actual_Price, '₹', ''), ',', '') as int) * oi.Quantity) AS order_value,
# MAGIC   floor(sum(oi.Quantity) / count(distinct o.OrderID)) as avg_basket_size,
# MAGIC   round(sum(cast(replace(replace(p.Actual_Price, '₹', ''), ',', '') as int) * oi.Quantity) / count(distinct o.OrderID),2) as avg_basket_value,
# MAGIC   datediff(max(o.OrderDate), min(o.OrderDate)) as length_of_stay_days,
# MAGIC   round(avg(datediff(od.next_order_date, o.OrderDate))) as order_purchase_frequency
# MAGIC from global_temp.customers c
# MAGIC join global_temp.orders o on c.CustomerID = o.CustomerID
# MAGIC left join global_temp.returns r on o.OrderID = r.OrderID
# MAGIC join global_temp.ord_items oi on o.OrderID = oi.OrderID
# MAGIC join global_temp.products p on p.Product_ID = oi.ProductID
# MAGIC join order_dates od ON o.CustomerID = od.CustomerID AND o.OrderDate = od.OrderDate
# MAGIC group by concat(c.FirstName, ' ',c.LastName)

# COMMAND ----------

# MAGIC %md
# MAGIC **A view to find out the number of products provided by each supplier**

# COMMAND ----------

suppliers_path = 'dbfs:/mnt/globalmart/ecom_data/suppliers.csv'
suppliers_df = spark.read.option("header","true").option("inferSchema","true").csv(suppliers_path)

# COMMAND ----------

suppliers_df.createOrReplaceGlobalTempView("suppliers")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view sup_prod_count as
# MAGIC select
# MAGIC   s.SupplierID,
# MAGIC   count(p.Product_ID) as NumOfProducts
# MAGIC from global_temp.suppliers s
# MAGIC join global_temp.orders o ON o.SupplierID = s.SupplierID
# MAGIC join global_temp.ord_items oi ON oi.OrderID = o.OrderID
# MAGIC join global_temp.products p ON oi.ProductID = p.Product_ID
# MAGIC group by s.SupplierID

# COMMAND ----------

# MAGIC %md
# MAGIC **Code to identify products with an average rating of 4.5 or higher**

# COMMAND ----------

avg_ratings_df = products_df.withColumn("Product_Rating", col("Product_Rating").cast("float")) \
    .groupBy("Product_ID") \
    .agg(round(avg("Product_Rating"), 1).alias("AvgRating"))

# COMMAND ----------

highly_rated_products_df = avg_ratings_df.filter(col("AvgRating") >= 4.5)
display(highly_rated_products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **To calculate the number of days between the order placement and shipping date for each order**

# COMMAND ----------

NoOfDays = orders_df.withColumn("Days Diff",floor((col("ShippingDate").cast("long")-col("OrderDate").cast("long"))/ 
          (24*60*60)))

# COMMAND ----------

orders_df_2 = orders_df.withColumn('OrderDate',to_date('OrderDate','yyyy-mm-dd'))
orders_df_2 = orders_df_2.withColumn('ShippingDate',to_date('ShippingDate','yyyy-mm-dd'))

orders_df_2 = orders_df_2.withColumn('Difference',datediff('ShippingDate','OrderDate'))

# COMMAND ----------

# MAGIC %md
# MAGIC **To calculate the month-over-month growth rate in sales**

# COMMAND ----------

cte1 = (
  orders_items_df
  .join(products_df, orders_items_df.ProductID == products_df.Product_ID)
  .withColumn("sales", col("Quantity") * regexp_replace(col("Actual_Price"),'[₹,]',''))
  .select("OrderID", orders_items_df.ProductID,"sales")
)

# COMMAND ----------

cte2 = (
  cte1
  .join(orders_df, "OrderID")
  .withColumn("year",year("OrderDate"))
  .withColumn("month",month("OrderDate"))
  .select("year","month","sales")
  .groupBy("year","month")
  .agg(sum("sales").alias("sales"))
  .orderBy("year","month")
)

# COMMAND ----------

cte3 =(
  cte2
  .withColumn("prevMonthSales",lag("sales").over(Window.partitionBy("year").orderBy("month")))
  .withColumn("mom", 100*(col("sales")-col("prevMonthSales"))/col("prevMonthSales"))
)

cte3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Code for the product analysis report**

# COMMAND ----------

# Calculate total orders, total units sold, total revenue, and average price
product_performance = (
  orders_items_df
  .join(products_df, orders_items_df.ProductID == products_df.Product_ID)
  .withColumn("revenue", col("Quantity") * regexp_replace(col("Actual_Price"), '[₹,]', '').cast("float"))
  .groupBy("ProductID", "Product_Name")
  .agg(count("OrderID").alias("Total_Orders"),
       sum("Quantity").alias("Total_Units_Sold"),
       sum("revenue").alias("Total_Revenue"),
       avg(regexp_replace(col("Actual_Price"), '[₹,]', '').cast("float")).alias("Avg_Price")
      )
)

# COMMAND ----------

# Calculate total returns and return rate
returns = (
  returns_df
  .groupBy("OrderId")
  .agg(count("OrderId").alias("Total_Returns"))
)

# COMMAND ----------

# Merge returns with orders_items_df on OrderID
orders_items_with_returns = (
  orders_items_df
  .join(returns, "OrderID", "left")
  .withColumn("Total_Returns", col("Total_Returns").cast("int"))
)

display(orders_items_with_returns)

# COMMAND ----------

# Join the performance and returns dataframes
product_analysis_report = (
  product_performance
  .join(orders_items_with_returns, "ProductID", "left")
  .withColumn("Total_Returns", col("Total_Returns").cast("int"))
  .withColumn("Return_rate", (col("Total_Returns") / col("Total_Orders")) * 100)
  .select("ProductID", "Product_Name", "Total_Orders", "Total_Units_Sold", "Total_Revenue", "Avg_Price", "Total_Returns", "Return_rate")
)
 
display(product_analysis_report)

# COMMAND ----------

# MAGIC %md
# MAGIC **Code to classify the customers using lit operator**

# COMMAND ----------

cte = (
  products_df
  .join(orders_items_df,orders_items_df.ProductID==products_df.Product_ID)
  .join(orders_df,"OrderID")
  .withColumn("sales",col("Quantity")*regexp_replace(col("Discounted_Price"),'[₹,]',''))
  .select("CustomerID","sales")
  .groupBy("CustomerID").agg(sum("sales").alias("total_spent"))
)

classify_customers = (
  cte
  .withColumn("type", when((col("total_spent")>1000), lit("Platinum")).otherwise(when((col("total_spent")<500), lit("Silver")).otherwise(lit("Gold"))))
)
classify_customers.display()
