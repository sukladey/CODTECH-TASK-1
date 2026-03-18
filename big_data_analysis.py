from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import matplotlib.pyplot as plt

# Create Spark Session
spark = SparkSession.builder \
    .appName("big_data_analysis") \
    .getOrCreate()

# Load Dataset
data = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

data.show(5)

# Data Cleaning
data.dropna().show()
data = data.withColumn("Price", col("Price").cast("double"))
data = data.withColumn("Quantity", col("Quantity").cast("int"))

# Data Processing
data = data.withColumn("Total_Sales", col("Price") * col("Quantity"))

# Total Sales by Category
category_sales = data.groupBy("Category") \
    .agg(sum("Total_Sales").alias("Total_Revenue"))

category_sales.show()

# Top Selling Products
top_products = data.groupBy("Product") \
    .agg(sum("Quantity").alias("Total_Sold")) \
    .orderBy(desc("Total_Sold"))

top_products.show(10)

# Sales by City
city_sales = data.groupBy("City") \
    .agg(sum("Total_Sales").alias("City_Revenue"))

city_sales.show()

# Average Product Price
avg_price = data.select(avg("Price"))

avg_price.show()

# Visualization
pandas_df = category_sales.toPandas()

plt.bar(pandas_df['Category'], pandas_df['Total_Revenue'])
plt.title("Sales by Category")
plt.xlabel("Category")
plt.ylabel("Revenue")
plt.show()
