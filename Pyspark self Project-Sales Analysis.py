# Databricks notebook source
/FileStore/tables/sales_csv.txt
/FileStore/tables/menu_csv.txt


# COMMAND ----------

#Sales dataframe
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schemaname = StructType([
    StructField("Product_ID",IntegerType(),True),
    StructField("Customer_ID",StringType(),True),
    StructField("Order_date",DateType(),True),
    StructField("Location",StringType(),True),
    StructField("Source_order",StringType(),True)
])

sales_df = spark.read.format("csv").option("inferschema","true").schema(schemaname).load("/FileStore/tables/sales_csv.txt")
display(sales_df)

# COMMAND ----------

# Deriving year,month,quarter

from pyspark.sql.functions import month,year,quarter
sales_df = sales_df.withColumn("Order_year",year(sales_df.Order_date))
sales_df = sales_df.withColumn("Order_month",month(sales_df.Order_date))
sales_df = sales_df.withColumn("Order_quarter",quarter(sales_df.Order_date))

display(sales_df)


# COMMAND ----------

#Menu Dataframe
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schemaname1 = StructType([
    StructField("Product_ID",IntegerType(),True),
    StructField("Product_Name",StringType(),True),
    StructField("Price",StringType(),True)
])

menu_df = spark.read.format("csv").option("inferschema","true").schema(schemaname1).load("/FileStore/tables/menu_csv.txt")
display(menu_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Total amount spent by each customer

# COMMAND ----------

total_amount_spent = sales_df.join(menu_df,'Product_ID').groupBy('Customer_ID').agg({'Price':'sum'}).orderBy('Customer_ID')

display(total_amount_spent)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total amount spent in each food category

# COMMAND ----------

total_amount_spent = sales_df.join(menu_df,'Product_ID').groupBy('Product_Name').agg({'Price':'sum'}).orderBy('Product_Name')

display(total_amount_spent)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total amount of sales in each month

# COMMAND ----------

df1 = sales_df.join(menu_df,'Product_ID').groupBy('Order_month').agg({'Price':'sum'}).orderBy('Order_month')

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total amount of sales in each year

# COMMAND ----------

df2 = sales_df.join(menu_df,'Product_ID').groupBy('Order_year').agg({'Price':'sum'}).orderBy('Order_year')

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarterly Sales

# COMMAND ----------

df3 = sales_df.join(menu_df,'Product_ID').groupBy('Order_quarter').agg({'Price':'sum'}).orderBy('Order_quarter')

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### No.of times each product was purchased
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count

most_times_df = sales_df.join(menu_df,'Product_ID').groupBy('Product_ID','Product_Name').agg(count('Product_ID').alias('Product_count'))\
                                                                                             .orderBy('Product_count',ascending = 0)

display(most_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 5 ordered items
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count

top5 = sales_df.join(menu_df,'Product_ID').groupBy('Product_ID','Product_Name').agg(count('Product_ID').alias('Product_count'))\
                                                                                             .orderBy('Product_count',ascending = 0).limit(5)

display(top5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most ordered item

# COMMAND ----------

from pyspark.sql.functions import count

topitem = sales_df.join(menu_df,'Product_ID').groupBy('Product_ID','Product_Name').agg(count('Product_ID').alias('Product_count'))\
                                                                                             .orderBy('Product_count',ascending = 0).limit(1)

display(topitem)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Frequency of customers visited to restaurant

# COMMAND ----------

from pyspark.sql.functions import countDistinct

freq_cust = sales_df.filter(sales_df.Source_order == 'Restaurant').groupBy('Customer_ID').agg(countDistinct('Order_date').alias('Customer_freq'))

display(freq_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total sales by each country

# COMMAND ----------

sales_country = sales_df.join(menu_df,'Product_ID').groupBy('Location').agg({'Price':'sum'})

display(sales_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total sales by source_order

# COMMAND ----------

source_order = sales_df.join(menu_df,'Product_ID').groupBy('Source_order').agg({'Price':'sum'})

display(source_order)
