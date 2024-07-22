# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://raw@pizzasales321.blob.core.windows.net",
  mount_point = "/mnt/raw2",
  extra_configs = {"fs.azure.account.key.pizzasales321.blob.core.windows.net":"41rJ105weLz7M0Exk5uMXD42d8Ta/NAenFsWTjivA9n9yW5MZ3lxWMducBxL+yt9wLRFLZU9oIM6+AStI24SDw=="})

# COMMAND ----------

dbutils.fs.ls("/mnt/raw2")

# COMMAND ----------

df = spark.read.format("csv").options(header='True',inferschema='True').load('dbfs:/mnt/raw2/dbo.pizza_sales.txt')

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("pizza_sales_analysis")



# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from pizza_sales_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table aggreage_table as
# MAGIC select
# MAGIC count(distinct order_id) order_id,
# MAGIC sum(quantity) quantity,
# MAGIC date_format(order_date, 'MMM') month_name,
# MAGIC date_format(order_date, 'EEEE') day_name,
# MAGIC hour(order_time) order_time,
# MAGIC sum(unit_price) unit_price,
# MAGIC sum(total_price) total_price,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_sales_analysis
# MAGIC group by 3,4,5,8,9,10;
# MAGIC
# MAGIC

# COMMAND ----------


