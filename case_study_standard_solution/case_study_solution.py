# Databricks notebook source
# DBTITLE 1,import libs
from pyspark.sql.functions import col, when, date_add, dayofweek, datediff, floor, lit, least, greatest, year, sum, desc, row_number, avg
from pyspark.sql.types import IntegerType, BooleanType, DecimalType, DoubleType, DateType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Ingest data
raw_products = spark.read.csv('/Volumes/case_study/landing/files/products.csv', header=True)

raw_sales_order_detail = spark.read.csv('/Volumes/case_study/landing/files/sales_order_detail.csv', header=True)

raw_sales_order_header = spark.read.csv('/Volumes/case_study/landing/files/sales_order_header.csv', header=True)

# COMMAND ----------

# DBTITLE 1,Célula 3
store_products = raw_products.select(
    col("ProductId").alias("ProductID"),
    "ProductDesc",
    "ProductNumber",
    col("MakeFlag").cast(BooleanType()).alias("MakeFlag"),
    "Color",
    col("SafetyStockLevel").cast(IntegerType()).alias("SafetyStockLevel"),
    col("ReorderPoint").cast(IntegerType()).alias("ReorderPoint"),
    col("StandardCost").cast(DoubleType()).alias("StandardCost"),
    col("ListPrice").cast(DecimalType()).alias("ListPrice"),
    "Size",
    "SizeUnitMeasureCode",
    col("Weight").cast(DecimalType()).alias("Weight"),
    "WeightUnitMeasureCode",
    "ProductCategoryName",
    "ProductSubCategoryName"
)

raw_products_count = raw_products.count()
store_products_count = store_products.count()
null_store_products_pk = store_products.filter(col('ProductID').isNull()).count()
duplicate_pk_store_products = store_products.groupBy('ProductID').count().filter((col('ProductID').isNotNull()) & (col('count') > 1)).count()

print(f"raw_products rows: {raw_products_count}")
print(f"store_products rows: {store_products_count}")
print(f"store_products ProductID null PK rows: {null_store_products_pk}")
print(f"store_products ProductID duplicate PK rows: {duplicate_pk_store_products}")

# COMMAND ----------

# DBTITLE 1,Célula 4
store_sales_order_detail = raw_sales_order_detail.select(
    "SalesOrderID",
    "SalesOrderDetailID",
    col("OrderQty").cast(IntegerType()).alias("OrderQty"),
    "ProductID",
    col("UnitPrice").cast(DoubleType()).alias("UnitPrice"),
    col("UnitPriceDiscount").cast(DoubleType()).alias("UnitPriceDiscount")
)

raw_sales_order_detail_count = raw_sales_order_detail.count()
store_sales_order_detail_count = store_sales_order_detail.count()
null_sales_order_pk = store_sales_order_detail.filter(col('SalesOrderDetailID').isNull()).count()
duplicate_sales_order_pk = store_sales_order_detail.groupBy('SalesOrderDetailID').count().filter((col('SalesOrderDetailID').isNotNull()) & (col('count') > 1)).count()

print(f"raw_sales_order_detail rows: {raw_sales_order_detail_count}")
print(f"store_sales_order_detail rows: {store_sales_order_detail_count}")
print(
    f"store_sales_order_detail SalesOrderDetailID null PK rows: {null_sales_order_pk}")
print(
    "store_sales_order_detail SalesOrderDetailID duplicate PK rows: "
    f"{duplicate_sales_order_pk}"
)

# COMMAND ----------

# DBTITLE 1,Célula 5
store_sales_order_header = raw_sales_order_header.select(
    "SalesOrderID",
    col("OrderDate").cast(DateType()).alias("OrderDate"),
    col("ShipDate").cast(DateType()).alias("ShipDate"),
    col("OnlineOrderFlag").cast(BooleanType()).alias("OnlineOrderFlag"),
    "AccountNumber",
    "CustomerID",
    "SalesPersonID",
    col("Freight").cast(DoubleType()).alias("Freight")
)

raw_sales_order_header_count = raw_sales_order_header.count()
store_sales_order_header_count = store_sales_order_header.count()
null_sales_order_header_pk = store_sales_order_header.filter(col('SalesOrderID').isNull()).count()
duplicate_sales_order_header_pk = store_sales_order_header.groupBy('SalesOrderID').count().filter((col('SalesOrderID').isNotNull()) & (col('count') > 1)).count()

print(f"raw_sales_order_header rows: {raw_sales_order_header_count}")
print(f"store_sales_order_header rows: {store_sales_order_header_count}")
print(f"store_sales_order_header SalesOrderID null PK rows: {null_sales_order_header_pk}")
print("store_sales_order_header SalesOrderID duplicate PK rows: "f"{duplicate_sales_order_header_pk}")

# COMMAND ----------

# DBTITLE 1,Célula 7
# Residual NULLs in ProductCategoryName can remain for subcategories outside the three ruels.
publish_product = (
    store_products
    .na.fill('N/A', subset=["Color"])
    .withColumn(
        "ProductCategoryName",
        when(
            col("ProductCategoryName").isNull() &
            col("ProductSubCategoryName").isin(['Gloves', 'Shorts', 'Socks', 'Tights', 'Vests']),
            "Clothing"
        )
        .when(
            col("ProductCategoryName").isNull() &
            col("ProductSubCategoryName").isin(['Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps']),
            "Accessories"
        )
        .when(
            col("ProductCategoryName").isNull() &
            (
                col("ProductSubCategoryName").contains("Frames") |
                col("ProductSubCategoryName").isin(['Wheels', 'Saddles'])
            ),
            "Components"
        )
        .otherwise(col("ProductCategoryName"))
    )
)

# publish_product.write.mode("overwrite").saveAsTable("publish_product")

# COMMAND ----------

# DBTITLE 1,Célula 8
# Monday reference date used by the accumulator formula.
# 2021-05-31 is the first Monday at or before the OrderDate range in this dataset.
_REF_MONDAY = lit("2021-05-31").cast("date")

def _bdays_acc(d):
    # datediff(d, _REF_MONDAY) gives the elapsed calendar days since the reference Monday.
    # Dividing by 7 and taking floor(...) gives the number of full weeks.
    # Each full week contributes exactly 5 business days.
    #
    # dayofweek(d) in Spark returns:
    # Sunday=1, Monday=2, Tuesday=3, Wednesday=4, Thursday=5, Friday=6, Saturday=7.
    # (dayofweek(d) + 5) % 7 remaps that to:
    # Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4, Saturday=5, Sunday=6.
    #
    # least(lit(5), ...) caps Saturday and Sunday at 5, so weekends do not add extra
    # business days beyond Friday. The result is an accumulated business-day counter
    # for any date without building a calendar table or expanding date sequences.
    return floor(datediff(d, _REF_MONDAY) / 7) * 5 + least(lit(5), (dayofweek(d) + 5) % 7)

def work_day(start_col, end_col):
    # Return NULL if either date is missing; otherwise return the business-day difference.
    return when(
        start_col.isNull() | end_col.isNull(), None
    ).otherwise(_bdays_acc(end_col) - _bdays_acc(start_col))

publish_orders = (
    store_sales_order_detail
    .join(store_sales_order_header, on="SalesOrderID", how="inner")
    .withColumn("LeadTimeInBusinessDays", work_day(col("OrderDate"), col("ShipDate")))
    .withColumn(
        "TotalLineExtendedPrice",
        col("OrderQty") * (col("UnitPrice") - col("UnitPriceDiscount"))
    )
    .withColumnRenamed("Freight", "TotalOrderFreight")
)

# publish_orders.write.mode("overwrite").saveAsTable("publish_orders")


# COMMAND ----------

# MAGIC %md
# MAGIC Which color generated the highest revenue each year?
# MAGIC
# MAGIC 2021 |	Red	| 6019614.015699884
# MAGIC
# MAGIC 2022 |	Black |	14005242.975200139
# MAGIC
# MAGIC 2023 |	Black |	15047694.36920016
# MAGIC
# MAGIC 2024 |	Yellow |	6480746.07220017
# MAGIC

# COMMAND ----------

highest_revenue = (
    publish_orders
    .join(publish_product, on="ProductID", how="inner")
    .groupBy(
        year(col("OrderDate")).alias("Year"),
        "Color"
    )
    .agg(
        sum("TotalLineExtendedPrice").alias("TotalRevenue")
    )
)

w = Window.partitionBy('Year').orderBy(col('TotalRevenue').desc())

highest_revenue_color = (
    highest_revenue.withColumn('rn', row_number().over(w)).filter(col('rn') == 1)
    .drop('rn')
)

display(highest_revenue_color)

# COMMAND ----------

# MAGIC %md
# MAGIC What is the average LeadTimeInBusinessDays by ProductCategoryName?
# MAGIC
# MAGIC Bikes | 	5.004896845147306
# MAGIC
# MAGIC null | 	5.00983409151726
# MAGIC
# MAGIC Clothing | 	5.005067001675042
# MAGIC
# MAGIC Accessories | 	5.0065279164426695
# MAGIC
# MAGIC Components | 	5.0032938844517

# COMMAND ----------

average_leadtime = (
    publish_orders
    .join(publish_product, on="ProductID", how="inner")
    .groupBy(
        'ProductCategoryName'
    )
    .agg(
        avg("LeadTimeInBusinessDays").alias("Average_LeadTimeBD")
    )
)

display(average_leadtime)