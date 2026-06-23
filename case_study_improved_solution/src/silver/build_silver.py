# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup path and imports
import sys
sys.path.insert(0, "/Workspace/Users/arthurferreirareis.arthur@studentambassadors.com/casestudy/case_study_improved_solution/src")

from utils.silver_transforms import type_products, type_sales_order_detail, type_sales_order_header

CATALOG = "case_study"

# COMMAND ----------

# DBTITLE 1,Build all silver tables
# Read bronze, apply typing, save to silver
tables = [
    (f"{CATALOG}.bronze.products_raw", type_products, f"{CATALOG}.silver.products"),
    (f"{CATALOG}.bronze.sales_order_detail_raw", type_sales_order_detail, f"{CATALOG}.silver.sales_order_detail"),
    (f"{CATALOG}.bronze.sales_order_header_raw", type_sales_order_header, f"{CATALOG}.silver.sales_order_header"),
]

for bronze_table, transform_fn, silver_table in tables:
    bronze_df = spark.table(bronze_table)
    silver_df = transform_fn(bronze_df)
    silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
    print(f"✓ {silver_table} — {silver_df.count()} rows")