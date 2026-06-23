# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Setup path and imports
import sys
sys.path.insert(0, "/Workspace/Users/arthurferreirareis.arthur@studentambassadors.com/casestudy/case_study_improved_solution/src")

from utils.csv_reader import read_csv, save_bronze

SOURCE_PATH = "/Volumes/case_study/landing/files"
CATALOG = "case_study"
SCHEMA = "bronze"


# COMMAND ----------

# DBTITLE 1,Ingest all sources to bronze
# Define sources: (file_name, table_name)
sources = [
    ("products.csv", f"{CATALOG}.{SCHEMA}.products_raw"),
    ("sales_order_detail.csv", f"{CATALOG}.{SCHEMA}.sales_order_detail_raw"),
    ("sales_order_header.csv", f"{CATALOG}.{SCHEMA}.sales_order_header_raw"),
]

for file_name, table_name in sources:
    df = read_csv(f"{SOURCE_PATH}/{file_name}", spark)
    save_bronze(df, table_name, file_name)
    print(f"✓ {table_name} — {df.count()} rows")
