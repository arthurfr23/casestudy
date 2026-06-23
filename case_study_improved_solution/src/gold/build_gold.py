# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold: publish_product
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE case_study.gold.publish_product AS
# MAGIC SELECT
# MAGIC   ProductID,
# MAGIC   ProductDesc,
# MAGIC   ProductNumber,
# MAGIC   MakeFlag,
# MAGIC   COALESCE(Color, 'N/A') AS Color,
# MAGIC   SafetyStockLevel,
# MAGIC   ReorderPoint,
# MAGIC   StandardCost,
# MAGIC   ListPrice,
# MAGIC   Size,
# MAGIC   SizeUnitMeasureCode,
# MAGIC   Weight,
# MAGIC   WeightUnitMeasureCode,
# MAGIC   CASE
# MAGIC     WHEN ProductCategoryName IS NULL AND ProductSubCategoryName IN ('Gloves','Shorts','Socks','Tights','Vests') THEN 'Clothing'
# MAGIC     WHEN ProductCategoryName IS NULL AND ProductSubCategoryName IN ('Locks','Lights','Headsets','Helmets','Pedals','Pumps') THEN 'Accessories'
# MAGIC     WHEN ProductCategoryName IS NULL AND (ProductSubCategoryName LIKE '%Frames%' OR ProductSubCategoryName IN ('Wheels','Saddles')) THEN 'Components'
# MAGIC     ELSE ProductCategoryName
# MAGIC   END AS ProductCategoryName,
# MAGIC   ProductSubCategoryName
# MAGIC FROM case_study.silver.products

# COMMAND ----------

# DBTITLE 1,Gold: publish_orders
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE case_study.gold.publish_orders AS
# MAGIC SELECT
# MAGIC   d.SalesOrderID,
# MAGIC   d.SalesOrderDetailID,
# MAGIC   d.OrderQty,
# MAGIC   d.ProductID,
# MAGIC   d.UnitPrice,
# MAGIC   d.UnitPriceDiscount,
# MAGIC   h.OrderDate,
# MAGIC   h.ShipDate,
# MAGIC   h.OnlineOrderFlag,
# MAGIC   h.AccountNumber,
# MAGIC   h.CustomerID,
# MAGIC   h.SalesPersonID,
# MAGIC   h.Freight AS TotalOrderFreight,
# MAGIC   -- LeadTime in business days
# MAGIC   CASE
# MAGIC     WHEN h.ShipDate IS NULL OR h.OrderDate IS NULL THEN NULL
# MAGIC     ELSE (
# MAGIC       (FLOOR(DATEDIFF(h.ShipDate, DATE'2021-05-31') / 7) * 5 + LEAST(5, (DAYOFWEEK(h.ShipDate) + 5) % 7))
# MAGIC       - (FLOOR(DATEDIFF(h.OrderDate, DATE'2021-05-31') / 7) * 5 + LEAST(5, (DAYOFWEEK(h.OrderDate) + 5) % 7))
# MAGIC     )
# MAGIC   END AS LeadTimeInBusinessDays,
# MAGIC   -- Total line extended price
# MAGIC   d.OrderQty * (d.UnitPrice - d.UnitPriceDiscount) AS TotalLineExtendedPrice
# MAGIC FROM case_study.silver.sales_order_detail d
# MAGIC INNER JOIN case_study.silver.sales_order_header h ON d.SalesOrderID = h.SalesOrderID

# COMMAND ----------

# DBTITLE 1,Gold: highest_revenue_color_by_year
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE case_study.gold.highest_revenue_color_by_year AS
# MAGIC WITH revenue AS (
# MAGIC   SELECT
# MAGIC     YEAR(o.OrderDate) AS Year,
# MAGIC     p.Color,
# MAGIC     SUM(o.TotalLineExtendedPrice) AS TotalRevenue
# MAGIC   FROM case_study.gold.publish_orders o
# MAGIC   INNER JOIN case_study.gold.publish_product p ON o.ProductID = p.ProductID
# MAGIC   GROUP BY YEAR(o.OrderDate), p.Color
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY TotalRevenue DESC) AS rn
# MAGIC   FROM revenue
# MAGIC )
# MAGIC SELECT Year, Color, TotalRevenue
# MAGIC FROM ranked
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,Gold: average_leadtime_by_category
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE case_study.gold.average_leadtime_by_category AS
# MAGIC SELECT
# MAGIC   p.ProductCategoryName,
# MAGIC   AVG(o.LeadTimeInBusinessDays) AS AverageLeadTimeInBusinessDays
# MAGIC FROM case_study.gold.publish_orders o
# MAGIC INNER JOIN case_study.gold.publish_product p ON o.ProductID = p.ProductID
# MAGIC GROUP BY p.ProductCategoryName