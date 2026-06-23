from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DecimalType, DoubleType, IntegerType


def type_products(df: DataFrame) -> DataFrame:
    """Cast products columns from bronze (all string) to proper types."""
    return df.select(
        F.col("ProductID").cast(IntegerType()).alias("ProductID"),
        F.col("ProductDesc"),
        F.col("ProductNumber"),
        F.col("MakeFlag").cast(BooleanType()).alias("MakeFlag"),
        F.col("Color"),
        F.col("SafetyStockLevel").cast(IntegerType()).alias("SafetyStockLevel"),
        F.col("ReorderPoint").cast(IntegerType()).alias("ReorderPoint"),
        F.col("StandardCost").cast(DoubleType()).alias("StandardCost"),
        F.col("ListPrice").cast(DecimalType(18, 4)).alias("ListPrice"),
        F.col("Size"),
        F.col("SizeUnitMeasureCode"),
        F.col("Weight").cast(DecimalType(18, 4)).alias("Weight"),
        F.col("WeightUnitMeasureCode"),
        F.col("ProductCategoryName"),
        F.col("ProductSubCategoryName"),
    )


def type_sales_order_detail(df: DataFrame) -> DataFrame:
    """Cast sales_order_detail columns from bronze to proper types."""
    return df.select(
        F.col("SalesOrderID"),
        F.col("SalesOrderDetailID"),
        F.col("OrderQty").cast(IntegerType()).alias("OrderQty"),
        F.col("ProductID"),
        F.col("UnitPrice").cast(DoubleType()).alias("UnitPrice"),
        F.col("UnitPriceDiscount").cast(DoubleType()).alias("UnitPriceDiscount"),
    )


def type_sales_order_header(df: DataFrame) -> DataFrame:
    """Cast sales_order_header columns from bronze to proper types."""
    return df.select(
        F.col("SalesOrderID"),
        F.col("OrderDate").cast(DateType()).alias("OrderDate"),
        F.col("ShipDate").cast(DateType()).alias("ShipDate"),
        F.col("OnlineOrderFlag").cast(BooleanType()).alias("OnlineOrderFlag"),
        F.col("AccountNumber"),
        F.col("CustomerID"),
        F.col("SalesPersonID"),
        F.col("Freight").cast(DoubleType()).alias("Freight"),
    )
