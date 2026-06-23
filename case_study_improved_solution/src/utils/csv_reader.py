from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_csv(path: str, spark: SparkSession) -> DataFrame:
    """Read a CSV file from the given path with header and all columns as string."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )


def save_bronze(df: DataFrame, table_name: str, source_file: str) -> DataFrame:
    """Add bronze metadata columns and save as a table (append mode)."""
    bronze_df = (
        df
        .withColumn("_bronze_loaded_at", F.current_timestamp())
        .withColumn("_bronze_source_file", F.lit(source_file))
    )
    bronze_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    return bronze_df
