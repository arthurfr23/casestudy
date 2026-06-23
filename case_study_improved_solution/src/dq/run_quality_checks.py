# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load YAML contracts and run DQ checks
import yaml
import os
from datetime import datetime, timezone
from pyspark.sql import functions as F

# Path to contracts
CONTRACTS_DIR = "/Workspace/Users/arthurferreirareis.arthur@studentambassadors.com/casestudy/case_study_improved_solution/src/dq/contracts"
CONTROL_TABLE = "case_study.gold.dq_control"


def load_contracts(contracts_dir: str) -> list:
    """Load all YAML contract files from directory."""
    contracts = []
    for file in sorted(os.listdir(contracts_dir)):
        if file.endswith(".yml"):
            with open(os.path.join(contracts_dir, file)) as f:
                contracts.append(yaml.safe_load(f))
    return contracts


def run_check(spark, table_name: str, check: dict) -> dict:
    """Run a single check and return the result."""
    df = spark.table(table_name)
    total_rows = df.count()
    failed_rows = 0

    if check["type"] == "row_count":
        min_rows = check.get("min_rows", 1)
        failed_rows = 0 if total_rows >= min_rows else 1

    elif check["type"] == "not_null":
        for col in check["columns"]:
            failed_rows += df.filter(F.col(col).isNull()).count()

    elif check["type"] == "unique":
        cols = check["columns"]
        failed_rows = df.groupBy(*cols).count().filter(F.col("count") > 1).count()

    elif check["type"] == "expression":
        expr = check["expr"]
        failed_rows = df.filter(~F.expr(expr)).count()

    return {
        "table_name": table_name,
        "check_name": check["name"],
        "check_type": check["type"],
        "status": "PASS" if failed_rows == 0 else "FAIL",
        "total_rows": total_rows,
        "failed_rows": failed_rows,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


# Run all checks
contracts = load_contracts(CONTRACTS_DIR)
results = []

for contract in contracts:
    table = contract["table"]
    for check in contract["checks"]:
        result = run_check(spark, table, check)
        result["layer"] = contract["layer"]
        results.append(result)
        status_icon = "✓" if result["status"] == "PASS" else "✗"
        print(f"  {status_icon} {result['table_name']} | {result['check_name']} | {result['status']}")

# Save to control table
results_df = spark.createDataFrame(results)
results_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CONTROL_TABLE)
print(f"\n✓ Results saved to {CONTROL_TABLE} ({len(results)} checks)")