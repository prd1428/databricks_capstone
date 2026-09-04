import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ============================================================
# 1. Source CSV path
# ============================================================

# source_path = "/Workspace/Users/dnyprasad14@gmail.com/databricks_capstone/data/sales_source_1500.csv"

source_path = sys.argv[1]

# ============================================================
# 2. Bronze table
# ============================================================

bronze_table = "workspace.default.capstone_bronze_sales"

# ============================================================
# 3. Read raw CSV
# ============================================================

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(source_path)
)

# ============================================================
# 4. Write to Bronze Delta table
# ============================================================

(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_table)
)

# ============================================================
# 5. Verification
# ============================================================

print("Bronze ingestion completed successfully.")
print(f"Bronze table: {bronze_table}")
print(f"Record count: {df.count()}")

display(spark.table(bronze_table))