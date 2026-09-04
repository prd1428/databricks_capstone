from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.getOrCreate()

# ============================================================
# 1. Bronze table
# ============================================================

bronze_table = "workspace.default.capstone_bronze_sales"

# ============================================================
# 2. Read Bronze table
# ============================================================

df = spark.table(bronze_table)

# ============================================================
# 3. Create data quality flags
# ============================================================

dq_df = (
    df
    .withColumn(
        "dq_flag",
        when(col("customer_id").isNull(), "INVALID_CUSTOMER")
        .when(col("quantity") <= 0, "INVALID_QUANTITY")
        .when(col("net_amount") < 0, "INVALID_AMOUNT")
        .when(col("product_id") == "UNKNOWN", "INVALID_PRODUCT")
        .otherwise("VALID")
    )
)

# ============================================================
# 4. Display records with quality flags
# ============================================================

display(dq_df)

# ============================================================
# 5. Data Quality Summary
# ============================================================

dq_summary = (
    dq_df
    .groupBy("dq_flag")
    .count()
    .orderBy("dq_flag")
)

print("Data Quality Summary")
display(dq_summary)

# ============================================================
# 6. Total records
# ============================================================

total_records = df.count()

print(f"Total Bronze records: {total_records}")

# ============================================================
# 7. Invalid records
# ============================================================

invalid_records = (
    dq_df
    .filter(col("dq_flag") != "VALID")
    .count()
)

print(f"Total invalid records: {invalid_records}")

# ============================================================
# 8. Valid records
# ============================================================

valid_records = (
    dq_df
    .filter(col("dq_flag") == "VALID")
    .count()
)

print(f"Total valid records: {valid_records}")