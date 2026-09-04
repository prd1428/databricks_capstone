from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.getOrCreate()

# Bronze table
bronze_table = "workspace.default.capstone_bronze_sales"


# Read Bronze table
df = spark.table(bronze_table)


# Create data quality flags
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


display(dq_df)


# Data Quality Summary
dq_summary = (
    dq_df
    .groupBy("dq_flag")
    .count()
    .orderBy("dq_flag")
)

print("Data Quality Summary")
display(dq_summary)


# Total records
total_records = df.count()
print(f"Total Bronze records: {total_records}")

# Invalid records
invalid_records = (
    dq_df
    .filter(col("dq_flag") != "VALID")
    .count()
)

print(f"Total invalid records: {invalid_records}")

# Valid records
valid_records = (
    dq_df
    .filter(col("dq_flag") == "VALID")
    .count()
)

print(f"Total valid records: {valid_records}")