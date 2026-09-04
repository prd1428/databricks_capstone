from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_date,
    year,
    month,
    date_format
)

spark = SparkSession.builder.getOrCreate()

# ============================================================
# 1. Source and target tables
# ============================================================

bronze_table = "workspace.default.capstone_bronze_sales"
silver_table = "workspace.default.capstone_silver_sales"

# ============================================================
# 2. Read Bronze
# ============================================================

df = spark.table(bronze_table)

print(f"Bronze record count: {df.count()}")

# ============================================================
# 3. Trim string columns
# ============================================================

df = (
    df
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("payment_method", trim(col("payment_method")))
    .withColumn("order_status", trim(col("order_status")))
)

# ============================================================
# 4. Convert data types
# ============================================================

df = (
    df
    .withColumn("order_date", to_date(col("order_date")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("gross_amount", col("gross_amount").cast("double"))
    .withColumn("discount_amount", col("discount_amount").cast("double"))
    .withColumn("net_amount", col("net_amount").cast("double"))
)

# ============================================================
# 5. Remove invalid business records
# ============================================================

df = (
    df
    .filter(col("customer_id").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("net_amount") >= 0)
    .filter(col("product_id") != "UNKNOWN")
)

# ============================================================
# 6. Create year, month and month_name
# ============================================================

df = (
    df
    .withColumn("year", year(col("order_date")))
    .withColumn("month", month(col("order_date")))
    .withColumn("month_name", date_format(col("order_date"), "MMMM"))
)

# ============================================================
# 7. Write Silver Delta table
# ============================================================

(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# ============================================================
# 8. Verification
# ============================================================

print("Silver cleaning completed successfully.")
print(f"Silver table: {silver_table}")
print(f"Silver record count: {df.count()}")

display(spark.table(silver_table))