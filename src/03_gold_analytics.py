from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum,
    avg,
    round
)

spark = SparkSession.builder.getOrCreate()

# ============================================================
# 1. Source and target tables
# ============================================================

silver_table = "workspace.default.capstone_silver_sales"
gold_table = "workspace.default.capstone_gold_sales_summary"

# ============================================================
# 2. Read Silver table
# ============================================================

df = spark.table(silver_table)

print(f"Silver record count: {df.count()}")

# ============================================================
# 3. Create Gold sales summary
# ============================================================

gold_df = (
    df
    .groupBy("year", "month", "month_name")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("quantity").alias("units_sold"),
        round(sum("gross_amount"), 2).alias("gross_sales"),
        round(sum("discount_amount"), 2).alias("discount_amount"),
        round(sum("net_amount"), 2).alias("net_sales"),
        round(avg("net_amount"), 2).alias("average_order_value")
    )
    .orderBy("year", "month")
)

# ============================================================
# 4. Write Gold Delta table
# ============================================================

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

# ============================================================
# 5. Verification
# ============================================================

print("Gold analytics completed successfully.")
print(f"Gold table: {gold_table}")

display(spark.table(gold_table))