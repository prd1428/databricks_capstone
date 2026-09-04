from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    countDistinct,
    sum,
    avg,
    round
)

spark = SparkSession.builder.getOrCreate()

# Source and target tables
silver_table = "workspace.default.capstone_silver_sales"
gold_table = "workspace.default.capstone_gold_sales_summary"


# Read Silver table
df = spark.table(silver_table)
print(f"Silver record count: {df.count()}")


# Create Gold sales summary
# Keep year/month for monthly analysis
# Keep state/category for dashboard analysis

gold_df = (
    df
    .groupBy(
        "year",
        "month",
        "month_name",
        "state",
        "category"
    )
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("quantity").alias("units_sold"),
        round(sum("gross_amount"), 2).alias("gross_sales"),
        round(sum("discount_amount"), 2).alias("discount_amount"),
        round(sum("net_amount"), 2).alias("net_sales"),
        round(avg("net_amount"), 2).alias("average_order_value")
    )
    .orderBy(
        "year",
        "month",
        "state",
        "category"
    )
)

# Write Gold Delta table
(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print("Gold analytics completed successfully.")
print(f"Gold table: {gold_table}")
print(f"Gold record count: {gold_df.count()}")
display(spark.table(gold_table))