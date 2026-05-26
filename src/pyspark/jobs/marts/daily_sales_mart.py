import argparse
import subprocess
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp,
    lit,
    countDistinct,
    sum as _sum,
    avg,
    to_date,
)


def load_config(config_path):
    local_path = config_path

    if config_path.startswith("gs://"):
        local_path = "/tmp/prod.yaml"
        subprocess.run(["gsutil", "cp", config_path, local_path], check=True)

    with open(local_path, "r") as file:
        return yaml.safe_load(file)


def create_spark():
    return (
        SparkSession.builder
        .appName("daily_sales_mart")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "20")
        .getOrCreate()
    )


def read_curated_table(spark, project_id, dataset, table):
    return (
        spark.read.format("bigquery")
        .option("table", f"{project_id}:{dataset}.{table}")
        .load()
    )


def build_mart(spark, config):
    project_id = config["project_id"]
    curated_dataset = config["bq"]["curated_dataset"]

    orders_tbl = config["bq"]["curated_orders_table"]
    order_items_tbl = config["bq"]["curated_order_items_table"]

    orders = (
        read_curated_table(spark, project_id, curated_dataset, orders_tbl)
        .select(
            trim(col("order_id")).alias("order_id"),
            to_date(col("order_purchase_date")).alias("order_date"),
            trim(col("customer_id")).alias("customer_id"),
        )
        .filter(col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
    )

    order_items = (
        read_curated_table(spark, project_id, curated_dataset, order_items_tbl)
        .select(
            trim(col("order_id")).alias("order_id"),
            trim(col("order_item_id")).alias("order_item_id"),
            col("price"),
        )
        .filter(col("order_id").isNotNull())
    )

    joined = (
        orders.alias("o")
        .join(order_items.alias("oi"), on="order_id", how="inner")
    )

    mart = (
        joined.groupBy("order_date")
        .agg(
            countDistinct(col("order_id")).alias("total_orders"),
            countDistinct(col("customer_id")).alias("total_customers"),
            countDistinct(col("order_item_id")).alias("total_items"),
            _sum(col("price")).alias("total_revenue"),
            avg(col("price")).alias("avg_item_price"),
        )
        .withColumn("record_source", lit("curated_layer.daily_sales_mart"))
        .withColumn("load_timestamp", current_timestamp())
    )

    return mart


def write_mart(df, config):
    project_id = config["project_id"]
    analytics_dataset = config["bq"]["analytics_dataset"]
    table_name = config["bq"]["daily_sales_table"]

    (
        df.write.format("bigquery")
        .option("table", f"{project_id}:{analytics_dataset}.{table_name}")
        .option("temporaryGcsBucket", config["gcs"]["temp_bucket"])
        .mode("overwrite")
        .save()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)

    spark = create_spark()

    df_mart = build_mart(spark, config)

    print("\n========== DAILY SALES MART ==========")
    print("Mart built successfully.")
    print("======================================\n")

    write_mart(df_mart, config)

    spark.stop()


if __name__ == "__main__":
    main()
