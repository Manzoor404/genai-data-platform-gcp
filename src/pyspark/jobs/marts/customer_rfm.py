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
    min as _min,
    max as _max,
    to_date,
    datediff,
    current_date,
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
        .appName("customer_rfm_mart")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "50")
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
    customers_tbl = config["bq"]["curated_customers_table"]

    orders = (
        read_curated_table(spark, project_id, curated_dataset, orders_tbl)
        .select(
            trim(col("order_id")).alias("order_id"),
            trim(col("customer_id")).alias("customer_id"),
            col("order_purchase_date"),
        )
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["order_id"])
    )

    order_items_agg = (
        read_curated_table(spark, project_id, curated_dataset, order_items_tbl)
        .select(
            trim(col("order_id")).alias("order_id"),
            trim(col("order_item_id")).alias("order_item_id"),
            col("price"),
        )
        .filter(col("order_id").isNotNull())
        .filter(col("order_item_id").isNotNull())
        .groupBy("order_id")
        .agg(
            _sum(col("price")).alias("order_value")
        )
    )

    customers = (
        read_curated_table(spark, project_id, curated_dataset, customers_tbl)
        .select(
            trim(col("customer_id")).alias("customer_id"),
            trim(col("customer_unique_id")).alias("customer_unique_id"),
            trim(col("customer_city")).alias("customer_city"),
            trim(col("customer_state")).alias("customer_state"),
        )
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    joined = (
        orders.alias("o")
        .join(order_items_agg.alias("oi"), on="order_id", how="left")
        .join(customers.alias("c"), on="customer_id", how="left")
    )

    mart = (
        joined.groupBy(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_city"),
            col("customer_state"),
        )
        .agg(
            countDistinct(col("order_id")).alias("frequency_orders"),
            _sum(col("order_value")).alias("monetary_value"),
            avg(col("order_value")).alias("avg_order_value"),
            _min(to_date(col("order_purchase_date"))).alias("first_order_date"),
            _max(to_date(col("order_purchase_date"))).alias("last_order_date"),
        )
        .withColumn("recency_days", datediff(current_date(), col("last_order_date")))
        .withColumn("record_source", lit("curated_layer.customer_rfm"))
        .withColumn("load_timestamp", current_timestamp())
    )

    return mart


def write_mart(df, config):
    project_id = config["project_id"]
    analytics_dataset = config["bq"]["analytics_dataset"]
    table_name = config["bq"]["customer_rfm_table"]

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

    print("\n========== MART SUMMARY: CUSTOMER RFM ==========")
    print("Mart built successfully.")
    print("================================================\n")

    write_mart(df_mart, config)
    spark.stop()


if __name__ == "__main__":
    main()
