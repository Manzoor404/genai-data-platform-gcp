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
        .appName("seller_performance_mart")
        .config("spark.sql.session.timeZone", "UTC")
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
    sellers_tbl = config["bq"]["curated_sellers_table"]
    reviews_tbl = config["bq"]["curated_reviews_table"]

    orders = (
        read_curated_table(spark, project_id, curated_dataset, orders_tbl)
        .select(
            trim(col("order_id")).alias("order_id"),
            trim(col("customer_id")).alias("customer_id"),
            col("order_purchase_timestamp"),
        )
        .dropDuplicates(["order_id"])
    )

    order_items = (
        read_curated_table(spark, project_id, curated_dataset, order_items_tbl)
        .select(
            trim(col("order_id")).alias("order_id"),
            trim(col("order_item_id")).alias("order_item_id"),
            trim(col("seller_id")).alias("seller_id"),
            col("price"),
            col("freight_value"),
        )
        .dropDuplicates(["order_id", "order_item_id"])
    )

    sellers = (
        read_curated_table(spark, project_id, curated_dataset, sellers_tbl)
        .select(
            trim(col("seller_id")).alias("seller_id"),
            trim(col("seller_city")).alias("seller_city"),
            trim(col("seller_state")).alias("seller_state"),
        )
        .dropDuplicates(["seller_id"])
    )

    reviews = (
        read_curated_table(spark, project_id, curated_dataset, reviews_tbl)
        .select(
            trim(col("order_id")).alias("order_id"),
            col("review_score"),
        )
        .dropDuplicates(["order_id"])
    )

    joined = (
        order_items.alias("oi")
        .join(orders.alias("o"), on="order_id", how="left")
        .join(sellers.alias("s"), on="seller_id", how="left")
        .join(reviews.alias("r"), on="order_id", how="left")
    )

    mart = (
        joined.groupBy(
            col("seller_id"),
            col("seller_city"),
            col("seller_state"),
        )
        .agg(
            countDistinct(col("order_id")).alias("total_orders"),
            countDistinct(col("order_item_id")).alias("total_order_items"),
            _sum(col("price")).alias("total_revenue"),
            avg(col("price")).alias("avg_item_price"),
            avg(col("freight_value")).alias("avg_freight_value"),
            avg(col("review_score")).alias("avg_review_score"),
            _min(to_date(col("order_purchase_timestamp"))).alias("min_order_date"),
            _max(to_date(col("order_purchase_timestamp"))).alias("max_order_date"),
        )
        .withColumn("record_source", lit("curated_layer.seller_performance"))
        .withColumn("load_timestamp", current_timestamp())
    )

    return mart


def write_mart(df, config):
    project_id = config["project_id"]
    analytics_dataset = config["bq"]["analytics_dataset"]
    table_name = config["bq"]["seller_performance_table"]

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

    print("\n========== MART SUMMARY: SELLER PERFORMANCE ==========")
    print(f"Row count: {df_mart.count()}")
    print("======================================================\n")

    write_mart(df_mart, config)
    spark.stop()


if __name__ == "__main__":
    main()
