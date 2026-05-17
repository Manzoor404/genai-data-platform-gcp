import argparse
import yaml
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp,
    lit,
    round
)


def load_config(config_path):
    local_path = config_path

    if config_path.startswith("gs://"):
        local_path = "/tmp/prod.yaml"
        subprocess.run(["gsutil", "cp", config_path, local_path], check=True)

    with open(local_path, "r") as file:
        return yaml.safe_load(file)


def create_spark():
    return SparkSession.builder.appName("curate_order_items").getOrCreate()


def read_processed(spark, config):
    project = config["project_id"]
    dataset = config["bq"]["processed_dataset"]
    table = config["bq"]["order_items_table"]

    return (
        spark.read.format("bigquery")
        .option("table", f"{project}:{dataset}.{table}")
        .load()
    )


def transform(df):
    return (
        df.select(
            trim(col("order_id")).alias("order_id"),
            trim(col("order_item_id")).alias("order_item_id"),
            trim(col("product_id")).alias("product_id"),
            trim(col("seller_id")).alias("seller_id"),

            col("shipping_limit_date"),

            round(col("price"), 2).alias("price"),

            round(col("freight_value"), 2).alias("freight_value"),

            lit("processed_layer.order_items").alias("record_source"),

            current_timestamp().alias("load_timestamp"),
        )

        .filter(col("order_id").isNotNull())
        .filter(col("order_item_id").isNotNull())
        .filter(col("product_id").isNotNull())
        .filter(col("seller_id").isNotNull())

        .dropDuplicates(["order_id", "order_item_id"])
    )


def write(df, config):
    project = config["project_id"]
    dataset = config["bq"]["curated_dataset"]
    table = config["bq"]["curated_order_items_table"]

    (
        df.write.format("bigquery")
        .option("table", f"{project}:{dataset}.{table}")
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

    df = read_processed(spark, config)
    df_curated = transform(df)

    print("========== CURATED SUMMARY: ORDER ITEMS ==========")
    print(f"Input count: {df.count()}")
    print(f"Output count: {df_curated.count()}")
    print("===================================================")

    write(df_curated, config)
    spark.stop()


if __name__ == "__main__":
    main()
