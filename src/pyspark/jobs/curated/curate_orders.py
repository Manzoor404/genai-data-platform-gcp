import argparse
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp,
    to_date,
    year,
    month,
    lit
)


import subprocess

def load_config(config_path):

    local_path = config_path

    if config_path.startswith("gs://"):
        local_path = "/tmp/prod.yaml"

        subprocess.run(
            ["gsutil", "cp", config_path, local_path],
            check=True
        )

    with open(local_path, "r") as file:
        return yaml.safe_load(file)


def create_spark_session(app_name="curate_orders"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    return spark


def read_processed_orders(spark, config):
    project_id = config["project_id"]
    dataset_name = config["bq"]["processed_dataset"]
    table_name = config["bq"]["orders_table"]

    df = (
        spark.read.format("bigquery")
        .option("table", f"{project_id}:{dataset_name}.{table_name}")
        .load()
    )
    return df


def transform_orders(df):

    curated_df = (
        df.select(
            trim(col("order_id")).alias("order_id"),
            trim(col("customer_id")).alias("customer_id"),
            trim(col("order_status")).alias("order_status"),

            col("order_purchase_timestamp"),
            col("order_approved_at"),
            col("order_delivered_carrier_date"),
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date"),

            to_date(col("order_purchase_timestamp")).alias("order_purchase_date"),

            year(col("order_purchase_timestamp")).alias("order_purchase_year"),

            month(col("order_purchase_timestamp")).alias("order_purchase_month"),

            lit("processed_layer.orders").alias("record_source"),

            current_timestamp().alias("load_timestamp")
        )

        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("order_purchase_timestamp").isNotNull())
        .filter(col("order_status").isNotNull())
        .filter(col("order_status") != "")

        .dropDuplicates(["order_id"])
    )

    return curated_df


def write_output(df, config):
    project_id = config["project_id"]
    dataset_name = config["bq"]["curated_dataset"]
    table_name = config["bq"]["curated_orders_table"]

    (
        df.write.format("bigquery")
        .option("table", f"{project_id}:{dataset_name}.{table_name}")
        .option("temporaryGcsBucket", config["gcs"]["temp_bucket"])
        .mode("overwrite")
        .save()
    )


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config",
        required=True,
        help="Path to config YAML"
    )

    args = parser.parse_args()

    config = load_config(args.config)

    spark = create_spark_session()

    df_processed = read_processed_orders(spark, config)

    df_curated = transform_orders(df_processed)

    print("\n========== CURATED SUMMARY: ORDERS ==========")

    print(f"Processed row count: {df_processed.count()}")

    print(f"Curated row count: {df_curated.count()}")

    print("=============================================\n")

    write_output(df_curated, config)

    print(
    f"Data written to BigQuery table: "
    f"{config['project_id']}:{config['bq']['curated_dataset']}.{config['bq']['curated_orders_table']}"
)

    spark.stop()


if __name__ == "__main__":
    main()
