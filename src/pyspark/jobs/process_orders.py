import argparse
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    lit,
    current_timestamp
)


import subprocess
import os

def load_config(config_path):
    # If path is GCS, download it first
    if config_path.startswith("gs://"):
        local_path = "/tmp/prod.yaml"
        subprocess.run(["gsutil", "cp", config_path, local_path], check=True)
        config_path = local_path

    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def build_spark(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()


# -----------------------
# DQ Checks
# -----------------------
def run_dq_checks_orders(df):
    raw_count = df.count()

    null_order_id_count = df.filter(col("order_id").isNull()).count()
    null_customer_id_count = df.filter(col("customer_id").isNull()).count()
    null_purchase_ts_count = df.filter(col("order_purchase_timestamp").isNull()).count()

    duplicate_order_id_count = (
        df.groupBy("order_id")
          .count()
          .filter(col("count") > 1)
          .count()
    )

    dq_results = {
        "raw_count": raw_count,
        "null_order_id_count": null_order_id_count,
        "null_customer_id_count": null_customer_id_count,
        "null_purchase_ts_count": null_purchase_ts_count,
        "duplicate_order_id_count": duplicate_order_id_count
    }

    return dq_results


# -----------------------
# Transformation
# -----------------------
def transform_orders(df, record_source):
    df_processed = (
        df.withColumn("order_id", col("order_id").cast("string"))
          .withColumn("customer_id", col("customer_id").cast("string"))
          .withColumn("order_status", col("order_status").cast("string"))
          .withColumn("order_purchase_timestamp", col("order_purchase_timestamp").cast("timestamp"))
          .withColumn("order_approved_at", col("order_approved_at").cast("timestamp"))
          .withColumn("order_delivered_carrier_date", col("order_delivered_carrier_date").cast("timestamp"))
          .withColumn("order_delivered_customer_date", col("order_delivered_customer_date").cast("timestamp"))
          .withColumn("order_estimated_delivery_date", col("order_estimated_delivery_date").cast("timestamp"))
          .withColumn("order_purchase_date", to_date(col("order_purchase_timestamp")))
          .withColumn("order_purchase_year", year(col("order_purchase_timestamp")))
          .withColumn("order_purchase_month", month(col("order_purchase_timestamp")))
          .withColumn("record_source", lit(record_source))
          .withColumn("load_timestamp", current_timestamp())
    )

    df_processed = df_processed.dropDuplicates(["order_id"])

    df_processed = (
        df_processed.filter(col("order_id").isNotNull())
                    .filter(col("customer_id").isNotNull())
                    .filter(col("order_purchase_timestamp").isNotNull())
    )

    return df_processed


# -----------------------
# Output
# -----------------------
def write_output(df, config):
    project_id = config["project_id"]
    dataset_id = config["bq"]["processed_dataset"]
    table_name = config["bq"]["orders_table"]

    full_table_name = f"{project_id}:{dataset_id}.{table_name}"

    (
        df.write.format("bigquery")
        .option("table", full_table_name)
        .option("temporaryGcsBucket", "syem-genai-temp")
        .mode("overwrite")
        .save()
    )

    print(f"Data written to BigQuery table: {full_table_name}")


# -----------------------
# Print DQ Summary
# -----------------------
def print_dq_summary(dq_results, processed_count):
    print("\n========== DQ SUMMARY: ORDERS ==========")
    print(f"Raw row count: {dq_results['raw_count']}")
    print(f"Null order_id count: {dq_results['null_order_id_count']}")
    print(f"Null customer_id count: {dq_results['null_customer_id_count']}")
    print(f"Null order_purchase_timestamp count: {dq_results['null_purchase_ts_count']}")
    print(f"Duplicate order_id groups: {dq_results['duplicate_order_id_count']}")
    print(f"Processed row count: {processed_count}")
    print("=======================================\n")


# -----------------------
# Main
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Process Olist orders data")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)

    spark = build_spark("ProcessOrders")

    input_path = config["paths"]["orders_input"]
    record_source = config["metadata"]["record_source"]

    df = spark.read.option("header", True).csv(input_path)

    dq_results = run_dq_checks_orders(df)

    df_processed = transform_orders(df, record_source)

    processed_count = df_processed.count()

    print_dq_summary(dq_results, processed_count)

    write_output(df_processed, config)

    spark.stop()


if __name__ == "__main__":
    main()
