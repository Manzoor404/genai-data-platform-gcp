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
    return SparkSession.builder.appName("curate_payments").getOrCreate()


def read_processed(spark, config):
    project = config["project_id"]
    dataset = config["bq"]["processed_dataset"]
    table = config["bq"]["payments_table"]

    return (
        spark.read.format("bigquery")
        .option("table", f"{project}:{dataset}.{table}")
        .load()
    )


def transform(df):
    return (
        df.select(
            trim(col("order_id")).alias("order_id"),
            trim(col("payment_sequential")).alias("payment_sequential"),
            trim(col("payment_type")).alias("payment_type"),
            trim(col("payment_installments")).alias("payment_installments"),
            round(col("payment_value"), 2).alias("payment_value"),
            lit("processed_layer.payments").alias("record_source"),
            current_timestamp().alias("load_timestamp"),
        )
        .filter(col("order_id").isNotNull())
        .filter(col("payment_type").isNotNull())
        .dropDuplicates(["order_id", "payment_sequential"])
    )


def write(df, config):
    project = config["project_id"]
    dataset = config["bq"]["curated_dataset"]
    table = config["bq"]["curated_payments_table"]

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

    print("========== CURATED SUMMARY: PAYMENTS ==========")
    print(f"Input count: {df.count()}")
    print(f"Output count: {df_curated.count()}")
    print("===============================================")

    write(df_curated, config)
    spark.stop()


if __name__ == "__main__":
    main()
