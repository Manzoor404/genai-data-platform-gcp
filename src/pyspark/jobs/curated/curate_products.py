import argparse
import yaml
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp,
    lit
)


def load_config(config_path):
    local_path = config_path

    if config_path.startswith("gs://"):
        local_path = "/tmp/prod.yaml"
        subprocess.run(["gsutil", "cp", config_path, local_path], check=True)

    with open(local_path, "r") as file:
        return yaml.safe_load(file)


def create_spark():
    return SparkSession.builder.appName("curate_products").getOrCreate()


def read_processed(spark, config):
    project = config["project_id"]
    dataset = config["bq"]["processed_dataset"]
    table = config["bq"]["products_table"]

    return (
        spark.read.format("bigquery")
        .option("table", f"{project}:{dataset}.{table}")
        .load()
    )


def transform(df):
    return (
        df.select(
            trim(col("product_id")).alias("product_id"),
            trim(col("product_category_name")).alias("product_category_name"),
            col("product_name_lenght"),
            col("product_description_lenght"),
            col("product_photos_qty"),
            col("product_weight_g"),
            col("product_length_cm"),
            col("product_height_cm"),
            col("product_width_cm"),
            lit("processed_layer.products").alias("record_source"),
            current_timestamp().alias("load_timestamp"),
        )
        .filter(col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )


def write(df, config):
    project = config["project_id"]
    dataset = config["bq"]["curated_dataset"]
    table = config["bq"]["curated_products_table"]

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

    print("========== CURATED SUMMARY: PRODUCTS ==========")
    print(f"Input count: {df.count()}")
    print(f"Output count: {df_curated.count()}")
    print("===============================================")

    write(df_curated, config)
    spark.stop()


if __name__ == "__main__":
    main()
