import argparse
import yaml
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp


def load_config(config_path):
    if config_path.startswith("gs://"):
        local_path = "/tmp/prod.yaml"
        subprocess.run(["gsutil", "cp", config_path, local_path], check=True)
        config_path = local_path

    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def build_spark():
    return SparkSession.builder.appName("ProcessCustomers").getOrCreate()


def run_dq(df):
    print("\n========== DQ SUMMARY: CUSTOMERS ==========")
    print(f"Raw row count: {df.count()}")

    print(f"Null customer_id: {df.filter(col('customer_id').isNull()).count()}")
    print(f"Null customer_unique_id: {df.filter(col('customer_unique_id').isNull()).count()}")
    print(f"Null city: {df.filter(col('customer_city').isNull()).count()}")
    print(f"Null state: {df.filter(col('customer_state').isNull()).count()}")

    dup_count = df.groupBy("customer_id") \
        .count().filter(col("count") > 1).count()

    print(f"Duplicate customer_id: {dup_count}")
    print("===========================================\n")


def transform(df, record_source):
    df = (
        df.withColumn("customer_id", col("customer_id").cast("string"))
          .withColumn("customer_unique_id", col("customer_unique_id").cast("string"))
          .withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix").cast("string"))
          .withColumn("customer_city", col("customer_city").cast("string"))
          .withColumn("customer_state", col("customer_state").cast("string"))
          .withColumn("record_source", lit(record_source))
          .withColumn("load_timestamp", current_timestamp())
    )

    df = df.dropDuplicates(["customer_id"])

    df = df.filter(
        col("customer_id").isNotNull() &
        col("customer_unique_id").isNotNull()
    )

    return df


def write_output(df, config):
    df.write.format("bigquery") \
        .option("table", f"{config['project_id']}:{config['bq']['processed_dataset']}.{config['bq']['customers_table']}") \
        .option("temporaryGcsBucket", "syem-genai-temp") \
        .mode("overwrite") \
        .save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)
    spark = build_spark()

    df = spark.read.option("header", True).csv(config["paths"]["customers_input"])

    run_dq(df)

    df = transform(df, config["metadata"]["record_source"])

    print(f"Processed count: {df.count()}")

    write_output(df, config)

    spark.stop()


if __name__ == "__main__":
    main()
