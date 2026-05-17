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
    return SparkSession.builder.appName("ProcessOrderItems").getOrCreate()


def run_dq(df):
    print("\n========== DQ SUMMARY: ORDER_ITEMS ==========")
    print(f"Raw row count: {df.count()}")

    print(f"Null order_id: {df.filter(col('order_id').isNull()).count()}")
    print(f"Null order_item_id: {df.filter(col('order_item_id').isNull()).count()}")
    print(f"Null product_id: {df.filter(col('product_id').isNull()).count()}")
    print(f"Null seller_id: {df.filter(col('seller_id').isNull()).count()}")
    print(f"Negative price: {df.filter(col('price') < 0).count()}")
    print(f"Negative freight_value: {df.filter(col('freight_value') < 0).count()}")

    dup_count = df.groupBy("order_id", "order_item_id") \
        .count().filter(col("count") > 1).count()

    print(f"Duplicate keys: {dup_count}")
    print("=============================================\n")


def transform(df, record_source):
    df = (
        df.withColumn("order_id", col("order_id").cast("string"))
          .withColumn("order_item_id", col("order_item_id").cast("int"))
          .withColumn("product_id", col("product_id").cast("string"))
          .withColumn("seller_id", col("seller_id").cast("string"))
          .withColumn("shipping_limit_date", col("shipping_limit_date").cast("timestamp"))
          .withColumn("price", col("price").cast("double"))
          .withColumn("freight_value", col("freight_value").cast("double"))
          .withColumn("record_source", lit(record_source))
          .withColumn("load_timestamp", current_timestamp())
    )

    df = df.dropDuplicates(["order_id", "order_item_id"])

    df = df.filter(
        col("order_id").isNotNull() &
        col("order_item_id").isNotNull() &
        col("product_id").isNotNull() &
        col("seller_id").isNotNull() &
        (col("price") >= 0) &
        (col("freight_value") >= 0)
    )

    return df


def write_output(df, config):
    df.write.format("bigquery") \
        .option("table", f"{config['project_id']}:{config['bq']['processed_dataset']}.{config['bq']['order_items_table']}") \
        .option("temporaryGcsBucket", "syem-genai-temp") \
        .mode("overwrite") \
        .save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)
    spark = build_spark()

    df = spark.read.option("header", True).csv(config["paths"]["order_items_input"])

    run_dq(df)

    df = transform(df, config["metadata"]["record_source"])

    print(f"Processed count: {df.count()}")

    write_output(df, config)

    spark.stop()


if __name__ == "__main__":
    main()
