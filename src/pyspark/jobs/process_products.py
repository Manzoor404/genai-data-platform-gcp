import argparse, yaml, subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def load_config(path):
    if path.startswith("gs://"):
        subprocess.run(["gsutil","cp",path,"/tmp/prod.yaml"],check=True)
        path="/tmp/prod.yaml"
    with open(path) as f:
        return yaml.safe_load(f)

def spark():
    return SparkSession.builder.appName("ProcessProducts").getOrCreate()

def dq(df):
    print("\n=== DQ PRODUCTS ===")
    print("rows:", df.count())
    print("null product_id:", df.filter(col("product_id").isNull()).count())

def transform(df, src):
    return (df
        .withColumn("product_id", col("product_id").cast("string"))
        .withColumn("product_category_name", col("product_category_name").cast("string"))
        .withColumn("record_source", lit(src))
        .withColumn("load_timestamp", current_timestamp())
        .dropDuplicates(["product_id"])
        .filter(col("product_id").isNotNull())
    )

def write(df, cfg):
    df.write.format("bigquery") \
      .option("table", f"{cfg['project_id']}:{cfg['bq']['processed_dataset']}.{cfg['bq']['products_table']}") \
      .option("temporaryGcsBucket","syem-genai-temp") \
      .mode("overwrite").save()

def main():
    args = argparse.ArgumentParser(); args.add_argument("--config",required=True)
    cfg = load_config(args.parse_args().config)
    sp = spark()
    df = sp.read.option("header",True).csv(cfg["paths"]["products_input"])
    dq(df)
    df = transform(df,cfg["metadata"]["record_source"])
    print("processed:",df.count())
    write(df,cfg)
    sp.stop()

if __name__=="__main__": main()
