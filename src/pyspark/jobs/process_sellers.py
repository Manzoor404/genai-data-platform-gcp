import argparse, yaml, subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def load_config(p):
    if p.startswith("gs://"):
        subprocess.run(["gsutil","cp",p,"/tmp/prod.yaml"],check=True)
        p="/tmp/prod.yaml"
    return yaml.safe_load(open(p))

def spark(): return SparkSession.builder.appName("ProcessSellers").getOrCreate()

def transform(df,src):
    return (df
        .withColumn("seller_id", col("seller_id").cast("string"))
        .withColumn("seller_city", col("seller_city").cast("string"))
        .withColumn("seller_state", col("seller_state").cast("string"))
        .withColumn("record_source", lit(src))
        .withColumn("load_timestamp", current_timestamp())
        .dropDuplicates(["seller_id"])
        .filter(col("seller_id").isNotNull())
    )

def write(df,cfg):
    df.write.format("bigquery") \
      .option("table",f"{cfg['project_id']}:{cfg['bq']['processed_dataset']}.{cfg['bq']['sellers_table']}") \
      .option("temporaryGcsBucket","syem-genai-temp") \
      .mode("overwrite").save()

def main():
    a=argparse.ArgumentParser();a.add_argument("--config",required=True)
    cfg=load_config(a.parse_args().config)
    sp=spark()
    df=sp.read.option("header",True).csv(cfg["paths"]["sellers_input"])
    df=transform(df,cfg["metadata"]["record_source"])
    write(df,cfg)
    sp.stop()

if __name__=="__main__": main()
