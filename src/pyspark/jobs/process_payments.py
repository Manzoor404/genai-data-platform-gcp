import argparse,yaml,subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp

def load_config(p):
    if p.startswith("gs://"):
        subprocess.run(["gsutil","cp",p,"/tmp/prod.yaml"],check=True)
        p="/tmp/prod.yaml"
    return yaml.safe_load(open(p))

def spark(): return SparkSession.builder.appName("ProcessPayments").getOrCreate()

def transform(df,src):
    return (df
      .withColumn("order_id",col("order_id").cast("string"))
      .withColumn("payment_value",col("payment_value").cast("double"))
      .withColumn("record_source",lit(src))
      .withColumn("load_timestamp",current_timestamp())
      .dropDuplicates(["order_id","payment_sequential"])
      .filter(col("order_id").isNotNull())
    )

def write(df,cfg):
    df.write.format("bigquery") \
      .option("table",f"{cfg['project_id']}:{cfg['bq']['processed_dataset']}.{cfg['bq']['payments_table']}") \
      .option("temporaryGcsBucket","syem-genai-temp") \
      .mode("overwrite").save()

def main():
    a=argparse.ArgumentParser();a.add_argument("--config",required=True)
    cfg=load_config(a.parse_args().config)
    sp=spark()
    df=sp.read.option("header",True).csv(cfg["paths"]["payments_input"])
    df=transform(df,cfg["metadata"]["record_source"])
    write(df,cfg)
    sp.stop()

if __name__=="__main__": main()
