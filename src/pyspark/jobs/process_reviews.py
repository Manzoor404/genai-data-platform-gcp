import argparse,yaml,subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp

def load_config(p):
    if p.startswith("gs://"):
        subprocess.run(["gsutil","cp",p,"/tmp/prod.yaml"],check=True)
        p="/tmp/prod.yaml"
    return yaml.safe_load(open(p))

def spark(): return SparkSession.builder.appName("ProcessReviews").getOrCreate()

def transform(df,src):
    return (df
      .withColumn("review_id",col("review_id").cast("string"))
      .withColumn("review_score",col("review_score").cast("int"))
      .withColumn("record_source",lit(src))
      .withColumn("load_timestamp",current_timestamp())
      .dropDuplicates(["review_id"])
      .filter(col("review_score").between(1,5))
    )

def write(df,cfg):
    df.write.format("bigquery") \
      .option("table",f"{cfg['project_id']}:{cfg['bq']['processed_dataset']}.{cfg['bq']['reviews_table']}") \
      .option("temporaryGcsBucket","syem-genai-temp") \
      .mode("overwrite").save()

def main():
    a=argparse.ArgumentParser();a.add_argument("--config",required=True)
    cfg=load_config(a.parse_args().config)
    sp=spark()
    df=sp.read.option("header",True)\
        .option("multiLine",True)\
        .option("quote","\"")\
        .option("escape","\"")\
        .csv(cfg["paths"]["reviews_input"])
    df=transform(df,cfg["metadata"]["record_source"])
    write(df,cfg)
    sp.stop()

if __name__=="__main__": main()
