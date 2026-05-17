import argparse,yaml,subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_config(p):
    if p.startswith("gs://"):
        subprocess.run(["gsutil","cp",p,"/tmp/prod.yaml"],check=True)
        p="/tmp/prod.yaml"
    return yaml.safe_load(open(p))

def spark(): return SparkSession.builder.appName("ProcessGeo").getOrCreate()

def transform(df):
    return df.dropDuplicates([
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng"
    ])

def write(df,cfg):
    df.write.format("bigquery") \
      .option("table",f"{cfg['project_id']}:{cfg['bq']['processed_dataset']}.{cfg['bq']['geolocation_table']}") \
      .option("temporaryGcsBucket","syem-genai-temp") \
      .mode("overwrite").save()

def main():
    a=argparse.ArgumentParser();a.add_argument("--config",required=True)
    cfg=load_config(a.parse_args().config)
    sp=spark()
    df=sp.read.option("header",True).csv(cfg["paths"]["geolocation_input"])
    df=transform(df)
    write(df,cfg)
    sp.stop()

if __name__=="__main__": main()
