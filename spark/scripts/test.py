import requests
from pyspark.sql import SparkSession
import pandas as pd
from io import StringIO
import os

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = (
    SparkSession.builder
    .appName("lakehouse-test")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)  
    .getOrCreate()
)

url = "https://raw.githubusercontent.com/hanvocado/data/main/sales.csv"
response = requests.get(url)

csv_data = StringIO(response.text)
df = pd.read_csv(csv_data)

spark_df = spark.createDataFrame(df)

minio_path = "s3a://warehouse/sales/sales.parquet"

spark_df.write.mode("overwrite").format("parquet").save(minio_path)

print("File uploaded to MinIO successfully!\n\n")