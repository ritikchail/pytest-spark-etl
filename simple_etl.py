import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()  # Load environment variables from .env file

def get_upstream_table(table_name: str, spark: SparkSession):
    host = os.getenv("UPSTREAM_HOST", "upstream")
    port = os.getenv("UPSTREAM_PORT", "5432")
    db = os.getenv("UPSTREAM_DATABASE", "upstreamdb")
    jdbc_url = f'jdbc:postgresql://{host}:{port}/{db}?sslmode=require'
    connection_properties = {
        "user": os.getenv("UPSTREAM_USERNAME", "sdeuser"),
        "password": os.getenv("UPSTREAM_PASSWORD", "sdepassword"),
        "driver": "org.postgresql.Driver",
    }
    return spark.read.jdbc(
        url=jdbc_url, table=table_name, properties=connection_properties
    )

def get_upstream_seller_data(spark):
    table_name = "rainforest.\"seller\""
    return get_upstream_table(table_name,  spark)

def transform_seller_data(raw_seller_data):
    return raw_seller_data.groupBy("user_id").agg(F.min("first_time_sold_timestamp").alias("first_time_sold"))

def load_seller_data(clean_seller_data):
    # We use collect to force spark to write out data
    return clean_seller_data

def run_code(spark):
    return load_seller_data(transform_seller_data(get_upstream_seller_data(spark)))

if __name__ == "__main__":
    # Create a spark session
    spark = (
        SparkSession.builder.appName("Naive Data Pipeline")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.10")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    df = run_code(spark)
    print(df.show(5))
    spark.stop()

# from dotenv import load_dotenv
# load_dotenv()
# import os
# print("Host:", os.getenv("UPSTREAM_HOST"))
# print("User:", os.getenv("UPSTREAM_USERNAME"))