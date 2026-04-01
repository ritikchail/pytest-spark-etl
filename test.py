from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

load_dotenv()  # Load environment variables from .env file

# Create Spark session with PostgreSQL package
spark = SparkSession.builder \
    .appName("Neon PostgreSQL Test") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.10") \
    .getOrCreate()

# Connection details from environment
host = os.getenv("UPSTREAM_HOST")
user = os.getenv("UPSTREAM_USERNAME")
password = os.getenv("UPSTREAM_PASSWORD")
database = os.getenv("UPSTREAM_DATABASE")

# JDBC URL with SSL (required for Neon)
jdbc_url = f"jdbc:postgresql://{host}:5432/{database}?sslmode=require"

# Connection properties
connection_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

# Read data from Neon PostgreSQL
df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT seller_id, user_id, first_time_sold_timestamp FROM rainforest.\"seller\" WHERE user_id < 100) AS seller_sample",
    properties=connection_properties
)

# Show results
print("Data from Neon PostgreSQL:")
df.show()
print(f"Row count: {df.count()}")

# Clean up
spark.stop()