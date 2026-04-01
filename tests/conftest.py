import pytest
import os
from dotenv import load_dotenv
import psycopg2
from pyspark.sql import SparkSession

load_dotenv()


@pytest.fixture(scope="session")
def spark():
    """Create and return a Spark session for tests"""
    spark_session = SparkSession.builder \
        .appName("Test Suite") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.10") \
        .master("local[*]") \
        .getOrCreate()
    
    spark_session.sparkContext.setLogLevel("ERROR")
    
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="session")
def db_connection():
    """Provide database connection for integration tests"""
    conn = psycopg2.connect(
        dbname=os.getenv("UPSTREAM_DATABASE", "upstreamdb"),
        user=os.getenv("UPSTREAM_USERNAME", "neondb_owner"),
        password=os.getenv("UPSTREAM_PASSWORD"),
        host=os.getenv("UPSTREAM_HOST"),
        port=os.getenv("UPSTREAM_PORT", "5432"),
        sslmode="require",
    )
    yield conn
    conn.close()


@pytest.fixture
def cleanup_db(db_connection):
    """Clean up test data after tests"""
    yield
    cur = db_connection.cursor()
    cur.execute("SET search_path TO rainforest")
    # Clean up any test data if needed
    db_connection.commit()
    cur.close()
