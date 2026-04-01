import pytest
import sys
import os
from pyspark.sql import functions as F
from pyspark.sql import Row

# Add parent directory to path so we can import simple_etl
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from simple_etl import transform_seller_data


class TestSparkBasics:
    """Test basic Spark functionality"""
    
    def test_spark_session_exists(self, spark):
        """Test that Spark session is created"""
        assert spark is not None
    
    def test_create_dataframe(self, spark):
        """Test creating a simple DataFrame"""
        data = [
            Row(id=1, name="Alice"),
            Row(id=2, name="Bob"),
            Row(id=3, name="Charlie")
        ]
        df = spark.createDataFrame(data)
        
        assert df.count() == 3
        assert df.columns == ["id", "name"]


class TestTransformSeller:
    """Test seller data transformations"""
    
    def test_transform_seller_data(self, spark):
        # Create a sample DataFrame with fake seller data
        raw_seller_data = spark.createDataFrame([
            Row(user_id=1, first_time_sold_timestamp="2022-01-01 10:00:00"),
            Row(user_id=1, first_time_sold_timestamp="2021-12-31 09:00:00"),
            Row(user_id=2, first_time_sold_timestamp="2023-01-01 12:00:00"),
            Row(user_id=3, first_time_sold_timestamp="2021-11-11 08:00:00")
        ])

        # Apply the transform_seller_data function
        clean_seller_data = transform_seller_data(raw_seller_data)

        # Expected result is the minimum timestamp for each user_id
        expected_data = spark.createDataFrame([
            Row(user_id=1, first_time_sold_timestamp="2021-12-31 09:00:00"),
            Row(user_id=2, first_time_sold_timestamp="2023-01-01 12:00:00"),
            Row(user_id=3, first_time_sold_timestamp="2021-11-11 08:00:00")
        ])

        # Collect data for assertion (convert to set for unordered comparison)
        clean_seller_set = set(clean_seller_data.collect())
        expected_set = set(expected_data.collect())

        # Assert that the transformed data matches the expected data
        assert clean_seller_set == expected_set
    
    def test_seller_data_groupby(self, spark):
        """Test GROUP BY aggregation on seller data"""
        data = [
            Row(user_id=1, first_time_sold_timestamp="2023-01-01"),
            Row(user_id=1, first_time_sold_timestamp="2023-01-02"),
            Row(user_id=2, first_time_sold_timestamp="2023-02-01"),
        ]
        df = spark.createDataFrame(data)
        
        # Group by user and find min timestamp
        result = df.groupBy("user_id").agg(
            F.min("first_time_sold_timestamp").alias("first_sale")
        )
        
        assert result.count() == 2
    
    def test_seller_data_column_rename(self, spark):
        """Test renaming columns"""
        data = [
            Row(user_id=1, first_time_sold_timestamp="2023-01-01"),
            Row(user_id=2, first_time_sold_timestamp="2023-02-01"),
        ]
        df = spark.createDataFrame(data)
        
        # Rename column
        renamed = df.withColumnRenamed("first_time_sold_timestamp", "first_sale")
        
        assert "first_sale" in renamed.columns
        assert "first_time_sold_timestamp" not in renamed.columns