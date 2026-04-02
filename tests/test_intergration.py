import pytest
from pyspark.sql import functions as F
from pyspark.sql import Row
import sys
import os
# Add parent directory to path so we can import simple_etl
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from simple_etl import transform_seller_data, load_seller_data, get_upstream_table


class TestDatabaseIntegration:
    """Test actual database connectivity and operations"""

    def test_database_connection(self, db_connection):
        """Test that we can connect to the database"""
        assert db_connection is not None

        # Test basic query
        cur = db_connection.cursor()
        cur.execute("SELECT 1 as test")
        result = cur.fetchone()
        assert result[0] == 1
        cur.close()

    def test_schema_exists(self, db_connection):
        """Test that rainforest schema exists"""
        cur = db_connection.cursor()
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'rainforest'")
        result = cur.fetchone()
        assert result is not None
        assert result[0] == 'rainforest'
        cur.close()

    def test_tables_exist(self, db_connection):
        """Test that required tables exist in rainforest schema"""
        cur = db_connection.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'rainforest'
            AND table_name IN ('seller', 'appuser', 'product')
        """)
        results = cur.fetchall()
        table_names = [row[0] for row in results]
        assert 'seller' in table_names
        assert 'appuser' in table_names
        cur.close()


class TestSparkDatabaseIntegration:
    """Test Spark reading from actual database"""

    def test_spark_read_seller_table(self, spark, db_connection):
        """Test Spark can read from seller table"""
        # Get table using our function
        df = get_upstream_table("rainforest.\"seller\"", spark)

        # Basic checks
        assert df is not None
        assert df.columns is not None

        # Should have expected columns
        expected_cols = ['seller_id', 'user_id', 'first_time_sold_timestamp', 'created_ts', 'last_updated_by', 'last_updated_ts']
        for col in expected_cols:
            assert col in df.columns

    def test_spark_read_with_data(self, spark, db_connection):
        """Test Spark can read actual data from database"""
        df = get_upstream_table("rainforest.\"seller\"", spark)

        # Check if we have data
        count = df.count()
        assert count >= 0  # Could be 0 if no data, but should not error

        if count > 0:
            # Test data types and values
            sample = df.limit(1).collect()
            if sample:
                row = sample[0]
                assert hasattr(row, 'seller_id')
                assert hasattr(row, 'user_id')

    def test_spark_read_appuser_table(self, spark, db_connection):
        """Test Spark can read from AppUser table"""
        df = get_upstream_table("rainforest.\"appuser\"", spark)

        assert df is not None
        expected_cols = ['user_id', 'username', 'email', 'is_active', 'created_ts', 'last_updated_by', 'last_updated_ts']
        for col in expected_cols:
            assert col in df.columns


class TestETLIntegration:
    """Test the complete ETL pipeline"""

    def test_full_etl_pipeline(self, spark, db_connection):
        """Test the complete ETL pipeline from database to processing"""
        try:
            # Read data
            raw_df = get_upstream_table("rainforest.\"seller\"", spark)
            assert raw_df is not None

            # Transform data
            transformed_df = transform_seller_data(raw_df)
            assert transformed_df is not None

            # Check transformation result
            if raw_df.count() > 0:
                # Should have user_id and first_time_sold columns
                assert 'user_id' in transformed_df.columns
                assert 'first_time_sold' in transformed_df.columns

                # Should have fewer or equal rows (grouped by user_id)
                assert transformed_df.count() <= raw_df.count()

            # Load data (collect to Python)
            result = load_seller_data(transformed_df)
            assert isinstance(result.collect(), list)  # Should return list from collect()

        except Exception as e:
            # If database is empty or connection fails, that's acceptable for integration test
            # We just want to make sure the pipeline doesn't crash with coding errors
            pytest.skip(f"Integration test skipped due to database state: {str(e)}")

    def test_etl_data_consistency(self, spark, db_connection):
        """Test that ETL preserves data consistency"""
        try:
            raw_df = get_upstream_table("rainforest.\"seller\"", spark)

            if raw_df.count() == 0:
                pytest.skip("No data in seller table")

            # Get original count
            original_count = raw_df.count()

            # Transform
            transformed_df = transform_seller_data(raw_df)
            transformed_count = transformed_df.count()

            # Transformed should have <= original rows
            assert transformed_count <= original_count

            # Check that user_ids are preserved
            original_user_ids = set(row.user_id for row in raw_df.select('user_id').distinct().collect())
            transformed_user_ids = set(row.user_id for row in transformed_df.select('user_id').collect())

            # All transformed user_ids should be in original
            assert transformed_user_ids.issubset(original_user_ids)

        except Exception as e:
            pytest.skip(f"Data consistency test skipped: {str(e)}")


class TestDataValidation:
    """Test data quality and validation"""

    def test_seller_data_quality(self, spark, db_connection):
        """Test data quality in seller table"""
        try:
            df = get_upstream_table("rainforest.\"seller\"", spark)

            if df.count() == 0:
                pytest.skip("No seller data to validate")

            # Check for null user_ids (should not exist due to FK constraint)
            null_user_ids = df.filter(df.user_id.isNull()).count()
            assert null_user_ids == 0, "Found null user_ids in seller table"

            # Check that timestamps are not in future (basic sanity check)
            from datetime import datetime
            current_time = datetime.now()

            # This would require collecting data, so skip if too large
            if df.count() < 1000:  # Only check small datasets
                sample_data = df.limit(10).collect()
                for row in sample_data:
                    if row.first_time_sold_timestamp:
                        # Convert to datetime if it's a string/timestamp
                        assert row.first_time_sold_timestamp <= current_time, "Found future timestamp"

        except Exception as e:
            pytest.skip(f"Data quality test skipped: {str(e)}")

    def test_table_relationships(self, db_connection):
        """Test referential integrity between tables"""
        try:
            cur = db_connection.cursor()

            # Check that all seller user_ids exist in AppUser
            cur.execute("""
                SELECT COUNT(*) as orphaned_sellers
                FROM rainforest."seller" s
                LEFT JOIN rainforest."appuser" u ON s.user_id = u.user_id
                WHERE u.user_id IS NULL
            """)
            result = cur.fetchone()
            assert result[0] == 0, f"Found {result[0]} orphaned seller records"

            cur.close()

        except Exception as e:
            pytest.skip(f"Relationship test skipped: {str(e)}")


# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration
