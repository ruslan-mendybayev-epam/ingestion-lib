import unittest
import pytest
import tempfile
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from ingestion_lib.provider import DatabricksIngestionProvider
from ingestion_lib.utils.databricks_utils import get_row_count_written


class TestWriteDeltaUnity(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def prepare_spark_session(self, spark_session: SparkSession):
        self.spark = spark_session
        import pyspark
        print("PySpark version:", pyspark.__version__)

# If you have access to the Spark shell, you can check the Spark version like this:
# !spark-submit --version

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory().name
        self.df = self.spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])

    @patch('ingestion_lib.provider.get_row_count_written')
    def test_write_delta_unity(self, mock_get_row_count_written):
        # Arrange
        mock_data = MagicMock()
        mock_data.distinct.return_value = mock_data
        mock_data.write.format.return_value.mode.return_value.options.return_value.saveAsTable = MagicMock()

        target_schema = "test_schema"
        table_name = "test_table"
        options = {"option1": "value1"}

        # Mock the return value of get_row_count_written
        mock_get_row_count_written.return_value = 10

        # Act
        provider = DatabricksIngestionProvider(mock_data)
        result = provider.write_delta_unity(mock_data, target_schema, table_name, options)

        # Assert
        full_name = f"{target_schema}.{table_name}"
        mock_data.printSchema.assert_called_once()
        mock_data.show.assert_called_once()
        mock_data.distinct().write.format.assert_called_once_with("delta")
        mock_data.distinct().write.format("delta").mode.assert_called_once_with("overwrite")
        mock_data.distinct().write.format("delta").mode("overwrite").options.assert_called_once_with(**options)
        mock_data.distinct().write.format("delta").mode("overwrite").options(**options).saveAsTable.assert_called_once_with(full_name)
        mock_get_row_count_written.assert_called_once_with(mock_data, table_name=table_name)
        self.assertEqual(result, 10)

    def test_get_row_count_written_location(self):
        location = f"{self.temp_dir}/silver/test_row_count"

        (self.df.distinct().write.format("delta").mode("overwrite").save(location))

        # Test with location
        row_count = get_row_count_written(self.df, location=location)
        self.assertEqual(row_count, 2)

    def test_get_row_count_written_table(self):
        location = f"{self.temp_dir}/silver/test_row_count_table"
        table_name = "test_table"

        self.df.write.format("delta").mode("overwrite").save(location)

        self.spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{location}'")

        # Test with table name
        row_count = get_row_count_written(self.df, table_name=table_name)
        self.assertEqual(row_count, 2)



