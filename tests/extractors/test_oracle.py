import unittest
from unittest.mock import MagicMock, call

from pyspark.sql import SparkSession, DataFrame

from ingestion_lib.extractors.oracle import OracleExtractor  # Adjust the import according to your project structure
from ingestion_lib.utils.data_contract import TableContract, DbCredentials


class TestOracleExtractor(unittest.TestCase):
    def setUp(self):
        # Mock the SparkSession and TableContract
        self.spark = MagicMock(spec=SparkSession)
        self.table_contract = MagicMock(spec=TableContract)
        self.table_contract.credentials = DbCredentials(jdbc_url="jdbc:oracle:thin:@host:port:sid", user="user", password="password")
        self.table_contract.table_name = "mock_table"
        self.table_contract.schema = "mock_schema"
        self.table_contract.watermark_columns = None  # Adjust based on actual usage

        # Create an instance of OracleExtractor
        self.extractor = OracleExtractor(self.table_contract, self.spark)

        # Setup the chaining of format and option calls
        self.mock_format = self.spark.read.format.return_value
        self.mock_option = MagicMock()
        self.mock_format.option.return_value = self.mock_option
        self.mock_option.option.return_value = self.mock_option  # Allows chaining of option calls
        self.mock_option.load.return_value = "Mocked DataFrame"

        # Mock DataFrame to be returned by the extract_data method
        self.mock_df = MagicMock(spec=DataFrame)

    def test_creds(self):
        # Execute
        credentials = self.extractor.creds()

        # Verify
        self.assertEqual(credentials, self.table_contract.credentials)

    def test_load_data_query(self):
        # Setup
        query = "SELECT * FROM DUAL"

        # Execute
        result = self.extractor.load_data_query(query)

        # Verify
        self.spark.read.format.assert_called_with("jdbc")
        expected_calls = [
            call.option('url', "jdbc:oracle:thin:@host:port:sid"),
            call.option().option('user', "user"),
            call.option().option('password', "password"),
            call.option().option('driver', "oracle.jdbc.driver.OracleDriver"),
            call.option().option('dbtable', f"({query}) temp"),
            call.option().load()
        ]
        self.mock_format.assert_has_calls(expected_calls, any_order=False)
        self.assertEqual(result, "Mocked DataFrame")

    def test_extract_data_returns_dataframe(self):
        # Assuming OracleExtractor's extract_data directly handles data extraction
        # and does not call the superclass method
        self.extractor.load_data_query = MagicMock(return_value=self.mock_df)

        # Execute
        result = self.extractor.extract_data()

        # Verify
        self.assertIsInstance(result, DataFrame)
        self.extractor.load_data_query.assert_called_once()  # Ensure load_data_query is called

    def test_condition_returns_empty_string(self):
        # Test scenarios where the condition should return an empty string

        # Scenario 1: No watermark columns
        self.extractor.table_contract = MagicMock(spec=TableContract, watermark_columns=[""], full_load=True, load_type="one_time")
        self.assertEqual(self.extractor._Extractor__build_condition(), "")

        # Scenario 2: full_load is "true"
        self.extractor.table_contract = MagicMock(spec=TableContract, watermark_columns=["timestamp"], full_load="true", load_type="incremental")
        self.assertEqual(self.extractor._Extractor__build_condition(), "")

        # Scenario 3: load_type is "one_time"
        self.extractor.table_contract = MagicMock(spec=TableContract, watermark_columns=["timestamp"], full_load="false", load_type="one_time")
        self.assertEqual(self.extractor._Extractor__build_condition(), "")
    def test_build_select_query(self):
        # Test scenarios for SQL query construction

        # Scenario 1: Basic select query
        self.extractor.table_contract = MagicMock(spec=TableContract, table_name="test_table", schema_name="test_schema", watermark_columns=None, full_load="false")
        expected_query = "SELECT * FROM test_schema.test_table"
        self.assertEqual(expected_query, self.extractor._Extractor__build_select_query())


if __name__ == '__main__':
    unittest.main()
