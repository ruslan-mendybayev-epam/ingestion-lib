import logging
import unittest
from unittest.mock import MagicMock, patch

from parameterized import parameterized
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from ingestion_lib.utils.data_quality import DataQualityValidator, DEFAULT_INDEX_COLUMN
from ingestion_lib.utils.log_analytics import CustomLogger
from great_expectations.data_context import EphemeralDataContext
from great_expectations.exceptions import GreatExpectationsValidationError

from test_contracts import CONTRACT_WITH_RULES_DIM_DATE_NO_INDEX_COL_QUARANTINE
import json

class TestDataQualityValidator(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("unittest").getOrCreate()
        self.df = self.spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        self.layer = "test_layer"
        self.table_name = "test_table"
        self.contract = CONTRACT_WITH_RULES_DIM_DATE_NO_INDEX_COL_QUARANTINE
        self.correlation_id = "test_correlation_id"
        self.logger = CustomLogger(logger_name="TestDataQualityValidator", logging_level=logging.DEBUG)

    def test_init(self):
        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=self.contract,
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        self.assertEqual(validator.layer, self.layer)
        self.assertEqual(validator.table_name, self.table_name)
        self.assertEqual(validator.correlation_id, self.correlation_id)
        self.assertIsNotNone(validator.context)

    @patch('great_expectations.data_context.EphemeralDataContext')
    def test_setup_data_context(self, mock_context):
        mock_context.return_value = MagicMock(spec=EphemeralDataContext)
        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=self.contract,
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        context = validator._setup_data_context()
        self.assertIsNotNone(context)

    def test_add_expectations(self):
        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=self.contract,
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        expectations, index_columns = validator._add_expectations()
        self.assertEqual(len(expectations), 1)
        self.assertEqual(expectations[0].type, "expect_column_values_to_not_be_null")
        self.assertEqual(index_columns, [DEFAULT_INDEX_COLUMN])

    @patch.object(DataQualityValidator, '_create_checkpoint')
    @patch.object(DataQualityValidator, '_quarantine_unexpected_records')
    def test_run(self, mock_quarantine, mock_checkpoint):
        mock_checkpoint.return_value.run.return_value = MagicMock()
        mock_quarantine.return_value = self.df

        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=self.contract,
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        result_df = validator.run()
        self.assertEqual(result_df.collect(), self.df.collect())


    def test_run_v1(self):

        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=self.contract,
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        result_df = validator.run_v1()
        self.assertEqual(result_df.collect(), self.df.collect())

    def test_create_index_columns(self):
        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=self.contract,
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        index_columns = ["__unique_id__"]
        df_with_index = validator._create_index_columns(self.df, index_columns)
        self.assertIn("__unique_id__", df_with_index.columns)

