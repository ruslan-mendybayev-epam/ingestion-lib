import json
import logging
import unittest
from unittest.mock import MagicMock, patch

import pytest
from great_expectations.data_context import EphemeralDataContext
from parameterized import parameterized
from pyspark.sql import SparkSession

from ingestion_lib.utils.data_quality import DataQualityValidator
from ingestion_lib.utils.log_analytics import CustomLogger
from tests.utils.test_contracts import CONTRACT_WITH_RULES_DIM_DATE_NO_INDEX_COL_QUARANTINE


class TestDataQualityValidator(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def prepare_spark_session(self, spark_session: SparkSession):
        self.spark = spark_session
        import pyspark
        print("PySpark version:", pyspark.__version__)

    def setUp(self):
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

    def test_get_index_columns_no_rules(self):
        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=json.dumps({}),  # No dq_rules
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        with self.assertRaises(ValueError) as context:
            validator._get_index_columns()
        self.assertEqual(str(context.exception),
                         "The 'dq_rules' attribute must in the contract to get or create index columns.")

    def test_get_index_columns_from_contract(self):
        contract = json.dumps({
            "dq_rules": [{"expectation_type": "expect_column_values_to_not_be_null", "columns": ["id"]}],
            "index_columns": ["__unique_id__"]
        })
        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=contract,
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        index_columns = validator._get_index_columns()
        self.assertEqual(index_columns, ["__unique_id__"])

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
        self.assertEqual(index_columns, ["id"])

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

    def test_create_index_columns_no_rules(self):
        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=json.dumps({}),  # No dq_rules
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        with self.assertRaises(ValueError) as context:
            validator._create_index_columns(self.df, ["__unique_id__"])
        self.assertEqual(str(context.exception),
                         "The 'dq_rules' attribute must in the contract to get or create index columns.")

    @parameterized.expand(
        [
            (CONTRACT_WITH_RULES_DIM_DATE_NO_INDEX_COL_QUARANTINE)
        ]
    )
    def test_run_v1(self, contract):
        validator = DataQualityValidator(
            spark=self.spark,
            df=self.df,
            layer=self.layer,
            table_name=self.table_name,
            contract=contract,
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        result_df = validator.run_v1()
        self.assertEqual(result_df.collect(), self.df.collect())

    @parameterized.expand(
        [
            (CONTRACT_WITH_RULES_DIM_DATE_NO_INDEX_COL_QUARANTINE)
        ]
    )
    def test_negative_run_v1(self, contract):
        df_with_null = self.spark.createDataFrame([(1, "a"), (2, "b"), (3, None)], ["id", "value"])
        validator = DataQualityValidator(
            spark=self.spark,
            df=df_with_null,
            layer=self.layer,
            table_name=self.table_name,
            contract=contract,
            correlation_id=self.correlation_id,
            logger=self.logger
        )
        result_df = validator.run_v1()
        self.assertNotEqual(validator._unexpected_records_count, 0)
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
