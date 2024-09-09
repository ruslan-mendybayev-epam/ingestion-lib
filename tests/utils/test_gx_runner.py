import json
import logging
import unittest
from unittest.mock import patch, MagicMock

import pytest
from great_expectations.data_context import EphemeralDataContext
from parameterized import parameterized
from pyspark.sql import SparkSession

from ingestion_lib.utils.data_contract import DQContract
from ingestion_lib.utils.gx import GXRunner
from ingestion_lib.utils.log_analytics import CustomLogger
from tests.utils.test_contracts import CONTRACT_WITH_RULES_DIM_DATE_NO_INDEX_COL_QUARANTINE


class TestGXRunner(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def prepare_spark_session(self, spark_session: SparkSession):
        self.spark = spark_session
        import pyspark
        print("PySpark version:", pyspark.__version__)

    def setUp(self):
        self.df = self.spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        self.layer = "test_layer"
        self.table_name = "test_table"
        self.contract = MagicMock(spec=DQContract)
        self.contract.table_name = MagicMock(return_value="table_name")
        self.result_format = {"result_format": "COMPLETE", "include_unexpected_rows": True}
        self.logger = CustomLogger(logger_name="TestGXRunner", logging_level=logging.DEBUG)
        self.gx_runner = GXRunner(
            spark=self.spark,
            contract=self.contract,
            df=self.df,
            logger=self.logger,
            result_format=self.result_format)

        self.spark.sql("CREATE SCHEMA IF NOT EXISTS test_silver")

    @patch('great_expectations.data_context.EphemeralDataContext')
    def test_setup_data_context(self, mock_context):
        mock_context.return_value = MagicMock(spec=EphemeralDataContext)
        context = self.gx_runner._setup_data_context()
        self.assertIsNotNone(context)

    def test_run_v1(self):
        pass
        #result_df = self.gx_runner.run()
        #self.assertEqual(result_df.collect(), self.df.collect())

    @parameterized.expand(
        [
            (CONTRACT_WITH_RULES_DIM_DATE_NO_INDEX_COL_QUARANTINE)
        ]
    )
    def test_negative_run_v1(self, contract):
        contract_dict: DQContract = DQContract.parse_raw(contract)
        parsed_expectations = contract_dict._dq_rule_generator(contract_dict.dq_rules)
        self.assertIsNotNone(parsed_expectations, "DQ Rules were not parsed properly.")
        df_with_null = self.spark.createDataFrame([(1, "a"), (2, "b"), (3, None), (4, "d"), (5, None)], ["id", "value"])
        gx_runner = GXRunner(
            spark=self.spark,
            contract=contract_dict,
            df=df_with_null,
            logger=self.logger,
            result_format=self.result_format
        )
        result_df = gx_runner.run()
        self.assertEqual([(1, "a"), (2, "b"),(4, "d")], result_df.collect())
        quarantine_table = self.spark.sql("select * from test_silver.dim_date_quarantine")
        self.assertEqual(2, quarantine_table.count())
