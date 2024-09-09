import json
import os
import signal
import traceback

from pyspark.sql.types import StringType
from slugify import slugify
from enum import Enum
from typing import Optional, Tuple, List

import great_expectations as gx
from great_expectations import ValidationDefinition, ExpectationSuite
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.data_context import AbstractDataContext, EphemeralDataContext
from great_expectations.data_context.types.base import DataContextConfig, InMemoryStoreBackendDefaults
from great_expectations.datasource.fluent import SparkDatasource
from great_expectations.datasource.fluent.spark_datasource import DataFrameAsset
from great_expectations.exceptions import GreatExpectationsValidationError
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import monotonically_increasing_id, lit, col

from ingestion_lib.utils.log_analytics import CustomLogger

CONTEXT_ROOT_DIR: str = "/mnt/bronze_raw/dq_checks"
DEFAULT_INDEX_COLUMN: str = "__unique_id__"
GE_USAGE_STATS: str = "GE_USAGE_STATS"


class DQValidationError(GreatExpectationsValidationError):
    def __init__(self, message):
        super().__init__(message)


def _drop_index_column(df: DataFrame):
    if DEFAULT_INDEX_COLUMN in df.columns:
        df = df.drop(DEFAULT_INDEX_COLUMN)
    return df


class Strategy(Enum):
    QUARANTINE = "quarantine"
    WARNING = "warning"
    ERROR = "error"


class DataQualityValidator:
    def __init__(
            self,
            spark: SparkSession,
            df: DataFrame,
            layer: str,
            table_name: str,
            contract: str,
            correlation_id: str,
            database_name: str = None,
            site_name: str = "PantherX Rare: Enterprise Data Warehouse",
            logger: Optional[CustomLogger] = None,
            context_root_dir: str = CONTEXT_ROOT_DIR,
            result_format: str = "COMPLETE",
    ):
        """
        Initialize the DataQualityValidator class with necessary parameters.
        """
        self.logger: CustomLogger = logger if logger else CustomLogger(__class__)
        os.environ[GE_USAGE_STATS] = "FALSE"
        self.contract: dict = json.loads(contract)
        self.dataset_rules = self.contract.get("dq_rules")
        self.index_columns = self._get_index_columns() if self.dataset_rules else None
        self.result_format = self._get_result_format(result_format)
        self._quarantine_table_name = None
        self._quarantine_table_location = None
        self._unexpected_records_count = 0
        self.spark = spark
        self.layer = layer
        self.database_name = database_name if database_name else layer
        self.table_name = table_name
        self.site_name = site_name
        self.context_root_dir = context_root_dir
        self.provider = self.contract.get("provider", self.contract.get("type", "unknown"))
        self.correlation_id = correlation_id
        self.df = df
        self.data_connector_name = f"prx-{self.provider}-df"
        self.data_source_name = f"prx-{self.provider}-{self._get_suite_name()}"
        self.context = self._setup_data_context() if self.dataset_rules else None

    def _setup_data_context(self) -> AbstractDataContext:
        try:
            project_config = DataContextConfig(store_backend_defaults=InMemoryStoreBackendDefaults())
            context = gx.get_context(project_config=project_config)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
        except BaseException as e:
            self._handle_exception(e)
            return None
        return context

    def _add_expectations(self) -> Tuple[List[ExpectationConfiguration], List[str]]:
        if not self.dataset_rules:
            return [], []
        expectations = []
        index_columns = self.index_columns or [DEFAULT_INDEX_COLUMN]
        self.df = self._create_index_columns(self.df, index_columns) if not self.index_columns else self.df

        for rule in self.dataset_rules:
            if "expectation_type" in rule:
                for column in rule.pop("columns", []):
                    kwargs = rule.get("kwargs", {}).copy()
                    kwargs.update({
                        "column": column,
                        "result_format": {
                            "result_format": rule.get("result_format", self.result_format).upper(),
                            "exclude_unexpected_values": False,
                            "unexpected_index_column_names": index_columns,
                            "include_unexpected_rows": True,
                        }
                    })
                    expectation_config = ExpectationConfiguration(
                        type=rule["expectation_type"],
                        kwargs=kwargs,
                        meta=rule.get("meta", {}),
                    )
                    expectations.append(expectation_config)
                    self.logger.debug(f"Added rule to the '{self._get_suite_name()}' suite:\n {expectation_config}")
        return expectations, index_columns

    def _create_checkpoint(
            self, suite_name: str, checkpoint_name: str, expectations: List[ExpectationConfiguration],
            index_columns: List[str]
    ) -> Checkpoint:
        pass

    def _run_checkpoint(self, checkpoint: Checkpoint) -> CheckpointResult:
        pass

    def _quarantine_unexpected_records(
            self, df: DataFrame, index_columns: List[str], checkpoint_result: CheckpointResult, mount_point: str
    ) -> DataFrame:
        run_name = checkpoint_result.run_id.run_name
        unexpected_index_df = df.select(index_columns).limit(0)
        all_unexpected_records: List[DataFrame] = []

        quarantine_table_name = self._get_quarantine_table_name(run_name=run_name)
        for _, validation_result in checkpoint_result.run_results.items():
            expectation_results = validation_result.get("results")
            for expectation_result in expectation_results:
                if not expectation_result.success and expectation_result.exception_info.get("raised_exception"):
                    self._log_gx_exception(expectation_result.exception_info["exception"], expectation_result)

                unexpected_count = expectation_result.get("result", {}).get("unexpected_count", 0)
                self._unexpected_records_count += unexpected_count
                unexpected_list = expectation_result["result"].get("unexpected_index_list", [])

                if unexpected_list:
                    self.logger.debug(
                        f"Unexpected index list for rule {expectation_result.get('expectation_config', {}).get('type')} with args \n{expectation_result.get('expectation_config', {}).get('kwargs')} \n\n{unexpected_list}")
                    unexpected_index_df = self.spark.createDataFrame(unexpected_list, unexpected_index_df.schema)
                    unexpected_df = (
                        df.join(unexpected_index_df, on=index_columns, how="inner")
                        .withColumn("__rule_type", lit(expectation_result.get('expectation_config', {}).get('type')).cast(StringType()))
                        .withColumn("__rule_kwargs", lit(str(expectation_result.get('expectation_config', {}).get('kwargs'))).cast(StringType()))
                        .withColumn("__rule_strategy", lit(str(expectation_result.get('expectation_config', {}).get('meta', {}).get('strategy', 'undefined'))).cast(StringType()))
                        .withColumn("__correlation_id", lit(self.correlation_id).cast(StringType()))
                    )
                    all_unexpected_records.append(self.handle_void_dtypes(unexpected_df))
        if all_unexpected_records:
            self._quarantine_table_name = self._save_unexpected_rows(run_name, all_unexpected_records)
            df = self._apply_strategy(
                df=df,
                index_columns=index_columns,
                all_unexpected_records=all_unexpected_records,
                expectation_config=expectation_result.get('expectation_config', {}),
                fully_qualified_table_name=self._quarantine_table_name,
            )
            self.logger.debug(
                f"Unexpected records saved to {self._quarantine_table_name} at {self._quarantine_table_location}.")
        else:
            self._quarantine_table_name = None
            self._quarantine_table_location = None
        return df

    def _handle_exception(self, e):
        msg = getattr(e, "message", str(e))
        stack_trace = traceback.format_exc()
        self.logger.log_error(f"Exception in data quality checker: {msg}",
                              operation={"message": msg, "stack_trace": stack_trace})

    def run(self) -> DataFrame:
        if not self.context:
            return self.df
        try:
            data_source: SparkDatasource = self.context.data_sources.add_spark(name="Databricks_runtime")
            data_asset: DataFrameAsset = data_source.add_dataframe_asset(name="Delta_table_dataframe")
            batch_definition = data_asset.add_batch_definition_whole_dataframe(name="Check_not_null")

            expectations, index_columns = self._add_expectations()
            expectation_suite = self.context.suites.add(
                ExpectationSuite(name=self._get_suite_name(), expectations=expectations))
            validation_definition = self.context.validation_definitions.add(ValidationDefinition(
                data=batch_definition, suite=expectation_suite, name="my_validation_definition"
            ))

            checkpoint = self.context.checkpoints.add(Checkpoint(
                name="my_checkpoint",
                validation_definitions=[validation_definition],
                result_format={"result_format": "COMPLETE", "include_unexpected_rows": True,
                               "unexpected_index_column_names": index_columns},
            ))

            batch_parameters = {"dataframe": self.df}
            runid = gx.RunIdentifier(run_name=f"DQ_run_{self.table_name}")
            checkpoint_result = checkpoint.run(batch_parameters=batch_parameters, run_id=runid)
            self.logger.debug(
                f"==========================\nGreat Expectations checkpoint run results: \n{checkpoint_result}\n=========================="
            )
            filtered_df = self._quarantine_unexpected_records(
                self.df, index_columns=index_columns, checkpoint_result=checkpoint_result, mount_point=""
            )
            filtered_df = _drop_index_column(filtered_df)

        except DQValidationError as dqe:
            raise dqe
        except BaseException as e:
            self._handle_exception(e)
            return _drop_index_column(self.df)
        return filtered_df

    def _get_index_columns(self) -> List[str]:
        if not self.dataset_rules:
            raise ValueError("The 'dq_rules' attribute must be in the contract to get or create index columns.")

        index_columns = self.contract.get("index_columns")
        if not index_columns:
            index_columns = next(
                (rule.get("index_columns") for rule in self.dataset_rules if rule.get("index_columns")), None)
        self.logger.debug(f"Index columns: {index_columns}")
        return index_columns

    def _get_suite_name(self) -> str:
        return ".".join(filter(None, [self.layer, self.database_name, self.table_name]))

    def _create_index_columns(self, df: DataFrame, index_columns: List[str]) -> DataFrame:
        if not self.dataset_rules:
            raise ValueError("The 'dq_rules' attribute must be in the contract to get or create index columns.")
        self.logger.debug("The 'index_columns' attribute not found in the contract. Creating index column.")

        for column in index_columns:
            df = df.withColumn(column, monotonically_increasing_id())

        return df

    def _get_quarantine_table_name(self, run_name: str) -> str:
        return slugify(run_name, separator="_")

    @staticmethod
    def handle_void_dtypes(df: DataFrame) -> DataFrame:
        for column_name, column_type in df.dtypes:
            if column_type == "void":
                df = df.withColumn(column_name, col(column_name).cast("string"))
        return df

    def _save_unexpected_rows(self, run_name: str, all_unexpected_records: List[DataFrame]) -> str:
        schema_name = f"{self.layer}_quarantine"
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        quarantine_table_name = self._get_quarantine_table_name(run_name)
        fully_qualified_table_name = f"{schema_name}.{quarantine_table_name}"

        for unexpected_df in all_unexpected_records:
            self.logger.debug(f"Saving unexpected records to {fully_qualified_table_name}")
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {fully_qualified_table_name} USING DELTA")
            unexpected_df.write.format("delta").mode("append").option("mergeSchema", "true").option("overwriteSchema",
                                                                                                    "true").saveAsTable(
                name=fully_qualified_table_name)
        return fully_qualified_table_name

    def _apply_strategy(
            self,
            df: DataFrame,
            index_columns: List[str],
            all_unexpected_records: List[DataFrame],
            expectation_config: dict,
            fully_qualified_table_name: str,
    ) -> DataFrame:
        for unexpected_df in all_unexpected_records:
            if "__rule_strategy" not in unexpected_df.columns:
                continue

            count = unexpected_df.count()
            if count > 0:
                strategy = unexpected_df.first()["__rule_strategy"]
                if strategy in [Strategy.QUARANTINE, Strategy.WARNING]:
                    self.logger.log_success(count, expectation_config, strategy)

                match Strategy(strategy):
                    case Strategy.QUARANTINE:
                        df = df.join(unexpected_df, on=index_columns, how="leftanti")
                    case Strategy.ERROR:
                        message = (
                            f"Expectation with strategy '{strategy}' found {count} "
                            f"unexpected records and sent them to quarantine table '{fully_qualified_table_name}'."
                        )
                        self.logger.log_error(message, expectation_config, strategy)
                        raise DQValidationError(message)
        return df

    def _get_result_format(self, default_format: str) -> str:
        if self.dataset_rules:
            return next(
                (rule.get("result_format") for rule in self.dataset_rules if rule.get("result_format")), default_format
            ).upper()
        return default_format
