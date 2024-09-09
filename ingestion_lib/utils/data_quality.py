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
        Initialize the DataQualityCheckerStub class with necessary parameters.
        """
        self.logger: CustomLogger = (
            logger if logger else CustomLogger(__class__)
        )
        os.environ[GE_USAGE_STATS] = "FALSE"
        self.contract: dict = json.loads(contract)
        self.dataset_rules = self.contract.get("dq_rules")
        self.index_columns = None
        if self.dataset_rules:
            self.result_format = next(
                (rule.get("result_format") for rule in self.dataset_rules if rule.get("result_format")), result_format
            ).upper()
            self.index_columns = self._get_index_columns()
        else:
            self.result_format = result_format
        if not self.index_columns:
            self.result_format = "BASIC"
        self._quarantine_table_name = None
        self._quarantine_table_location = None
        self._unexpected_records_count = 0
        self.spark = spark
        self.layer = layer
        self.database_name = database_name if database_name else layer
        self.table_name = table_name
        self.site_name = site_name
        self.context_root_dir = context_root_dir
        self.provider = (
            self.contract["provider"] if "provider" in self.contract else self.contract[
                "type"] if "type" in self.contract else "unknown"
        )
        self.correlation_id = correlation_id
        self.df = df
        self.data_connector_name = f"prx-{self.provider}-df"
        self.data_source_name = f"prx-{self.provider}-{self._get_suite_name()}"

        self.context = self._setup_data_context() if self.dataset_rules else None

    def _setup_data_context(self) -> AbstractDataContext:
        try:
            # Initialize Great Expectations DataContext as EphemeralDataContext to avoid concurrency issues with file-based context.
            # If you need to replicate the concurrency issue, run `pytest -n 3 tests/test_dq_checks.py`
            project_config = DataContextConfig(store_backend_defaults=InMemoryStoreBackendDefaults())
            context = gx.get_context(project_config=project_config)

            # Configuring Data Doc sites makes no sense for EphemeralDataContext as the execution results are not stored
            # between the runs.
            # self._configure_data_docs_sites(self.site_name, self.should_publish, self.azure_storage_connection_string, context)

            # Resetting the handler for SIGTERM to the default behavior to prevent the calling notebook from hanging up
            # See great_expectations/core/usage_statistics/usage_statistics.py for details.
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
        except BaseException as e:
            self._handle_exception(e)
            return None
        return context

    def _add_expectations(self) -> Tuple[list[dict | ExpectationConfiguration], list[str]]:
        """
        Step 2: Add expectations
        Add expectations to the context based on the dataset rules defined in the contract.
        This involves iterating through the dataset rules and creating expectation configurations.
        """
        # Iterate through datasets and their dq_rules to add expectations
        if not self.dataset_rules:
            return None
        expectations = []
        index_columns = self.index_columns
        if not self.index_columns:
            index_columns = [DEFAULT_INDEX_COLUMN]
            self.df = self._create_index_columns(self.df, index_columns)

        for rule in self.dataset_rules:
            if "expectation_type" in rule:
                for column in rule.pop("columns", []):
                    meta = rule.get(
                        "meta",
                        {},
                    )
                    result_format = rule.get(
                        "result_format",
                        self.result_format,
                    ).upper()
                    # For each column, create an expectation
                    if "kwargs" in rule:
                        kwargs = rule["kwargs"].copy()
                    else:
                        kwargs = {}
                    kwargs["column"] = column  # Add the column name to kwargs
                    kwargs["result_format"] = {
                        "result_format": result_format,
                        "exclude_unexpected_values": False,
                        "unexpected_index_column_names": index_columns,
                        "include_unexpected_rows": True,
                    }
                    expectation_config = ExpectationConfiguration(
                        type=rule["expectation_type"],
                        kwargs=kwargs,
                        meta=meta,
                    )
                    expectations.append(expectation_config)
                    self.logger.debug(f"Added rule to the '{self._get_suite_name()}' suite:\n {expectation_config}")
        return expectations, index_columns

    def _create_checkpoint(
            self, suite_name: str, checkpoint_name: str, expectations: list[dict | ExpectationConfiguration],
            index_columns: list[str]
    ) -> Checkpoint:
        """
        Step 3: Create a checkpoint
        Create a checkpoint using the expectations added in the previous step.
        This involves building a batch request and configuring the checkpoint with the necessary actions.
        """
        pass

    def _run_checkpoint(self, checkpoint: Checkpoint) -> CheckpointResult:
        """
        Step 4: Run the checkpoint
        Run the checkpoint and capture the results.
        This involves executing the checkpoint and retrieving the validation results.
        """
        pass

    def _quarantine_unexpected_records(
            self, df: DataFrame, index_columns: list[str], checkpoint_result: CheckpointResult, mount_point: str
    ) -> DataFrame:

        run_name = checkpoint_result.run_id.run_name
        unexpected_index_df = df.select(index_columns).limit(0)
        all_unexpected_records: List[DataFrame] = []

        quarantine_table_name = self._get_quarantine_table_name(run_name=run_name)
        for _, validation_result in checkpoint_result.run_results.items():
            expectation_results = validation_result.get("results")
            for expectation_result in expectation_results:
                if (
                        not expectation_result.success
                        and "raised_exception" in expectation_result.exception_info
                        and expectation_result.exception_info["raised_exception"]
                ):
                    self._log_gx_exception(exception, expectation_result)

                expectation_config = expectation_result.get("expectation_config", {})
                rule_type = expectation_config.get("type", {})
                rule_kwargs = expectation_config.get("kwargs", {})
                rule_strategy = expectation_config.get("meta", {}).get("strategy", "undefined")

                unexpected_count = expectation_result.get("result", {}).get("unexpected_count", 0)
                if unexpected_count:
                    self._unexpected_records_count += unexpected_count
                if "unexpected_index_list" in expectation_result["result"]:
                    unexpected_list = list(expectation_result["result"]["unexpected_index_list"])

                    if unexpected_list:
                        self.logger.debug(
                            f"Unexpected index list for rule {rule_type} with args \n{rule_kwargs} \n\n{unexpected_list}")
                        unexpected_index_df = self.spark.createDataFrame(unexpected_list, unexpected_index_df.schema)
                        unxepected_df: DataFrame = (
                            df.join(unexpected_index_df, on=index_columns, how="inner")
                            .withColumn("__rule_type", lit(rule_type).cast(StringType()))
                            .withColumn("__rule_kwargs", lit(str(rule_kwargs)).cast(StringType()))
                            .withColumn("__rule_strategy", lit(str(rule_strategy)).cast(StringType()))
                            .withColumn("__correlation_id", lit(self.correlation_id).cast(StringType()))
                        )
                        all_unexpected_records.append(self._handle_void_dtypes(unxepected_df))
        if all_unexpected_records:
            self._quarantine_table_name = self._save_unexpected_rows(run_name,
                                                                     all_unexpected_records)
            df = self._apply_strategy(
                df=df,
                index_columns=index_columns,
                all_unexpected_records=all_unexpected_records,
                expectation_config=expectation_config,
                fully_qualified_table_name=self._quarantine_table_name,
            )
            self.logger.debug(
                f"Unexpected records saved to {self._quarantine_table_name} at {self._quarantine_table_location}.")
        else:
            self._quarantine_table_name = None
            self._quarantine_table_location = None
        return df

    def _handle_exception(self, e):
        if hasattr(e, "message"):
            msg = e.message
        else:
            msg = str(e)
        stack_trace = traceback.format_exc()
        self.logger.log_error(f"Exception in data quality checker: {msg}",
                              operation={"message": msg, "stack_trace": stack_trace})

    def run(self, mount_point: str = "/mnt") -> DataFrame:
        if not self.context:
            return self.df
        try:
            expectation_suite = self._add_expectations()
            if not expectation_suite:
                return _drop_index_column(self.df)
            expectations, index_columns = expectation_suite
            checkpoint = self._create_checkpoint(
                self._get_suite_name(),
                f"{self.table_name}_checkpoint",
                expectations,
                index_columns)
            checkpoint_result = checkpoint.run()
            filtered_df = self._quarantine_unexpected_records(
                self.df, index_columns, checkpoint_result, mount_point
            )
            filtered_df = _drop_index_column(filtered_df)

        except DQValidationError as dqe:
            raise dqe
        except BaseException as e:
            # suppress all other errors and return original df
            self._handle_exception(e)
            return _drop_index_column(self.df)
        return filtered_df

    def run_v1(self, ) -> DataFrame:
        # Connect to data
        data_source: SparkDatasource = self.context.data_sources.add_spark(name="Databricks_runtime")
        data_asset: DataFrameAsset = data_source.add_dataframe_asset(name="Delta_table_dataframe")
        batch_definition = data_asset.add_batch_definition_whole_dataframe(name="Check_not_null")

        # Create Expectation Suite
        expectations, index_columns = self._add_expectations()
        expectation_suite = self.context.suites.add(
            ExpectationSuite(name=self._get_suite_name(), expectations=expectations))
        # Create a Validation Definition
        definition_name = "my_validation_definition"
        validation_definition = self.context.validation_definitions.add(ValidationDefinition(
            data=batch_definition, suite=expectation_suite, name=definition_name
        ))

        action_list = [
        ]
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
        return filtered_df

    def _get_index_columns(self) -> list[str]:
        if not self.dataset_rules:
            raise ValueError("The 'dq_rules' attribute must in the contract to get or create index columns.")

        index_columns = self.contract.get("index_columns")
        if not index_columns:
            index_columns = next(
                (rule.get("index_columns") for rule in self.dataset_rules if rule.get("index_columns")), None)
        self.logger.debug(f"Index columns: {index_columns}")
        return index_columns

    def _get_suite_name(self) -> str:
        """
        Helper method to get the suite name.
        """
        return ".".join(filter(None, [self.layer, self.database_name, self.table_name]))

    def _create_index_columns(self, df: DataFrame, index_columns: list[str]) -> DataFrame:
        if not self.dataset_rules:
            raise ValueError("The 'dq_rules' attribute must in the contract to get or create index columns.")
        # generate distributed unique values
        self.logger.debug("The 'index_columns' attribute not found in the contract. Creating index column.")

        for column in index_columns:
            df = df.withColumn(column, monotonically_increasing_id())

        return df

    def _get_quarantine_table_name(self, run_name):
        # date_time_layer_database_table
        quarantine_table_name = slugify(run_name, separator="_")
        return quarantine_table_name

    def _handle_void_dtypes(self, df: DataFrame) -> DataFrame:
        for column_name, column_type in df.dtypes:
            # If the column type is 'void', cast it to 'string'
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
            unexpected_df.write.format("delta").mode("append").option("mergeSchema", "true").option("overwriteSchema", "true").saveAsTable(name=fully_qualified_table_name)
        return fully_qualified_table_name

    def _apply_strategy(
            self,
            df: DataFrame,
            index_columns,
            all_unexpected_records: List[DataFrame],
            expectation_config,
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
