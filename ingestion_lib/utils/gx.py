import traceback
from typing import Union, Optional, List

import great_expectations as gx
from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.types.base import DataContextConfig, InMemoryStoreBackendDefaults
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from ingestion_lib.utils.data_contract import DQContract
from ingestion_lib.utils.data_quality import DQValidationError, Strategy, DataQualityValidator
from ingestion_lib.utils.log_analytics import CustomLogger


class GXRunner:

    def __init__(self,
                 spark: SparkSession,
                 contract: Union[DQContract],
                 df: DataFrame,
                 logger: CustomLogger,
                 result_format: Optional[dict] = None
                 ):
        self.spark = spark
        self.data_contract = contract
        self.df = df
        self.result_format = result_format if result_format else self._default_result_format()
        self.context = self._setup_data_context()
        self.logger: CustomLogger = logger if logger else CustomLogger(__class__)

    def run(self) -> DataFrame:
        if not self.context:
            return self.df
        try:
            batch_parameters = {"dataframe": self.df}
            run_id = gx.RunIdentifier(run_name=f"GXRunner_on_{self.data_contract.table_name}")
            checkpoint = self.data_contract.register_checkpoint(
                context=self.context,
                result_format=self.result_format
            )
            checkpoint_result = checkpoint.run(
                batch_parameters=batch_parameters,
                run_id=run_id
            )
            self.logger.debug(
                f"===================Great Expectations checkpoint run results: \n{checkpoint_result}\n================"
            )
            filtered_df = self._interpret_results(checkpoint_result=checkpoint_result)
            return filtered_df

        except DQValidationError as dqe:
            raise dqe
        except BaseException as e:
            self._handle_exception(e)
            raise e

    def _setup_data_context(self) -> AbstractDataContext:
        try:
            project_config = DataContextConfig(store_backend_defaults=InMemoryStoreBackendDefaults())
            context = gx.get_context(project_config=project_config)
        except BaseException as e:
            self._handle_exception(e)
            return None
        return context

    def _handle_exception(self, e):
        msg = getattr(e, "message", str(e))
        stack_trace = traceback.format_exc()
        self.logger.log_error(f"Exception in data quality checker: {msg}",
                              operation={"message": msg, "stack_trace": stack_trace})

    def _default_result_format(self):
        return {
            "result_format": "COMPLETE",
            "include_unexpected_rows": True,
        }

    def _interpret_results(self, checkpoint_result: CheckpointResult) -> DataFrame:
        run_name = checkpoint_result.run_id.run_name
        columns = self.data_contract.unique_columns
        unexpected_index_df = self.df.select(columns).limit(0)
        all_unexpected_records: List[DataFrame] = []
        df = self.df
        for _, validation_result in checkpoint_result.run_results.items():
            expectation_results = validation_result.get("results")
            for expectation_result in expectation_results:

                unexpected_count = expectation_result.get("result", {}).get("unexpected_count", 0)
                unexpected_list = expectation_result["result"].get("unexpected_index_list", [])
                if unexpected_list:
                    self.logger.debug(
                        f"Unexpected index list for "
                        f"rule {expectation_result.get('expectation_config', {}).get('type')} "
                        f"with args "
                        f"\n{expectation_result.get('expectation_config', {}).get('kwargs')} \n\n{unexpected_list}")
                    unexpected_index_df = self.spark.createDataFrame(unexpected_list, unexpected_index_df.schema)
                    unexpected_df = (
                        df.join(unexpected_index_df, on=self.data_contract.unique_columns, how="inner")
                        .withColumn(
                            "__rule_type",
                            lit(expectation_result.get('expectation_config', {}).get('type')).cast(StringType()))
                        .withColumn(
                            "__rule_kwargs",
                            lit(str(expectation_result.get('expectation_config', {}).get('kwargs'))).cast(StringType()))
                        .withColumn(
                            "__rule_strategy",
                            lit(str(
                                expectation_result
                                .get('expectation_config', {})
                                .get('meta', {}).get('strategy', 'undefined')))
                            .cast(StringType()))
                    )
                    all_unexpected_records.append(DataQualityValidator.handle_void_dtypes(unexpected_df))

                if all_unexpected_records:
                    df = self._appy_strategy(
                        run_name=run_name,
                        all_unexpected_records=all_unexpected_records,
                        expectation_config=expectation_result.get('expectation_config', {})
                    )
                    self.logger.debug(
                        f"Unexpected records processed.")
        df = df or self.df
        return df

    def _appy_strategy(self,
                       run_name: str,
                       all_unexpected_records: List[DataFrame],
                       expectation_config: dict
                       ) -> DataFrame:
        df = self.df

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
                        self._save_unexpected_rows(unexpected_df=unexpected_df)
                        df = self.df.join(unexpected_df, on=self.data_contract.unique_columns, how="leftanti")
                    case Strategy.ERROR:
                        message = (
                            f"Expectation with strategy '{strategy}' found {count} "
                            f"unexpected records and sent them to quarantine table '{fully_qualified_table_name}'."
                        )
                        self.logger.log_error(message, expectation_config, strategy)
                        raise DQValidationError(message)
        return df

    def _save_unexpected_rows(self, unexpected_df: DataFrame):
        quarantine_table_name = f"{self.data_contract.table_name}_quarantine"
        fully_qualified_table_name = f"{self.data_contract.target_schema}.{quarantine_table_name}"

        self.logger.debug(f"Saving unexpected records to {fully_qualified_table_name}")
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {fully_qualified_table_name} USING DELTA")
        (unexpected_df.write.format("delta").mode("append")
         .option("mergeSchema", "true")
         .option("overwriteSchema","true")
         .saveAsTable(name=fully_qualified_table_name)
         )

        return fully_qualified_table_name
