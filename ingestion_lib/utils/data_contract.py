from dataclasses import dataclass
from typing import Optional, List, Union, Sequence, Dict

from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent import SparkDatasource
from great_expectations.datasource.fluent.spark_datasource import DataFrameAsset
from great_expectations.expectations import Expectation
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from pydantic import BaseModel, Field


class DbCredentials(BaseModel):
    """
    Class containing credentials like user, password and jdbc_url for connecting to oracle_EBS
    """
    user: str
    password: str
    jdbc_url: str


class DataContract(BaseModel):
    batch_timestamp: str
    scope: str
    env_key: str
    target_schema: str
    table_name: str


class TableContract(DataContract):
    table_name: str
    schema_name: str

    watermark_columns: Optional[List[str]]
    lower_bound: Optional[str] = None
    upper_bound: Optional[str] = None
    full_load: bool = False
    load_type: str = 'incremental'
    target_schema: str
    credentials: Optional[DbCredentials] = None

    class Config:
        anystr_lower = True
        allow_population_by_field_name = True


class APIDataContract(DataContract):
    base_url: str
    api_key: str  # or other authentication fields
    spec_path: str
    endpoint_url: str
    model: str  # tricky part to be honest
    credentials: DbCredentials


class DataQualityRule(BaseModel):
    expectation_type: str
    meta: dict
    columns: Optional[List[str]] = None
    kwargs: Optional[Dict] = Field(default_factory=dict)


class DQContract(DataContract):
    dq_rules: List[DataQualityRule]
    unique_columns: List[str]
    result_format: dict = {"result_format": "BASIC"}

    def register_checkpoint(self, context: AbstractDataContext, result_format: Optional[dict] = None) -> Checkpoint:
        data_source: SparkDatasource = context.data_sources.add_spark(
            name=f"{self.env_key}_databricks_runtime")
        data_asset: DataFrameAsset = data_source.add_dataframe_asset(
            name=f"df_{self.target_schema}_{self.table_name}")
        batch_definition: BatchDefinition = data_asset.add_batch_definition_whole_dataframe(
            name=f"run_{self.batch_timestamp}"

        )

        expectation_suite = context.suites.add(
            ExpectationSuite(
                name=f"dq_rules_{self.target_schema}_{self.table_name}",
                expectations=self._dq_rule_generator()
            )
        )
        validation_definition = context.validation_definitions.add(
            ValidationDefinition(
                data=batch_definition,
                suite=expectation_suite,
                name=f"validation_dq_rules_{self.target_schema}_{self.table_name}"
            ))
        result_format = result_format or self.result_format
        result_format.update({
            "unexpected_index_column_names": self.unique_columns
        })
        checkpoint = Checkpoint(
            name=f"checkpoint_dq_{self.target_schema}_{self.table_name}",
            validation_definitions=[validation_definition],
            result_format=result_format
        )
        return checkpoint

    def _dq_rule_generator(self, dq_rules: Optional[List[DataQualityRule]] = None) -> (
            Optional)[Sequence[Union[dict, ExpectationConfiguration, Expectation]]]:
        expectations = []
        dq_rules = dq_rules or self.dq_rules
        for rule in dq_rules:
            if rule.expectation_type:
                for column in rule.columns or []:
                    kwargs = rule.kwargs.copy()
                    kwargs.update({
                        "column": column,
                        "result_format": {
                            "result_format": rule.meta.get("result_format", self.result_format["result_format"]).upper(),
                            "exclude_unexpected_values": False,
                            "unexpected_index_column_names": self.unique_columns,
                            "include_unexpected_rows": True,
                        }
                    })
                    expectation_config = ExpectationConfiguration(
                        type=rule.expectation_type,
                        kwargs=kwargs,
                        meta=rule.meta,
                    )
                    expectations.append(expectation_config)
        return expectations if expectations else None
