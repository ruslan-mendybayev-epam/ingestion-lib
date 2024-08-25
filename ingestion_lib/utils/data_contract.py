from typing import Optional, List

from pydantic import BaseModel

class DataContract(BaseModel):
    class Config:
        allow_population_by_field_name = True

class DbCredentials(BaseModel):
    """
    Class containing credentials like user, password and jdbc_url for connecting to oracle_EBS
    """
    user: str
    password: str
    jdbc_url: str


class TableContract(DataContract):
    table_name: str
    schema_name: str
    batch_timestamp: str
    watermark_columns: Optional[List[str]]
    lower_bound: Optional[str] = None
    upper_bound: Optional[str] = None
    full_load: bool = False
    load_type: str = 'incremental'
    target_schema: str
    credentials: Optional[DbCredentials] = None

class APIDataContract(DataContract):
    base_url: str
    api_key: str  # or other authentication fields
    swagger_path: str
    endpoint_url: str
    endpoint_model: str
    credentials: DbCredentials
