from typing import Optional, List

from pydantic import BaseModel


class DbCredentials(BaseModel):
    """
    Class containing credentials like user, password and jdbc_url for connecting to oracle_EBS
    """
    user: str
    password: str
    jdbc_url: str

class DataContract(BaseModel):
    batch_timestamp: str

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
    model: str
    credentials: DbCredentials
