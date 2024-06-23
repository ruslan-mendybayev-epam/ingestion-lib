from typing import Optional, List

from pydantic import BaseModel


class DbCredentials(BaseModel):
    """
    Class containing credentials like user, password and jdbc_url for connecting to oracle_EBS
    """
    user: str
    password: str
    jdbc_url: str


class TableContract(BaseModel):
    table_name: str
    schema_name: str
    batch_timestamp: str
    watermark_columns: List[str]
    lower_bound: Optional[str] = None
    upper_bound: Optional[str] = None
    full_load: bool = False
    load_type: str = 'incremental'
    target_schema: str
    credentials: Optional[DbCredentials] = None

    class Config:
        anystr_lower = True
        allow_population_by_field_name = True

