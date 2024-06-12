from pydantic import BaseModel, Field
from typing import Optional


class TableContract(BaseModel):
    db_name: str
    table_name: str
    schema: str
    batch_timestamp: str
    watermark_columns: [str]
    lower_bound: Optional[str] = None
    upper_bound: Optional[str] = None
    full_load: bool = False
    load_type: str = 'incremental'
    target_schema: str
    mount_point: str

    class Config:
        anystr_lower = True  # Example of Pydantic config: convert all strings to lower case
