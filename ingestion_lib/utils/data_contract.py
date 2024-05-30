from pydantic import BaseModel, Field
from typing import Optional


class DataContract(BaseModel):
    db_name: str
    table_name: str
    schema: str
    watermark_column: str
    lower_bound: Optional[str] = None
    upper_bound: Optional[str] = None
    full_load: bool = False
    load_type: str = 'incremental'

    class Config:
        anystr_lower = True  # Example of Pydantic config: convert all strings to lower case
