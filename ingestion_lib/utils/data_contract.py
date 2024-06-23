from typing import Optional, List

from pydantic import BaseModel


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
    credentials: Optional[any] = None

    class Config:
        anystr_lower = True
        allow_population_by_field_name = True

