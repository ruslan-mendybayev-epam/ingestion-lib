from typing import Optional, List

from pydantic import BaseModel


class TableContract(BaseModel):
    db_name: str
    table_name: str
    schema_: str
    batch_timestamp: str
    watermark_columns: List[str]
    lower_bound: Optional[str] = None
    upper_bound: Optional[str] = None
    full_load: bool = False
    load_type: str = 'incremental'
    target_schema: str
    mount_point: str

    class Config:
        anystr_lower = True
        allow_population_by_field_name = True
        fields = {
            'schema_': {'alias': 'schema'}
        }
