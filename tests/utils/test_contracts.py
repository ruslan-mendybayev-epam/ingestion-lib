CONTRACT_WITH_RULES_DIM_DATE_NO_INDEX_COL_QUARANTINE = """
        {
            "name": "test_dataset",
            "batch_timestamp": "1970-13-13",
            "scope": "test_external",
            "env_key": "test_env",
            "target_schema": "test_silver",
            "table_name": "dim_date",
            "type": "transformation",
            "unique_columns": ["id"],
            "dq_rules": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "columns": [
                        "value"
                    ],
                    "meta": {
                        "strategy": "quarantine"
                    }
                }
            ],
            "transform_operation": null
        }
"""
