CONTRACT_WITH_RULES_DIM_DATE_NO_INDEX_COL_QUARANTINE = """
        {
            "name": "dim_date",
            "class_name": "prx_dwh_lib.data_transformation.silver.dim_date.DimDateTransformation",
            "type": "transformation",
            "dq_rules": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "columns": [
                        "id"
                    ],
                    "meta": {
                        "strategy": "warning"
                    }
                }
            ],
            "transform_operation": null
        }
"""
