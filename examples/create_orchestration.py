from ingestion_lib.utils.databricks_utils import DatabricksWorkflowGenerator

if __name__ == '__main__':
    task_config = {
        'existing_cluster_id': '1234-567890-abcde123',
        'notebook_task': {
            'notebook_path': './hello.py'
        }
    }
    updater = DatabricksWorkflowGenerator(ingestion_contract_path='resources/ingestion.yml', workflow_template_path='resources/databricks_job.yml',
                                          output_path='resources/databricks_workflow.yml', task_config=task_config)
    updater.generate_workflow()
