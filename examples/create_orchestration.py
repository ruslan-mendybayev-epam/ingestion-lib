from ingestion_lib.utils.databricks_utils import DatabricksJobUpdater

if __name__ == '__main__':
    updater = DatabricksJobUpdater(input_path='resources/ingestion.yml', template='resources/databricks_job.yml', output_path='resources/databricks_workflow.yml')
    updater.update_jobs()


