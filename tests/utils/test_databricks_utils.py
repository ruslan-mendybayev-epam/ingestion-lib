import unittest
from unittest.mock import mock_open, patch

from ingestion_lib.utils.databricks_utils import DatabricksJobUpdater


class TestDatabricksJobUpdater(unittest.TestCase):
    def setUp(self):
        self.updater = DatabricksJobUpdater(ingestion_path='resources/ingestion.yml', job_path='resources/databricks_job.yml')

    @patch('builtins.open', new_callable=mock_open, read_data='datasets:\n  - name: "hr.something"')
    def test_read_yaml(self, mock_file):
        result = self.updater.read_yaml('ingestion.yml')
        self.assertIn('datasets', result)

    def test_transform_dataset_name(self):
        result = self.updater.transform_dataset_name('hr.something')
        self.assertEqual(result, 'hr_something')

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.safe_dump')
    def test_write_yaml(self, mock_safe_dump, mock_file):
        self.updater.write_yaml({'key': 'value'}, 'test.yml')
        mock_safe_dump.assert_called_once()

    @patch.object(DatabricksJobUpdater, 'read_yaml')
    @patch.object(DatabricksJobUpdater, 'write_yaml')
    def test_update_jobs(self, mock_write_yaml, mock_read_yaml):
        mock_read_yaml.side_effect = [
            {'datasets': [{'name': 'hr.something'}]},  # ingestion.yml
            {'resources': {'jobs': {'ingestion': {'tasks': []}}}}  # databricks_job.yml
        ]
        self.updater.update_jobs()
        args, kwargs = mock_write_yaml.call_args
        self.assertIn('tasks', args[0]['resources']['jobs']['ingestion'])
