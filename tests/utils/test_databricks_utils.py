import unittest
from unittest.mock import patch, mock_open

from ingestion_lib.utils.databricks_utils import DatabricksJobUpdater


class TestDatabricksJobUpdater(unittest.TestCase):
    def setUp(self):
        self.updater = DatabricksJobUpdater('ingestion.yml', 'template.yml', 'output.yml')

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.safe_load')
    @patch('yaml.safe_dump')
    def test_empty_datasets(self, mock_safe_dump, mock_safe_load, mock_file):
        # Setup mock responses
        mock_safe_load.side_effect = [
            {'datasets': []},  # Empty datasets from ingestion.yml
            {}  # Empty template
        ]

        # Run the updater
        self.updater.update_jobs()

        # Check that no tasks are written
        mock_safe_dump.assert_called_once()
        args, kwargs = mock_safe_dump.call_args
        self.assertNotIn('tasks', args[0]['resources']['jobs']['ingestion'])

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.safe_load')
    @patch('yaml.safe_dump')
    def test_task_creation(self, mock_safe_dump, mock_safe_load, mock_file):
        # Setup mock responses
        mock_safe_load.side_effect = [
            {'datasets': [{'name': 'hr.something'}]},  # Non-empty datasets
            {}  # Empty template
        ]

        # Run the updater
        self.updater.update_jobs()

        # Check that tasks are created correctly
        mock_safe_dump.assert_called_once()
        args, kwargs = mock_safe_dump.call_args
        self.assertIn('tasks', args[0]['resources']['jobs']['ingestion'])
        self.assertEqual(args[0]['resources']['jobs']['ingestion']['tasks'][0]['task_key'], 'hr_something')

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.safe_load')
    @patch('yaml.safe_dump')
    def test_task_update(self, mock_safe_dump, mock_safe_load, mock_file):
        # Setup mock responses
        mock_safe_load.side_effect = [
            {'datasets': [{'name': 'hr.something'}]},
            {'resources': {'jobs': {'ingestion': {'tasks': [{'task_key': 'hr_something', 'existing_cluster_id': 'old_id'}]}}}}
        ]

        # Run the updater
        self.updater.update_jobs()

        # Check that the existing task is updated
        mock_safe_dump.assert_called_once()
        args, kwargs = mock_safe_dump.call_args
        self.assertEqual(args[0]['resources']['jobs']['ingestion']['tasks'][0]['existing_cluster_id'], '1234-567890-abcde123')


if __name__ == '__main__':
    unittest.main()
