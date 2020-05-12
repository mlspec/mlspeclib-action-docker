import os
import sys
import io
import unittest
import logging
from unittest.mock import patch
import yaml as YAML

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(myPath, "..", "code"))

from execution_code.main import (
    main,
    setupLogger,
    convert_environment_variables_to_dict,
    report_found_params,
)  # noqa
from execution_code.utils import ConfigurationException  # noqa


class HelpersTestSuite(unittest.TestCase):
    """Helpers test cases."""

    def test_main_no_input(self):
        """
        Unit test to check the main function with no inputs
        """
        with self.assertRaises(ConfigurationException):
            main()

    @patch("sys.stdout", new_callable=io.StringIO)
    def test_setup_logger_returns(self, mock_stdout):
        (rootLogger, _) = setupLogger()
        self.assertTrue(rootLogger, logging.getLogger())

        message_string = "Test log message"
        rootLogger.debug(message_string)

        return_string = mock_stdout.getvalue()
        assert message_string in return_string

        with patch.object(rootLogger, "debug") as mock_log:
            rootLogger.debug("foo")
            mock_log.assert_called_once_with("foo")

    def test_convert_environment_variables(self):
        mock_variables = """\
            INPUT_workflow_id: '0.0.1'
            INPUT_step_name: 'process_data'
            INPUT_execution_parameters: 'tests/execution_parameters.yaml'
            INPUT_schemas_directory: 'tests/workflow_schemas'
            INPUT_input_parameters: 'input_parameters'
            GITHUB_RUN_ID: 'github_run_id'
            GITHUB_WORKSPACE: 'github_workspace'
            INPUT_METASTORE_CREDENTIALS: 'a: b\nc: d'"""

        mock_dict = YAML.safe_load(mock_variables)
        with patch.dict(os.environ, mock_dict):
            return_dict = convert_environment_variables_to_dict()
            self.assertTrue(
                mock_dict["INPUT_step_name"] == return_dict["INPUT_step_name"]
            )

    def test_report_found_params(self):
        string_name = "parameter_name"
        dict_to_validate = {string_name: "VALUE"}
        report_found_params([string_name], dict_to_validate)

        dict_to_validate.pop(string_name)
        with self.assertRaises(ValueError) as context:
            report_found_params([string_name], dict_to_validate)

        self.assertTrue(string_name in str(context.exception))

    # def test_main_invalid_azure_credentials():
    #     os.environ["INPUT_AZURE_CREDENTIALS"] = ""
    #     with pytest.raises(AMLConfigurationException):
    #         assert main()

    # def test_main_invalid_parameters_file():
    #     os.environ["INPUT_AZURE_CREDENTIALS"] = """{
    #         'clientId': 'test',
    #         'clientSecret': 'test',
    #         'subscriptionId': 'test',
    #         'tenantId': 'test'
    #     }"""
    #     os.environ["INPUT_PARAMETERS_FILE"] = "wrongfile.json"
    #     with pytest.raises(AMLConfigurationException):
    #         assert main()


if __name__ == "__main__":
    unittest.main()
