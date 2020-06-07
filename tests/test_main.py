import os
import sys
import io
import unittest
import logging
import pathlib
from pathlib import Path
from unittest.mock import patch, MagicMock
import yaml as YAML
import gremlin_python
from gremlin_python.driver import client
import mlspeclib.experimental.metastore
from mlspeclib import MLObject
from collections import namedtuple
from unittest import mock
from unittest.mock import Mock
from box import Box
import base64
from marshmallow.class_registry import RegistryError

if Path("src").exists():
    sys.path.append(str(Path("src")))
sys.path.append(str(Path.cwd()))
sys.path.append(str(Path.cwd().parent))

from main import (
    main,
    convert_environment_variables_to_dict,
    verify_parameters_folder_and_file_exist,
    load_metastore_connection,
    load_workflow_object,
    load_contract_object,
    execute_step,
    load_parameters,
)

from utils.utils import (  # noqa E402
    report_found_params,
    setupLogger,
    KnownException
)

from step_execution import StepExecution  # noqa E402


class test_main(unittest.TestCase):
    """Main test cases."""

    def test_main_no_input(self):
        """
        Unit test to check the main function with no inputs
        """
        buffer = setupLogger().get_buffer()
        with self.assertRaises(SystemExit) as context:
            main()

        self.assertEqual(context.exception.code, 1)
        self.assertTrue("No value provided for" in str(buffer.getvalue()))

    def test_setup_logger_returns(self):
        (rootLogger, buffer) = setupLogger().get_loggers()
        self.assertTrue(rootLogger, logging.getLogger())

        message_string = "Test log message"
        rootLogger.warning(message_string)

        return_string = buffer.getvalue()
        assert message_string in return_string

        with patch.object(rootLogger, "debug") as mock_log:
            rootLogger.debug("foo")
            mock_log.assert_called_once_with("foo")

    def test_convert_environment_variables(self):
        mock_variables = """\
            INPUT_WORKFLOW_NODE_ID: 'xxxxx'
            INPUT_STEP_NAME: 'process_data'
            INPUT_PARAMETERS_DIRECTORY: '.parameters'
            INPUT_EXECUTION_PARAMETERS: 'tests/execution_parameters.yaml'
            INPUT_SCHEMAS_DIRECTORY: 'tests/workflow_schemas'
            INPUT_INPUT_PARAMETERS_RAW: 'input_parameters'
            GITHUB_RUN_ID: 'github_run_id'
            GITHUB_WORKSPACE: 'github_workspace'
            INPUT_METASTORE_CREDENTIALS: 'a: b\nc: d'"""

        mock_dict = YAML.safe_load(mock_variables)
        with patch.dict(os.environ, mock_dict):
            return_dict = convert_environment_variables_to_dict()
            self.assertTrue(
                mock_dict["INPUT_STEP_NAME"] == return_dict["INPUT_STEP_NAME"]
            )

    def test_report_found_params(self):
        string_name = "parameter_name"
        dict_to_validate = {string_name: "VALUE"}
        report_found_params([string_name], dict_to_validate)

        dict_to_validate.pop(string_name)
        with self.assertRaises(KnownException) as context:
            report_found_params([string_name], dict_to_validate)

        self.assertTrue(string_name in str(context.exception))

    def test_verify_parameters_folder_and_file_exists(self):
        with patch.object(Path, "exists") as mock_exists:
            mock_exists.return_value = True
            self.assertTrue(
                verify_parameters_folder_and_file_exist("foo", "bar", "qaz")
            )

    def encode_dict(self, dict_to_encode: dict):
        yaml_string = YAML.safe_dump(dict_to_encode)
        encoded_string = yaml_string.encode("utf-8")
        base64_string = str(base64.urlsafe_b64encode(encoded_string), "utf-8")
        return base64_string

    def test_connect_to_metastore(self):
        with patch.object(gremlin_python.driver.client, "Client") as mock_connect:
            mock_connect.return_value = True
            cred_dict = {}
            with self.assertRaises(KeyError) as context:
                load_metastore_connection(self.encode_dict(cred_dict))
            self.assertTrue("url" in str(context.exception))

            cred_dict["url"] = "foo"
            with self.assertRaises(KeyError) as context:
                load_metastore_connection(self.encode_dict(cred_dict))
            self.assertTrue("key" in str(context.exception))

            cred_dict["key"] = "foo"
            with self.assertRaises(KeyError) as context:
                load_metastore_connection(self.encode_dict(cred_dict))
            self.assertTrue("database_name" in str(context.exception))

            cred_dict["database_name"] = "foo"
            with self.assertRaises(KeyError) as context:
                load_metastore_connection(self.encode_dict(cred_dict))
            self.assertTrue("container_name" in str(context.exception))

            cred_dict["container_name"] = "foo"

            self.assertTrue(load_metastore_connection(self.encode_dict(cred_dict)))

    #    @unittest.skip("Binary Search")
    def test_load_workflow_object(self):
        with patch.object(
            mlspeclib.experimental.metastore, "Metastore"
        ) as mock_metastore:
            mock_metastore.get_workflow_object.return_value = (None, None)

            with self.assertRaises(KnownException) as context:
                load_workflow_object("0.0.1", mock_metastore)

            self.assertTrue("load workflow" in str(context.exception))

            workflow_object = MLObject()
            mock_metastore.get_workflow_object.return_value = (workflow_object, None)
            with self.assertRaises(KnownException) as context:
                load_workflow_object("0.0.1", mock_metastore)
            self.assertTrue("field 'steps'" in str(context.exception))

            workflow_object = MLObject()
            workflow_object.steps = {}
            mock_metastore.get_workflow_object.return_value = (workflow_object, None)
            return_object = load_workflow_object("0.0.1", mock_metastore)

            self.assertTrue(isinstance(return_object, MLObject))

    def test_load_contract_object_bad_contract(self):
        with self.assertRaises(KnownException) as context:
            load_contract_object(None, None, None, "bad_contract")

        self.assertTrue("bad_contract" in str(context.exception))

    def test_load_contract_object_bad_loading_contract(self):
        with patch.object(
            mlspeclib.mlobject.MLObject, "create_object_from_string"
        ) as mock_mlobject:
            mock_mlobject.return_value = (None, "error")
            with self.assertRaises(KnownException) as context:
                load_contract_object("FAKEFIELD: FAKEVALUE", None, None, "input")

            self.assertTrue("validate the contract" in str(context.exception))

    def test_load_contract_object_no_steps(self):
        with patch.object(
            mlspeclib.mlobject.MLObject, "create_object_from_string"
        ) as mock_mlobject:
            mock_mlobject.return_value = (None, None)
            with self.assertRaises(KnownException) as context:
                load_contract_object(
                    "FAKEFIELD: FAKEVALUE", {"steps": {}}, None, "input"
                )

            self.assertTrue("contain the step" in str(context.exception))

    def test_load_contract_object_no_contract_type(self):
        with patch.object(
            mlspeclib.mlobject.MLObject, "create_object_from_string"
        ) as mock_mlobject:
            mock_mlobject.return_value = (None, None)
            with self.assertRaises(KnownException) as context:
                load_contract_object(
                    "FAKEFIELD: FAKEVALUE",
                    {"steps": {"step_name": "NO_CONTRACT"}},
                    "step_name",
                    "input",
                )

            self.assertTrue("the contract type" in str(context.exception))

    def test_load_contract_object_schema_type_mismatch(self):
        with patch.object(
            mlspeclib.mlobject.MLObject, "create_object_from_string"
        ) as mock_mlobject:
            mock_schema_type = namedtuple("Object", ["schema_type", "schema_version"])
            mock_mlobject.return_value = (
                mock_schema_type(
                    schema_type="EXPECTED_TYPE", schema_version="EXPECTED_VERSION"
                ),
                None,
            )
            with self.assertRaises(KnownException) as context:
                load_contract_object(
                    "FAKEFIELD: FAKEVALUE",
                    {
                        "steps": {
                            "step_name": {
                                "input": mock_schema_type(
                                    schema_type="ACTUAL_TYPE",
                                    schema_version="EXPECTED_VERSION",
                                )
                            }
                        }
                    },
                    "step_name",
                    "input",
                )

            self.assertTrue("schema and version" in str(context.exception))

    def test_load_contract_object_schema_version_mismatch(self):
        with patch.object(
            mlspeclib.mlobject.MLObject, "create_object_from_string"
        ) as mock_mlobject:
            mock_schema_type = namedtuple("Object", ["schema_type", "schema_version"])
            mock_mlobject.return_value = (
                mock_schema_type(
                    schema_type="EXPECTED_TYPE", schema_version="EXPECTED_VERSION"
                ),
                None,
            )
            with self.assertRaises(KnownException) as context:
                load_contract_object(
                    "FAKEFIELD: FAKEVALUE",
                    {
                        "steps": {
                            "step_name": {
                                "input": mock_schema_type(
                                    schema_type="EXPECTED_TYPE",
                                    schema_version="ACTUAL_VERSION",
                                )
                            }
                        }
                    },
                    "step_name",
                    "input",
                )

            self.assertTrue("schema and version" in str(context.exception))

    @patch.object(StepExecution, "__init__", return_value=None)
    @patch.object(StepExecution, "execute", return_value=None)
    def test_step_exection_return_no_result_object(self, *mock_step_execution):
        workflow_box = Box({"steps": {"FAKESTEP": {"output": {}}}})
        workflow_box.steps.FAKESTEP.output.schema_type = "FAKETYPE"
        workflow_box.steps.FAKESTEP.output.schema_version = "0.0.1"
        with self.assertRaises(KnownException) as context:
            execute_step(None, workflow_box, None, None, "FAKESTEP", None)

        self.assertTrue("No value was assigned" in str(context.exception))

    @patch.object(Path, "exists", return_value=True)
    @patch.object(Path, "read_text", return_value="")
    def test_step_exec_no_result_object(self, *mock_step_execution):
        workflow_box = Box({"steps": {"FAKESTEP": {"output": {}}}})
        workflow_box.steps.FAKESTEP.output.schema_type = "FAKETYPE"
        workflow_box.steps.FAKESTEP.output.schema_version = "0.0.1"
        with self.assertRaises(KnownException) as context:
            execute_step('fakefile.py', workflow_box, None, None, "FAKESTEP", None)

        self.assertTrue("No value was assigned" in str(context.exception))

    @patch.object(Path, "exists", return_value=False)
    def test_step_exec_no_file(self, *mock_step_execution):
        workflow_box = Box({"steps": {"FAKESTEP": {"output": {}}}})
        workflow_box.steps.FAKESTEP.output.schema_type = "FAKETYPE"
        workflow_box.steps.FAKESTEP.output.schema_version = "0.0.1"
        with self.assertRaises(KnownException) as context:
            execute_step('fakefile.py', workflow_box, None, None, "FAKESTEP", None)

        self.assertTrue("provided as the file" in str(context.exception))

    @patch.object(Path, "__init__", return_value=None)
    @patch.object(Path, "exists", return_value=False)
    def test_load_param_from_file_does_not_exist(self, *mock_path):
        contract_type = "INPUT"

        os.environ[f"INPUT_{contract_type}_parameters_file_path"] = "FAKEPATH"

        with self.assertRaises(KnownException):
            load_parameters(contract_type, False)

    @unittest.skip("Need to fix path mocks")
    def test_load_param_from_file_exists(self):
        contract_type = "INPUT"
        os.environ[f"INPUT_{contract_type}_PARAMETERS_FILE_PATH"] = "FAKEPATH"
        expected_dict = YAML.safe_load("FAKEFIELD: 'FAKEVALUE'")

        with patch.object(pathlib, "Path", return_value=MagicMock()) as mock_path:
            mock_path.exists.return_value = True
            mock_path.read_text.return_value = 'FAKEFIELD": "FAKEVALUE"'

            return_dict = load_parameters(contract_type, None)

        self.assertTrue(return_dict["FAKEFIELD"] == expected_dict["FAKEFIELD"])

    @unittest.skip("Fail")
    @patch("sys.stdout", new_callable=io.StringIO)
    def test_registry_error_catcher(self, mock_stdout, *mock_patched_obj):
        mock_variables = """\
            INPUT_WORKFLOW_NODE_ID: 'xxxxx'
            INPUT_STEP_NAME: 'process_data'
            INPUT_PARAMETERS_DIRECTORY: '.parameters'
            INPUT_EXECUTION_PARAMETERS: 'tests/execution_parameters.yaml'
            INPUT_SCHEMAS_DIRECTORY: 'tests/bad_schemas/bad_base'
            INPUT_INPUT_PARAMETERS_RAW: 'input_parameters'
            GITHUB_RUN_ID: 'github_run_id'
            GITHUB_WORKSPACE: 'github_workspace'
            INPUT_METASTORE_CREDENTIALS: 'a: b'"""

        mock_dict = YAML.safe_load(mock_variables)
        with patch.dict(os.environ, mock_dict):
            with self.assertRaises(SystemExit):
                main()

            self.assertTrue(
                "Could not find the base schema" in str(mock_stdout.getvalue())
            )


if __name__ == "__main__":
    unittest.main()
    # unittest.load_tests()
