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
from mlspeclib import MLObject, MLSchema
from collections import namedtuple
from unittest import mock
from unittest.mock import Mock
from box import Box
import base64
from marshmallow.class_registry import RegistryError
from random import random, randint, randrange
import datetime
import uuid

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
    KnownException,
    verify_result_contract,
)

from step_execution import StepExecution  # noqa E402


class test_e2e(unittest.TestCase):
    """Main test cases."""

    input_parameters = {
        # Put sample required input parameters here
    }

    execution_parameters = {
        # Put sample required execution parameters here
    }

    def setUp(self):
        (self.rootLogger, self._buffer) = setupLogger().get_loggers()

        MLSchema.populate_registry()
        MLSchema.append_schema_to_registry(Path.cwd() / "tests" / "schemas_for_test")

    def test_process_data(self):
        """
        Full E2E of Process Data
        """
        # THESE SHOULD BE THE ONLY SETTINGS FOR THIS FILE
        step_name = "process_data"
        expected_results_schema_type = "data_result"  # MUST BE A LOADED SCHEMA
        expected_results_schema_version = "9999.0.1"  # MUST BE A SEMVER

        results_ml_object = MLObject()
        results_ml_object.set_type(
            schema_type=expected_results_schema_type,
            schema_version=expected_results_schema_version,
        )

        # Should error due to missing fields
        with self.assertRaises(ValueError) as context:
            verify_result_contract(
                results_ml_object,
                expected_results_schema_type,
                expected_results_schema_version,
                step_name,
            )

        self.assertTrue(
            f"Error verifying result object for '{step_name}.output'"
            in str(context.exception)
        )

        results_ml_object = MLObject()

        result_ml_object_schema_type = expected_results_schema_type
        result_ml_object_schema_version = expected_results_schema_version

        exec(
            (Path("tests") / "sample_process_data_execution.py").read_text(),
            globals(),
            locals(),
        )

        results_ml_object.run_date = datetime.datetime.now()
        results_ml_object.step_id = str(uuid.uuid4())
        results_ml_object.run_id = str(uuid.uuid4())

        results_ml_object.execution_profile.system_memory_utilization = random()
        results_ml_object.execution_profile.network_traffic_in_bytes = randint(
            7e9, 9e10
        )
        results_ml_object.execution_profile.gpu_temperature = randint(70, 130)
        results_ml_object.execution_profile.disk_io_utilization = random()
        results_ml_object.execution_profile.gpu_percent_of_time_accessing_memory = (
            random()
        )
        results_ml_object.execution_profile.cpu_utilization = random()
        results_ml_object.execution_profile.gpu_utilization = random()
        results_ml_object.execution_profile.gpu_memory_allocation = random()

        self.assertTrue(
            verify_result_contract(
                results_ml_object,
                expected_results_schema_type,
                expected_results_schema_version,
                step_name,
            )
        )

    def test_train(self):
        step_name = "train"
        expected_results_schema_type = "training_results"  # MUST BE A LOADED SCHEMA
        expected_results_schema_version = "9999.0.1"  # MUST BE A SEMVER

        step_execution_object = StepExecution(
            self.input_parameters, self.execution_parameters
        )

        results_ml_object = MLObject()
        results_ml_object.set_type(
            schema_type=expected_results_schema_type,
            schema_version=expected_results_schema_version,
        )

        # Should error due to missing fields
        with self.assertRaises(ValueError) as context:
            verify_result_contract(
                results_ml_object,
                expected_results_schema_type,
                expected_results_schema_version,
                step_name,
            )

        self.assertTrue(
            f"Error verifying result object for '{step_name}.output'"
            in str(context.exception)
        )

        result_ml_object_schema_type = expected_results_schema_type
        result_ml_object_schema_version = expected_results_schema_version

        exec(
            (Path("tests") / "sample_train_execution.py").read_text(),
            globals(),
            locals(),
        )

        results_ml_object.run_date = datetime.datetime.now()
        results_ml_object.step_id = uuid.uuid4()
        results_ml_object.run_id = uuid.uuid4()

        results_ml_object.execution_profile.system_memory_utilization = random()
        results_ml_object.execution_profile.network_traffic_in_bytes = randint(7e9, 9e10)
        results_ml_object.execution_profile.gpu_temperature = randint(70, 130)
        results_ml_object.execution_profile.disk_io_utilization = random()
        results_ml_object.execution_profile.gpu_percent_of_time_accessing_memory = random()
        results_ml_object.execution_profile.cpu_utilization = random()
        results_ml_object.execution_profile.gpu_utilization = random()
        results_ml_object.execution_profile.gpu_memory_allocation = random()

        self.assertTrue(
            verify_result_contract(
                results_ml_object,
                expected_results_schema_type,
                expected_results_schema_version,
                step_name,
            )
        )

    def test_package(self):
        step_name = "train"
        expected_results_schema_type = "package_results"  # MUST BE A LOADED SCHEMA
        expected_results_schema_version = "9999.0.1"  # MUST BE A SEMVER

        step_execution_object = StepExecution(
            self.input_parameters, self.execution_parameters
        )

        results_ml_object = MLObject()
        results_ml_object.set_type(
            schema_type=expected_results_schema_type,
            schema_version=expected_results_schema_version,
        )

        # Should error due to missing fields
        with self.assertRaises(ValueError) as context:
            verify_result_contract(
                results_ml_object,
                expected_results_schema_type,
                expected_results_schema_version,
                step_name,
            )

        self.assertTrue(
            f"Error verifying result object for '{step_name}.output'"
            in str(context.exception)
        )

        result_ml_object_schema_type = expected_results_schema_type
        result_ml_object_schema_version = expected_results_schema_version

        exec(
            (Path("tests") / "sample_package_execution.py").read_text(),
            globals(),
            locals(),
        )

        results_ml_object.run_date = datetime.datetime.now()
        results_ml_object.step_id = uuid.uuid4()
        results_ml_object.run_id = uuid.uuid4()

        results_ml_object.execution_profile.system_memory_utilization = random()
        results_ml_object.execution_profile.network_traffic_in_bytes = randint(7e9, 9e10)
        results_ml_object.execution_profile.gpu_temperature = randint(70, 130)
        results_ml_object.execution_profile.disk_io_utilization = random()
        results_ml_object.execution_profile.gpu_percent_of_time_accessing_memory = random()
        results_ml_object.execution_profile.cpu_utilization = random()
        results_ml_object.execution_profile.gpu_utilization = random()
        results_ml_object.execution_profile.gpu_memory_allocation = random()

        self.assertTrue(
            verify_result_contract(
                results_ml_object,
                expected_results_schema_type,
                expected_results_schema_version,
                step_name,
            )
        )

if __name__ == "__main__":
    unittest.main()
    # unittest.load_tests()
