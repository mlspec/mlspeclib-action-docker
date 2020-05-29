import os
import json
import yaml as YAML

import logging
from box import Box
from pathlib import Path
import sys
from io import StringIO

import datetime
import uuid

from mlspeclib import MLObject, MLSchema
from mlspeclib.experimental.metastore import Metastore

from utils import ( # noqa
    report_found_params,
    raise_schema_mismatch,
    setupLogger,
    KnownException,
)


# Making this a class in case we want sub functions.
class StepExecution:
    input_params = {}  # noqa
    execution_params = {}  # noqa
    ml_object = MLObject()  # noqa
    logger = None  # noqa

    def __init__(self, input_params, execution_params):
        self.input_params = input_params
        self.execution_params = execution_params
        self.logger = setupLogger().get_root_logger()

        # Execute all work in here.

        # Output input params & execution params
        if self.input_params is not None:
            self.logger.debug(f"Input params: {self.input_params}")

        if self.execution_params is not None:
            self.logger.debug(f"Execution params: {self.execution_params}")
        else:
            raise KnownException("No execution parameters provided.")

    def execute(self, result_object_schema_type, result_object_schema_version):
        # Create Result object
        results_object = MLObject()
        results_object.set_type(
            schema_type=result_object_schema_type,
            schema_version=result_object_schema_version,
        )

        # Mocked up results
        return_dict = {
            "data_output_path": str(Path("tests/data/data_output.csv")),
            "data_statistics_path": str(Path("tests/data/data_stats.csv")),
            "data_schemas_path": str(Path("tests/data/data_schemas.yaml")),
            "feature_file_path": str(Path("tests/data/feature_file.yaml")),
        }

        results_object.data_output_path = return_dict["data_output_path"]
        results_object.data_statistics_path = return_dict["data_statistics_path"]
        results_object.data_schemas_path = return_dict["data_schemas_path"]
        results_object.feature_file_path = return_dict["feature_file_path"]

        _ = results_object.validate() # noqa
        return results_object
