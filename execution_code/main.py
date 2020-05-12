import os
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

sys.path.append(str(Path.cwd()))

from execution_code.utils import (
    ConfigurationException,
    report_found_params,
    raise_schema_mismatch,
) # noqa
from execution_code.step_execution import step_execution # noqa

REQUIRED = [
    "INPUT_schemas_directory",
    "INPUT_workflow_id",
    "INPUT_step_name",
    "INPUT_input_parameters",
    "INPUT_execution_parameters",
    "INPUT_METASTORE_CREDENTIALS",
    "GITHUB_RUN_ID",
    "GITHUB_WORKSPACE",
]


def main():

    (rootLogger, logBuffer) = setupLogger()

    # Loading input values
    rootLogger.debug("::debug::Loading input values")

    parameters = convert_environment_variables_to_dict()

    MLSchema.append_schema_to_registry(Path(parameters.INPUT_schemas_directory))

    parameters.previous_step_name = os.environ.get(
        "INPUT_previous_step_name", default=None
    )
    parameters.next_step_name = os.environ.get("INPUT_next_step_name", default=None)
    rootLogger.debug("::debug:: Finished main")

    rootLogger.debug("::debug:: Loading credentials")
    metastore_cred_string_blob = os.environ.get(
        "INPUT_METASTORE_CREDENTIALS", default="{}"
    )

    metastore_credentials = YAML.safe_load(metastore_cred_string_blob)
    report_found_params(
        ["url", "key", "database_name", "container_name"], metastore_credentials
    )

    # Loading parameters file
    rootLogger.debug("::debug::Loading parameters file")
    execution_parameters_file = os.environ.get(
        "INPUT_execution_parameters", default="execution_parameters.yaml"
    )

    # TODO check to see if .parameters folder is there
    # TODO check to see if parameters file is there

    execution_parameters_file_path = (
        Path(parameters.GITHUB_WORKSPACE) / ".parameters" / execution_parameters_file
    )

    execution_parameters = {}
    try:
        execution_parameters = execution_parameters_file_path.read_text('utf-8')
    except FileNotFoundError:
        print(
            f"::debug::Could not find parameter file in {execution_parameters_file_path}. Please provide a parameter file in your repository if you do not want to use default settings (e.g. .parameters/execution_parameters.yaml)."
        )

    rootLogger.debug("::debug::Starting metastore connection")
    rootLogger.debug(parameters)

    ms = Metastore(credentials=metastore_credentials)

    rootLogger.setLevel(logging.CRITICAL)
    (workflow_object, errors) = ms.get_workflow_object("0.0.1")
    rootLogger.setLevel(logging.DEBUG)

    # Show count of errors, then errors
    rootLogger.debug(f"Workflow loading errors: {errors}")

    step_name = parameters.INPUT_step_name

    # TODO errors if schema is invalid
    (input_object, errors) = MLObject.create_object_from_string(
        parameters.INPUT_input_parameters
    )
    rootLogger.debug(f"Input object loading errors: {errors}")

    input_object.validate()

    if (
        ("steps" not in workflow_object)
        or (step_name not in workflow_object["steps"])
        or (
            input_object.schema_type
            != workflow_object["steps"][step_name].input.schema_type
        )
        or (
            input_object.schema_version
            != workflow_object["steps"][step_name].input.schema_version
        )
    ):
        raise_schema_mismatch(
            expected_type=workflow_object["steps"][step_name].input.schema_type,
            actual_type=input_object.schema_type,
            expected_version=workflow_object["steps"][step_name].input.schema_version,
            actual_version=input_object.schema_version,
        )

    rootLogger.debug(f"Successfully loaded and validated input: {input_object}")

    # TODO don't hard code any of these
    exec_dict = YAML.safe_load(execution_parameters)
    exec_dict["run_id"] = parameters.GITHUB_RUN_ID
    exec_dict["run_date"] = datetime.datetime.now()
    exec_dict["step_id"] = str(uuid.uuid4())

    (execution_object, errors) = MLObject.create_object_from_string(
        YAML.safe_dump(exec_dict)
    )

    # TODO bail if execution object not valid
    rootLogger.debug(f"Execution object loading errors: {errors}")

    execution_object.validate()

    if (
        ("steps" not in workflow_object)
        or (step_name not in workflow_object["steps"])
        or (
            execution_object.schema_type
            != workflow_object["steps"][step_name].execution.schema_type
        )
        or (
            execution_object.schema_version
            != workflow_object["steps"][step_name].execution.schema_version
        )
    ):
        raise_schema_mismatch(
            expected_type=workflow_object["steps"][step_name].execution.schema_type,
            actual_type=execution_object.schema_type,
            expected_version=workflow_object["steps"][
                step_name
            ].execution.schema_version,
            actual_version=execution_object.schema_version,
        )

    rootLogger.debug(f"Successfully loaded and validated execution: {execution_object}")

    # Attempting to save execution metadata
    rootLogger.setLevel(logging.CRITICAL)
    all_items = ms.get_all_runs(workflow_object.schema_version, step_name)
    rootLogger.setLevel(logging.DEBUG)
    rootLogger.debug(f"Number of metadata in step: {len(list(all_items))}")

    ms.save(execution_object, workflow_object.schema_version, step_name, "execution")

    rootLogger.debug(f"Successfully saved: {execution_object}")
    all_items = ms.get_all_runs(workflow_object.schema_version, step_name)
    rootLogger.debug(f"Number of metadata in step: {len(list(all_items))}")

    step_execution_object = step_execution(input_object, execution_object)
    results_ml_object = step_execution_object.execute(
        result_object_schema_type=workflow_object.steps[step_name].output.schema_type,
        result_object_schema_version=workflow_object.steps[
            step_name
        ].output.schema_version,
    )

    results_ml_object.run_id = parameters.GITHUB_RUN_ID
    results_ml_object.step_id = uuid.uuid4()
    results_ml_object.run_date = datetime.datetime.now()

    errors = results_ml_object.validate()

    ms.save(results_ml_object, workflow_object.schema_version, step_name, "output")

    # Recording raw log info
    logBuffer.flush()
    log_contents = logBuffer.getvalue()

    log_object = MLObject()
    log_object.set_type(schema_version="0.1.0", schema_type="log")
    log_object.run_id = parameters.GITHUB_RUN_ID
    log_object.step_name = step_name
    log_object.run_date = datetime.datetime.now()
    log_object.raw_log = "NO RAW LOGS YET (NEED TO FIGURE OUT WHERE I CAN PUSH A LARGE OBJECT)"
    # log_object.raw_log = log_contents
    log_object.log_property_bag = {}

    errors = log_object.validate()

    ms.save(log_object, workflow_object.schema_version, step_name, "log")


def setupLogger():
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s\n"
    )

    buffer = StringIO()
    bufferHandler = logging.StreamHandler(buffer)
    bufferHandler.setLevel(logging.DEBUG)
    bufferHandler.setFormatter(formatter)
    rootLogger.addHandler(bufferHandler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)
    rootLogger.addHandler(stdout_handler)

    return (rootLogger, buffer)


def convert_environment_variables_to_dict():
    return_dict = Box()

    for var in REQUIRED:
        return_dict[var] = os.environ.get(var, default=None)
        if return_dict[var] is None:
            raise ConfigurationException(f"No value provided for {var}.")

    return return_dict


if __name__ == "__main__":
    main()
