import os
from os import path
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

from src.utils import (
    ConfigurationException,
    report_found_params,
    raise_schema_mismatch,
)  # noqa
from src.step_execution import StepExecution  # noqa

REQUIRED = [
    "INPUT_schemas_directory",
    "INPUT_workflow_id",
    "INPUT_step_name",
    "INPUT_parameters_directory",
    "INPUT_input_parameters",
    "INPUT_execution_parameters",
    "INPUT_METASTORE_CREDENTIALS",
    "GITHUB_RUN_ID",
    "GITHUB_WORKSPACE",
]

CONTRACT_TYPES = ["input", "execution", "output", "log"]
rootLogger = None
logBuffer = None


def main():

    (rootLogger, logBuffer) = setupLogger()

    # Loading input values
    rootLogger.debug("::debug::Loading input values")

    parameters = convert_environment_variables_to_dict()
    parameters_directory = Path(parameters.INPUT_parameters_directory)

    MLSchema.append_schema_to_registry(parameters_directory / parameters.INPUT_schemas_directory)

    parameters.previous_step_name = os.environ.get(
        "INPUT_previous_step_name", default=None
    )
    parameters.next_step_name = os.environ.get("INPUT_next_step_name", default=None)
    rootLogger.debug("::debug:: Finished main")

    # Load metastore credentials

    rootLogger.debug("::debug:: Loading credentials")
    metastore_cred_string_blob = os.environ.get(
        "INPUT_METASTORE_CREDENTIALS", default="{}"
    )

    metastore_credentials = YAML.safe_load(metastore_cred_string_blob)
    report_found_params(
        ["url", "key", "database_name", "container_name"], metastore_credentials
    )

    # Loading execution parameters file

    rootLogger.debug("::debug::Loading parameters file")
    execution_parameters_file = os.environ.get(
        "INPUT_execution_parameters", default="execution_parameters.yaml"
    )

    verify_parameters_folder_and_file_exist(
        parameters.GITHUB_WORKSPACE, parameters_directory, execution_parameters_file
    )

    execution_parameters_file_path = (
        Path(parameters.GITHUB_WORKSPACE)
        / parameters_directory
        / execution_parameters_file
    )

    execution_parameters = {}
    try:
        execution_parameters = execution_parameters_file_path.read_text("utf-8")
    except FileNotFoundError:
        rootLogger.debug(
            f"::debug:: Could not find parameter file in {execution_parameters_file_path}. Please provide a parameter file in your repository if you do not want to use default settings (e.g. .parameters/execution_parameters.yaml)."
        )

    rootLogger.debug("::debug::Starting metastore connection")

    ms = load_metastore_connection(metastore_credentials)

    workflow_object = load_workflow_object("0.0.1", ms)

    step_name = parameters.INPUT_step_name

    input_object = load_contract_object(
        parameter_string=parameters["INPUT_input_parameters"],
        workflow_object=workflow_object,
        step_name=step_name,
        contract_type="input",
    )

    # TODO don't hard code any of these
    exec_dict = YAML.safe_load(execution_parameters)
    exec_dict["run_id"] = parameters.GITHUB_RUN_ID
    exec_dict["run_date"] = datetime.datetime.now()
    exec_dict["step_id"] = str(uuid.uuid4())

    execution_object = load_contract_object(
        parameter_string=YAML.safe_dump(exec_dict),
        workflow_object=workflow_object,
        step_name=step_name,
        contract_type="execution",
    )

    rootLogger.debug(f"Successfully loaded and validated execution: {execution_object}")

    ms.save(execution_object, workflow_object.schema_version, step_name, "execution")
    rootLogger.debug(f"Successfully saved: {execution_object}")

    results_ml_object = execute_step(workflow_object, input_object, execution_object, step_name, parameters.GITHUB_RUN_ID)

    ms.save(results_ml_object, workflow_object.schema_version, step_name, "output")

    # Recording raw log info
    logBuffer.flush()
    log_contents = logBuffer.getvalue()

    log_object = MLObject()
    log_object.set_type(schema_version="0.1.0", schema_type="log")
    log_object.run_id = parameters.GITHUB_RUN_ID
    log_object.step_name = step_name
    log_object.run_date = datetime.datetime.now()
    log_object.raw_log = (
        "NO RAW LOGS YET (NEED TO FIGURE OUT WHERE I CAN PUSH A LARGE OBJECT)"
    )
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


def verify_parameters_folder_and_file_exist(
    workspace_directory, parameters_directory, parameters_file
):
    full_params_dir = Path(workspace_directory) / parameters_directory

    if not full_params_dir.exists():
        raise ValueError(
            f"{parameters_directory} was not found in the workspace directory {workspace_directory}"
        )

    full_file_location = full_params_dir / parameters_file

    if not full_file_location.exists():
        raise ValueError(f"{full_file_location.resolve()} was not found.")

    return True


def load_metastore_connection(metastore_credentials: dict):
    return Metastore(credentials=metastore_credentials)


def load_workflow_object(
    workflow_schema: str, metastore_connection: Metastore
) -> MLObject:
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.CRITICAL)
    workflow_schema = "0.0.1"
    (workflow_object, errors) = metastore_connection.get_workflow_object(
        workflow_schema
    )
    rootLogger.setLevel(logging.DEBUG)

    if workflow_object is None:
        raise ValueError(
            f"No workflow loaded when attempting to load workflow schema: {workflow_schema}"
        )

    if "steps" not in workflow_object:
        raise ValueError(f"Workflow object does not contain the field 'steps'.")

    # Show count of errors, then errors
    rootLogger.debug(f"Workflow loading errors: {errors}")
    if errors is not None and len(errors) > 0:
        return None
    else:
        return workflow_object

# TODO Break down into verifying contract_type, verify workflow object, and then verify just the MLObject
def load_contract_object(
    parameter_string: str, workflow_object: MLObject, step_name: str, contract_type: str
):
    """ Creates an MLObject based on an input string, and validates it against the workflow object
    and step_name provided.

    Will fail if the .validate() fails on the object or the schema mismatches what is seen in the
    workflow.
    """
    rootLogger = logging.getLogger()

    if contract_type not in CONTRACT_TYPES:
        raise ValueError(
            f"{contract_type} not in the expected list of contract types: {CONTRACT_TYPES}."
        )

    (contract_object, errors) = MLObject.create_object_from_string(parameter_string)

    if errors is not None and len(errors) > 0:
        rootLogger.debug(f"{contract_type} object loading errors: {errors}")
        raise ValueError(f"Error when trying to validate the contract object {step_name}.{contract_type}.")

    if step_name not in workflow_object["steps"]:
        raise ValueError(f"Workflow object does not contain the step '{step_name}'.")

    if contract_type not in workflow_object["steps"][step_name]:
        raise ValueError(f"Workflow object for step '{step_name}' does not contain a spec for the contract type: '{contract_type}'.")

    if (
        contract_object.schema_type
        != workflow_object["steps"][step_name][contract_type].schema_type
    ) or (
        contract_object.schema_version
        != workflow_object["steps"][step_name][contract_type].schema_version
    ):
        raise_schema_mismatch(
            expected_type=workflow_object["steps"][step_name][
                contract_type
            ].schema_type,
            actual_type=contract_object.schema_type,
            expected_version=workflow_object["steps"][step_name][
                contract_type
            ].schema_version,
            actual_version=contract_object.schema_version,
        )
    rootLogger.debug(
        f"Successfully loaded and validated contract object: {contract_object.schema_type} on step {step_name}.{contract_type}"
    )
    return contract_object

def execute_step(workflow_object: MLObject, input_object: MLObject, execution_object: MLObject, step_name, run_id):
    rootLogger = logging.getLogger()
    step_execution_object = StepExecution(input_object, execution_object)
    results_ml_object = step_execution_object.execute(
        result_object_schema_type=workflow_object.steps[step_name].output.schema_type,
        result_object_schema_version=workflow_object.steps[
            step_name
        ].output.schema_version,
    )

    if results_ml_object is None or not isinstance(results_ml_object, MLObject):
        raise ValueError(f"Execution failed to return an MLObject. Cannot save output.")

    results_ml_object.run_id = run_id
    results_ml_object.step_id = uuid.uuid4()
    results_ml_object.run_date = datetime.datetime.now()

    # Using the below to validate the object, even though we already have it created.
    load_contract_object(
        parameter_string=YAML.safe_dump(results_ml_object.dict_without_internal_variables()),
        workflow_object=workflow_object,
        step_name=step_name,
        contract_type="output",
    )

    return results_ml_object

if __name__ == "__main__":
    main()
