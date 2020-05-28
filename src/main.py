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
import marshmallow
import base64
import git
from git import GitCommandError

from mlspeclib import MLObject, MLSchema
from mlspeclib.experimental.metastore import Metastore

sys.path.append(str(Path.cwd()))

from utils import (
    ConfigurationException,
    report_found_params,
    raise_schema_mismatch,
)  # noqa
from step_execution import StepExecution  # noqa

REQUIRED = [
    "INPUT_workflow_node_id",
    "INPUT_step_name",
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

    parameters.INPUT_schemas_directory = os.environ.get("INPUT_schemas_directory", ".parameters/schemas")

    if "INPUT_schemas_git_url" in os.environ:
        parameters.INPUT_schemas_git_url = os.environ.get("INPUT_schemas_git_url")
        try:
            git.Git(parameters.INPUT_schemas_directory).clone(parameters.INPUT_schemas_git_url, str(uuid.uuid4()), depth=1)
            # TODO: Authenticate with GH Token?
        except GitCommandError as gce:
            raise ValueError(f"Trying to read from the git repo ({parameters.INPUT_schemas_git_url}) and write to the directory ({parameters.INPUT_schemas_directory}). Full error follows: {str(gce)}")

    MLSchema.append_schema_to_registry(Path(parameters.INPUT_schemas_directory))

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

    metastore_credentials_packed = YAML.safe_load(metastore_cred_string_blob)
    metastore_credentials_string = base64.urlsafe_b64decode(metastore_credentials_packed).decode('utf-8')
    metastore_credentials = YAML.safe_load(metastore_credentials_string)

    report_found_params(
        ["url", "key", "database_name", "container_name"], metastore_credentials
    )

    rootLogger.debug("::debug::Starting metastore connection")

    ms = load_metastore_connection(metastore_credentials_packed)
    workflow_node_id = os.environ.get("INPUT_workflow_node_id")
    if workflow_node_id is None:
        raise ValueError(f"INPUT_workflow_node_id - No workflow node id was provided.")
    workflow_object = load_workflow_object(workflow_node_id, ms)

    rootLogger.debug("::debug::Loading input parameters")
    input_parameters = load_parameters('input', ms)

    rootLogger.debug("::debug::Loading execution parameters file")
    execution_parameters = load_parameters('execution', ms)

    step_name = parameters.INPUT_step_name
    input_object = load_contract_object(
        parameters=input_parameters,
        workflow_object=workflow_object,
        step_name=step_name,
        contract_type="input",
    )

    input_node_id = ms.attach_step_info(
        input_object,
        workflow_object.schema_version,
        workflow_node_id,
        step_name,
        "input",
    )
    rootLogger.debug(f"Successfully saved: {input_object}")

    # TODO don't hard code any of these
    exec_dict = execution_parameters
    exec_dict["run_id"] = parameters.GITHUB_RUN_ID
    exec_dict["run_date"] = datetime.datetime.now()
    exec_dict["step_id"] = str(uuid.uuid4())

    execution_object = load_contract_object(
        parameters=exec_dict,
        workflow_object=workflow_object,
        step_name=step_name,
        contract_type="execution",
    )

    rootLogger.debug(f"Successfully loaded and validated execution: {execution_object}")

    execution_node_id = ms.attach_step_info(
        execution_object,
        workflow_object.schema_version,
        workflow_node_id,
        step_name,
        "execution",
    )
    rootLogger.debug(f"Successfully saved: {execution_object}")

    results_ml_object = execute_step(
        workflow_object,
        input_object,
        execution_object,
        step_name,
        parameters.GITHUB_RUN_ID,
    )

    output_node_id = ms.attach_step_info(
        results_ml_object,
        workflow_object.schema_version,
        workflow_node_id,
        step_name,
        "output",
    )

    dict_conversion = results_ml_object.dict_without_internal_variables()

    string_io_handle = StringIO()
    YAML.SafeDumper.add_representer(uuid.UUID, repr_uuid)
    YAML.safe_dump(dict_conversion, string_io_handle)
    yaml_conversion = string_io_handle.getvalue()

    encode_to_utf8_bytes = yaml_conversion.encode("utf-8")
    base64_encode = base64.urlsafe_b64encode(encode_to_utf8_bytes)
    final_encode_to_utf8 = str(base64_encode, "utf-8")

    # Recording raw log info
    logBuffer.flush()
    # log_contents = logBuffer.getvalue()

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

    # errors = log_object.validate()

    log_node_id = ms.attach_step_info(
        log_object, workflow_object.schema_version, workflow_node_id, step_name, "log"
    )

    print(f"::set-output name=output_raw::{results_ml_object.dict_without_internal_variables()}")
    print(f"::set-output name=output_base64_encoded::{final_encode_to_utf8}")
    print(f"::set-output name=input_node_id::{input_node_id}")
    print(f"::set-output name=execution_node_id::{execution_node_id}")
    print(f"::set-output name=output_node_id::{output_node_id}")
    print(f"::set-output name=log_node_id::{log_node_id}")

def repr_uuid(dumper, uuid_obj):
    return YAML.ScalarNode("tag:yaml.org,2002:str", str(uuid_obj))

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


def load_metastore_connection(credentials_packed: str):
    return Metastore(credentials_packed=credentials_packed)


def load_workflow_object(
    workflow_node_id: str, metastore_connection: Metastore
) -> MLObject:
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.CRITICAL)
    (workflow_object, errors) = metastore_connection.get_workflow_object(
        workflow_node_id
    )
    rootLogger.setLevel(logging.DEBUG)

    if workflow_object is None:
        raise ValueError(
            f"No workflow loaded when attempting to load workflow node id: {workflow_node_id}"
        )

    if "steps" not in workflow_object:
        raise ValueError(f"Workflow object does not contain the field 'steps'.")

    # Show count of errors, then errors
    rootLogger.debug(f"Workflow loading errors: {errors}")
    if errors is not None and len(errors) > 0:
        return None
    else:
        return workflow_object

def load_parameters(contract_type: str, metastore_connection: Metastore):
    """ Loads parameters for 'input' or 'execution' from one of metastore, file path, base64 encoded string or raw parameters. If more than one are set, the first available in this list overrides. If none are set, raises a ValueError."""
    if contract_type not in ['input', 'execution']:
        raise ValueError(f"{contract_type} is not either 'input' or 'execution'")

    parameters_raw = os.environ.get(f"INPUT_{contract_type}_parameters_raw", None)
    parameters_base64 = os.environ.get(f"INPUT_{contract_type}_parameters_base64", None)
    parameters_node_id = os.environ.get(f"INPUT_{contract_type}_parameters_node_id", None)
    parameters_file_path = os.environ.get(f"INPUT_{contract_type}_parameters_file_path", None)

    if parameters_node_id is not None:
        contract_object = metastore_connection.get_object(parameters_node_id)
        return contract_object.dict_without_internal_variables()
    elif parameters_file_path is not None:
        file_path = Path(parameters_file_path)
        if file_path.exists():
            file_contents = file_path.read_text()
            return YAML.safe_load(file_contents)
        else:
            raise ValueError(f"'{str(file_path)}' was provided as an input for the '{contract_type}' parameter of this step, but that file does not exist.")
    elif parameters_base64 is not None:
        base64_decode = base64.urlsafe_b64decode(parameters_base64)
        return YAML.safe_load(base64_decode)
    elif parameters_raw is not None:
        return YAML.safe_load(parameters_raw)
    else:
        raise ValueError(f"No values were set for '{contract_type}'. Was expecting one of INPUT_{contract_type}_parameters_raw, INPUT_{contract_type}_parameters_base64,  INPUT_{contract_type}_parameters_node_id to be available in the environment variables.")

# TODO Break down into verifying contract_type, verify workflow object, and then verify just the MLObject
def load_contract_object(
    parameters: dict, workflow_object: MLObject, step_name: str, contract_type: str
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

    if isinstance(parameters, dict):
        parameters_string = YAML.safe_dump(parameters)
    elif isinstance(parameters, str):
        parameters_string = parameters
    else:
        raise ValueError(f'load_contract_object was called with neither a string nor a dict. Value: {parameters}')

    (contract_object, errors) = MLObject.create_object_from_string(parameters_string)

    if errors is not None and len(errors) > 0:
        rootLogger.debug(f"{contract_type} object loading errors: {errors}")
        raise ValueError(
            f"Error when trying to validate the contract object {step_name}.{contract_type}. Errors: {errors}"
        )

    if step_name not in workflow_object["steps"]:
        raise ValueError(f"Workflow object does not contain the step '{step_name}'.")

    if contract_type not in workflow_object["steps"][step_name]:
        raise ValueError(
            f"Workflow object for step '{step_name}' does not contain a spec for the contract type: '{contract_type}'."
        )

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


def execute_step(
    workflow_object: MLObject,
    input_object: MLObject,
    execution_object: MLObject,
    step_name,
    run_id,
):
    # rootLogger = logging.getLogger()
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
    results_ml_object.step_id = str(uuid.uuid4())
    results_ml_object.run_date = datetime.datetime.now().isoformat()

    # Using the below to validate the object, even though we already have it created.
    load_contract_object(
        parameters=results_ml_object.dict_without_internal_variables(),
        workflow_object=workflow_object,
        step_name=step_name,
        contract_type="output",
    )

    return results_ml_object


if __name__ == "__main__":
    main()
