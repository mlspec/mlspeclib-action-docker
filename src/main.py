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
from marshmallow.class_registry import RegistryError
import base64
import git
from git import GitCommandError
import tempfile
from mlspeclib import MLObject, MLSchema
from mlspeclib.experimental.metastore import Metastore

if Path("src").exists():
    sys.path.append(str(Path("src")))
sys.path.append(str(Path.cwd()))
sys.path.append(str(Path.cwd().parent))

from utils.utils import (  # noqa
    report_found_params,
    raise_schema_mismatch,
    setupLogger,
    KnownException,
)  # noqa
from step_execution import StepExecution  # noqa

REQUIRED = [
    "INPUT_WORKFLOW_NODE_ID",
    "INPUT_STEP_NAME",
    "INPUT_METASTORE_CREDENTIALS",
    "GITHUB_RUN_ID",
    "GITHUB_WORKSPACE",
]

CONTRACT_TYPES = ["input", "execution", "output", "log"]


def main():
    turn_debug_on = os.environ.get("INPUT_ACTIONS_STEP_DEBUG", False)

    rootLogger = setupLogger(turn_debug_on).get_root_logger()

    try:
        sub_main()
    except KnownException as ve:
        rootLogger.critical(str(ve))
        exit(1)
    except KeyError as ke:
        matches = ["url", "key", "database_name", "container_name"]
        if any(x in str(ke) for x in matches):
            rootLogger.critical(str(ke))
            exit(1)
        else:
            raise ke
    except FileNotFoundError as fnfe:
        if "No files ending in" in str(fnfe):
            rootLogger.critical(str(fnfe))
            exit(1)
        else:
            raise fnfe
    except RegistryError as re:
        rootLogger.critical(str(re))
        exit(1)


def sub_main():
    rootLogger = setupLogger().get_root_logger()

    # Loading input values
    msg = "::debug::Loading input values"
    print_left_message("Loading variables from environment...")
    rootLogger.debug(msg)

    parameters = convert_environment_variables_to_dict()

    print("{:>15}".format("ok"))  # Finished loading from environment

    parameters.INPUT_SCHEMAS_DIRECTORY = os.environ.get("INPUT_SCHEMAS_DIRECTORY")

    if "INPUT_SCHEMAS_GIT_URL" in os.environ and os.environ.get != "":
        parameters.INPUT_SCHEMAS_GIT_URL = os.environ.get("INPUT_SCHEMAS_GIT_URL")
        print_left_message(
            f"Downloading schemas from {parameters.INPUT_SCHEMAS_GIT_URL}..."
        )
        try:
            git.Git(parameters.INPUT_SCHEMAS_DIRECTORY).clone(
                parameters.INPUT_SCHEMAS_GIT_URL, str(uuid.uuid4()), depth=1
            )
            # TODO: Authenticate with GH Token?
            print("{:>15}".format("ok"))  # Finished loading from GIT URL
        except GitCommandError as gce:
            raise KnownException(
                f"Trying to read from the git repo ({parameters.INPUT_SCHEMAS_GIT_URL}) and write to the directory ({parameters.INPUT_SCHEMAS_DIRECTORY}). Full error follows: {str(gce)}"
            )

    print_left_message("Appending schemas to registry...")
    MLSchema.append_schema_to_registry(Path(parameters.INPUT_SCHEMAS_DIRECTORY))
    print("{:>15}".format("ok"))  # Finished loading registry

    parameters.previous_step_name = os.environ.get("INPUT_PREVIOUS_STEP_NAME", "")
    parameters.next_step_name = os.environ.get("INPUT_NEXT_STEP_NAME", "")
    rootLogger.debug("::debug:: Finished main")

    # Load metastore credentials

    rootLogger.debug("::debug:: Loading credentials")
    print_left_message("Loading and validating metastore credentials...")
    metastore_cred_string_blob = os.environ.get("INPUT_METASTORE_CREDENTIALS")

    metastore_credentials_packed = YAML.safe_load(metastore_cred_string_blob)
    metastore_credentials_string = base64.urlsafe_b64decode(
        metastore_credentials_packed
    ).decode("utf-8")
    metastore_credentials = YAML.safe_load(metastore_credentials_string)

    report_found_params(
        ["url", "key", "database_name", "container_name"], metastore_credentials
    )
    print("{:>15}".format("ok"))  # Finished loading and validating metastore
    rootLogger.debug("::debug::Starting metastore connection")

    print_left_message("Starting connection to metastore...")
    ms = load_metastore_connection(metastore_credentials_packed)
    print("{:>15}".format("ok"))  # Finished connecting to metastore

    workflow_node_id = os.environ.get("INPUT_WORKFLOW_NODE_ID")
    if workflow_node_id == "":
        raise KnownException(
            "INPUT_WORKFLOW_NODE_ID - No workflow node id was provided."
        )

    print_left_message(f"Loading workflow object ID: '{workflow_node_id}' ...")
    workflow_object = load_workflow_object(workflow_node_id, ms)
    print("{:>15}".format("ok"))  # Finished loading workload abject

    rootLogger.debug("::debug::Loading input parameters")
    print_left_message("Loading input parameters ...")
    input_parameters = load_parameters("INPUT", ms)
    print("{:>15}".format("ok"))  # Finished loading input parameters from metastore

    rootLogger.debug("::debug::Loading execution parameters file")
    print_left_message("Loading execution parameters ...")
    execution_parameters = load_parameters("EXECUTION", ms)
    print(
        "{:>15}".format("ok")
    )  # Finished loading execution  parameters from metastore

    step_name = parameters.INPUT_STEP_NAME
    print_left_message(f"Loading contract for '{step_name}.input' ...")
    input_object = load_contract_object(
        parameters=input_parameters,
        workflow_object=workflow_object,
        step_name=step_name,
        contract_type="input",
    )
    print(
        "{:>15}".format("ok")
    )  # Finished loading execution  parameters from metastore

    print(f"Attaching step info to input for '{step_name}.input' ... ")
    input_node_id = ms.attach_step_info(
        input_object,
        workflow_object.schema_version,
        workflow_node_id,
        step_name,
        "input",
    )
    print(f"     Input Node ID: {input_node_id}")  # Finished attaching step ID to input

    rootLogger.debug(f"Successfully saved: {input_object}")

    # TODO don't hard code any of these
    exec_dict = execution_parameters
    exec_dict["run_id"] = parameters.GITHUB_RUN_ID
    exec_dict["run_date"] = datetime.datetime.now()
    exec_dict["step_id"] = str(uuid.uuid4())

    print_left_message(f"Loading contract for '{step_name}.execution' ...")
    execution_object = load_contract_object(
        parameters=exec_dict,
        workflow_object=workflow_object,
        step_name=step_name,
        contract_type="execution",
    )
    print(
        "{:>15}".format("ok")
    )  # Finished loading execution  parameters from metastore

    rootLogger.debug(f"Successfully loaded and validated execution: {execution_object}")

    print(f"Attaching step info to input for '{step_name}.execution' ... ")
    execution_node_id = ms.attach_step_info(
        execution_object,
        workflow_object.schema_version,
        workflow_node_id,
        step_name,
        "execution",
    )
    rootLogger.debug(f"Successfully saved: {execution_object}")
    print(
        f"      Execution Node ID: {execution_node_id}"
    )  # Finished attaching step ID to input

    # Branching between use step_execution.py or execution file.
    execution_file = os.environ.get("INPUT_EXECUTION_FILE")

    print_left_message("Executing step ... ")
    results_ml_object = execute_step(
        execution_file,
        workflow_object,
        input_object,
        execution_object,
        step_name,
        parameters.GITHUB_RUN_ID,
    )
    print("{:>15}".format("ok"))  # Finished executing step

    # TODO: Need to add next and previous steps to attach_step_info
    print(f"Attaching step info to output for '{step_name}.output' ... ")
    output_node_id = ms.attach_step_info(
        results_ml_object,
        workflow_object.schema_version,
        workflow_node_id,
        step_name,
        "output",
    )
    print(
        f"      Output Node ID: {output_node_id}"
    )  # Finished attaching step ID to output

    dict_conversion = results_ml_object.dict_without_internal_variables()

    string_io_handle = StringIO()
    YAML.SafeDumper.add_representer(uuid.UUID, repr_uuid)
    YAML.safe_dump(dict_conversion, string_io_handle)
    yaml_conversion = string_io_handle.getvalue()

    encode_to_utf8_bytes = yaml_conversion.encode("utf-8")
    base64_encode = base64.urlsafe_b64encode(encode_to_utf8_bytes)
    final_encode_to_utf8 = str(base64_encode, "utf-8")

    # Recording raw log info
    # logBuffer.flush()
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

    rootLogger.debug(
        f"::set-output name=output_raw::{results_ml_object.dict_without_internal_variables()}"
    )

    print("Printing output ... \n \n")
    logger = setupLogger()
    output_message = ""
    output_message += f"{logger.print_and_log('output_raw', results_ml_object.dict_without_internal_variables())}\n"
    output_message += (
        f"{logger.print_and_log('output_base64_encoded', final_encode_to_utf8)}\n"
    )
    output_message += f"{logger.print_and_log('input_node_id', input_node_id)}\n"
    output_message += (
        f"{logger.print_and_log('execution_node_id', execution_node_id)}\n"
    )
    output_message += f"{logger.print_and_log('output_node_id', output_node_id)}\n"
    output_message += f"{logger.print_and_log('log_node_id', log_node_id)}\n"

    rootLogger.debug(f"Complete output: \n {output_message}")
    print("\n\n... finished printing output")  # Finished printing output

    print_left_message("Generating /output_message.txt ...")
    if is_docker():
        Path("/output_message.txt").write_text(output_message)
    else:
        fp = tempfile.TemporaryFile()
        fp.write(output_message.encode("utf-8"))
    print("{:>15}".format("ok"))  # Finished printing output


def is_docker():
    cgroup_path = "/proc/self/cgroup"
    return (
        os.path.exists("/.dockerenv")
        or os.path.isfile(cgroup_path)
        and any("docker" in line for line in open(cgroup_path))
    )


def print_left_message(msg):
    print(msg.ljust(120), end="")


def repr_uuid(dumper, uuid_obj):
    return YAML.ScalarNode("tag:yaml.org,2002:str", str(uuid_obj))


def convert_environment_variables_to_dict():
    return_dict = Box()

    for var in REQUIRED:
        return_dict[var] = os.environ.get(var, "")
        if return_dict[var] == "":
            raise KnownException(f"No value provided for {var}.")

    return return_dict


def verify_parameters_folder_and_file_exist(
    workspace_directory, parameters_directory, parameters_file
):
    full_params_dir = Path(workspace_directory) / parameters_directory

    if not full_params_dir.exists():
        raise KnownException(
            f"{parameters_directory} was not found in the workspace directory {workspace_directory}"
        )

    full_file_location = full_params_dir / parameters_file

    if not full_file_location.exists():
        raise KnownException(f"{full_file_location.resolve()} was not found.")

    return True


def load_metastore_connection(credentials_packed: str):
    return Metastore(credentials_packed=credentials_packed)


def load_workflow_object(
    workflow_node_id: str, metastore_connection: Metastore
) -> MLObject:
    rootLogger = setupLogger().get_root_logger()
    (workflow_object, errors) = metastore_connection.get_workflow_object(
        workflow_node_id
    )

    if workflow_object is None:
        raise KnownException(
            f"No workflow loaded when attempting to load workflow node id: {workflow_node_id}"
        )

    if "steps" not in workflow_object:
        raise KnownException("Workflow object does not contain the field 'steps'.")

    # Show count of errors, then errors
    rootLogger.debug(f"Workflow loading errors: {errors}")
    if errors is not None and len(errors) > 0:
        return None
    else:
        return workflow_object


def load_parameters(contract_type: str, metastore_connection: Metastore):
    """ Loads parameters for 'INPUT' or 'EXECUTION' from one of metastore, file path, base64 encoded string or raw parameters. If more than one are set, the first available in this list overrides. If none are set, raises a KnownException."""
    if contract_type not in ["INPUT", "EXECUTION"]:
        raise KnownException(f"{contract_type} is not either 'INPUT' or 'EXECUTION'")

    parameters_raw = os.environ.get(f"INPUT_{contract_type}_PARAMETERS_RAW", "")
    parameters_base64 = os.environ.get(f"INPUT_{contract_type}_PARAMETERS_BASE64", "")
    parameters_node_id = os.environ.get(f"INPUT_{contract_type}_PARAMETERS_NODE_ID", "")
    parameters_file_path = os.environ.get(
        f"INPUT_{contract_type}_PARAMETERS_FILE_PATH", ""
    )

    if parameters_node_id != "":
        contract_object = metastore_connection.get_object(parameters_node_id)
        return contract_object.dict_without_internal_variables()
    elif parameters_file_path != "":
        file_path = Path(parameters_file_path)
        if file_path.exists():
            file_contents = file_path.read_text()
            return YAML.safe_load(file_contents)
        else:
            raise KnownException(
                f"'{str(file_path)}' was provided as an input for the '{contract_type}' parameter of this step, but that file does not exist."
            )
    elif parameters_base64 != "":
        base64_decode = base64.urlsafe_b64decode(parameters_base64)
        return YAML.safe_load(base64_decode)
    elif parameters_raw != "":
        return YAML.safe_load(parameters_raw)
    else:
        raise KnownException(
            f"No values were set for '{contract_type}'. Was expecting one of INPUT_{contract_type}_PARAMETERS_RAW, INPUT_{contract_type}_PARAMETERS_FILE_PATH, INPUT_{contract_type}_PARAMETERS_BASE64,  INPUT_{contract_type}_PARAMETERS_NODE_ID to be available in the environment variables."
        )


# TODO Break down into verifying contract_type, verify workflow object, and then verify just the MLObject
def load_contract_object(
    parameters: dict, workflow_object: MLObject, step_name: str, contract_type: str
):
    """ Creates an MLObject based on an input string, and validates it against the workflow object
    and step_name provided.

    Will fail if the .validate() fails on the object or the schema mismatches what is seen in the
    workflow.
    """
    rootLogger = setupLogger().get_root_logger()

    if contract_type not in CONTRACT_TYPES:
        raise KnownException(
            f"{contract_type} not in the expected list of contract types: {CONTRACT_TYPES}."
        )

    if isinstance(parameters, dict):
        parameters_string = YAML.safe_dump(parameters)
    elif isinstance(parameters, str):
        parameters_string = parameters
    else:
        raise KnownException(
            f"load_contract_object was called with neither a string nor a dict. Value: {parameters}"
        )

    (contract_object, errors) = MLObject.create_object_from_string(parameters_string)

    if errors is not None and len(errors) > 0:
        rootLogger.debug(f"{contract_type} object loading errors: {errors}")
        raise KnownException(
            f"Error when trying to validate the contract object {step_name}.{contract_type}. Errors: {errors}"
        )

    if step_name not in workflow_object["steps"]:
        raise KnownException(
            f"Workflow object does not contain the step '{step_name}'."
        )

    if contract_type not in workflow_object["steps"][step_name]:
        raise KnownException(
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
    execution_file: str,
    workflow_object: MLObject,
    input_object: MLObject,
    execution_object: MLObject,
    step_name,
    run_id,
):

    rootLogger = setupLogger().get_root_logger()

    results_ml_object = MLObject()

    if execution_file is None:
        msg = "Did not find any value for INPUT_EXECUTION_FILE, using /src/step_execution.py"

        print_left_message(msg)
        rootLogger.debug("::debug::" + msg)

        print("{:>15}".format("ok"))  # Finished loading from environment

        step_execution_object = StepExecution(input_object, execution_object)
        results_ml_object = step_execution_object.execute(
            result_object_schema_type=workflow_object.steps[
                step_name
            ].output.schema_type,
            result_object_schema_version=workflow_object.steps[
                step_name
            ].output.schema_version,
        )

    else:
        # TODO: Critical error if variable set but file not found
        msg = f"Executing '${execution_file}' (found in INPUT_EXECUTION_FILE env var)"

        print_left_message(msg)
        rootLogger.debug("::debug::" + msg)

        execution_file_path = Path(execution_file)

        if execution_file_path.exists() is False:
            raise KnownException(
                f"'{execution_file}' was provided as the file, but it does not appear to exist at {str(execution_file_path.resolve())} -- exiting."
            )

        # The below are used in the execution file
        result_ml_object_schema_type = workflow_object.steps[  # noqa
            step_name
        ].output.schema_type
        result_ml_object_schema_version = workflow_object.steps[  # noqa
            step_name
        ].output.schema_version
        exec(execution_file_path.read_text(), globals(), locals())

        print("{:>15}".format("ok"))  # Finished executing step

    if results_ml_object is None:
        raise KnownException(
            "No value was assigned to the variable 'results_ml_object' -- exiting."
        )
    elif isinstance(results_ml_object, MLObject) is False:
        raise KnownException(
            "The variable 'results_ml_object' was not of type MLObject -- exiting."
        )

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
