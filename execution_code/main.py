import os
import json
import yaml as YAML
import logging
from box import Box
from pathlib import Path
import pdb
import debugpy
import sys
from io import StringIO
import datetime
import uuid

from mlspeclib import MLObject, MLSchema
from mlspeclib.experimental.metastore import Metastore

sys.path.append(str(Path.cwd()))
logger = logging.getLogger()
logger.debug(sys.path)

from execution_code.utils import (
    ConfigurationException,
    report_found_params,
    raise_schema_mismatch,
) # noqa
from execution_code.step_execution import step_execution # noqa

def main():

    # debugpy.listen(5678)
    # print("Waiting for debugger attach")
    # debugpy.wait_for_client()
    # debugpy.breakpoint()
    # print('break on this line')

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

    # Loading input values
    rootLogger.debug("::debug::Loading input values")

    required_variables = [
        "INPUT_schemas_directory",
        "INPUT_workflow_id",
        "INPUT_step_name",
        "INPUT_input_parameters",
        "INPUT_execution_parameters",
        "INPUT_METASTORE_CREDENTIALS",
        "GITHUB_RUN_ID",
        "GITHUB_WORKSPACE",
    ]

    parameters = Box()

    for var in required_variables:
        parameters[var] = os.environ.get(var, default=None)
        if parameters[var] is None:
            raise ConfigurationException(f"No value provided for {var}.")

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

    rootLogger.debug(f"Workflow loading errors: {errors}")

    step_name = parameters.INPUT_step_name

    (input_object, errors) = MLObject.create_object_from_string(
        parameters.INPUT_input_parameters
    )
    rootLogger.debug(f"Input object loading errors: {errors}")

    input_object.validate()

    # pdb.set_trace()

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

    exec_dict = YAML.safe_load(execution_parameters)
    exec_dict["run_id"] = parameters.GITHUB_RUN_ID
    exec_dict["run_date"] = datetime.datetime.now()
    exec_dict["step_id"] = str(uuid.uuid4())

    (execution_object, errors) = MLObject.create_object_from_string(
        YAML.safe_dump(exec_dict)
    )
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
    bufferHandler.flush()
    buffer.flush()
    log_contents = buffer.getvalue()

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


# for i in range(10):
#     (datasource_object, _) = MLObject.create_object_from_file(
#         Path(__file__).parent.parent
#         / "tests"
#         / "data"
#         / "0"
#         / "0"
#         / "1"
#         / "datasource.yaml"
#     )
#     datasource_object.run_date = datetime.now()
#     datasource_object.run_id = uuid.uuid4()
#     # ms.save(datasource_object, '0.0.1', 'process_data', 'input')

# all_items = ms.get_all_runs("0.0.1", "process_data")
# first_item_id = next(iter(all_items))
# first_item = all_items[first_item_id]

# this_item = ms.load("0.0.1", "process_data", first_item["id"])

# (rehydrated_object, _) = ms.get_ml_object("0.0.1", "process_data", first_item["id"])

# print(rehydrated_object.website)

# # Loading azure credentials
# print("::debug::Loading azure credentials")
# azure_credentials = os.environ.get("INPUT_AZURE_CREDENTIALS", default="{}")
# try:
#     azure_credentials = json.loads(azure_credentials)
# except JSONDecodeError:
#     print("::error::Please paste output of `az ad sp create-for-rbac --name <your-sp-name> --role contributor --scopes /subscriptions/<your-subscriptionId>/resourceGroups/<your-rg> --sdk-auth` as value of secret variable: AZURE_CREDENTIALS")
#     raise AMLConfigurationException(f"Incorrect or poorly formed output from azure credentials saved in AZURE_CREDENTIALS secret. See setup in https://github.com/Azure/aml-workspace/blob/master/README.md")

# # Checking provided parameters
# print("::debug::Checking provided parameters")
# validate_json(
#     data=azure_credentials,
#     schema=azure_credentials_schema,
#     input_name="AZURE_CREDENTIALS"
# )

# # Mask values
# print("::debug::Masking parameters")
# mask_parameter(parameter=azure_credentials.get("tenantId", ""))
# mask_parameter(parameter=azure_credentials.get("clientId", ""))
# mask_parameter(parameter=azure_credentials.get("clientSecret", ""))
# mask_parameter(parameter=azure_credentials.get("subscriptionId", ""))

# # Loading parameters file
# print("::debug::Loading parameters file")
# parameters_file = os.environ.get("INPUT_PARAMETERS_FILE", default="registermodel.json")
# parameters_file_path = os.path.join(".cloud", ".azure", parameters_file)
# try:
#     with open(parameters_file_path) as f:
#         parameters = json.load(f)
# except FileNotFoundError:
#     print(f"::debug::Could not find parameter file in {parameters_file_path}. Please provide a parameter file in your repository if you do not want to use default settings (e.g. .cloud/.azure/registermodel.json).")
#     parameters = {}

# # Checking provided parameters
# print("::debug::Checking provided parameters")
# validate_json(
#     data=parameters,
#     schema=parameters_schema,
#     input_name="PARAMETERS_FILE"
# )

# # Loading Workspace
# print("::debug::Loading AML Workspace")
# config_file_path = os.environ.get("GITHUB_WORKSPACE", default=".cloud/.azure")
# config_file_name = "aml_arm_config.json"
# sp_auth = ServicePrincipalAuthentication(
#     tenant_id=azure_credentials.get("tenantId", ""),
#     service_principal_id=azure_credentials.get("clientId", ""),
#     service_principal_password=azure_credentials.get("clientSecret", "")
# )
# try:
#     ws = Workspace.from_config(
#         path=config_file_path,
#         _file_name=config_file_name,
#         auth=sp_auth
#     )
# except AuthenticationException as exception:
#     print(f"::error::Could not retrieve user token. Please paste output of `az ad sp create-for-rbac --name <your-sp-name> --role contributor --scopes /subscriptions/<your-subscriptionId>/resourceGroups/<your-rg> --sdk-auth` as value of secret variable: AZURE_CREDENTIALS: {exception}")
#     raise AuthenticationException
# except AuthenticationError as exception:
#     print(f"::error::Microsoft REST Authentication Error: {exception}")
#     raise AuthenticationError
# except AdalError as exception:
#     print(f"::error::Active Directory Authentication Library Error: {exception}")
#     raise AdalError
# except ProjectSystemException as exception:
#     print(f"::error::Workspace authorization failed: {exception}")
#     raise ProjectSystemException

# # Loading experiment
# print("::debug::Loading experiment")
# try:
#     experiment = Experiment(
#         workspace=ws,
#         name=experiment_name
#     )
# except UserErrorException as exception:
#     print(f"::error::Loading experiment failed: {exception}")
#     raise AMLConfigurationException(f"Could not load experiment. Please your experiment name as input parameter.")

# # Loading run by run id
# print("::debug::Loading run by run id")
# try:
#     run = Run(
#         experiment=experiment,
#         run_id=run_id
#     )
# except KeyError as exception:
#     print(f"::error::Loading run failed: {exception}")
#     raise AMLConfigurationException(f"Could not load run. Please your run id as input parameter.")

# # Loading best run
# print("::debug::Loading best run")
# best_run = get_best_run(
#     experiment=experiment,
#     run=run,
#     pipeline_child_run_name=parameters.get("pipeline_child_run_name", "model_training")
# )

# # Comparing metrics of runs
# print("::debug::Comparing metrics of runs")
# # Default model name
# repository_name = os.environ.get("GITHUB_REPOSITORY").split("/")[-1]
# branch_name = os.environ.get("GITHUB_REF").split("/")[-1]
# default_model_name = f"{repository_name}-{branch_name}"
# if not parameters.get("force_registration", False):
#     compare_metrics(
#         workspace=ws,
#         run=best_run,
#         model_name=parameters.get("model_name", default_model_name)[:32],
#         metrics_max=parameters.get("metrics_max", []),
#         metrics_min=parameters.get("metrics_min", [])
#     )

# # Defining model framework
# print("::debug::Defining model framework")
# model_framework = get_model_framework(
#     name=parameters.get("model_framework", None)
# )

# # Defining model path
# print("::debug::Defining model path")
# model_file_name = parameters.get("model_file_name", "model.pkl")
# model_file_name = os.path.split(model_file_name)[-1]
# model_path = [file_name for file_name in best_run.get_file_names() if model_file_name in os.path.split(file_name)[-1]][0]

# # Defining datasets
# print("::debug::Defining datasets")
# datasets = []
# for dataset_name in parameters.get("datasets", []):
#     dataset = get_dataset(
#         workspace=ws,
#         name=dataset_name
#     )
#     if dataset is not None:
#         datasets.append((f"{dataset_name}", dataset))
# input_dataset = get_dataset(
#     workspace=ws,
#     name=parameters.get("sample_input_dataset", None)
# )
# output_dataset = get_dataset(
#     workspace=ws,
#     name=parameters.get("sample_output_dataset", None)
# )

# # Defining resource configuration
# print("::debug::Defining resource configuration")
# cpu = parameters.get("cpu_cores", None)
# memory = parameters.get("memory_gb", None)
# resource_configuration = ResourceConfiguration(cpu=cpu, memory_in_gb=memory) if (cpu is not None and memory is not None) else None

# try:
#     model = best_run.register_model(
#         model_name=parameters.get("model_name", default_model_name)[:32],
#         model_path=model_path,
#         tags=parameters.get("model_tags", None),
#         properties=parameters.get("model_properties", None),
#         model_framework=model_framework,
#         model_framework_version=parameters.get("model_framework_version", None),
#         description=parameters.get("model_description", None),
#         datasets=datasets,
#         sample_input_dataset=input_dataset,
#         sample_output_dataset=output_dataset,
#         resource_configuration=resource_configuration
#     )
# except ModelPathNotFoundException as exception:
#     print(f"::error::Model name not found in outputs folder. Please provide the correct model file name and make sure that the model was saved by the run: {exception}")
#     raise AMLConfigurationException(f"Model name not found in outputs folder. Please provide the correct model file name and make sure that the model was saved by the run.")
# except WebserviceException as exception:
#     print(f"::error::Model could not be registered: {exception}")
#     raise AMLConfigurationException("Model could not be registered")

# # Create outputs
# print("::debug::Creating outputs")
# print(f"::set-output name=model_name::{model.name}")
# print(f"::set-output name=model_version::{model.version}")
# print(f"::set-output name=model_id::{model.id}")
# print("::debug::Successfully completed Azure Machine Learning Register Model Action")


if __name__ == "__main__":
    main()
