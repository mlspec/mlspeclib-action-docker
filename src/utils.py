import mlspeclib
import logging
from io import StringIO
import sys
from pathlib import Path

if Path("src").exists():
    sys.path.append(str(Path("src")))
sys.path.append(str(Path.cwd()))
sys.path.append(str(Path.cwd().parent))

class KnownException(Exception):
    pass

def report_found_params(expected_params: list, offered_params: dict) -> None:
    (rootLogger, _) = setupLogger()
    for param in expected_params:
        if param not in offered_params or offered_params[param] is None:
            raise KnownException(f"No parameter set for {param}.")
        else:
            rootLogger.debug(f"Found value for {param}.")


def raise_schema_mismatch(
    expected_type: str, expected_version: str, actual_type: str, actual_version: str
):
    raise KnownException(
        f"""Actual data does not match the expected schema and version:
    Expected Type: {expected_type}
    Actual Type: {actual_type}

    Expected Version: {expected_version}
    Actual Version: {actual_version}")"""
    )


def setupLogger():
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "::%(levelname)s - %(message)s"
    )

    if 'buffer.logger' not in rootLogger.handlers:
        buffer = StringIO()
        bufferHandler = logging.StreamHandler(buffer)
        bufferHandler.setLevel(logging.DEBUG)
        bufferHandler.setFormatter(formatter)
        bufferHandler.set_name('buffer.logger')
        rootLogger.addHandler(bufferHandler)

    if 'stdout.logger' not in rootLogger.handlers:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.setFormatter(formatter)
        bufferHandler.set_name('stdout.logger')
        rootLogger.addHandler(stdout_handler)

    return (rootLogger, buffer)


# def get_best_run(experiment, run, pipeline_child_run_name=None):
#     # Handle pipeline run
#     print("::debug::Handling pipeline run")
#     if run.type == "azureml.PipelineRun":
#         # Loading pipeline run
#         run = PipelineRun(
#             experiment=experiment,
#             run_id=run.id
#         )

#         # Loading pipeline step by name
#         run = run.find_step_run(name=pipeline_child_run_name)
#         if len(run) < 1:
#             print(f"::error::Found no step in the pipeline with the name '{pipeline_child_run_name}'. Please provide the name of the step in your pipeline that produced the model file with the 'pipeline_child_run_name' parameter.")
#             raise AMLConfigurationException(f"Foundno step in the pipeline with the name '{pipeline_child_run_name}'. Please provide the name of the step in your pipeline that produced the model file with the 'pipeline_child_run_name' parameter.")
#         elif len(run) > 1:
#             print(f"::error::Found more than one step in the pipeline with the name '{pipeline_child_run_name}'. All step names should be unique in the pipeline.")
#             raise AMLConfigurationException(f"Found more than one step in the pipeline with the name '{pipeline_child_run_name}'. All step names should be unique in the pipeline.")
#         else:
#             run = run[0]

#         # Checking if run has childs and therefore is a hyperparameter run
#         if len(list(run.get_children())) > 0:
#             run = list(run.get_children())[0]

#     # Handle hyperdrive run
#     print("::debug::Handling hyperdrive run")
#     if run.type == "hyperdrive":
#         run = HyperDriveRun(
#             experiment=experiment,
#             run_id=run.id
#         )
#         best_run = run.get_best_run_by_primary_metric()
#     else:
#         best_run = run
#     return best_run


# def get_dataset(workspace, name):
#     try:
#         dataset = Dataset.get_by_name(
#             workspace=workspace,
#             name=name,
#             version="latest"
#         )
#     except Exception:
#         dataset = None
#     return dataset


# def get_model_framework(name):
#     if name is not None and name.lower() == "scikitlearn":
#         model_framework = Model.Framework.SCIKITLEARN
#     elif name is not None and name.lower() == "onnx":
#         model_framework = Model.Framework.ONNX
#     elif name is not None and name.lower() == "tensorflow":
#         model_framework = Model.Framework.TENSORFLOW
#     elif name is not None and name.lower() == "keras":
#         model_framework = Model.Framework.TFKERAS
#     else:
#         model_framework = Model.Framework.CUSTOM
#     return model_framework


# def compare_metrics(workspace, run, model_name, metrics_max, metrics_min):
#     # Loading production model
#     print("::debug::Loading production model")
#     try:
#         production_model = Model(
#             workspace=workspace,
#             name=model_name
#         )
#     except WebserviceException as exception:
#         print(f"::debug::Model with same name not found. Assuming that it is the first model that is registered: {exception}")
#         return

#     # Loading run of production model
#     print("::debug::Loading run of production model")
#     production_model_run = production_model.run
#     if production_model_run is None:
#         print("::debug::Previous model was not registered from run object")
#         return

#     # Loading metrics of runs
#     production_model_metrics = production_model_run.get_metrics()
#     run_metrics = run.get_metrics()

#     # Comparing metrics to maximize
#     print("::debug::Comparing metrics to maximize")
#     for metric_max in metrics_max:
#         try:
#             if run_metrics.get(metric_max) < production_model_metrics.get(metric_max):
#                 print(f"::error::New model does not perform better than production model for metric '{metric_max}'")
#                 raise AMLModelPerformanceException(f"New model does not perform better than production model for metric '{metric_max}'")
#         except TypeError as exception:
#             print(f"::error::Metric comparison failed for metric name '{metric_max}': {exception}")
#             raise AMLConfigurationException(f"::error::Metric comparison failed for metric name '{metric_max}'")

#     # Comparing metrics to minimize
#     print("::debug::Comparing metrics to minimize")
#     for metric_min in metrics_min:
#         try:
#             if run_metrics.get(metric_min) < production_model_metrics.get(metric_min):
#                 print(f"::error::New model does not perform better than production model for metric '{metric_min}'")
#                 raise AMLModelPerformanceException(f"New model does not perform better than production model for metric '{metric_min}'")
#         except TypeError as exception:
#             print(f"::error::Metric comparison failed for metric name '{metric_min}': {exception}")
#             raise AMLConfigurationException(f"::error::Metric comparison failed for metric name '{metric_min}'")


# def mask_parameter(parameter):
#     print(f"::add-mask::{parameter}")


# def validate_json(data, schema, input_name):
#     validator = jsonschema.Draft7Validator(schema)
#     errors = list(validator.iter_errors(data))
#     if len(errors) > 0:
#         for error in errors:
#             print(f"::error::JSON validation error: {error}")
#         raise AMLConfigurationException(f"JSON validation error for '{input_name}'. Provided object does not match schema. Please check the output for more details.")
#     else:
#         print(f"::debug::JSON validation passed for '{input_name}'. Provided object does match schema.")
