from mlspeclib import MLSchema, MLObject
from random import randint, random
from pathlib import Path

results_ml_object.set_type(
    schema_type=result_ml_object_schema_type,  # noqa
    schema_version=result_ml_object_schema_version,  # noqa
)

return_dict = {
    "data_output_path": str(Path("tests/data/data_output.csv").absolute()),
    "data_statistics_path": str(Path("tests/data/data_stats.csv").absolute()),
    "data_schemas_path": str(Path("tests/data/data_schemas.yaml").absolute()),
    "feature_file_path": str(Path("tests/data/feature_file.yaml").absolute()),
}

results_ml_object.data_output_path = return_dict["data_output_path"]
results_ml_object.data_statistics_path = return_dict["data_statistics_path"]
results_ml_object.data_schemas_path = return_dict["data_schemas_path"]
results_ml_object.feature_file_path = return_dict["feature_file_path"]
