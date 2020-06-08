from mlspeclib import MLSchema, MLObject
from random import randint, random, randrange
from pathlib import Path
import uuid

results_ml_object.set_type(
    schema_type=result_ml_object_schema_type, schema_version=result_ml_object_schema_version,
)

# Mocked up results
return_dict = YAML.safe_load(
    """
servable: True
package_size: 1029310298
tested_platforms: ['kubeflow', 'azureml', 'sagemaker']
model_source:
    servable_model:
        data_store: 's3'
        bucket: 'nlp-bucket'
        path: 'a231653454ca8e07f42adc7941aeec6b'
serving_container_image:
    container_image_url: 'https://hub.docker.com/repository/docker/contoso/nlp-base-images'
"""
)

results_ml_object.servable = return_dict["servable"]
results_ml_object.tested_platforms = return_dict["tested_platforms"]
results_ml_object.package_size = return_dict["package_size"]
results_ml_object.model_source.servable_model.data_store = return_dict["model_source"][
    "servable_model"
]["data_store"]
results_ml_object.model_source.servable_model.bucket = return_dict["model_source"][
    "servable_model"
]["bucket"]
results_ml_object.model_source.servable_model.path = return_dict["model_source"][
    "servable_model"
]["path"]
results_ml_object.serving_container_image.container_image_url = return_dict[
    "serving_container_image"
]["container_image_url"]
