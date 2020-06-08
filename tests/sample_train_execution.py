from mlspeclib import MLSchema, MLObject
from random import randint, random, randrange
from pathlib import Path
import uuid

results_ml_object.set_type(
    schema_type=result_ml_object_schema_type,  # noqa
    schema_version=result_ml_object_schema_version,  # noqa
)

# Mocked up results
return_dict = {
    "training_execution_id": uuid.uuid4(),
    "accuracy": float(f"{randrange(93000,99999)/100000}"),
    "global_step": int(f"{randrange(50,150) * 100}"),
    "loss": float(f"{randrange(10000,99999)/1000000}"),
}

results_ml_object.training_execution_id = return_dict["training_execution_id"]
results_ml_object.accuracy = return_dict["accuracy"]
results_ml_object.global_step = return_dict["global_step"]
results_ml_object.loss = return_dict["loss"]

# Execution metrics
results_ml_object.execution_profile.system_memory_utilization = random()
results_ml_object.execution_profile.network_traffic_in_bytes = randint(7e9, 9e10)
results_ml_object.execution_profile.gpu_temperature = randint(70, 130)
results_ml_object.execution_profile.disk_io_utilization = random()
results_ml_object.execution_profile.gpu_percent_of_time_accessing_memory = random()
results_ml_object.execution_profile.cpu_utilization = random()
results_ml_object.execution_profile.gpu_utilization = random()
results_ml_object.execution_profile.gpu_memory_allocation = random()

