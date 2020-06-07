from mlspeclib import MLSchema, MLObject
from random import randint, random

results_ml_object = MLObject()

results_ml_object.set_type(
    schema_type=result_ml_object_schema_type,
    schema_version=result_ml_object_schema_version,
)

# Execute code below. Examples:

"""
1) Execute on the worker node this step is running on

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn import metrics

dataset = pd.read_csv('Weather.csv') # Example dataset

X = dataset['MinTemp'].values.reshape(-1,1)
y = dataset['MaxTemp'].values.reshape(-1,1)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
regressor = LinearRegression()
regressor.fit(X_train, y_train) # Training the algorithm

y_pred = regressor.predict(X_test)
print('Mean Absolute Error:', metrics.mean_absolute_error(y_test, y_pred))
print('Mean Squared Error:', metrics.mean_squared_error(y_test, y_pred))
print('Root Mean Squared Error:', np.sqrt(metrics.mean_squared_error(y_test, y_pred)))

"""

"""
2) Execute a long running job on an external service

from pyspark import SparkContext
sc = SparkContext("local", "Log Processing App")
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
"""

"""
3) Execute a job on a hosted service

from azureml.core import Workspace, Experiment
from azureml.core.authentication import ServicePrincipalAuthentication

azure_credentials = os.environ.get("INPUT_AZURE_CREDENTIALS")
sp_auth = ServicePrincipalAuthentication(
    tenant_id=azure_credentials.get("tenantId"),
    service_principal_id=azure_credentials.get("clientId"),
    service_principal_password=azure_credentials.get("clientSecret",)
)
ws = Workspace.from_config(
    path=config_file_path,
    _file_name=config_file_name,
    auth=sp_auth
)

experiment = Experiment(workspace=ws, "this_experiment_name")
run_config = load_runconfig_yaml(runconfig_yaml_file="run_config.yml")
run = experiment.submit(config=run_config, tags=parameters.get("tags", {}))
"""


# Execution metrics
results_ml_object.execution_profile.system_memory_utilization = random()
results_ml_object.execution_profile.network_traffic_in_bytes = randint(7e9, 9e10)
results_ml_object.execution_profile.gpu_temperature = randint(70, 130)
results_ml_object.execution_profile.disk_io_utilization = random()
results_ml_object.execution_profile.gpu_percent_of_time_accessing_memory = random()
results_ml_object.execution_profile.cpu_utilization = random()
results_ml_object.execution_profile.gpu_utilization = random()
results_ml_object.execution_profile.gpu_memory_allocation = random()
