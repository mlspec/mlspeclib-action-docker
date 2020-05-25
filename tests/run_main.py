import os
import logging
from pathlib import Path
import yaml as YAML
import uuid
import debugpy
from io import StringIO
import sys
import datetime
from mlspeclib.experimental.metastore import Metastore
from mlspeclib import MLObject, MLSchema
import random
import base64
import tempfile
import shutil

sys.path.append(str(Path.cwd().resolve()))
import src  # noqa
from src.main import main  # noqa

RUN_TYPES = ["main", "entrypoint.sh", "container interactive", "container pure"]
RUN_TYPE = RUN_TYPES[os.environ.get('RUN_TYPE', 0)]

rootLogger = logging.getLogger()
rootLogger.setLevel(logging.DEBUG)

buffer = StringIO()
bufferHandler = logging.StreamHandler(buffer)
bufferHandler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s\n")
bufferHandler.setFormatter(formatter)
rootLogger.addHandler(bufferHandler)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stdout_handler.setFormatter(formatter)
rootLogger.addHandler(stdout_handler)

for i in os.environ:
    rootLogger.debug(f"{i}:\t{os.environ.get(i)}")

parameters = {}
parameters = YAML.safe_load((Path("tests") / "env_variables.yaml").read_text('utf-8'))

for param in parameters:
    rootLogger.debug(f"{i}:\t{param}")
    if isinstance(parameters[param], dict):
        env_value = YAML.safe_dump(parameters[param])
    else:
        env_value = parameters[param]

    os.environ[param] = env_value

os.environ["GITHUB_RUN_ID"] = str(uuid.uuid4())
os.environ["GITHUB_WORKSPACE"] = str(str(Path.cwd().resolve()))
os.environ["VSCODE_DEBUGGING"] = "True"

credentials_packed = parameters['INPUT_METASTORE_CREDENTIALS']

ms = Metastore(credentials_packed)

workflow_dict = parameters['INPUT_workflow']
workflow_dict["workflow_version"] = str('999999999999.9.' + str(random.randint(0, 9999)))
workflow_dict["run_id"] = str(uuid.uuid4())
workflow_dict["step_id"] = str(uuid.uuid4())
workflow_dict["run_date"] = datetime.datetime.now()

MLSchema.append_schema_to_registry(Path('tests/schemas'))

(workflow_object, err) = MLObject.create_object_from_string(workflow_dict)
if len(err) != 0:
    raise ValueError(f"Error creating mock workflow_object. Errors: {err}")

parameters['INPUT_workflow_node_id'] = ms.create_workflow_node(workflow_object, str(uuid.uuid4()))

parameters["GITHUB_RUN_ID"] = workflow_dict["run_id"]
parameters["GITHUB_WORKSPACE"] = '/src'

rootLogger.debug(os.environ)
bar = buffer.getvalue()

environment_vars = ""

for param in parameters:
    if param == 'ENTRYPOINT_OVERRIDE':
        continue

    if isinstance(parameters[param], dict):
        env_value = YAML.safe_dump(parameters[param])
    else:
        env_value = parameters[param]
    environment_vars += f' -e "{param}={env_value}"'

    os.environ[param] = env_value

if RUN_TYPE == "main":
    main()
elif RUN_TYPE == "entrypoint.sh":
    p = Path.cwd().resolve
    os.system(str(Path.cwd() / "src" / "entrypoint.sh"))
elif RUN_TYPE == "container interactive" or RUN_TYPE == "container pure":
    entrypoint_string = ""
    if RUN_TYPE != "container pure":
        entrypoint_string = "--entrypoint /bin/bash"

    exec_statement = f"docker run -it {environment_vars} {entrypoint_string} mlspeclib-action-sample-process-data"
#    print(exec_statement)
    os.system(exec_statement)

p = Path(parameters['INPUT_schemas_directory'])
for x in p.iterdir():
    if x.is_dir():
        shutil.rmtree(str(x.resolve()), ignore_errors=True)