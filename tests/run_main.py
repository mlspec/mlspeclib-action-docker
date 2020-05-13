import os
import logging
from pathlib import Path
import yaml as YAML
import uuid
import debugpy
from io import StringIO
import sys

sys.path.append(str(Path.cwd().resolve()))
import src  # noqa
from src.main import main  # noqa

RUN_TYPES = ["main", "entrypoint.sh", "container interactive", "container pure"]
RUN_TYPE = RUN_TYPES[3]

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
parameters["INPUT_input_parameters"] = (
    Path(".parameters") / "input" / "datasource.yaml"
).read_text('utf-8')

for param in parameters:
    rootLogger.debug(f"{i}:\t{param}")
    if isinstance(parameters[param], dict):
        env_value = YAML.safe_dump(parameters[param])
    else:
        env_value = parameters[param]

    os.environ[param] = env_value

# marshmallow.class_registry._registry['0_0_1_training_run']

os.environ["GITHUB_RUN_ID"] = str(uuid.uuid4())
os.environ["GITHUB_WORKSPACE"] = str(str(Path.cwd().resolve() / "tests"))
os.environ["VSCODE_DEBUGGING"] = "True"

rootLogger.debug(os.environ)
bar = buffer.getvalue()

if RUN_TYPE == "main":
    main()
elif RUN_TYPE == "entrypoint.sh":
    p = Path.cwd().resolve
    os.system(str(Path.cwd() / "src" / "entrypoint.sh"))
elif RUN_TYPE == "container interactive" or RUN_TYPE == "container pure":
    environment_vars = ""
    os.environ.pop('ENTRYPOINT_OVERRIDE')
    parameters['GITHUB_RUN_ID'] = os.environ["GITHUB_RUN_ID"]
    parameters["GITHUB_WORKSPACE"] = '/src'
    parameters['INPUT_parameters_directory'] = '.parameters'
    parameters['INPUT_schemas_directory'] = 'schemas'
    parameters['INPUT_execution_parameters'] = 'execution/execution_parameters.yaml'

    for param in parameters:
        if param == 'ENTRYPOINT_OVERRIDE':
            continue
        if isinstance(parameters[param], dict):
            env_value = YAML.safe_dump(parameters[param])
        else:
            env_value = parameters[param]
        environment_vars += f' -e "{param}={env_value}"'

    entrypoint_string = ""
    if RUN_TYPE != "container pure":
        entrypoint_string = "--entrypoint /bin/bash"

    exec_statement = f"docker run -it {environment_vars} {entrypoint_string} gcr.io/scorpio-216915/mlspeclibdocker"
    print(exec_statement)
    os.system(exec_statement)
