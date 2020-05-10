import os
import logging
from pathlib import Path
import yaml as YAML
import uuid
import debugpy
from io import StringIO
import sys
sys.path.append(str(Path.cwd().resolve()))
import execution_code
from execution_code.main import main

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
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_handler.setFormatter(formatter)
rootLogger.addHandler(stdout_handler)


for i in os.environ:
    rootLogger.debug(f"{i}:\t{os.environ.get(i)}")

parameters = {}
parameters = YAML.safe_load((Path("tests") / "env_variables.yaml").read_text())
parameters["INPUT_input_parameters"] = (
    Path("tests") / "schemas" / "datasource.yaml"
).read_text()

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

#main()
# p = Path.cwd().resolve
os.system(str(Path.cwd() / 'execution_code' / 'entrypoint.sh'))

