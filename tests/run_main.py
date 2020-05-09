import os
import logging
from pathlib import Path
import yaml as YAML
import uuid

logging.basicConfig(level=logging.DEBUG)

for i in os.environ:
    logging.debug(f"{i}:\t{os.environ.get(i)}")

parameters = {}
parameters = YAML.safe_load((Path("tests") / "env_variables.yaml").read_text())
parameters["INPUT_input_parameters"] = (
    Path("tests") / "schemas" / "datasource.yaml"
).read_text()

for param in parameters:
    logging.debug(f"{i}:\t{param}")
    if isinstance(parameters[param], dict):
        env_value = YAML.safe_dump(parameters[param])
    else:
        env_value = parameters[param]

    os.environ[param] = env_value

# marshmallow.class_registry._registry['0_0_1_training_run']

os.environ["GITHUB_RUN_ID"] = str(uuid.uuid4())
os.environ["GITHUB_WORKSPACE"] = str(str(Path.cwd().resolve() / "tests"))

print(os.system("code/entrypoint.sh"))
