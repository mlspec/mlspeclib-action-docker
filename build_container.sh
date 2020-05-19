#!/bin/bash
pipenv update mlspeclib
pipenv lock -r > requirements.txt

python3 -m unittest tests/tests_*
RUN_TYPE=0
python3 tests/run_main.py

docker build --no-cache -t mlspec/mlspeclib-action-docker .

RUN_TYPE=3
python3 tests/run_main.py
