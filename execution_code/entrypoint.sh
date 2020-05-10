#!/bin/sh

set -e

# ls -la code

# Allows for overriding the entrypoint script (usually for testing purposes)
if [ -z ${ENTRYPOINT_OVERRIDE+x} ]; then
    python3 /execution_code/main.py
else
    if [ -z ${VSCODE_DEBUGGING+x} ]; then
        # python3 $ENTRYPOINT_OVERRIDE
        python3 -m pdb $ENTRYPOINT_OVERRIDE
        echo "foo"
    else
        python3 $ENTRYPOINT_OVERRIDE
        # python3 -m debugpy --listen 5678 $ENTRYPOINT_OVERRIDE
    fi
fi
