#!/bin/sh

set -e

# ls -la code

# Allows for overriding the entrypoint script (usually for testing purposes)
if [ -z ${ENTRYPOINT_OVERRIDE+x} ]; then
    python3 /code/main.py
else
    python3 $ENTRYPOINT_OVERRIDE
    #python3 -m pdb $ENTRYPOINT_OVERRIDE
fi
