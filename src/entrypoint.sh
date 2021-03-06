#!/bin/sh

set -e

# ls -la code
echo "::set-output name=test_output_before::before"

# Allows for overriding the entrypoint script (usually for testing purposes)
if [ -z ${ENTRYPOINT_OVERRIDE+x} ]; then
    python3 /src/main.py
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

echo "::set-output name=test_output_after::after"
# #TODO: wish the below wasn't so hard coded, but don't care for now
cat /output_message.txt