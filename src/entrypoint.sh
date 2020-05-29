#!/bin/sh

set -e

# ls -la code

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

# echo "::set-output name=time::$time"
# #TODO: wish the below wasn't so hard coded, but don't care for now

echo "::set-output name=output_base64_encoded::$output_base64_encoded"
echo "::set-output name=input_node_id::$input_node_id"
echo "::set-output name=execution_node_id::$execution_node_id"
echo "::set-output name=output_node_id::$output_node_id"
echo "::set-output name=log_node_id::$log_node_id"
