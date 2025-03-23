#!/bin/bash

# Exit on error
set -e

# Get environment variables
BUCKET_NAME=$1
S3_KEY=$2
SCRIPT_PATH=$3
METHOD_NAME=$4
PARAMS_JSON=$5

if [ -z "$BUCKET_NAME" ] || [ -z "$S3_KEY" ] || [ -z "$SCRIPT_PATH" ]; then
    echo "Usage: entrypoint.sh <bucket_name> <s3_key> <script_path> [method_name] [params_json]"
    exit 1
fi

# Log task ID information
TASK_ID=${CLOUDRUN_TASK_ID:-"unknown"}
echo "Starting CloudRun task with ID: $TASK_ID"

# Download the zip file from S3
echo "Downloading code from S3..."
aws s3 cp "s3://${BUCKET_NAME}/${S3_KEY}" /app/code.zip

# Unzip the code
echo "Extracting code..."
unzip /app/code.zip -d /app/code

# Change to the code directory
cd /app/code

# Create a wrapper script to handle method execution
if [ ! -z "$METHOD_NAME" ]; then
    echo "Creating method execution wrapper..."
    cat > /app/code/run_method.py << EOL
import sys
import json
from importlib import import_module

def run_method(module_path, method_name, params=None):
    # Import the module
    module = import_module(module_path)
    
    # Get the method
    method = getattr(module, method_name)
    
    # Call the method with parameters if provided
    if params:
        result = method(params)
    else:
        result = method()
    
    return result

if __name__ == "__main__":
    # Get command line arguments
    module_path = sys.argv[1]
    method_name = sys.argv[2]
    params_json = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Parse parameters if provided
    params = json.loads(params_json) if params_json else None
    
    # Run the method
    result = run_method(module_path, method_name, params)
    
    # Print result if not None
    if result is not None:
        print(result)
EOL

    # Extract module path from script path
    MODULE_PATH=$(echo "$SCRIPT_PATH" | sed 's/\.py$//' | tr '/' '.')
    
    # Run the wrapper script
    echo "Running method: ${MODULE_PATH}.${METHOD_NAME}"
    python run_method.py "$MODULE_PATH" "$METHOD_NAME" "$PARAMS_JSON"
else
    # Run the specified Python script
    echo "Running script: ${SCRIPT_PATH}"
    python "${SCRIPT_PATH}"
fi

echo "Execution completed"
