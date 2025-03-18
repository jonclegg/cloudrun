#!/bin/bash

# Exit on error
set -e

# Get environment variables
BUCKET_NAME=$1
S3_KEY=$2
SCRIPT_PATH=$3

if [ -z "$BUCKET_NAME" ] || [ -z "$S3_KEY" ] || [ -z "$SCRIPT_PATH" ]; then
    echo "Usage: entrypoint.sh <bucket_name> <s3_key> <script_path>"
    exit 1
fi

# Download the zip file from S3
echo "Downloading code from S3..."
aws s3 cp "s3://${BUCKET_NAME}/${S3_KEY}" /app/code.zip

# Unzip the code
echo "Extracting code..."
unzip /app/code.zip -d /app/code

# Change to the code directory
cd /app/code

# Run the specified Python script
echo "Running script: ${SCRIPT_PATH}"
python "${SCRIPT_PATH}" 

# Run the specified Python script
echo "Script completed"
