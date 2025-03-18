# CloudRun

A Python package that enables running Python code in AWS cloud using Fargate containers.

## Features

- Automatically sets up required AWS infrastructure
- Uses AWS Fargate for serverless container execution
- Handles code packaging and deployment
- Automatic VPC and subnet configuration
- Docker container management

## Prerequisites

- Python 3.9+
- Docker installed and running
- AWS credentials configured
- AWS CLI installed

## Installation

```bash
pip install -r requirements.txt
```

## Usage

1. Initialize the infrastructure:

```python
from cloudrun.setup import create_infrastructure

# This will create all necessary AWS resources
create_infrastructure()
```

2. Run your code in the cloud:

```python
from cloudrun import run

# Run a Python script in the cloud
job_id = run("src/your_script.py")
print(f"Job started with ID: {job_id}")
```

## Infrastructure Created

The setup process creates:
- S3 bucket for code storage
- IAM roles and policies
- ECS Fargate cluster
- ECR repository with Docker image
- ECS task definition
- Uses default VPC and subnet

## Environment Variables

After setup, the following environment variables are configured in `.env`:
- `CLOUDRUN_BUCKET_NAME`: S3 bucket for code storage
- `CLOUDRUN_CLUSTER_ARN`: ECS cluster ARN
- `CLOUDRUN_TASK_ROLE_ARN`: IAM role ARN
- `CLOUDRUN_REPOSITORY_URI`: ECR repository URI
- `CLOUDRUN_IMAGE_URI`: Docker image URI
- `CLOUDRUN_VPC_ID`: VPC ID
- `CLOUDRUN_SUBNET_ID`: Subnet ID
- `CLOUDRUN_TASK_DEFINITION_ARN`: ECS task definition ARN
- `CLOUDRUN_REGION`: AWS region
- `CLOUDRUN_INITIALIZED`: Setup status

## License

MIT 