# CloudRun

CloudRun is a Python package that makes it easy to run Python scripts in AWS ECS Fargate with scheduled execution capabilities.

## Features

- Run Python scripts in AWS ECS Fargate
- Schedule script execution using AWS EventBridge
- Support for spot instances to reduce costs
- Automatic infrastructure provisioning
- Simple configuration management
- Multiple environment support (e.g., development, staging, production)

## Installation

```bash
pip install cloudrun
```

## Quick Start

1. Initialize the infrastructure:
```python
from cloudrun import create_infrastructure

# Create infrastructure in us-east-1 for the default environment
create_infrastructure(region='us-east-1')

# Create infrastructure for a specific environment (e.g., production)
create_infrastructure(
    env_name='production',
    region='us-east-1'
)
```

2. Create a scheduled job:
```python
from cloudrun import create_scheduled_job

# Create a job that runs every day at midnight in the default environment
create_scheduled_job(
    name='my-job',
    script_path='my_script.py',
    schedule='0 0 * * *',  # Cron expression
    vcpus=0.25,  # 0.25 vCPUs
    memory=512,  # 512 MB
    use_spot=True  # Use spot instances for cost savings
)

# Create a job in a specific environment (e.g., production)
create_scheduled_job(
    name='my-job',
    script_path='my_script.py',
    schedule='0 0 * * *',
    env_name='production',
    vcpus=0.25,
    memory=512,
    use_spot=True
)
```

3. List scheduled jobs:
```python
from cloudrun import list_scheduled_jobs

# List jobs in the default environment
jobs = list_scheduled_jobs()
for job in jobs:
    print(f"Job: {job['name']}")
    print(f"Environment: {job['environment']}")
    print(f"Schedule: {job['schedule']}")
    print(f"Script: {job['script_path']}")

# List jobs in a specific environment
production_jobs = list_scheduled_jobs(env_name='production')
```

4. Delete a scheduled job:
```python
from cloudrun import delete_scheduled_job

# Delete a job from the default environment
delete_scheduled_job('my-job')

# Delete a job from a specific environment
delete_scheduled_job('my-job', env_name='production')
```

5. Clean up infrastructure:
```python
from cloudrun import destroy_infrastructure

# Destroy infrastructure for the default environment
destroy_infrastructure()

# Destroy infrastructure for a specific environment
destroy_infrastructure(env_name='production')
```

## Configuration

CloudRun uses a configuration system stored in the `.cloudrun` directory in your home folder. The configuration is stored in JSON format in `~/.cloudrun/config.json`.

Each environment has its own configuration section. The following configuration values are used for each environment:

- `CLOUDRUN_REGION`: AWS region to use (default: us-east-1)
- `CLOUDRUN_BUCKET_NAME`: S3 bucket for storing scripts
- `CLOUDRUN_SUBNET_ID`: Subnet ID for ECS tasks
- `CLOUDRUN_VPC_ID`: VPC ID for ECS tasks
- `CLOUDRUN_TASK_DEFINITION_ARN`: ECS task definition ARN
- `CLOUDRUN_TASK_FAMILY`: ECS task family name
- `CLOUDRUN_TASK_ROLE_ARN`: IAM role ARN for ECS tasks
- `CLOUDRUN_ECR_REPO`: ECR repository for the executor container
- `CLOUDRUN_CLUSTER_NAME`: ECS cluster name
- `CLOUDRUN_SCHEDULER_LAMBDA_ARN`: Lambda function ARN for job scheduling
- `CLOUDRUN_INITIALIZED`: Whether infrastructure has been initialized

You can access these values programmatically:

```python
from cloudrun import (
    get_region,
    get_bucket_name,
    get_subnet_id,
    get_vpc_id,
    get_task_definition_arn,
    get_task_role_arn,
    get_ecr_repo,
    get_cluster_name,
    get_scheduler_lambda_arn
)

# Get configuration values for the default environment
region = get_region()
bucket_name = get_bucket_name()
subnet_id = get_subnet_id()
# ... etc.

# Get configuration values for a specific environment
region = get_region('production')
bucket_name = get_bucket_name('production')
subnet_id = get_subnet_id('production')
# ... etc.
```

## Development

1. Clone the repository:
```bash
git clone https://github.com/yourusername/cloudrun.git
cd cloudrun
```

2. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

4. Run tests:
```bash
pytest
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 