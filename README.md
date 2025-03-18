# CloudRun

Run Python scripts in AWS Fargate with ease.

## Features

- Run Python scripts in AWS Fargate with configurable vCPUs and memory
- Support for spot instances for cost optimization
- Automatic infrastructure setup and management
- Simple CLI interface

## Installation

You can install CloudRun directly from GitHub:

```bash
pip install git+https://github.com/jonclegg/cloudrun.git
```

## Usage

### Command Line Interface

1. Initialize AWS infrastructure:

```bash
cloudrun setup [--region REGION] [--profile PROFILE]
```

This will create:
- S3 bucket for script uploads
- IAM roles and policies
- ECS cluster
- ECR repository
- VPC and networking components

2. Run a Python script:

```python
from cloudrun import run

job_id = run(
    script_path="your_script.py",
    vcpus=0.25,  # Optional: 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, or 16.0
    memory=512,  # Optional: Memory in MB (must match Fargate requirements)
    use_spot=False  # Optional: Use spot instances for cost savings
)
print(f"Job ID: {job_id}")
```

### Environment Variables

CloudRun uses the following environment variables:

- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_DEFAULT_REGION`: AWS region (default: us-east-1)
- `CLOUDRUN_REGION`: Region for CloudRun resources
- `CLOUDRUN_BUCKET_NAME`: S3 bucket name for script uploads
- `CLOUDRUN_SUBNET_ID`: Subnet ID for ECS tasks
- `CLOUDRUN_TASK_DEFINITION_ARN`: ECS task definition ARN

These are automatically set when running `cloudrun setup`.

## Development

1. Clone the repository:
```bash
git clone https://github.com/jonclegg/cloudrun.git
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

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 