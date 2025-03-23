# CloudRun

Run Python scripts in AWS Fargate with ease.

## Features

- Run Python scripts in AWS Fargate with configurable vCPUs and memory
- Support for spot instances for cost optimization
- Automatic infrastructure setup and management
- Simple CLI interface

### Scheduled Jobs

CloudRun supports scheduling jobs to run at regular intervals. You can use cron or rate expressions to specify when jobs should run:

```bash
# Schedule a job to run every day at 8 AM UTC
cloudrun schedule \
  --file-method-path your_script.py \
  --name daily-morning-job \
  --schedule-expression "cron(0 8 * * ? *)" \
  --description "Daily morning job to process data"

# Schedule a job to run every 2 hours
cloudrun schedule \
  --file-method-path your_script.py \
  --name every-2-hours \
  --schedule-expression "rate(2 hours)" \
  --description "Job that runs every 2 hours"

# Schedule a job that calls a specific method
cloudrun schedule \
  --file-method-path your_script.process_data \
  --name method-job \
  --schedule-expression "rate(1 day)" \
  --params '{"key": "value"}' \
  --description "Job that calls a specific method with parameters"
```

When you schedule a job, CloudRun automatically:
1. Packages your code into a zip file
2. Uploads it to S3
3. Creates an EventBridge rule with your schedule
4. Sets up a Lambda function to launch an ECS task when triggered

View scheduled jobs with:

```bash
cloudrun jobs
```

Delete a scheduled job with:

```bash
cloudrun delete-job --name your-job-name
```

See `examples/scheduled_job.md` for more detailed examples.

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

2. Destroy AWS infrastructure:

```bash
cloudrun destroy
```

This will remove all CloudRun infrastructure. You will be prompted for confirmation before proceeding.

3. Run a Python script:

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
- `CLOUDRUN_LOG_GROUP`: CloudWatch log group name (default: /ecs/cloudrun)

These are automatically set when running `cloudrun setup`.

### Logging

CloudRun provides a pre-configured logger that automatically sends logs to CloudWatch. You can use it in your scripts like this:

```python
from cloudrun.logger import get_logger

# Get a logger instance
logger = get_logger(__name__)

# Use the logger
logger.info("Starting job...")
logger.debug("Processing data...")
logger.error("An error occurred")
```

The logger will automatically:
- Create the log group if it doesn't exist
- Send logs to CloudWatch with proper timestamps and formatting
- Use the log group specified in the `run()` function or environment variable

You can specify a custom log group when running your script:

```python
job_id = run(
    script_path="your_script.py",
    log_group="/custom/log/group"  # Optional: Custom CloudWatch log group
)
```

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