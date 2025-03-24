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

### Python API Examples

Here are examples of how to use CloudRun in your Python code:

```python
from cloudrun import run
from cloudrun.infrastructure import create_infrastructure, destroy_infrastructure
from cloudrun.scheduler import create_scheduled_job, list_scheduled_jobs, delete_scheduled_job
from cloudrun.logger import get_logger

# Set up CloudRun infrastructure (only needed once)
resources = create_infrastructure(region="us-east-1")
print(f"Infrastructure created: {resources}")

# Set up infrastructure with custom Docker commands
# This allows installing additional system packages or running custom commands in the Docker image
resources = create_infrastructure(
    region="us-east-1",
    custom_docker_commands="""
RUN apt-get update && apt-get install -y \\
    postgresql-client \\
    ffmpeg \\
    && rm -rf /var/lib/apt/lists/*

# Install additional system tools
RUN pip install --no-cache-dir \\
    psycopg2-binary
"""
)

# Get a logger that automatically sends logs to CloudWatch
logger = get_logger(__name__)

# Run a basic script
job_id = run(
    script_path="scripts/process_data.py",
    vcpus=0.25,
    memory=512
)
print(f"Job started with ID: {job_id}")

# Run a script with a specific function
job_id = run(
    script_path="scripts/analytics.process_data",  # Will call process_data() function in analytics.py
    vcpus=1.0,
    memory=1024,
    use_spot=True,  # Use spot instances for cost savings (may be interrupted)
    params={
        "date": "2023-01-01",
        "batch_size": 100
    }
)
print(f"Job started with ID: {job_id}")

# Schedule a job to run daily at 2 AM UTC
job_arn = create_scheduled_job(
    name="daily-data-processing",
    file_method_path="scripts/analytics.process_data",
    schedule_expression="cron(0 2 * * ? *)",
    description="Daily data processing job",
    vcpus=1.0,
    memory=1024,
    use_spot=True,
    params={"mode": "daily"}
)
print(f"Scheduled job created: {job_arn}")

# List all scheduled jobs
jobs = list_scheduled_jobs()
for job in jobs:
    print(f"Job: {job['Name']}, Schedule: {job['ScheduleExpression']}")

# Delete a scheduled job
delete_scheduled_job("daily-data-processing")

# Completely remove all CloudRun infrastructure when you're done
destroy_infrastructure()
```

### Command Line Interface

CloudRun provides a comprehensive CLI for managing your jobs and infrastructure.

#### Setting Up Infrastructure

```bash
# Initialize AWS infrastructure in a specific region
cloudrun setup --region us-west-2

# Use a specific AWS profile
cloudrun setup --profile development

# Set up with custom Docker commands to install additional packages
cloudrun setup --region us-west-2 --custom-docker-commands "RUN apt-get update && apt-get install -y libpq-dev"
```

#### Running Scripts

The `run` module can be used in your Python script as shown in the Python examples above.

#### Managing Scheduled Jobs

```bash
# Create a scheduled job that runs every day at 8 AM UTC
cloudrun schedule create \
  --file-method-path your_script.py \
  --name daily-morning-job \
  --schedule-expression "cron(0 8 * * ? *)" \
  --description "Daily morning job to process data"

# Create a job to run every 2 hours with 1 vCPU and 2GB memory
cloudrun schedule create \
  --file-method-path your_script.py \
  --name every-2-hours \
  --schedule-expression "rate(2 hours)" \
  --description "Job that runs every 2 hours" \
  --vcpus 1.0 \
  --memory 2048

# Schedule a job that calls a specific method with parameters
cloudrun schedule create \
  --file-method-path your_script.process_data \
  --name method-job \
  --schedule-expression "rate(1 day)" \
  --params '{"key": "value", "date": "2023-01-01"}' \
  --description "Job that calls a specific method with parameters" \
  --use-spot
```

#### Listing Scheduled Jobs

```bash
# List all scheduled jobs
cloudrun schedule list
```

#### Deleting Scheduled Jobs

```bash
# Delete a scheduled job
cloudrun schedule delete --name daily-morning-job
```

#### Working with Logs

```bash
# Fetch the last hour of logs from the CloudRun log group
cloudrun logs get --log-group /ecs/cloudrun --hours 1

# Filter logs with a specific pattern
cloudrun logs get --log-group /ecs/cloudrun --filter "ERROR"

# Filter logs for a specific task
cloudrun logs get --log-group /ecs/cloudrun --task-id your-task-id

# Tail logs in real-time (like 'tail -f')
cloudrun logs get --log-group /ecs/cloudrun --tail

# Tail logs with a filter pattern
cloudrun logs get --log-group /ecs/cloudrun --tail --filter "ERROR"

# Tail logs for a specific task
cloudrun logs get --log-group /ecs/cloudrun --tail --task-id your-task-id

# Show stream names in the log output
cloudrun logs get --log-group /ecs/cloudrun --tail --show-stream
```

CloudRun's log tailing feature works like `tail -f` on Linux/macOS, displaying new log entries as they arrive in real-time. This is especially useful when monitoring running jobs. Press Ctrl+C to stop tailing logs.

#### Destroying Infrastructure

```bash
# Remove all CloudRun infrastructure
cloudrun destroy
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

### Scheduled Jobs Details

When you schedule a job, CloudRun automatically:
1. Packages your code into a zip file
2. Uploads it to S3
3. Creates an EventBridge rule with your schedule
4. Sets up a Lambda function to launch an ECS task when triggered

CloudRun supports two types of schedule expressions:

1. **Cron Expressions**: `cron(0 8 * * ? *)` - Run at 8:00 AM UTC every day
2. **Rate Expressions**: `rate(1 hour)` - Run every hour

See `examples/scheduled_job.md` for more detailed examples.

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