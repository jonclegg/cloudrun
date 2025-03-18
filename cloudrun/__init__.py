import os
import boto3
import zipfile
import json
from pathlib import Path
from typing import Optional, Tuple
from .cli import cli
from .setup import create_infrastructure
from dotenv import load_dotenv

def ensure_infrastructure() -> Tuple[str, str]:
    """
    Ensures all required AWS infrastructure exists.
    Creates resources if they don't exist.
    
    Returns:
        Tuple[str, str]: Bucket name and task role ARN
    """
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    iam = boto3.client('iam', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    ecs = boto3.client('ecs', region_name=region)
    ecr = boto3.client('ecr', region_name=region)

    # Create S3 bucket
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME', f'cloudrun-{region}-{os.getenv("USER", "default")}')
    try:
        s3.create_bucket(Bucket=bucket_name)
    except s3.exceptions.BucketAlreadyExists:
        pass

    # Create ECS task execution role
    task_role_name = 'cloudrun-task-role'
    try:
        task_role = iam.create_role(
            RoleName=task_role_name,
            AssumeRolePolicyDocument=json.dumps({
                'Version': '2012-10-17',
                'Statement': [{
                    'Effect': 'Allow',
                    'Principal': {'Service': 'ecs-tasks.amazonaws.com'},
                    'Action': 'sts:AssumeRole'
                }]
            })
        )
        
        # Attach policies
        policies = [
            'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy',
            'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        ]
        for policy in policies:
            iam.attach_role_policy(
                RoleName=task_role_name,
                PolicyArn=policy
            )
    except iam.exceptions.EntityAlreadyExistsException:
        task_role = iam.get_role(RoleName=task_role_name)

    # Create ECS cluster
    cluster_name = 'cloudrun-cluster'
    try:
        ecs.create_cluster(
            clusterName=cluster_name,
            capacityProviders=['FARGATE']
        )
    except ecs.exceptions.ClusterExists:
        pass

    # Create ECR repository
    repo_name = 'cloudrun-executor'
    try:
        ecr.create_repository(repositoryName=repo_name)
    except ecr.exceptions.RepositoryAlreadyExistsException:
        pass

    return bucket_name, task_role['Role']['Arn']

def check_initialization() -> bool:
    """
    Checks if CloudRun has been initialized.
    Returns True if initialized, False otherwise.
    """
    load_dotenv()
    return os.getenv('CLOUDRUN_INITIALIZED') == 'true'

def run(script_path: str, aws_region: Optional[str] = None) -> str:
    """
    Run a Python script in the cloud.
    
    Args:
        script_path: Path to the Python script to run
        aws_region: AWS region to use (defaults to environment variable or 'us-east-1')
    
    Returns:
        str: Job ID for tracking the execution
    
    Raises:
        RuntimeError: If CloudRun hasn't been initialized
    """
    if not check_initialization():
        raise RuntimeError(
            "\nCloudRun has not been initialized. "
            "Please run the following command first:\n\n"
            "    from cloudrun.setup import create_infrastructure\n"
            "    create_infrastructure()\n\n"
            "This will create the necessary AWS resources and save the configuration."
        )
    
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script not found: {script_path}")
    
    # Get AWS region from saved configuration
    region = aws_region or os.getenv('CLOUDRUN_REGION', 'us-east-1')
    
    # Initialize AWS clients
    s3 = boto3.client('s3', region_name=region)
    ecs = boto3.client('ecs', region_name=region)
    
    # Create a temporary zip file
    zip_path = Path('temp.zip')
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk('.'):
            for file in files:
                if file != 'temp.zip' and not file.startswith('.'):
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, '.')
                    zipf.write(file_path, arcname)
    
    # Upload to S3
    s3_key = f"jobs/{os.path.basename(script_path)}/{zip_path.name}"
    s3.upload_file(str(zip_path), os.getenv('CLOUDRUN_BUCKET_NAME'), s3_key)
    
    # Clean up temporary zip
    zip_path.unlink()
    
    # Get subnet ID from environment
    subnet_id = os.getenv('CLOUDRUN_SUBNET_ID')
    if not subnet_id:
        raise RuntimeError("CLOUDRUN_SUBNET_ID not found. Please run create_infrastructure() first")
    
    bucket_name, task_role_arn = ensure_infrastructure()
    task_definition_arn = os.getenv('CLOUDRUN_TASK_DEFINITION_ARN')
    if not task_definition_arn:
        raise RuntimeError("CLOUDRUN_TASK_DEFINITION_ARN not found. Please run create_infrastructure() first")
    
    # Run the task with the configured task definition
    task = ecs.run_task(
        cluster='cloudrun-cluster',
        taskDefinition=task_definition_arn,  # Use the full ARN
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [subnet_id],
                'assignPublicIp': 'ENABLED'
            }
        },
        overrides={
            'containerOverrides': [{
                'name': 'cloudrun-executor',
                'command': [bucket_name, s3_key, script_path]
            }]
        }
    )
    
    return f"job-{task['tasks'][0]['taskArn'].split('/')[-1]}"

__all__ = ['cli', 'create_infrastructure', 'run'] 