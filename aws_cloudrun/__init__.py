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

def run(
    script_path: str,
    vcpus: float = 0.25,
    memory: int = 512,
    use_spot: bool = False
) -> str:
    """
    Run a Python script in the cloud.
    
    Args:
        script_path: Path to the Python script to run
        vcpus: Number of vCPUs to allocate (default: 0.25). Must be one of [0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0]
        memory: Memory in MB to allocate (default: 512). Must follow Fargate's valid CPU/memory combinations
        use_spot: Whether to use spot instances (default: False)
    
    Returns:
        str: Job ID for tracking the execution
    
    Raises:
        RuntimeError: If CloudRun hasn't been initialized
        ValueError: If invalid vcpus or memory values are provided
    """
    if not check_initialization():
        raise RuntimeError(
            "\nCloudRun has not been initialized. "
            "Please run the following command first:\n\n"
            "    from cloudrun.setup import create_infrastructure\n"
            "    create_infrastructure()\n\n"
            "This will create the necessary AWS resources and save the configuration."
        )
    
    # Validate CPU and memory values based on Fargate requirements
    cpu_memory_combinations = {
        0.25: [512, 1024, 2048],  # 256 (.25 vCPU): 512MB, 1GB, 2GB
        0.5: [1024, 2048, 3072, 4096],  # 512 (.5 vCPU): 1GB, 2GB, 3GB, 4GB
        1.0: [2048, 3072, 4096, 5120, 6144, 7168, 8192],  # 1024 (1 vCPU): 2GB-8GB
        2.0: list(range(4096, 16385, 1024)),  # 2048 (2 vCPU): 4GB-16GB in 1GB increments
        4.0: list(range(8192, 30721, 1024)),  # 4096 (4 vCPU): 8GB-30GB in 1GB increments
        8.0: list(range(16384, 61441, 4096)),  # 8192 (8 vCPU): 16GB-60GB in 4GB increments
        16.0: list(range(32768, 122881, 8192))  # 16384 (16 vCPU): 32GB-120GB in 8GB increments
    }
    
    if vcpus not in cpu_memory_combinations:
        raise ValueError(f"vcpus must be one of {list(cpu_memory_combinations.keys())}")
    
    if memory not in cpu_memory_combinations[vcpus]:
        raise ValueError(
            f"For {vcpus} vCPUs, memory must be one of these values (in MB): "
            f"{cpu_memory_combinations[vcpus]}"
        )
    
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script not found: {script_path}")
    
    # Get AWS region from saved configuration
    region = os.getenv('CLOUDRUN_REGION', 'us-east-1')
    
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
    
    # Convert vCPUs to Fargate CPU units
    cpu_units = str(int(vcpus * 1024))
    
    # Run the task with the configured task definition
    task_params = {
        'cluster': 'cloudrun-cluster',
        'taskDefinition': task_definition_arn,
        'launchType': 'FARGATE',
        'networkConfiguration': {
            'awsvpcConfiguration': {
                'subnets': [subnet_id],
                'assignPublicIp': 'ENABLED'
            }
        },
        'overrides': {
            'cpu': cpu_units,
            'memory': str(memory),
            'containerOverrides': [{
                'name': 'cloudrun-executor',
                'command': [bucket_name, s3_key, script_path]
            }]
        }
    }
    
    if use_spot:
        task_params['capacityProviderStrategy'] = [{
            'capacityProvider': 'FARGATE_SPOT',
            'weight': 1
        }]
    
    task = ecs.run_task(**task_params)
    
    return f"job-{task['tasks'][0]['taskArn'].split('/')[-1]}"

__all__ = ['cli', 'create_infrastructure', 'run'] 