import os
import boto3
import zipfile
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any
from .config import (
    get_config_value,
    set_config_value,
    get_environment_config,
    save_environment_config,
    validate_environment,
    clear_environment,
    list_environments
)
import json
import uuid
import time

###############################################################################

def check_initialization() -> bool:
    """
    Checks if CloudRun has been initialized.
    Returns True if initialized, False otherwise.
    """
    return get_config_value('CLOUDRUN_INITIALIZED') == 'true'

###############################################################################

def validate_cpu_memory(vcpus: float, memory: int) -> None:
    """
    Validates CPU and memory values against Fargate requirements.
    
    Args:
        vcpus: Number of vCPUs
        memory: Memory in MB
    
    Raises:
        ValueError: If invalid vcpus or memory values are provided
    """
    cpu_memory_combinations = {
        0.25: [512, 1024, 2048],
        0.5: [1024, 2048, 3072, 4096],
        1.0: [2048, 3072, 4096, 5120, 6144, 7168, 8192],
        2.0: list(range(4096, 16385, 1024)),
        4.0: list(range(8192, 30721, 1024)),
        8.0: list(range(16384, 61441, 4096)),
        16.0: list(range(32768, 122881, 8192))
    }
    
    if vcpus not in cpu_memory_combinations:
        raise ValueError(f"vcpus must be one of {list(cpu_memory_combinations.keys())}")
    
    if memory not in cpu_memory_combinations[vcpus]:
        raise ValueError(
            f"For {vcpus} vCPUs, memory must be one of these values (in MB): "
            f"{cpu_memory_combinations[vcpus]}"
        )

###############################################################################

def validate_environment() -> None:
    """
    Validates required configuration values are set.
    
    Raises:
        RuntimeError: If required configuration values are missing
    """
    validate_environment()

###############################################################################

def get_aws_session(profile: Optional[str] = None) -> boto3.Session:
    """Configure and return an AWS session with the given profile."""
    if profile:
        session = boto3.Session(profile_name=profile)
        # Set credentials for boto3 default session
        credentials = session.get_credentials()
        os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
        if session.region_name:
            os.environ['AWS_DEFAULT_REGION'] = session.region_name
        return session
    return boto3.Session()

###############################################################################

def get_region(env_name: str = 'default') -> str:
    """Get the configured AWS region for a specific environment."""
    return get_config_value('CLOUDRUN_REGION', env_name, 'us-east-1')

###############################################################################

def get_bucket_name(env_name: str = 'default') -> Optional[str]:
    """Get the configured S3 bucket name for a specific environment."""
    return get_config_value('CLOUDRUN_BUCKET_NAME', env_name)

###############################################################################

def get_task_role_arn(env_name: str = 'default') -> Optional[str]:
    """Get the configured task role ARN for a specific environment."""
    return get_config_value('CLOUDRUN_TASK_ROLE_ARN', env_name)

###############################################################################

def get_task_definition_arn(env_name: str = 'default') -> Optional[str]:
    """Get the configured task definition ARN for a specific environment."""
    return get_config_value('CLOUDRUN_TASK_DEFINITION_ARN', env_name)

###############################################################################

def get_subnet_id(env_name: str = 'default') -> Optional[str]:
    """Get the configured subnet ID for a specific environment."""
    return get_config_value('CLOUDRUN_SUBNET_ID', env_name)

###############################################################################

def get_vpc_id(env_name: str = 'default') -> Optional[str]:
    """Get the configured VPC ID for a specific environment."""
    return get_config_value('CLOUDRUN_VPC_ID', env_name)

###############################################################################

def get_ecr_repo(env_name: str = 'default') -> Optional[str]:
    """Get the configured ECR repository for a specific environment."""
    return get_config_value('CLOUDRUN_ECR_REPO', env_name)

###############################################################################

def get_cluster_name(env_name: str = 'default') -> Optional[str]:
    """Get the configured ECS cluster name for a specific environment."""
    return get_config_value('CLOUDRUN_CLUSTER_NAME', env_name)

###############################################################################

def get_scheduler_lambda_arn(env_name: str = 'default') -> Optional[str]:
    """Get the configured scheduler Lambda ARN for a specific environment."""
    return get_config_value('CLOUDRUN_SCHEDULER_LAMBDA_ARN', env_name)

###############################################################################

def set_region(region: str, env_name: str = 'default') -> None:
    """Set the AWS region for a specific environment."""
    set_config_value('CLOUDRUN_REGION', region, env_name)

###############################################################################

def set_bucket_name(bucket_name: str, env_name: str = 'default') -> None:
    """Set the S3 bucket name for a specific environment."""
    set_config_value('CLOUDRUN_BUCKET_NAME', bucket_name, env_name)

###############################################################################

def set_task_role_arn(task_role_arn: str, env_name: str = 'default') -> None:
    """Set the task role ARN for a specific environment."""
    set_config_value('CLOUDRUN_TASK_ROLE_ARN', task_role_arn, env_name)

###############################################################################

def set_task_definition_arn(task_definition_arn: str, env_name: str = 'default') -> None:
    """Set the task definition ARN for a specific environment."""
    set_config_value('CLOUDRUN_TASK_DEFINITION_ARN', task_definition_arn, env_name)

###############################################################################

def set_subnet_id(subnet_id: str, env_name: str = 'default') -> None:
    """Set the subnet ID for a specific environment."""
    set_config_value('CLOUDRUN_SUBNET_ID', subnet_id, env_name)

###############################################################################

def set_vpc_id(vpc_id: str, env_name: str = 'default') -> None:
    """Set the VPC ID for a specific environment."""
    set_config_value('CLOUDRUN_VPC_ID', vpc_id, env_name)

###############################################################################

def set_ecr_repo(ecr_repo: str, env_name: str = 'default') -> None:
    """Set the ECR repository for a specific environment."""
    set_config_value('CLOUDRUN_ECR_REPO', ecr_repo, env_name)

###############################################################################

def set_cluster_name(cluster_name: str, env_name: str = 'default') -> None:
    """Set the ECS cluster name for a specific environment."""
    set_config_value('CLOUDRUN_CLUSTER_NAME', cluster_name, env_name)

###############################################################################

def set_scheduler_lambda_arn(lambda_arn: str, env_name: str = 'default') -> None:
    """Set the scheduler Lambda ARN for a specific environment."""
    set_config_value('CLOUDRUN_SCHEDULER_LAMBDA_ARN', lambda_arn, env_name)

###############################################################################

def set_initialized(initialized: bool, env_name: str = 'default') -> None:
    """Set whether infrastructure is initialized for a specific environment."""
    set_config_value('CLOUDRUN_INITIALIZED', initialized, env_name)

###############################################################################

def clear_environment() -> None:
    """Clear all configuration values."""
    clear_environment()

###############################################################################

def _run_local(script_path: str, method_name: Optional[str], params: Optional[Dict[str, Any]]) -> str:
    """
    Runs a script locally.
    
    Args:
        script_path: Path to the Python script
        method_name: Optional method name to call
        params: Optional parameters to pass to the method
    
    Returns:
        str: Always returns "local"
    """
    import importlib.util
    spec = importlib.util.spec_from_file_location("module", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    if method_name:
        method = getattr(module, method_name)
        if params:
            result = method(params)
        else:
            result = method()
    else:
        if params:
            for key, value in params.items():
                setattr(module, key, value)
        result = None
    
    return "local"

###############################################################################

def create_and_upload_zip(script_path: str, exclude_paths: Optional[list[str]], verbose: bool) -> str:
    """
    Creates a zip file of the project and uploads it to S3.
    
    Args:
        script_path: Path to the script being run
        exclude_paths: Optional list of paths to exclude
        verbose: Whether to print verbose output
    
    Returns:
        str: S3 key where the zip was uploaded
    """
    default_excludes = {'.venv/', 'venv/', '__pycache__/', '*.pyc', ".git/"}
    if exclude_paths:
        default_excludes.update(exclude_paths)

    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
        zip_path = Path(tmp.name)
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk('.'):
            if any(pattern in root for pattern in default_excludes):
                continue
                
            for file in files:
                if file != 'temp.zip':
                    file_path = os.path.join(root, file)
                    if any(pattern in file_path for pattern in default_excludes):
                        continue
                    arcname = os.path.relpath(file_path, '.')
                    zipf.write(file_path, arcname)
                    if verbose:
                        size = os.path.getsize(file_path)
                        print(f"Added {file_path} to zip file {size}")
    
    s3_key = f"jobs/{os.path.basename(script_path)}/{zip_path.name}"
    s3 = boto3.client('s3', region_name=os.getenv('CLOUDRUN_REGION', 'us-east-1'))
    s3.upload_file(str(zip_path), os.getenv('CLOUDRUN_BUCKET_NAME'), s3_key)
    
    zip_path.unlink()
    return s3_key

###############################################################################

def run_ecs_task(
    script_path: str,
    s3_key: str,
    vcpus: float,
    memory: int,
    method_name: Optional[str],
    params: Optional[Dict[str, Any]],
    use_spot: bool
) -> str:
    """
    Runs a task on ECS Fargate.
    
    Args:
        script_path: Path to the script
        s3_key: S3 key where the zip file is stored
        vcpus: Number of vCPUs
        memory: Memory in MB
        method_name: Optional method name to call
        params: Optional parameters to pass to the method
        use_spot: Whether to use spot instances
    
    Returns:
        str: Job ID
    """
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME')
    subnet_id = os.getenv('CLOUDRUN_SUBNET_ID')
    cpu_units = str(int(vcpus * 1024))
    
    # Generate a custom task ID with timestamp for uniqueness
    custom_task_id = f"cloudrun-{int(time.time())}-{str(uuid.uuid4())[:8]}"
    
    command = [bucket_name, s3_key, script_path]
    if method_name:
        command.append(method_name)
    if params:
        command.append(json.dumps(params))
    
    task_params = {
        'cluster': 'cloudrun-cluster',
        'taskDefinition': os.getenv('CLOUDRUN_TASK_DEFINITION_ARN'),
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
                'command': command,
                'environment': [
                    {
                        'name': 'CLOUDRUN_TASK_ID',
                        'value': custom_task_id
                    }
                ]
            }]
        }
    }
    
    if use_spot:
        task_params['capacityProviderStrategy'] = [{
            'capacityProvider': 'FARGATE_SPOT',
            'weight': 1
        }]
    else:
        task_params['launchType'] = 'FARGATE'
    
    ecs = boto3.client('ecs', region_name=os.getenv('CLOUDRUN_REGION', 'us-east-1'))
    task = ecs.run_task(**task_params)
    
    # Return the custom task ID instead of the auto-generated ID
    return custom_task_id

###############################################################################

def run(
    script_path: str,
    vcpus: float = 0.25,
    memory: int = 512,
    use_spot: bool = False,
    exclude_paths: Optional[list[str]] = None,
    verbose: bool = False,
    params: Optional[Dict[str, Any]] = None,
    run_local: bool = False
) -> str:
    """
    Run a Python script or method in the cloud or locally.
    
    Args:
        script_path: Path to the Python script or module.method to run (e.g. "main.hello_world")
        vcpus: Number of vCPUs to allocate (default: 0.25). Must be one of [0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0]
        memory: Memory in MB to allocate (default: 512). Must follow Fargate's valid CPU/memory combinations
        use_spot: Whether to use spot instances (default: False)
        exclude_paths: List of path patterns to exclude from the zip file (default: None)
        verbose: Whether to print verbose output (default: False)
        params: Dictionary of parameters to pass to the method (default: None)
        run_local: Whether to run the script locally instead of in the cloud (default: False)
    
    Returns:
        str: Job ID for tracking the execution (or 'local' if run_local is True)
    
    Raises:
        RuntimeError: If CloudRun hasn't been initialized and run_local is False
        ValueError: If invalid vcpus or memory values are provided
    """
    if not check_initialization() and not run_local:
        raise RuntimeError(
            "\nCloudRun has not been initialized. "
            "Please run the following command first:\n\n"
            "    from cloudrun.setup import create_infrastructure\n"
            "    create_infrastructure()\n\n"
            "This will create the necessary AWS resources and save the configuration."
        )
    
    # Parse script path to determine if it's a module.method
    if not '.' in script_path:
        raise ValueError("Script path must be a module.method (e.g. 'main.my_method')")


    module_path, method_name = script_path.rsplit('.', 1)
    script_path = f"{module_path}.py"
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Module not found: {script_path}")

    if run_local:
        return _run_local(script_path, method_name, params)

    validate_cpu_memory(vcpus, memory)
    validate_environment()
    
    s3_key = create_and_upload_zip(script_path, exclude_paths, verbose)
    return run_ecs_task(script_path, s3_key, vcpus, memory, method_name, params, use_spot)

###############################################################################

__all__ = [
    # Configuration functions
    'get_region',
    'set_region',
    'get_bucket_name',
    'set_bucket_name',
    'get_subnet_id',
    'set_subnet_id',
    'get_vpc_id',
    'set_vpc_id',
    'get_task_definition_arn',
    'set_task_definition_arn',
    'get_task_role_arn',
    'set_task_role_arn',
    'get_ecr_repo',
    'set_ecr_repo',
    'get_cluster_name',
    'set_cluster_name',
    'get_scheduler_lambda_arn',
    'set_scheduler_lambda_arn',
    'is_initialized',
    'set_initialized',
    'get_environment',
    'save_environment',
    'clear_environment_config',
    'get_environments',
    'validate_env_config',
    
    # Infrastructure functions
    'create_infrastructure',
    'destroy_infrastructure',
    'rebuild_infrastructure',
    
    # Scheduler functions
    'create_scheduled_job',
    'list_scheduled_jobs',
    'delete_scheduled_job'
]

###############################################################################