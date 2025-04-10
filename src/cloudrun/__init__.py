import os
import boto3
import zipfile
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any
import json
import uuid
import time
import cloudrun._infrastructure as _infrastructure

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

def create_and_upload_zip(region, script_path: str, exclude_paths: Optional[list[str]], verbose: bool) -> str:
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
    s3 = boto3.client('s3', region_name=region)
    s3.upload_file(str(zip_path), _infrastructure.get_bucket_name(region), s3_key)
    
    zip_path.unlink()
    return s3_key

###############################################################################

def get_default_vpc_and_subnet(region: str) -> tuple:
    """Get VPC and subnet information, either from provided values or default."""
    print("\nGetting VPC and subnet information...")
    
    ec2_client = boto3.client('ec2', region_name=region)
    vpcs = ec2_client.describe_vpcs(
        Filters=[{'Name': 'isDefault', 'Values': ['true']}]
    )['Vpcs']
    
    if not vpcs:
        raise Exception("No default VPC found. Please ensure your AWS account has a default VPC or specify a VPC ID.")
    
    vpc_id = vpcs[0]['VpcId']
    print(f"Found default VPC: {vpc_id}")
    
    subnets = ec2_client.describe_subnets(
        Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
    )['Subnets']
    
    if not subnets:
        raise Exception("No subnets found in VPC. Please specify a subnet ID or ensure the VPC has subnets.")
    
    subnet_id = subnets[0]['SubnetId']
    print(f"Found subnet: {subnet_id}")
    
    return vpc_id, subnet_id

###############################################################################


def run_ecs_task(
    script_path: str,
    method_name: str,
    s3_key: str,
    **kwargs
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
        region: Name of the environment to use
    
    Returns:
        str: AWS ECS Task ID
    """

    region = kwargs.get('region', 'us-east-1')
    bucket_name = _infrastructure.get_bucket_name(region)

    user_vpc_id = kwargs.get('vpc_id', None)
    user_subnet_id = kwargs.get('subnet_id', None)
    if user_vpc_id and user_subnet_id:
        _, subnet_id = user_vpc_id, user_subnet_id
    else:
        _, subnet_id = get_default_vpc_and_subnet(region)

    task_definition_arn = _infrastructure.get_task_definition_arn(region)
    vcpus = kwargs.get('vcpus', 0.25)
    cpu_units = str(int(vcpus * 1024))
    memory = kwargs.get('memory', 512)
    validate_cpu_memory(vcpus, memory)

    params = kwargs.get('params', None)
    command = [bucket_name, s3_key, script_path, method_name, json.dumps(params)]

    task_params = {
        'cluster': _infrastructure.get_cluster_name(),
        'taskDefinition': task_definition_arn,
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
            }]
        }
    }
    
    use_spot = kwargs.get('use_spot', False)
    if use_spot:
        task_params['capacityProviderStrategy'] = [{
            'capacityProvider': 'FARGATE_SPOT',
            'weight': 1
        }]
    else:
        task_params['launchType'] = 'FARGATE'
    
    ecs = boto3.client('ecs', region_name=region)
    task = ecs.run_task(**task_params)
    
    # Return the actual AWS task ID
    return task['tasks'][0]['taskArn'].split('/')[-1]

###############################################################################

def run(
    script_path: str, 
    **kwargs
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
    
    # _infrastructure.create_infrastructure(**kwargs)

    # Parse script path to determine if it's a module.method
    if not '.' in script_path:
        raise ValueError("Script path must be a module.method (e.g. 'main.my_method')")

    module_path, method_name = script_path.rsplit('.', 1)
    script_path = f"{module_path}.py"
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Module not found: {script_path}")

    run_local = kwargs.get('run_local', False)
    if run_local:
        return _run_local(script_path, method_name, kwargs)

    exclude_paths = kwargs.get('exclude_paths', None)
    verbose = kwargs.get('verbose', False)
    region = kwargs.get('region', 'us-east-1')

    s3_key = create_and_upload_zip(region, script_path, exclude_paths, verbose)
    return run_ecs_task(script_path, method_name, s3_key, **kwargs)

###############################################################################

def wait_for_task_completion(task_id: str, region: str = 'us-east-1', poll_interval: int = 10) -> None:
    """
    Wait for a task to complete by polling its status.
    
    Args:
        task_id: The ID of the task to wait for
        env_name: Name of the environment to use (default: 'default')
        poll_interval: How often to check task status in seconds (default: 10)
    
    Raises:
        RuntimeError: If the task fails or is stopped
    """
    ecs = boto3.client('ecs', region_name=region)
    cluster_name = _infrastructure.get_cluster_name()
    
    while True:
        response = ecs.describe_tasks(cluster=cluster_name, tasks=[task_id])
        task = response['tasks'][0]
        
        if task['lastStatus'] == 'STOPPED':
            if task['stopCode'] != 'EssentialContainerExited':
                raise RuntimeError(f"Task failed with stop code: {task['stopCode']}")
            return
            
        time.sleep(poll_interval)

###############################################################################

__all__ = [
    # Configuration functions
    # Task functions
    'run',
    'wait_for_task_completion',
    # CLI functions
    'get_tasks',
    'delete_task'
]

###############################################################################