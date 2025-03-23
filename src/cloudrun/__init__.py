import os
import boto3
import zipfile
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import json
import uuid
import time

###############################################################################

def check_initialization() -> bool:
    """
    Checks if CloudRun has been initialized.
    Returns True if initialized, False otherwise.
    """
    load_dotenv()
    return os.getenv('CLOUDRUN_INITIALIZED') == 'true'

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
    Validates required environment variables are set.
    
    Raises:
        RuntimeError: If required environment variables are missing
    """
    required_vars = [
        'CLOUDRUN_BUCKET_NAME',
        'CLOUDRUN_TASK_ROLE_ARN',
        'CLOUDRUN_TASK_DEFINITION_ARN',
        'CLOUDRUN_INITIALIZED',
        'CLOUDRUN_SUBNET_ID'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing_vars)}. "
            "Please run create_infrastructure() first"
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
                if file != 'temp.zip' and not file.startswith('.'):
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
    custom_task_id = f"task-{int(time.time())}-{str(uuid.uuid4())[:8]}"
    
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

__all__ = ['cli', 'create_infrastructure', 'destroy_infrastructure', 'ensure_infrastructure'] 

###############################################################################