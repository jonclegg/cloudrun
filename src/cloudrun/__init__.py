import os
import boto3
import zipfile
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import json

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
    if '.' in script_path:
        module_path, method_name = script_path.rsplit('.', 1)
        script_path = f"{module_path}.py"
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Module not found: {script_path}")
    else:
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found: {script_path}")
        method_name = None

    if run_local:
        # Import the module dynamically
        import importlib.util
        spec = importlib.util.spec_from_file_location("module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        if method_name:
            # If method name is provided, call the specific method
            method = getattr(module, method_name)
            if params:
                result = method(params)
            else:
                result = method()
        else:
            # If no method name, just execute the script
            if params:
                # If params are provided, we need to modify the script's globals
                for key, value in params.items():
                    setattr(module, key, value)
            result = None
        
        return "local"

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
    
    # Get subnet ID from environment
    subnet_id = os.getenv('CLOUDRUN_SUBNET_ID')
    if not subnet_id:
        raise RuntimeError("CLOUDRUN_SUBNET_ID not found. Please run create_infrastructure() first")
    
    # Load environment variables
    load_dotenv()
    
    # Check for required environment variables
    required_vars = [
        'CLOUDRUN_BUCKET_NAME',
        'CLOUDRUN_TASK_ROLE_ARN',
        'CLOUDRUN_TASK_DEFINITION_ARN',
        'CLOUDRUN_INITIALIZED'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing_vars)}. "
            "Please run create_infrastructure() first"
        )
    
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME')
    task_role_arn = os.getenv('CLOUDRUN_TASK_ROLE_ARN')
    
    # Get AWS region from saved configuration
    region = os.getenv('CLOUDRUN_REGION', 'us-east-1')
    
    # Initialize AWS clients
    s3 = boto3.client('s3', region_name=region)
    ecs = boto3.client('ecs', region_name=region)
    
    # Default exclude patterns
    default_excludes = {'.venv/', 'venv/', '__pycache__/', '*.pyc', ".git/"}
    if exclude_paths:
        default_excludes.update(exclude_paths)

    # Create a temporary zip file
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
        zip_path = Path(tmp.name)
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk('.'):
            # Check if the current directory should be excluded
            if any(pattern in root for pattern in default_excludes):
                continue
                
            for file in files:
                if file != 'temp.zip' and not file.startswith('.'):
                    file_path = os.path.join(root, file)
                    # Skip files matching exclude patterns
                    if any(pattern in file_path for pattern in default_excludes):
                        continue
                    arcname = os.path.relpath(file_path, '.')
                    zipf.write(file_path, arcname)
                    if verbose:
                        size = os.path.getsize(file_path)
                        print(f"Added {file_path} to zip file {size}")
    
    # Upload to S3
    s3_key = f"jobs/{os.path.basename(script_path)}/{zip_path.name}"
    s3.upload_file(str(zip_path), os.getenv('CLOUDRUN_BUCKET_NAME'), s3_key)
    
    # Clean up temporary zip
    zip_path.unlink()
    
    # Convert vCPUs to Fargate CPU units
    cpu_units = str(int(vcpus * 1024))
    
    # Prepare command arguments
    command = [bucket_name, s3_key, script_path]
    if method_name:
        command.append(method_name)
    if params:
        command.append(json.dumps(params))
    
    # Run the task with the configured task definition
    task_params = {
        'cluster': 'cloudrun-cluster',
        'taskDefinition': os.getenv('CLOUDRUN_TASK_DEFINITION_ARN'),
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
                'command': command
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

__all__ = ['cli', 'create_infrastructure', 'destroy_infrastructure', 'ensure_infrastructure'] 