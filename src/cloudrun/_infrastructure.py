import os
import boto3
import json
from typing import Dict, Any
import subprocess
import shutil
import tempfile

###############################################################################

def get_task_family() -> str:
    """Get the task family for the environment."""
    return f"cloudrun-task"

###############################################################################

def get_log_group() -> str:
    """Get the log group for the environment."""
    return f"/ecs/cloudrun"

###############################################################################

def get_task_definition_arn(region: str) -> str:
    """Get the task definition ARN for the environment."""
    sts_client = boto3.client('sts', region_name=region)
    account_id = get_account_id(sts_client)
    return f"arn:aws:ecs:{region}:{account_id}:task-definition/cloudrun-task"

###############################################################################

def get_ecr_repository_name() -> str:
    """Get the ECR repository name for the environment."""
    return f"cloudrun-executor"

###############################################################################

def get_ecr_repository_url(sts_client, region: str) -> str:
    """Get the ECR repository URL for the environment."""
    return f"{get_account_id(sts_client)}.dkr.ecr.{region}.amazonaws.com/cloudrun-executor"

###############################################################################

def get_account_id(sts_client) -> str:
    """Get the AWS account ID for the environment."""
    return sts_client.get_caller_identity()['Account']

###############################################################################

def get_bucket_name(region: str) -> str:
    """Get the bucket name for the environment."""
    sts_client = boto3.client('sts', region_name=region)
    aws_acccount_id = get_account_id(sts_client)
    bucket_name = f"cloudrun-bucket-{region}-{aws_acccount_id}"

    return bucket_name

###############################################################################

def get_task_role_name() -> str:
    """Get the task role name for the environment."""
    return f"cloudrun-task-role"

###############################################################################

def get_cluster_name() -> str:
    """Get the cluster name for the environment."""
    return f"cloudrun-cluster"

###############################################################################

def _initialize_aws_clients(region: str) -> Dict[str, Any]:
    """Initialize and return AWS clients for various services."""
    print("\nInitializing AWS clients...")
    return {
        'iam': boto3.client('iam', region_name=region),
        's3': boto3.client('s3', region_name=region),
        'ecs': boto3.client('ecs', region_name=region),
        'ecr': boto3.client('ecr', region_name=region),
        'logs': boto3.client('logs', region_name=region),
        'ec2': boto3.client('ec2', region_name=region),
        'dynamodb': boto3.client('dynamodb', region_name=region),
        'sts': boto3.client('sts', region_name=region)
    }

###############################################################################

def _create_s3_bucket(s3_client, region: str) -> None:
    """Create an S3 bucket if it doesn't exist."""
    bucket_name = get_bucket_name(region)
    print(f"\nCreating S3 bucket: {bucket_name}")
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except s3_client.exceptions.BucketAlreadyExists:
        print("S3 bucket already exists")

###############################################################################

def _create_task_role(iam_client, task_role_name: str, additional_policies: list = None) -> Dict[str, Any]:
    """Create ECS task execution role and attach necessary policies."""
    print("\nCreating ECS task execution role...")
    try:
        task_role = iam_client.create_role(
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
        
        print("Attaching policies to task role...")
        policies = [
            'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy',
            'arn:aws:iam::aws:policy/AmazonS3FullAccess',
            'arn:aws:iam::aws:policy/CloudWatchLogsFullAccess'
        ]
        
        if additional_policies:
            policies.extend(additional_policies)
            
        for policy in policies:
            iam_client.attach_role_policy(
                RoleName=task_role_name,
                PolicyArn=policy
            )
    except iam_client.exceptions.EntityAlreadyExistsException:
        print("ECS task role already exists")
        task_role = iam_client.get_role(RoleName=task_role_name)
        
        if additional_policies:
            print("Attaching additional policies to existing role...")
            for policy in additional_policies:
                try:
                    iam_client.attach_role_policy(
                        RoleName=task_role_name,
                        PolicyArn=policy
                    )
                except iam_client.exceptions.EntityAlreadyExistsException:
                    print(f"Policy {policy} already attached to role")
    
    return task_role

###############################################################################

def _create_ecs_cluster(ecs_client) -> None:
    """Create ECS cluster if it doesn't exist."""
    print(f"\nCreating ECS cluster: {get_cluster_name()}")
    try:
        ecs_client.create_cluster(
            clusterName=get_cluster_name(),
            capacityProviders=['FARGATE', 'FARGATE_SPOT']
        )
    except ecs_client.exceptions.ClusterExists:
        print("ECS cluster already exists")

###############################################################################

def _create_ecr_repository(ecr_client) -> None:
    """Create ECR repository if it doesn't exist."""
    print(f"\nCreating ECR repository: {get_ecr_repository_name()}")
    try:
        ecr_client.create_repository(repositoryName=get_ecr_repository_name())
    except ecr_client.exceptions.RepositoryAlreadyExistsException:
        print("ECR repository already exists")

###############################################################################

def _check_docker_daemon() -> None:
    """Check if Docker daemon is running."""
    try:
        subprocess.run(['docker', 'info'], check=True, capture_output=True, text=True)
        return
    except subprocess.CalledProcessError:
        raise RuntimeError(
            "\n========================================================================\n"
            "ERROR: Cannot connect to the Docker daemon.\n"
            "       Is Docker running? Please start Docker Desktop or the Docker service\n"
            "       and try again.\n"
            "========================================================================\n"
        )

###############################################################################

def _prepare_build_context(temp_dir: str, **kwargs) -> None:
    """Prepare Docker build context in a temporary directory."""
    print("Creating temporary build directory...")
    src_dir = os.path.join(temp_dir, 'src')
    os.makedirs(src_dir, exist_ok=True)
    cloudrun_dir = os.path.join(src_dir, 'cloudrun')
    os.makedirs(cloudrun_dir, exist_ok=True)

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    pyproject_toml = os.path.join(base_dir, 'pyproject.toml')
    
    if not os.path.exists(pyproject_toml):
        raise FileNotFoundError(f"pyproject.toml not found at {pyproject_toml}")
        
    shutil.copy2(pyproject_toml, temp_dir)
    print("Copied package configuration files")
    
    print("Copying package files...")
    files_to_copy = [
        '__init__.py',
        'cli.py',
        '_infrastructure.py',
    ]
    
    cloudrun_src_dir = os.path.join(base_dir, 'src', 'cloudrun')
    for file in files_to_copy:
        src = os.path.join(cloudrun_src_dir, file)
        shutil.copy2(src, cloudrun_dir)
    print("Copied package files")

    _prepare_docker_files(temp_dir, cloudrun_src_dir, **kwargs)

###############################################################################

def _prepare_docker_files(temp_dir: str, cloudrun_src_dir: str, **kwargs) -> None:
    """Prepare Docker-related files with optional custom commands."""
    print("Copying Docker-related files...")
    docker_dir_path = os.path.join(cloudrun_src_dir, 'docker')
    dockerfile_path = os.path.join(docker_dir_path, 'Dockerfile')
    if not os.path.exists(dockerfile_path):
        raise FileNotFoundError(f"Dockerfile not found at {dockerfile_path}")
    
    # Check if we have custom Docker commands
    custom_docker_commands = kwargs.get('custom_docker_commands')
    if custom_docker_commands:
        _create_custom_dockerfile(dockerfile_path, temp_dir, custom_docker_commands)
    else:
        # No custom commands, just copy the original
        shutil.copy2(dockerfile_path, temp_dir)
    
    print("Creating additional requirements file...")
    additional_requirements_text = kwargs.get('additional_requirements_text', '')
    
    with open(os.path.join(temp_dir, 'additional_requirements.txt'), 'w') as f:
        f.write(additional_requirements_text)

    docker_dir = os.path.join(os.path.join(temp_dir, 'src', 'cloudrun'), 'docker')
    os.makedirs(docker_dir, exist_ok=True)
    shutil.copy2(os.path.join(docker_dir_path, 'entrypoint.sh'), docker_dir)
    print("Copied Docker-related files")

###############################################################################

def _create_custom_dockerfile(dockerfile_path: str, temp_dir: str, custom_docker_commands: str) -> None:
    """Create a modified Dockerfile with custom commands."""
    print("Adding custom Docker commands to Dockerfile...")
    with open(dockerfile_path, 'r') as f:
        dockerfile_content = f.read()
    
    # Find the position after the system dependencies installation but before additional requirements
    insertion_point = dockerfile_content.find("# Install additional requirements if they exist")
    
    if insertion_point == -1:
        # Fallback to inserting before the last COPY command
        insertion_point = dockerfile_content.rfind("COPY additional_requirements.txt")
    
    if insertion_point != -1:
        # Insert custom commands
        modified_dockerfile = (
            dockerfile_content[:insertion_point] + 
            "# Custom Docker commands\n" + 
            custom_docker_commands + 
            "\n\n" + 
            dockerfile_content[insertion_point:]
        )
        
        # Write the modified Dockerfile
        temp_dockerfile_path = os.path.join(temp_dir, 'Dockerfile')
        with open(temp_dockerfile_path, 'w') as f:
            f.write(modified_dockerfile)
        print("Created modified Dockerfile with custom commands")
    else:
        # If insertion point not found, just copy the original
        shutil.copy2(dockerfile_path, temp_dir)
        print("Warning: Could not find insertion point for custom Docker commands. Using original Dockerfile.")

###############################################################################
def _docker_login_build_push(ecr_repo: str, region: str, temp_dir: str) -> None:
    """Login to ECR, build and push Docker image."""
    print("\nLogging into ECR...")
    try:
        password = subprocess.check_output([
            'aws', 'ecr', 'get-login-password',
            '--region', region
        ]).decode('utf-8').strip()
        
        subprocess.run([
            'docker', 'login',
            '--username', 'AWS',
            '--password-stdin',
            ecr_repo
        ], check=True, input=password.encode('utf-8'))
        
        print("\nBuilding Docker image...")
        subprocess.run([
            'docker', 'build',
            '--platform', 'linux/amd64',
            '-t', ecr_repo,
            temp_dir
        ], check=True)
        
        print("\nPushing Docker image to ECR...")
        subprocess.run(['docker', 'push', ecr_repo], check=True)
    except subprocess.CalledProcessError as e:
        _handle_docker_error(e)
    except Exception as e:
        if "docker" in str(e).lower():
            raise RuntimeError(f"\nERROR: An unexpected Docker-related error occurred: {str(e)}\nPlease ensure Docker is installed, running, and properly configured.") from e
        else:
            raise

###############################################################################

def _handle_docker_error(e: subprocess.CalledProcessError) -> None:
    """Handle Docker-related errors with clear error messages."""
    error_output = str(e.stderr) if e.stderr else str(e)
    if "Cannot connect to the Docker daemon" in error_output:
        raise RuntimeError("\nERROR: Cannot connect to the Docker daemon. Is Docker running? Please start Docker Desktop or the Docker service and try again.") from e
    elif "permission denied" in error_output.lower():
        raise RuntimeError("\nERROR: Permission denied when connecting to Docker. Make sure you have the right permissions and that Docker is running.") from e
    else:
        raise RuntimeError(f"\nERROR: Docker command failed. Please ensure Docker is running and properly configured: {error_output}") from e

###############################################################################

def _build_and_push_docker_image(ecr_repo: str, region: str, **kwargs) -> None:
    """Build and push Docker image to ECR."""
    print("\nPreparing Docker build context...")
    
    # Check if Docker daemon is running before proceeding
    _check_docker_daemon()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        _prepare_build_context(temp_dir, **kwargs)
        _docker_login_build_push(ecr_repo, region, temp_dir)

###############################################################################

def _create_task_definition(ecs_client, task_role: Dict[str, Any], ecr_repo: str, region: str) -> Dict[str, Any]:
    """Create ECS task definition."""
    print("\nCreating ECS task definition...")
    task_family = get_task_family()
    
    return ecs_client.register_task_definition(
        family=task_family,
        networkMode='awsvpc',
        requiresCompatibilities=['FARGATE'],
        cpu='256',
        memory='512',
        executionRoleArn=task_role['Role']['Arn'],
        taskRoleArn=task_role['Role']['Arn'],
        containerDefinitions=[{
            'name': 'cloudrun-executor',
            'image': ecr_repo,
            'essential': True,
            'logConfiguration': {
                'logDriver': 'awslogs',
                'options': {
                    'awslogs-group': '/ecs/cloudrun',
                    'awslogs-region': region,
                    'awslogs-stream-prefix': 'ecs'
                }
            }
        }]
    )

###############################################################################

def _delete_ecs_cluster(ecs_client) -> None:
    """Delete ECS cluster and its tasks."""
    print(f"\nDeleting ECS cluster and tasks...")
    cluster_name = f'cloudrun-cluster'
    try:
        tasks = ecs_client.list_tasks(cluster=cluster_name)
        if tasks.get('taskArns'):
            print("Stopping running tasks...")
            ecs_client.stop_task(
                cluster=cluster_name,
                task=tasks['taskArns'][0]
            )
            waiter = ecs_client.get_waiter('tasks_stopped')
            waiter.wait(cluster=cluster_name, tasks=tasks['taskArns'])
        
        print("Deleting cluster...")
        ecs_client.delete_cluster(cluster=cluster_name)
    except ecs_client.exceptions.ClusterNotFoundException:
        print("No ECS cluster found to delete")

###############################################################################

def _delete_task_definitions(ecs_client) -> None:
    """Delete all task definitions."""
    print(f"\nDeleting task definitions...")
    try:
        task_definitions = ecs_client.list_task_definitions(familyPrefix='cloudrun-task')
        for task_def in task_definitions.get('taskDefinitionArns', []):
            ecs_client.deregister_task_definition(taskDefinition=task_def)
    except ecs_client.exceptions.ClientException:
        print("No task definitions found to delete")

###############################################################################

def _delete_iam_role(iam_client) -> None:
    """Delete IAM role and its attached policies."""
    print(f"\nDeleting IAM roles...")
    roles_to_delete = ['cloudrun-task-role', 'cloudrun-lambda-role']
    
    for role_name in roles_to_delete:
        try:
            # First, detach managed policies
            print(f"Detaching policies from role {role_name}...")
            policies = iam_client.list_attached_role_policies(RoleName=role_name)
            for policy in policies.get('AttachedPolicies', []):
                iam_client.detach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy['PolicyArn']
                )
            
            # Next, delete inline policies
            print(f"Deleting inline policies from role {role_name}...")
            inline_policies = iam_client.list_role_policies(RoleName=role_name)
            for policy_name in inline_policies.get('PolicyNames', []):
                iam_client.delete_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name
                )
            
            print(f"Deleting role {role_name}...")
            iam_client.delete_role(RoleName=role_name)
        except iam_client.exceptions.NoSuchEntityException:
            print(f"No IAM role {role_name} found to delete")

###############################################################################

def _delete_s3_bucket(s3_client, region: str) -> None:
    """Delete S3 bucket and its contents."""
    print(f"\nDeleting S3 bucket...")
    bucket_name = get_bucket_name(region)
    if bucket_name:
        try:
            print("Deleting bucket contents...")
            objects = s3_client.list_objects_v2(Bucket=bucket_name)
            if objects.get('Contents'):
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
                )
            
            print("Deleting bucket...")
            s3_client.delete_bucket(Bucket=bucket_name)
        except s3_client.exceptions.NoSuchBucket:
            print("No S3 bucket found to delete")

###############################################################################

def _delete_ecr_repository(ecr_client) -> None:
    """Delete ECR repository."""
    print(f"\nDeleting ECR repository...")
    repo_name = f'cloudrun-executor'
    try:
        ecr_client.delete_repository(repositoryName=repo_name, force=True)
    except ecr_client.exceptions.RepositoryNotFoundException:
        print("No ECR repository found to delete")

###############################################################################

def create_infrastructure(**kwargs) -> Dict[str, Any]:
    """
    Initialize AWS infrastructure for CloudRun.
    Creates all necessary resources and saves configuration.
    
    Args:
        **kwargs: Additional arguments including:
            - region: Optional AWS region to use. If None, uses AWS_DEFAULT_REGION or us-east-1
            - additional_policies: Optional list of additional IAM policy ARNs to attach to the task role
            - vpc_id: Optional VPC ID to use for ECS tasks
            - subnet_id: Optional subnet ID to use for ECS tasks
            - additional_requirements_text: Optional string containing additional Python package requirements
            - custom_docker_commands: Optional string containing custom Docker commands to insert into the Dockerfile
            - force_rebuild: Optional boolean to force a rebuild of the infrastructure
    
    Returns:
        Dict[str, Any]: Dictionary containing created resource information
    """
    print(f"\n=== Starting CloudRun Infrastructure Creation ===")
    
    region = kwargs.get('region', 'us-east-1')
    print(f"Using AWS region: {region}")
    
    # Initialize AWS clients
    aws_clients = _initialize_aws_clients(region)
    
    # Create S3 bucket with environment name
    _create_s3_bucket(aws_clients['s3'], region)

    # Create ECS task execution role with environment name
    task_role = _create_task_role(aws_clients['iam'], get_task_role_name(), kwargs.get('additional_policies'))

    # Create ECS cluster with environment name
    _create_ecs_cluster(aws_clients['ecs'])

    # Create ECR repository with environment name
    _create_ecr_repository(aws_clients['ecr'])
    
    # Create task definition first
    ecr_repo = get_ecr_repository_url(aws_clients['sts'], region)
    _create_task_definition(aws_clients['ecs'], task_role, ecr_repo, region)

    # Build and push Docker image
    _build_and_push_docker_image(ecr_repo, region, **kwargs)

    print(f"\n=== CloudRun Infrastructure Creation Complete ===")

###############################################################################

def destroy_infrastructure(region: str) -> None:
    """
    Destroy all AWS infrastructure created by CloudRun for a specific environment.
    This includes ECS cluster, IAM roles, S3 bucket, and ECR repository.
    Also cleans up the configuration by removing all CLOUDRUN_ variables.
    
    Args:
        env_name: Name of the environment to destroy (default: 'default')
    """
    
    print(f"Using AWS region: {region}")
    
    # Initialize AWS clients
    aws_clients = _initialize_aws_clients(region)
    
    # Delete ECS cluster and tasks
    _delete_ecs_cluster(aws_clients['ecs'])
    
    # Delete task definition
    _delete_task_definitions(aws_clients['ecs'])
    
    # Delete IAM roles
    _delete_iam_role(aws_clients['iam'])
    
    # Delete S3 bucket
    _delete_s3_bucket(aws_clients['s3'])
    
    # Delete ECR repository
    _delete_ecr_repository(aws_clients['ecr'])

    print(f"\n=== CloudRun Infrastructure Destruction Complete ===")

###############################################################################

