import os
import boto3
import json
from typing import Dict, Any
from dotenv import load_dotenv
from datetime import datetime
import time
import subprocess
import shutil
import tempfile
import zipfile
import uuid
import logging
from cloudrun.dynamo_config import (
    get_config_value,
    set_config_value,
    validate_environment,
    clear_environment,
    create_dynamo_table,
    set_user_params,
    set_region,
    set_bucket_name,
    set_subnet_id,
    set_vpc_id,
    set_task_definition_arn,
    set_task_role_arn,
    set_ecr_repo,
    set_cluster_name,
    set_scheduler_lambda_arn,
    set_initialized,
    get_bucket_name,
    get_subnet_id,
    get_vpc_id,
    get_task_definition_arn,
    get_region,
    get_cluster_name,
    get_task_role_arn,
    check_initialization,
)

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
        'dynamodb': boto3.client('dynamodb', region_name=region)
    }

###############################################################################

def _create_s3_bucket(s3_client, bucket_name: str) -> None:
    """Create an S3 bucket if it doesn't exist."""
    print(f"\nCreating S3 bucket: {bucket_name}")
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except s3_client.exceptions.BucketAlreadyExists:
        print("S3 bucket already exists")

def _create_cloudwatch_log_group(logs_client) -> None:
    """Create CloudWatch log group if it doesn't exist."""
    print("\nCreating CloudWatch log group...")
    log_group = '/ecs/cloudrun'
    try:
        logs_client.create_log_group(logGroupName=log_group)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        print("CloudWatch log group already exists")

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

def _create_ecs_cluster(ecs_client, cluster_name: str) -> None:
    """Create ECS cluster if it doesn't exist."""
    print(f"\nCreating ECS cluster: {cluster_name}")
    try:
        ecs_client.create_cluster(
            clusterName=cluster_name,
            capacityProviders=['FARGATE', 'FARGATE_SPOT']
        )
    except ecs_client.exceptions.ClusterExists:
        print("ECS cluster already exists")
        # Update existing cluster to include FARGATE_SPOT if not already present
        try:
            cluster = ecs_client.describe_clusters(clusters=[cluster_name])['clusters'][0]
            if 'FARGATE_SPOT' not in cluster.get('capacityProviders', []):
                ecs_client.put_cluster_capacity_providers(
                    cluster=cluster_name,
                    capacityProviders=['FARGATE', 'FARGATE_SPOT'],
                    defaultCapacityProviderStrategy=[
                        {'capacityProvider': 'FARGATE', 'weight': 1}
                    ]
                )
        except ecs_client.exceptions.ClusterNotFoundException:
            pass

###############################################################################

def _create_ecr_repository(ecr_client, env_name: str) -> None:
    """Create ECR repository if it doesn't exist."""
    print(f"\nCreating ECR repository: cloudrun-executor-{env_name}")
    repo_name = f'cloudrun-executor-{env_name}'
    try:
        ecr_client.create_repository(repositoryName=repo_name)
    except ecr_client.exceptions.RepositoryAlreadyExistsException:
        print("ECR repository already exists")

###############################################################################

def _get_vpc_and_subnet(ec2_client, vpc_id: str = None, subnet_id: str = None) -> tuple:
    """Get VPC and subnet information, either from provided values or default."""
    print("\nGetting VPC and subnet information...")
    
    if not vpc_id or not subnet_id:
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
    else:
        print(f"Using provided VPC: {vpc_id}")
        print(f"Using provided subnet: {subnet_id}")
        
        subnet_info = ec2_client.describe_subnets(SubnetIds=[subnet_id])['Subnets'][0]
        if subnet_info['VpcId'] != vpc_id:
            raise Exception(f"Subnet {subnet_id} does not belong to VPC {vpc_id}")
    
    return vpc_id, subnet_id

###############################################################################

def _build_and_push_docker_image(ecr_repo: str, region: str, current_dir: str, docker_dir_path: str, **kwargs) -> None:
    """Build and push Docker image to ECR."""
    print("\nPreparing Docker build context...")
    
    # Check if Docker daemon is running before proceeding
    try:
        subprocess.run(['docker', 'info'], check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError:
        raise RuntimeError(
            "\n========================================================================\n"
            "ERROR: Cannot connect to the Docker daemon.\n"
            "       Is Docker running? Please start Docker Desktop or the Docker service\n"
            "       and try again.\n"
            "========================================================================\n"
        )
    
    with tempfile.TemporaryDirectory() as temp_dir:
        print("Creating temporary build directory...")
        src_dir = os.path.join(temp_dir, 'src')
        os.makedirs(src_dir, exist_ok=True)
        cloudrun_dir = os.path.join(src_dir, 'cloudrun')
        os.makedirs(cloudrun_dir, exist_ok=True)
        
        root_dir = os.path.dirname(os.path.dirname(current_dir))
        pyproject_toml = os.path.join(root_dir, 'pyproject.toml')
        
        if not os.path.exists(pyproject_toml):
            raise FileNotFoundError(f"pyproject.toml not found at {pyproject_toml}")
            
        shutil.copy2(pyproject_toml, temp_dir)
        print("Copied package configuration files")
        
        print("Copying package files...")
        files_to_copy = [
            '__init__.py',
            'cli.py',
            'logger.py',
            'infrastructure.py',
            'scheduler.py',
            'dynamo_config.py'
        ]
        
        for file in files_to_copy:
            src = os.path.join(current_dir, file)
            if os.path.exists(src):
                shutil.copy2(src, cloudrun_dir)
        print("Copied package files")
        
        print("Copying Docker-related files...")
        dockerfile_path = os.path.join(docker_dir_path, 'Dockerfile')
        if not os.path.exists(dockerfile_path):
            raise FileNotFoundError(f"Dockerfile not found at {dockerfile_path}")
        
        # Check if we have custom Docker commands
        custom_docker_commands = kwargs.get('custom_docker_commands')
        if custom_docker_commands:
            print("Adding custom Docker commands to Dockerfile...")
            with open(dockerfile_path, 'r') as f:
                dockerfile_content = f.read()
            
            # Find the position after the system dependencies installation but before additional requirements
            # Look for the marker indicating where to insert custom commands - after base package installation
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
        else:
            # No custom commands, just copy the original
            shutil.copy2(dockerfile_path, temp_dir)
        
        print("Creating additional requirements file...")
        additional_requirements_path = kwargs.get('additional_requirements_path')
        additional_requirements_text = kwargs.get('additional_requirements_text')
        
        if additional_requirements_path:
            if not os.path.exists(additional_requirements_path):
                raise FileNotFoundError(f"Additional requirements file not found at {additional_requirements_path}")
            shutil.copy2(additional_requirements_path, os.path.join(temp_dir, 'additional_requirements.txt'))
        elif additional_requirements_text:
            with open(os.path.join(temp_dir, 'additional_requirements.txt'), 'w') as f:
                f.write(additional_requirements_text)
        else:
            with open(os.path.join(temp_dir, 'additional_requirements.txt'), 'w') as f:
                f.write('')
        
        docker_dir = os.path.join(cloudrun_dir, 'docker')
        os.makedirs(docker_dir, exist_ok=True)
        shutil.copy2(os.path.join(docker_dir_path, 'entrypoint.sh'), docker_dir)
        print("Copied Docker-related files")
        
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
            error_output = str(e.stderr) if e.stderr else str(e)
            if "Cannot connect to the Docker daemon" in error_output:
                raise RuntimeError("\nERROR: Cannot connect to the Docker daemon. Is Docker running? Please start Docker Desktop or the Docker service and try again.") from e
            elif "permission denied" in error_output.lower():
                raise RuntimeError("\nERROR: Permission denied when connecting to Docker. Make sure you have the right permissions and that Docker is running.") from e
            else:
                raise RuntimeError(f"\nERROR: Docker command failed. Please ensure Docker is running and properly configured: {error_output}") from e
        except Exception as e:
            if "docker" in str(e).lower():
                raise RuntimeError(f"\nERROR: An unexpected Docker-related error occurred: {str(e)}\nPlease ensure Docker is installed, running, and properly configured.") from e
            else:
                raise

###############################################################################

def _create_task_definition(ecs_client, task_role: Dict[str, Any], ecr_repo: str, region: str) -> Dict[str, Any]:
    """Create ECS task definition."""
    print("\nCreating ECS task definition...")
    task_family = 'cloudrun-task'
    
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

def _save_configuration(env_vars: Dict[str, str], env_name: str) -> None:
    """Save configuration to config file."""
    print(f"\nSaving configuration for environment '{env_name}'...")
    for key, value in env_vars.items():
        set_config_value(key, value, env_name)
    print("Configuration saved")

###############################################################################

def _cleanup_config(env_name: str) -> None:
    """Clean up configuration by removing CLOUDRUN_ variables."""
    print(f"\nCleaning up configuration for environment '{env_name}'...")
    clear_environment(env_name)
    print("Configuration cleaned")

###############################################################################

def _delete_ecs_cluster(ecs_client, env_name: str) -> None:
    """Delete ECS cluster and its tasks."""
    print(f"\nDeleting ECS cluster and tasks for environment '{env_name}'...")
    cluster_name = f'cloudrun-cluster-{env_name}'
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

def _delete_task_definitions(ecs_client, env_name: str) -> None:
    """Delete all task definitions."""
    print(f"\nDeleting task definitions for environment '{env_name}'...")
    try:
        task_definitions = ecs_client.list_task_definitions(familyPrefix='cloudrun-task')
        for task_def in task_definitions.get('taskDefinitionArns', []):
            ecs_client.deregister_task_definition(taskDefinition=task_def)
    except ecs_client.exceptions.ClientException:
        print("No task definitions found to delete")

###############################################################################

def _delete_iam_role(iam_client, env_name: str) -> None:
    """Delete IAM role and its attached policies."""
    print(f"\nDeleting IAM roles for environment '{env_name}'...")
    roles_to_delete = [f'cloudrun-task-role-{env_name}', f'cloudrun-lambda-role-{env_name}']
    
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

def _delete_s3_bucket(s3_client, env_name: str) -> None:
    """Delete S3 bucket and its contents."""
    print(f"\nDeleting S3 bucket for environment '{env_name}'...")
    bucket_name = get_bucket_name(env_name)
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

def _delete_ecr_repository(ecr_client, env_name: str) -> None:
    """Delete ECR repository."""
    print(f"\nDeleting ECR repository for environment '{env_name}'...")
    repo_name = f'cloudrun-executor-{env_name}'
    try:
        ecr_client.delete_repository(repositoryName=repo_name, force=True)
    except ecr_client.exceptions.RepositoryNotFoundException:
        print("No ECR repository found to delete")

###############################################################################

def _load_infrastructure_settings() -> Dict[str, Any]:
    """Load infrastructure creation settings from file."""
    settings_file = '.cloudrun_settings.json'
    if os.path.exists(settings_file):
        with open(settings_file, 'r') as f:
            return json.load(f)
    return {}

###############################################################################

def _save_infrastructure_settings(settings: Dict[str, Any]) -> None:
    """Save infrastructure creation settings to file."""
    settings_file = '.cloudrun_settings.json'
    with open(settings_file, 'w') as f:
        json.dump(settings, f, indent=2)

###############################################################################

def _create_lambda_execution_role(iam_client, role_name: str) -> Dict[str, Any]:
    """Create a dedicated execution role for Lambda with proper trust relationship.
    
    Args:
        iam_client: IAM client
        role_name: Name for the Lambda execution role
        
    Returns:
        Dict[str, Any]: Role information
    """
    print("\nCreating Lambda execution role...")
    
    # Define inline policy
    inline_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "events:*",
                    "lambda:InvokeFunction",
                    "iam:PassRole",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            }
        ]
    }
    
    try:
        lambda_role = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                'Version': '2012-10-17',
                'Statement': [{
                    'Effect': 'Allow',
                    'Principal': {'Service': 'lambda.amazonaws.com'},
                    'Action': 'sts:AssumeRole'
                }]
            })
        )
        
        print("Attaching policies to Lambda role...")
        policies = [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
            'arn:aws:iam::aws:policy/AmazonECS_FullAccess',
            'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        ]
            
        for policy in policies:
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy
            )
        
        # Add an inline policy for additional permissions
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=f"{role_name}-inline-policy",
            PolicyDocument=json.dumps(inline_policy)
        )
        print("Added inline policy to Lambda role")
        
        # No static delay here - IAM role propagation is handled by retries in the calling function
        # See _create_scheduler_lambda where it catches "The role defined for the function cannot be assumed by Lambda"
        # errors and implements exponential backoff retries
        return lambda_role
        
    except iam_client.exceptions.EntityAlreadyExistsException:
        print("Lambda execution role already exists")
        lambda_role = iam_client.get_role(RoleName=role_name)
        
        # Ensure the inline policy exists on the existing role
        try:
            iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName=f"{role_name}-inline-policy",
                PolicyDocument=json.dumps(inline_policy)
            )
            print("Updated inline policy on existing Lambda role")
        except Exception as e:
            print(f"Warning: Failed to update inline policy: {str(e)}")
            
        return lambda_role

###############################################################################

def _create_scheduler_lambda(iam_client, region: str, env_name: str) -> Dict[str, Any]:
    """
    Create a Lambda function to execute scheduled jobs.
    
    Args:
        iam_client: IAM client
        region: AWS region
        env_name: Name of the environment
        
    Returns:
        Dict[str, Any]: Lambda function information
    """
    print(f"\nCreating scheduler Lambda function for environment '{env_name}'...")
    
    # Create Lambda client
    lambda_client = boto3.client('lambda', region_name=region)
    
    # Create Lambda execution role
    lambda_role = _create_lambda_execution_role(iam_client, f'cloudrun-lambda-role-{env_name}')
    lambda_role_arn = lambda_role['Role']['Arn']
    
    # Create a temporary directory for Lambda code
    with tempfile.TemporaryDirectory() as temp_dir:
        # Read Lambda function template from file
        lambda_template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lambda_function_template.py')
        with open(lambda_template_path, 'r') as f:
            lambda_code = f.read()
        
        # Write Lambda code to file
        lambda_file = os.path.join(temp_dir, 'lambda_function.py')
        with open(lambda_file, 'w') as f:
            f.write(lambda_code)
        
        # Create zip file
        zip_file = os.path.join(temp_dir, 'lambda_function.zip')
        with zipfile.ZipFile(zip_file, 'w') as z:
            z.write(lambda_file, 'lambda_function.py')
        
        # Read zip file
        with open(zip_file, 'rb') as f:
            zip_bytes = f.read()
        
        # Create Lambda function
        function_name = f'cloudrun-scheduler-{env_name}' # Use env-specific name
        
        # Try to update existing function first
        try:
            lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_bytes
            )
            print("Updated existing Lambda function code")
            
            # Update function configuration with retries
            max_retries = 5
            retry_delay = 5
            attempt = 0
            last_error = None
            
            while attempt < max_retries:
                try:
                    print(f"Updating Lambda function configuration (attempt {attempt + 1}/{max_retries})...")
                    lambda_client.update_function_configuration(
                        FunctionName=function_name,
                        Role=lambda_role_arn,
                        Environment={
                            'Variables': {
                                'CLOUDRUN_BUCKET_NAME': get_config_value('CLOUDRUN_BUCKET_NAME', env_name),
                                'CLOUDRUN_SUBNET_ID': get_config_value('CLOUDRUN_SUBNET_ID', env_name),
                                'CLOUDRUN_TASK_DEFINITION_ARN': get_config_value('CLOUDRUN_TASK_DEFINITION_ARN', env_name),
                                'CLOUDRUN_REGION': get_config_value('CLOUDRUN_REGION', env_name),
                                'CLOUDRUN_CLUSTER_NAME': get_cluster_name(env_name) # Pass cluster name
                            }
                        }
                    )
                    print("Successfully updated Lambda function configuration")
                    break
                except lambda_client.exceptions.ResourceConflictException as e:
                    last_error = e
                    print(f"Function update in progress, retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    attempt += 1
                except Exception as e:
                    # Some other error occurred, don't retry
                    print(f"Unexpected error updating Lambda function configuration: {str(e)}")
                    raise
            
            if attempt >= max_retries:
                print(f"Failed to update Lambda function configuration after {max_retries} attempts")
                if last_error:
                    raise last_error
        except lambda_client.exceptions.ResourceNotFoundException:
            # Function doesn't exist, create it with retries
            max_retries = 5
            retry_delay = 5
            attempt = 0
            last_error = None
            
            while attempt < max_retries:
                try:
                    print(f"Creating Lambda function (attempt {attempt + 1}/{max_retries})...")
                    lambda_client.create_function(
                        FunctionName=function_name,
                        Runtime='python3.9',
                        Role=lambda_role_arn,
                        Handler='lambda_function.lambda_handler',
                        Code={'ZipFile': zip_bytes},
                        Timeout=30,
                        MemorySize=128,
                        Description=f'CloudRun Scheduler Lambda Function for {env_name}', # Add env to description
                        Environment={
                            'Variables': {
                                'CLOUDRUN_BUCKET_NAME': get_bucket_name(env_name),
                                'CLOUDRUN_SUBNET_ID': get_subnet_id(env_name),
                                'CLOUDRUN_TASK_DEFINITION_ARN': get_task_definition_arn(env_name),
                                'CLOUDRUN_REGION': get_region(env_name),
                                'CLOUDRUN_CLUSTER_NAME': get_cluster_name(env_name) # Pass cluster name
                            }
                        }
                    )
                    print("Successfully created Lambda function")
                    break
                except lambda_client.exceptions.InvalidParameterValueException as e:
                    last_error = e
                    error_message = str(e)
                    if "The role defined for the function cannot be assumed by Lambda" in error_message:
                        print(f"IAM role propagation delay, retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        attempt += 1
                    else:
                        # This is another kind of parameter error, should not retry
                        raise
                except Exception as e:
                    # Some other error occurred, don't retry
                    print(f"Unexpected error creating Lambda function: {str(e)}")
                    raise
            
            if attempt >= max_retries:
                print(f"Failed to create Lambda function after {max_retries} attempts")
                if last_error:
                    raise last_error
        
        # Add permission for EventBridge to invoke the Lambda function
        try:
            lambda_client.add_permission(
                FunctionName=function_name,
                StatementId='AllowEventBridgeInvoke',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com'
            )
            print("Added permission for EventBridge to invoke Lambda function")
        except lambda_client.exceptions.ResourceConflictException:
            print("Permission for EventBridge to invoke Lambda function already exists")
            
        # Get Lambda function information
        lambda_function = lambda_client.get_function(FunctionName=function_name)
        return lambda_function

###############################################################################

def _delete_scheduled_jobs(region: str, env_name: str) -> None:
    """
    Delete all scheduled jobs created by CloudRun.
    
    Args:
        region: AWS region
        env_name: Name of the environment
    """
    print(f"\nDeleting scheduled jobs for environment '{env_name}'...")
    events = boto3.client('events', region_name=region)
    lambda_client = boto3.client('lambda', region_name=region)
    
    # Construct the expected Lambda function name
    lambda_function_name = f'cloudrun-scheduler-{env_name}'
    
    # Try to find all EventBridge rules with 'cloudrun-' in their name
    # This is a more general approach that doesn't depend on the Lambda function existing
    try:
        # List all rules first
        all_rules = events.list_rules()['Rules']
        # Filter for rules potentially related to this environment
        env_prefix = f'cloudrun-{env_name}-'
        cloudrun_rules = [rule for rule in all_rules if rule['Name'].startswith(env_prefix)]
        
        # If we found any cloudrun rules, delete them
        deleted_count = 0
        processed_rule_names = set() # Keep track of rules already handled

        for rule in cloudrun_rules:
            rule_name = rule['Name']
            processed_rule_names.add(rule_name)
            # Don't expose the prefix in the logs
            display_name = rule_name[len(env_prefix):]
            print(f"Found scheduled job: {display_name}")
            
            try:
                # Get all targets for this rule
                targets = events.list_targets_by_rule(Rule=rule_name)['Targets']
                
                if targets:
                    # Remove all targets from the rule first
                    target_ids = [target['Id'] for target in targets]
                    events.remove_targets(
                        Rule=rule_name,
                        Ids=target_ids
                    )
                
                # Delete the rule
                events.delete_rule(
                    Name=rule_name
                )
                print(f"Deleted scheduled job: {display_name}")
                deleted_count += 1
                
            except Exception as e:
                print(f"Error deleting job {display_name}: {str(e)}")
                
        # Now also try the original approach to catch any rules targeting our Lambda function
        # that might have been missed by the naming convention (less likely now)
        try:
            lambda_function = lambda_client.get_function(FunctionName=lambda_function_name)
            lambda_arn = lambda_function['Configuration']['FunctionArn']
            
            # List all rules that might not have been caught by the previous filter
            remaining_rules = events.list_rules()['Rules']
            
            for rule in remaining_rules:
                rule_name = rule['Name']
                # Skip rules we already processed
                if rule_name in processed_rule_names:
                    continue
                    
                try:
                    targets = events.list_targets_by_rule(Rule=rule_name)['Targets']
                    for target in targets:
                        if target['Arn'] == lambda_arn:
                            # Found a target pointing to our Lambda function
                            display_name = rule_name
                            if rule_name.startswith(env_prefix):
                                display_name = rule_name[len(env_prefix):]

                            # Remove the target
                            events.remove_targets(
                                Rule=rule_name,
                                Ids=[target['Id']]
                            )
                            
                            # Delete the rule
                            events.delete_rule(
                                Name=rule_name
                            )
                            
                            print(f"Deleted job (found via Lambda target): {display_name}")
                            deleted_count += 1
                            processed_rule_names.add(rule_name) # Mark as processed
                            break # Move to the next rule
                except Exception as e:
                    display_name = rule_name
                    if rule_name.startswith(env_prefix):
                        display_name = rule_name[len(env_prefix):]
                    print(f"Error checking targets for job {display_name}: {str(e)}")
                    
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"Scheduler Lambda function '{lambda_function_name}' not found, skipping Lambda-target specific cleanup")
            
        if deleted_count > 0:
            print(f"Deleted {deleted_count} scheduled jobs")
        else:
            print("No scheduled jobs found")
            
    except Exception as e:
        print(f"Error cleaning up scheduled jobs: {str(e)}")

###############################################################################

def _check_infrastructure_changes(env_name: str, region: str, **kwargs) -> bool:
    """
    Check if infrastructure parameters have changed from what's stored in config.
    
    Args:
        env_name: Name of the environment
        region: AWS region
        **kwargs: Additional infrastructure parameters
        
    Returns:
        bool: True if parameters have changed, False otherwise
    """

    if kwargs.get('force_rebuild'):
        return True

    # Get current parameters
    current_params = {
        'CLOUDRUN_REGION': region,
        'CLOUDRUN_ADDITIONAL_POLICIES': kwargs.get('additional_policies', []),
        'CLOUDRUN_ADDITIONAL_REQUIREMENTS_TEXT': kwargs.get('additional_requirements_text'),
        'CLOUDRUN_ADDITIONAL_REQUIREMENTS_PATH': kwargs.get('additional_requirements_path'),
        'CLOUDRUN_CUSTOM_DOCKER_COMMANDS': kwargs.get('custom_docker_commands')
    }
    
    # Compare parameters with stored values
    for key, current_value in current_params.items():
        stored_value = get_config_value(key, env_name)
        if current_value != stored_value:
            print(f"Infrastructure parameter '{key}' has changed:")
            print(f"  Current: {current_value}")
            print(f"  Stored: {stored_value}")
            return True
    
    return False

###############################################################################

def create_infrastructure(env_name: str = 'default', region: str = None, **kwargs) -> Dict[str, Any]:
    """
    Initialize AWS infrastructure for CloudRun.
    Creates all necessary resources and saves configuration.
    
    Args:
        env_name: Name of the environment to create (default: 'default')
        region: AWS region to use. If None, uses AWS_DEFAULT_REGION or us-east-1
        **kwargs: Additional arguments including:
            - additional_policies: Optional list of additional IAM policy ARNs to attach to the task role
            - vpc_id: Optional VPC ID to use for ECS tasks
            - subnet_id: Optional subnet ID to use for ECS tasks
            - additional_requirements_text: Optional string containing additional Python package requirements
            - additional_requirements_path: Optional path to a requirements.txt file
            - custom_docker_commands: Optional string containing custom Docker commands to insert into the Dockerfile
            - force_rebuild: Optional boolean to force a rebuild of the infrastructure
    
    Returns:
        Dict[str, Any]: Dictionary containing created resource information
    """
    print(f"\n=== Starting CloudRun Infrastructure Creation for environment '{env_name}' ===")
    

    # Set region
    if region:
        set_region(region, env_name)
    else:
        region = get_config_value('CLOUDRUN_REGION', env_name, 'us-east-1')
        set_region(region, env_name)

    print(f"Using AWS region: {region}")
    
    # Initialize AWS clients
    aws_clients = _initialize_aws_clients(region)
    
    # Check if infrastructure parameters have changed
    if check_initialization(env_name):
        if not _check_infrastructure_changes(env_name, region, **kwargs):
            print(f"Infrastructure parameters unchanged for environment '{env_name}', skipping creation.")
            return {
                'CLOUDRUN_REGION': region,
                'CLOUDRUN_BUCKET_NAME': get_config_value('CLOUDRUN_BUCKET_NAME', env_name),
                'CLOUDRUN_SUBNET_ID': get_config_value('CLOUDRUN_SUBNET_ID', env_name),
                'CLOUDRUN_VPC_ID': get_config_value('CLOUDRUN_VPC_ID', env_name),
                'CLOUDRUN_TASK_DEFINITION_ARN': get_config_value('CLOUDRUN_TASK_DEFINITION_ARN', env_name),
                'CLOUDRUN_TASK_FAMILY': get_config_value('CLOUDRUN_TASK_FAMILY', env_name),
                'CLOUDRUN_TASK_ROLE_ARN': get_config_value('CLOUDRUN_TASK_ROLE_ARN', env_name),
                'CLOUDRUN_ECR_REPO': get_config_value('CLOUDRUN_ECR_REPO', env_name),
                'CLOUDRUN_CLUSTER_NAME': get_config_value('CLOUDRUN_CLUSTER_NAME', env_name),
                'CLOUDRUN_SCHEDULER_LAMBDA_ARN': get_config_value('CLOUDRUN_SCHEDULER_LAMBDA_ARN', env_name),
            }
        else:
            print(f"Infrastructure parameters have changed for environment '{env_name}', destroying existing infrastructure...")
            destroy_infrastructure(env_name)
    
    set_user_params(kwargs, env_name)


    bucket_name = f"cloudrun-bucket-{env_name}-{region}"
    set_bucket_name(bucket_name, env_name)

    # Create S3 bucket with environment name
    _create_s3_bucket(aws_clients['s3'], bucket_name)

    # Create CloudWatch log group
    _create_cloudwatch_log_group(aws_clients['logs'])

    # Create ECS task execution role with environment name
    task_role = _create_task_role(aws_clients['iam'], f'cloudrun-task-role-{env_name}', kwargs.get('additional_policies'))
    set_task_role_arn(task_role['Role']['Arn'], env_name)

    # Create ECS cluster with environment name
    cluster_name = f'cloudrun-cluster-{env_name}'
    _create_ecs_cluster(aws_clients['ecs'], cluster_name)
    set_cluster_name(cluster_name, env_name)

    # Create ECR repository with environment name
    _create_ecr_repository(aws_clients['ecr'], env_name)
    
    # Get default VPC and subnet
    vpc_id, subnet_id = _get_vpc_and_subnet(aws_clients['ec2'], kwargs.get('vpc_id'), kwargs.get('subnet_id'))
    set_vpc_id(vpc_id, env_name)
    set_subnet_id(subnet_id, env_name)
    
    # Get AWS account ID
    print("\nGetting AWS account information...")
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    print(f"Using AWS account: {account_id}")
    
    # Create task definition first
    ecr_repo = f'{account_id}.dkr.ecr.{region}.amazonaws.com/cloudrun-executor-{env_name}'
    task_definition = _create_task_definition(aws_clients['ecs'], task_role, ecr_repo, region)
    set_task_definition_arn(task_definition['taskDefinition']['taskDefinitionArn'], env_name)
    set_ecr_repo(ecr_repo, env_name)

    # Create a scheduled job Lambda function with environment name
    lambda_function = _create_scheduler_lambda(aws_clients['iam'], region, env_name)
    set_scheduler_lambda_arn(lambda_function['Configuration']['FunctionArn'], env_name)
    
    # Build and push Docker image
    _build_and_push_docker_image(ecr_repo, region, os.path.dirname(os.path.abspath(__file__)), os.path.join(os.path.dirname(os.path.abspath(__file__)), 'docker'), **kwargs)

    # Save configuration
    env_vars = {
        'CLOUDRUN_REGION': region,
        'CLOUDRUN_BUCKET_NAME': bucket_name,
        'CLOUDRUN_SUBNET_ID': subnet_id,
        'CLOUDRUN_VPC_ID': vpc_id,
        'CLOUDRUN_TASK_DEFINITION_ARN': task_definition['taskDefinition']['taskDefinitionArn'],
        'CLOUDRUN_TASK_FAMILY': task_definition['taskDefinition']['family'],
        'CLOUDRUN_TASK_ROLE_ARN': task_role['Role']['Arn'],
        'CLOUDRUN_ECR_REPO': ecr_repo,
        'CLOUDRUN_CLUSTER_NAME': cluster_name,
        'CLOUDRUN_SCHEDULER_LAMBDA_ARN': lambda_function['Configuration']['FunctionArn'],
        # Store infrastructure parameters
        'CLOUDRUN_ADDITIONAL_POLICIES': kwargs.get('additional_policies', []),
        'CLOUDRUN_ADDITIONAL_REQUIREMENTS_TEXT': kwargs.get('additional_requirements_text'),
        'CLOUDRUN_ADDITIONAL_REQUIREMENTS_PATH': kwargs.get('additional_requirements_path'),
        'CLOUDRUN_CUSTOM_DOCKER_COMMANDS': kwargs.get('custom_docker_commands')
    }
    for key, value in env_vars.items():
        set_config_value(key, value, env_name)
    
    set_initialized(True, env_name)
    
    print(f"\n=== CloudRun Infrastructure Creation Complete for environment '{env_name}' ===")
    return env_vars

###############################################################################

def destroy_infrastructure(env_name: str = 'default') -> None:
    """
    Destroy all AWS infrastructure created by CloudRun for a specific environment.
    This includes ECS cluster, IAM roles, S3 bucket, and ECR repository.
    Also cleans up the configuration by removing all CLOUDRUN_ variables.
    
    Args:
        env_name: Name of the environment to destroy (default: 'default')
    """
    print(f"\n=== Starting CloudRun Infrastructure Destruction for environment '{env_name}' ===")
    
    region = get_config_value('CLOUDRUN_REGION', env_name, 'us-east-1')
    print(f"Using AWS region: {region}")
    
    # Initialize AWS clients
    aws_clients = _initialize_aws_clients(region)
    
    # Delete all scheduled jobs first
    _delete_scheduled_jobs(region, env_name)
    
    # Delete ECS cluster and tasks
    _delete_ecs_cluster(aws_clients['ecs'], env_name)
    
    # Delete task definition
    _delete_task_definitions(aws_clients['ecs'], env_name)
    
    # Delete IAM roles
    _delete_iam_role(aws_clients['iam'], env_name)
    
    # Delete S3 bucket
    _delete_s3_bucket(aws_clients['s3'], env_name)
    
    # Delete ECR repository
    _delete_ecr_repository(aws_clients['ecr'], env_name)

    # Delete scheduled jobs Lambda function
    print("\nDeleting scheduled jobs Lambda function...")
    lambda_client = boto3.client('lambda', region_name=region)
    try:
        lambda_client.delete_function(
            FunctionName=f'cloudrun-scheduler-{env_name}'
        )
        print("Lambda function deleted")
    except lambda_client.exceptions.ResourceNotFoundException:
        print("Lambda function not found or already deleted")

    # Clean up configuration
    clear_environment(env_name)
    
    print(f"\n=== CloudRun Infrastructure Destruction Complete for environment '{env_name}' ===")

###############################################################################

def rebuild_infrastructure(env_name: str = 'default') -> Dict[str, Any]:
    """
    Rebuild the AWS infrastructure using previously saved settings.
    This will destroy the existing infrastructure and recreate it with the same settings.
    
    Args:
        env_name: Name of the environment to rebuild (default: 'default')
        
    Returns:
        Dict[str, Any]: Dictionary containing created resource information
    """
    print(f"\n=== Starting CloudRun Infrastructure Rebuild for environment '{env_name}' ===")
    
    # Load previous settings
    settings = _load_infrastructure_settings()
    if not settings:
        raise ValueError("No previous infrastructure settings found. Please use create_infrastructure first.")
    
    # Destroy existing infrastructure
    destroy_infrastructure(env_name)
    
    # Recreate infrastructure with saved settings
    return create_infrastructure(env_name=env_name, **settings)

############################################################################### 

