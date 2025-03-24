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
        'ec2': boto3.client('ec2', region_name=region)
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

def _create_ecs_cluster(ecs_client) -> None:
    """Create ECS cluster if it doesn't exist."""
    print("\nCreating ECS cluster...")
    cluster_name = 'cloudrun-cluster'
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

def _create_ecr_repository(ecr_client) -> None:
    """Create ECR repository if it doesn't exist."""
    print("\nCreating ECR repository...")
    repo_name = 'cloudrun-executor'
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
    
    with tempfile.TemporaryDirectory() as temp_dir:
        print("Creating temporary build directory...")
        src_dir = os.path.join(temp_dir, 'src')
        os.makedirs(src_dir, exist_ok=True)
        cloudrun_dir = os.path.join(src_dir, 'cloudrun')
        os.makedirs(cloudrun_dir, exist_ok=True)
        
        root_dir = os.path.dirname(os.path.dirname(current_dir))
        setup_py = os.path.join(root_dir, 'setup.py')
        pyproject_toml = os.path.join(root_dir, 'pyproject.toml')
        
        if not os.path.exists(pyproject_toml):
            raise FileNotFoundError(f"pyproject.toml not found at {pyproject_toml}")
            
        shutil.copy2(setup_py, temp_dir)
        shutil.copy2(pyproject_toml, temp_dir)
        print("Copied package configuration files")
        
        print("Copying package files...")
        files_to_copy = [
            '__init__.py',
            'cli.py',
            'logger.py',
            'infrastructure.py',
            'scheduler.py'
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

def _save_configuration(env_vars: Dict[str, str]) -> None:
    """Save configuration to .env file."""
    print("\nSaving configuration to .env file...")
    with open('.env', 'a') as f:
        for key, value in env_vars.items():
            f.write(f'{key}={value}\n')
    print("Configuration saved")

###############################################################################

def _delete_ecs_cluster(ecs_client) -> None:
    """Delete ECS cluster and its tasks."""
    print("\nDeleting ECS cluster and tasks...")
    cluster_name = 'cloudrun-cluster'
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
    print("\nDeleting task definitions...")
    try:
        task_definitions = ecs_client.list_task_definitions(familyPrefix='cloudrun-task')
        for task_def in task_definitions.get('taskDefinitionArns', []):
            ecs_client.deregister_task_definition(taskDefinition=task_def)
    except ecs_client.exceptions.ClientException:
        print("No task definitions found to delete")

###############################################################################

def _delete_iam_role(iam_client) -> None:
    """Delete IAM role and its attached policies."""
    print("\nDeleting IAM roles...")
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

def _delete_s3_bucket(s3_client) -> None:
    """Delete S3 bucket and its contents."""
    print("\nDeleting S3 bucket...")
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME')
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
    print("\nDeleting ECR repository...")
    repo_name = 'cloudrun-executor'
    try:
        ecr_client.delete_repository(repositoryName=repo_name, force=True)
    except ecr_client.exceptions.RepositoryNotFoundException:
        print("No ECR repository found to delete")

###############################################################################

def _cleanup_env_file() -> None:
    """Clean up .env file by removing CLOUDRUN_ variables."""
    print("\nCleaning up .env file...")
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            lines = f.readlines()
        with open('.env', 'w') as f:
            for line in lines:
                if not line.startswith('CLOUDRUN_'):
                    f.write(line)
        print("Configuration cleaned")

###############################################################################

def _save_infrastructure_settings(settings: Dict[str, Any]) -> None:
    """Save infrastructure creation settings to a file."""
    print("\nSaving infrastructure settings...")
    settings_file = '.cloudrun_settings.json'
    with open(settings_file, 'w') as f:
        json.dump(settings, f, indent=2)
    print("Settings saved")

###############################################################################

def _load_infrastructure_settings() -> Dict[str, Any]:
    """Load infrastructure creation settings from file."""
    settings_file = '.cloudrun_settings.json'
    if os.path.exists(settings_file):
        with open(settings_file, 'r') as f:
            return json.load(f)
    return {}

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
        
        # Add a small delay to allow IAM role propagation
        time.sleep(10)
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

def _create_scheduler_lambda(iam_client, role_arn: str, region: str) -> Dict[str, Any]:
    """
    Create a Lambda function to execute scheduled jobs.
    
    Args:
        iam_client: IAM client
        role_arn: Role ARN to use for the Lambda function
        region: AWS region
        
    Returns:
        Dict[str, Any]: Lambda function information
    """
    print("\nCreating scheduler Lambda function...")
    
    # Create Lambda client
    lambda_client = boto3.client('lambda', region_name=region)
    
    # Create Lambda execution role
    lambda_role = _create_lambda_execution_role(iam_client, 'cloudrun-lambda-role')
    lambda_role_arn = lambda_role['Role']['Arn']
    
    # Create a temporary directory for Lambda code
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create Lambda function code
        lambda_code = '''
import os
import boto3
import json
import uuid
import time
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to execute CloudRun jobs by launching ECS tasks directly.
    
    Args:
        event: Event data containing job configuration
        context: Lambda context
        
    Returns:
        Dict[str, Any]: Job execution information
    """
    logger.info(f"CloudRun Scheduler Lambda invoked with event: {json.dumps(event)}")
    
    # Get job configuration from event
    script_path = event.get('script_path')
    vcpus = event.get('vcpus', 0.25)
    memory = event.get('memory', 512)
    use_spot = event.get('use_spot', False)
    method_name = event.get('method_name')
    params = event.get('params')
    s3_key = event.get('s3_key')
    
    logger.info(f"Job configuration: script_path={script_path}, vcpus={vcpus}, memory={memory}, "
                f"use_spot={use_spot}, method_name={method_name}, s3_key={s3_key}")
    
    # Get environment variables
    bucket_name = os.environ.get('CLOUDRUN_BUCKET_NAME')
    subnet_id = os.environ.get('CLOUDRUN_SUBNET_ID')
    task_definition_arn = os.environ.get('CLOUDRUN_TASK_DEFINITION_ARN')
    region = os.environ.get('CLOUDRUN_REGION', 'us-east-1')
    
    logger.info(f"Environment configuration: bucket_name={bucket_name}, subnet_id={subnet_id}, "
                f"region={region}, task_definition_arn={task_definition_arn}")
    
    # Validate required parameters
    if not all([bucket_name, subnet_id, task_definition_arn, script_path, s3_key]):
        error_msg = "Missing required parameters. Ensure all environment variables and required event fields are set."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Calculate CPU units
    cpu_units = str(int(vcpus * 1024))
    logger.info(f"Calculated CPU units: {cpu_units}")
    
    # Generate a custom task ID with timestamp for uniqueness
    custom_task_id = f"scheduled-task-{int(time.time())}-{str(uuid.uuid4())[:8]}"
    logger.info(f"Generated task ID: {custom_task_id}")
    
    # Prepare command for the container
    command = [bucket_name, s3_key, script_path]
    if method_name:
        command.append(method_name)
    if params:
        command.append(json.dumps(params))
    
    logger.info(f"Container command: {command}")
    
    # Prepare task parameters
    task_params = {
        'cluster': 'cloudrun-cluster',
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
                'environment': [
                    {
                        'name': 'CLOUDRUN_TASK_ID',
                        'value': custom_task_id
                    }
                ]
            }]
        }
    }
    
    # Add spot configuration if requested
    if use_spot:
        task_params['capacityProviderStrategy'] = [{
            'capacityProvider': 'FARGATE_SPOT',
            'weight': 1
        }]
        logger.info("Using FARGATE_SPOT capacity provider")
    else:
        task_params['launchType'] = 'FARGATE'
        logger.info("Using standard FARGATE launch type")
    
    # Run the task
    try:
        logger.info(f"Attempting to run ECS task with parameters: {json.dumps(task_params)}")
        ecs = boto3.client('ecs', region_name=region)
        response = ecs.run_task(**task_params)
        
        if response.get('tasks'):
            task_arn = response['tasks'][0]['taskArn']
            logger.info(f"Successfully started ECS task: {task_arn}")
        else:
            logger.warning(f"Task started but no task ARN returned. Full response: {json.dumps(response)}")
            task_arn = None
            
        if response.get('failures'):
            logger.error(f"Received failures when starting task: {json.dumps(response['failures'])}")
    except Exception as e:
        error_msg = f"Error running ECS task: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    result = {
        'job_id': custom_task_id,
        'task_arn': task_arn
    }
    
    logger.info(f"Lambda execution completed successfully. Result: {json.dumps(result)}")
    return result
'''
        
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
        function_name = 'cloudrun-scheduler'
        
        # Try to update existing function first
        try:
            lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_bytes
            )
            print("Updated existing Lambda function code")
            
            # Update function configuration
            lambda_client.update_function_configuration(
                FunctionName=function_name,
                Role=lambda_role_arn,
                Environment={
                    'Variables': {
                        'CLOUDRUN_BUCKET_NAME': os.getenv('CLOUDRUN_BUCKET_NAME', ''),
                        'CLOUDRUN_SUBNET_ID': os.getenv('CLOUDRUN_SUBNET_ID', ''),
                        'CLOUDRUN_TASK_DEFINITION_ARN': os.getenv('CLOUDRUN_TASK_DEFINITION_ARN', ''),
                        'CLOUDRUN_REGION': os.getenv('CLOUDRUN_REGION', 'us-east-1')
                    }
                }
            )
            print("Updated Lambda function configuration")
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
                        Description='CloudRun Scheduler Lambda Function',
                        Environment={
                            'Variables': {
                                'CLOUDRUN_BUCKET_NAME': os.getenv('CLOUDRUN_BUCKET_NAME', ''),
                                'CLOUDRUN_SUBNET_ID': os.getenv('CLOUDRUN_SUBNET_ID', ''),
                                'CLOUDRUN_TASK_DEFINITION_ARN': os.getenv('CLOUDRUN_TASK_DEFINITION_ARN', ''),
                                'CLOUDRUN_REGION': os.getenv('CLOUDRUN_REGION', 'us-east-1')
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

def _attach_scheduler_policies(iam_client, task_role_name: str) -> None:
    """
    Attach policies needed for scheduled jobs to the task role.
    
    Args:
        iam_client: IAM client
        task_role_name: Task role name
    """
    print("\nAttaching scheduler policies to task role...")
    
    # Create a policy for EventBridge to invoke Lambda
    policy_name = f"{task_role_name}-scheduler-policy"
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "events:PutRule",
                    "events:PutTargets",
                    "events:DeleteRule",
                    "events:RemoveTargets",
                    "events:ListRules",
                    "events:ListTargetsByRule",
                    "lambda:InvokeFunction",
                    "ecs:RunTask",
                    "ecs:DescribeTasks",
                    "iam:PassRole",
                    "s3:GetObject"
                ],
                "Resource": "*"
            }
        ]
    }
    
    # Get account ID
    account_id = boto3.client('sts').get_caller_identity()['Account']
    policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"
    
    try:
        # Try to create a new policy
        iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )
        print("Created scheduler policy")
    except iam_client.exceptions.EntityAlreadyExistsException:
        # If policy exists, always delete the oldest non-default version before creating a new one
        print("Policy already exists, updating...")
        
        try:
            # Get all policy versions
            policy_versions = iam_client.list_policy_versions(
                PolicyArn=policy_arn
            )['Versions']
            
            # Find non-default versions
            non_default_versions = [v for v in policy_versions if not v.get('IsDefaultVersion')]
            
            # If there are any non-default versions, delete the oldest one
            if non_default_versions:
                # Sort by create date (oldest first)
                non_default_versions.sort(key=lambda x: x['CreateDate'])
                oldest_version = non_default_versions[0]
                
                print(f"Deleting policy version: {oldest_version['VersionId']}")
                iam_client.delete_policy_version(
                    PolicyArn=policy_arn,
                    VersionId=oldest_version['VersionId']
                )
            
            # Create new policy version
            iam_client.create_policy_version(
                PolicyArn=policy_arn,
                PolicyDocument=json.dumps(policy_document),
                SetAsDefault=True
            )
            print("Updated existing scheduler policy")
            
        except Exception as e:
            print(f"Warning: Failed to update policy version: {str(e)}")
            print("Continuing with existing policy")
    
    # Attach policy to task role
    try:
        iam_client.attach_role_policy(
            RoleName=task_role_name,
            PolicyArn=policy_arn
        )
        print("Attached scheduler policy to task role")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print("Scheduler policy already attached to task role")

###############################################################################

def _delete_scheduled_jobs(region: str) -> None:
    """
    Delete all scheduled jobs created by CloudRun.
    
    Args:
        region: AWS region
    """
    print("\nDeleting scheduled jobs...")
    events = boto3.client('events', region_name=region)
    lambda_client = boto3.client('lambda', region_name=region)
    
    # Try to find all EventBridge rules with 'cloudrun-' in their name
    # This is a more general approach that doesn't depend on the Lambda function existing
    try:
        # List all rules first
        all_rules = events.list_rules()['Rules']
        cloudrun_rules = [rule for rule in all_rules if 'cloudrun-' in rule['Name'].lower()]
        
        # If we found any cloudrun rules, delete them
        deleted_count = 0
        for rule in cloudrun_rules:
            rule_name = rule['Name']
            # Don't expose the prefix in the logs
            display_name = rule_name[9:] if rule_name.startswith('cloudrun-') else rule_name
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
                
        # Now also try the original approach to catch any rules that don't have 'cloudrun' in name
        # but are targeting our Lambda function
        try:
            lambda_function = lambda_client.get_function(FunctionName='cloudrun-scheduler')
            lambda_arn = lambda_function['Configuration']['FunctionArn']
            
            # List all rules that might not have been caught by the previous filter
            remaining_rules = events.list_rules()['Rules']
            
            for rule in remaining_rules:
                rule_name = rule['Name']
                # Skip rules we already processed
                if rule in cloudrun_rules:
                    continue
                    
                try:
                    targets = events.list_targets_by_rule(Rule=rule_name)['Targets']
                    for target in targets:
                        if target['Arn'] == lambda_arn:
                            # Found a target pointing to our Lambda function
                            # Don't expose the prefix in the logs
                            display_name = rule_name[9:] if rule_name.startswith('cloudrun-') else rule_name
                            
                            # Remove the target
                            events.remove_targets(
                                Rule=rule_name,
                                Ids=[target['Id']]
                            )
                            
                            # Delete the rule
                            events.delete_rule(
                                Name=rule_name
                            )
                            
                            print(f"Deleted job: {display_name}")
                            deleted_count += 1
                            break
                except Exception as e:
                    # Don't expose the prefix in the logs
                    display_name = rule_name[9:] if rule_name.startswith('cloudrun-') else rule_name
                    print(f"Error checking targets for job {display_name}: {str(e)}")
                    
        except lambda_client.exceptions.ResourceNotFoundException:
            print("Scheduler Lambda function not found, skipping Lambda-target specific cleanup")
            
        if deleted_count > 0:
            print(f"Deleted {deleted_count} scheduled jobs")
        else:
            print("No scheduled jobs found")
            
    except Exception as e:
        print(f"Error cleaning up scheduled jobs: {str(e)}")

###############################################################################

def create_infrastructure(region: str = None, **kwargs) -> Dict[str, Any]:
    """
    Initialize AWS infrastructure for CloudRun.
    Creates all necessary resources and saves configuration.
    
    Args:
        region: AWS region to use. If None, uses AWS_DEFAULT_REGION or us-east-1
        **kwargs: Additional arguments including:
            - additional_policies: Optional list of additional IAM policy ARNs to attach to the task role
            - vpc_id: Optional VPC ID to use for ECS tasks
            - subnet_id: Optional subnet ID to use for ECS tasks
            - additional_requirements_text: Optional string containing additional Python package requirements
            - additional_requirements_path: Optional path to a requirements.txt file
            - custom_docker_commands: Optional string containing custom Docker commands to insert into the Dockerfile
    
    Returns:
        Dict[str, Any]: Dictionary containing created resource information
    """
    print("\n=== Starting CloudRun Infrastructure Creation ===")
    load_dotenv()
    
    # Set region
    if region:
        os.environ['AWS_DEFAULT_REGION'] = region
    elif 'AWS_DEFAULT_REGION' not in os.environ:
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    
    region = os.getenv('AWS_DEFAULT_REGION')
    print(f"Using AWS region: {region}")
    
    # Save settings for future use
    settings = {
        'region': region,
        **kwargs
    }
    _save_infrastructure_settings(settings)
    
    # Initialize AWS clients
    aws_clients = _initialize_aws_clients(region)
    
    # Create S3 bucket
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME', f'cloudrun-{region}-{os.getenv("USER", "default")}')
    _create_s3_bucket(aws_clients['s3'], bucket_name)

    # Create CloudWatch log group
    _create_cloudwatch_log_group(aws_clients['logs'])

    # Create ECS task execution role
    task_role = _create_task_role(aws_clients['iam'], 'cloudrun-task-role', kwargs.get('additional_policies'))

    # Create ECS cluster
    _create_ecs_cluster(aws_clients['ecs'])

    # Create ECR repository
    _create_ecr_repository(aws_clients['ecr'])
    
    # Get default VPC and subnet
    vpc_id, subnet_id = _get_vpc_and_subnet(aws_clients['ec2'], kwargs.get('vpc_id'), kwargs.get('subnet_id'))
    
    # Get AWS account ID
    print("\nGetting AWS account information...")
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    print(f"Using AWS account: {account_id}")
    
    # Create task definition first
    ecr_repo = f'{account_id}.dkr.ecr.{region}.amazonaws.com/cloudrun-executor'
    task_definition = _create_task_definition(aws_clients['ecs'], task_role, ecr_repo, region)
    
    # Set environment variables needed for Lambda function
    os.environ['CLOUDRUN_BUCKET_NAME'] = bucket_name
    os.environ['CLOUDRUN_SUBNET_ID'] = subnet_id
    os.environ['CLOUDRUN_TASK_DEFINITION_ARN'] = task_definition['taskDefinition']['taskDefinitionArn']
    os.environ['CLOUDRUN_REGION'] = region

    # Create a scheduled job Lambda function
    lambda_function = _create_scheduler_lambda(aws_clients['iam'], task_role['Role']['Arn'], region)
    
    # Attach scheduler policies to task role
    _attach_scheduler_policies(aws_clients['iam'], 'cloudrun-task-role')

    # Build and push Docker image
    _build_and_push_docker_image(ecr_repo, region, os.path.dirname(os.path.abspath(__file__)), os.path.join(os.path.dirname(os.path.abspath(__file__)), 'docker'), **kwargs)

    # Save configuration to .env file
    env_vars = {
        'CLOUDRUN_REGION': region,
        'CLOUDRUN_BUCKET_NAME': bucket_name,
        'CLOUDRUN_SUBNET_ID': subnet_id,
        'CLOUDRUN_VPC_ID': vpc_id,
        'CLOUDRUN_TASK_DEFINITION_ARN': task_definition['taskDefinition']['taskDefinitionArn'],
        'CLOUDRUN_TASK_FAMILY': task_definition['taskDefinition']['family'],
        'CLOUDRUN_TASK_ROLE_ARN': task_role['Role']['Arn'],
        'CLOUDRUN_ECR_REPO': ecr_repo,
        'CLOUDRUN_CLUSTER_NAME': 'cloudrun-cluster',
        'CLOUDRUN_SCHEDULER_LAMBDA_ARN': lambda_function['Configuration']['FunctionArn'],
        'CLOUDRUN_INITIALIZED': 'true'
    }
    _save_configuration(env_vars)
    
    print("\n=== CloudRun Infrastructure Creation Complete ===")
    return env_vars

###############################################################################

def destroy_infrastructure() -> None:
    """
    Destroy all AWS infrastructure created by CloudRun.
    This includes ECS cluster, IAM roles, S3 bucket, and ECR repository.
    Also cleans up the .env file by removing all CLOUDRUN_ environment variables.
    """
    print("\n=== Starting CloudRun Infrastructure Destruction ===")
    load_dotenv()
    
    region = os.getenv('CLOUDRUN_REGION', 'us-east-1')
    print(f"Using AWS region: {region}")
    
    # Initialize AWS clients
    aws_clients = _initialize_aws_clients(region)
    
    # Delete all scheduled jobs first
    _delete_scheduled_jobs(region)
    
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

    # Delete scheduled jobs Lambda function
    print("\nDeleting scheduled jobs Lambda function...")
    lambda_client = boto3.client('lambda', region_name=region)
    try:
        lambda_client.delete_function(
            FunctionName='cloudrun-scheduler'
        )
        print("Lambda function deleted")
    except lambda_client.exceptions.ResourceNotFoundException:
        print("Lambda function not found or already deleted")

    # Clean up .env file
    _cleanup_env_file()
    
    print("\n=== CloudRun Infrastructure Destruction Complete ===") 

###############################################################################

def rebuild_infrastructure() -> Dict[str, Any]:
    """
    Rebuild the AWS infrastructure using previously saved settings.
    This will destroy the existing infrastructure and recreate it with the same settings.
    
    Returns:
        Dict[str, Any]: Dictionary containing created resource information
    """
    print("\n=== Starting CloudRun Infrastructure Rebuild ===")
    
    # Load previous settings
    settings = _load_infrastructure_settings()
    if not settings:
        raise ValueError("No previous infrastructure settings found. Please use create_infrastructure first.")
    
    # Destroy existing infrastructure
    destroy_infrastructure()
    
    # Recreate infrastructure with saved settings
    return create_infrastructure(**settings)

############################################################################### 

