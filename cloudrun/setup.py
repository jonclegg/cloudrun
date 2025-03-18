import os
import boto3
import json
import subprocess
from typing import Dict, Tuple
from pathlib import Path
from dotenv import load_dotenv, set_key

def check_docker_running() -> bool:
    """Check if Docker daemon is running."""
    try:
        subprocess.run(['docker', 'info'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def create_s3_bucket(s3_client, region: str) -> str:
    """Create S3 bucket for code storage."""
    bucket_name = f'cloudrun-{region}-{os.getenv("USER", "default")}'
    try:
        print(f"Creating S3 bucket: {bucket_name}")
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
    except s3_client.exceptions.BucketAlreadyExists:
        print(f"Bucket {bucket_name} already exists")
    return bucket_name

def create_iam_role(iam_client) -> str:
    """Create IAM role for task execution."""
    role_name = 'cloudrun-task-role'
    try:
        print(f"Creating IAM role: {role_name}")
        role = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                'Version': '2012-10-17',
                'Statement': [{
                    'Effect': 'Allow',
                    'Principal': {'Service': 'ecs-tasks.amazonaws.com'},
                    'Action': 'sts:AssumeRole'
                }]
            })
        )
        
        # Attach required policies
        policies = [
            'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy',
            'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess',
            'arn:aws:iam::aws:policy/CloudWatchLogsFullAccess'  # Add CloudWatch Logs access
        ]
        for policy in policies:
            print(f"Attaching policy: {policy}")
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy
            )
        return role['Role']['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"Role {role_name} already exists")
        role = iam_client.get_role(RoleName=role_name)
        return role['Role']['Arn']

def create_ecs_cluster(ecs_client) -> str:
    """Create ECS cluster for running tasks."""
    cluster_name = 'cloudrun-cluster'
    try:
        print(f"Creating ECS cluster: {cluster_name}")
        cluster = ecs_client.create_cluster(
            clusterName=cluster_name,
            capacityProviders=['FARGATE']
        )
        return cluster['cluster']['clusterArn']
    except ecs_client.exceptions.ClusterExists:
        print(f"Cluster {cluster_name} already exists")
        cluster = ecs_client.describe_clusters(clusters=[cluster_name])['clusters'][0]
        return cluster['clusterArn']

def setup_ecr_repository(ecr_client) -> str:
    """Create ECR repository for container images."""
    repo_name = 'cloudrun-executor'
    try:
        print(f"Creating ECR repository: {repo_name}")
        repo = ecr_client.create_repository(repositoryName=repo_name)
        return repo['repository']['repositoryUri']
    except ecr_client.exceptions.RepositoryAlreadyExistsException:
        print(f"Repository {repo_name} already exists")
        repo = ecr_client.describe_repositories(repositoryNames=[repo_name])['repositories'][0]
        return repo['repositoryUri']

def build_and_push_image(ecr_client, repository_uri: str, region: str) -> str:
    """
    Build and push Docker image to ECR.
    
    Args:
        ecr_client: Boto3 ECR client
        repository_uri: URI of the ECR repository
        region: AWS region
    
    Returns:
        str: Full image URI
    """
    try:
        # Get AWS account ID
        sts = boto3.client('sts', region_name=region)
        account_id = sts.get_caller_identity()['Account']
        
        repo_name = 'cloudrun-executor'
        ecr_uri = f"{account_id}.dkr.ecr.{region}.amazonaws.com/{repo_name}"
        
        print("\nBuilding and pushing Docker image...")
        print(f"ECR URI: {ecr_uri}")
        
        # Get ECR login password and login
        login_cmd = f"aws ecr get-login-password --region {region} | docker login --username AWS --password-stdin {ecr_uri}"
        subprocess.run(login_cmd, shell=True, check=True)
        
        # Build with platform specification
        build_cmd = f"docker build --platform linux/amd64 -t {ecr_uri} . -f Dockerfile"
        subprocess.run(build_cmd, shell=True, check=True)
        
        # Push the image
        push_cmd = f"docker push {ecr_uri}"
        subprocess.run(push_cmd, shell=True, check=True)
        
        return f"{ecr_uri}:latest"
        
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Docker operation failed: {str(e)}")

def get_default_vpc_subnet(region: str) -> Dict[str, str]:
    """
    Get the default VPC and first subnet ID.
    
    Args:
        region: AWS region
    
    Returns:
        Dict containing vpc_id and subnet_id
    """
    ec2 = boto3.client('ec2', region_name=region)
    
    # Get default VPC
    vpcs = ec2.describe_vpcs(
        Filters=[{'Name': 'isDefault', 'Values': ['true']}]
    )['Vpcs']
    
    if not vpcs:
        raise RuntimeError("No default VPC found. Please create a VPC or specify a subnet ID.")
    
    vpc_id = vpcs[0]['VpcId']
    
    # Get subnets in the default VPC
    subnets = ec2.describe_subnets(
        Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
    )['Subnets']
    
    if not subnets:
        raise RuntimeError(f"No subnets found in default VPC {vpc_id}")
    
    # Use the first subnet
    subnet_id = subnets[0]['SubnetId']
    
    print(f"Using default VPC: {vpc_id}")
    print(f"Using subnet: {subnet_id}")
    
    return {'vpc_id': vpc_id, 'subnet_id': subnet_id}

def create_task_definition(ecs_client, task_role_arn: str, image_uri: str, region: str) -> str:
    """
    Create ECS task definition for running cloud tasks.
    
    Args:
        ecs_client: Boto3 ECS client
        task_role_arn: IAM role ARN for task execution
        image_uri: ECR image URI
        region: AWS region
    
    Returns:
        str: Task definition ARN
    """
    # Create CloudWatch log group first
    logs_client = boto3.client('logs', region_name=region)
    log_group_name = '/cloudrun/tasks'
    
    try:
        print(f"Creating CloudWatch log group: {log_group_name}")
        logs_client.create_log_group(logGroupName=log_group_name)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        print(f"Log group {log_group_name} already exists")

    task_definition_name = 'cloudrun-task'
    
    try:
        response = ecs_client.register_task_definition(
            family=task_definition_name,
            requiresCompatibilities=['FARGATE'],
            networkMode='awsvpc',
            cpu='256',
            memory='512',
            executionRoleArn=task_role_arn,
            taskRoleArn=task_role_arn,
            containerDefinitions=[
                {
                    'name': 'cloudrun-executor',
                    'image': image_uri,
                    'essential': True,
                    'logConfiguration': {
                        'logDriver': 'awslogs',
                        'options': {
                            'awslogs-group': log_group_name,
                            'awslogs-region': region,
                            'awslogs-stream-prefix': 'cloudrun'
                        }
                    }
                }
            ]
        )
        return response['taskDefinition']['taskDefinitionArn']
    except Exception as e:
        raise RuntimeError(f"Failed to create task definition: {str(e)}")

def create_infrastructure(region: str = None) -> Dict[str, str]:
    """
    Creates all required AWS infrastructure for CloudRun.
    
    Args:
        region: AWS region to use (defaults to AWS_DEFAULT_REGION or 'us-east-1')
    
    Returns:
        Dict containing created resource identifiers
    """
    if not check_docker_running():
        raise RuntimeError(
            "Docker is not running. Please start Docker and try again.\n"
            "If Docker is not installed, please install it from: https://docs.docker.com/get-docker/"
        )

    region = region or os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    # Initialize AWS clients
    iam = boto3.client('iam', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    ecs = boto3.client('ecs', region_name=region)
    ecr = boto3.client('ecr', region_name=region)
    
    resources = {}
    
    print("Setting up CloudRun infrastructure...")
    
    # Get default VPC and subnet
    vpc_info = get_default_vpc_subnet(region)
    resources.update(vpc_info)
    
    # Create resources
    resources['bucket_name'] = create_s3_bucket(s3, region)
    resources['task_role_arn'] = create_iam_role(iam)
    resources['cluster_arn'] = create_ecs_cluster(ecs)
    resources['repository_uri'] = setup_ecr_repository(ecr)
    resources['image_uri'] = build_and_push_image(ecr, resources['repository_uri'], region)
    resources['task_definition_arn'] = create_task_definition(
        ecs, 
        resources['task_role_arn'],
        resources['image_uri'],
        region
    )
    
    # Save configuration
    save_configuration(resources, region)
    
    print("\nInfrastructure setup complete!")
    print(f"Docker image pushed to: {resources['image_uri']}")
    
    return resources

def save_configuration(resources: Dict[str, str], region: str):
    """Save infrastructure configuration to .env file."""
    env_vars = {
        'CLOUDRUN_BUCKET_NAME': resources['bucket_name'],
        'CLOUDRUN_CLUSTER_ARN': resources['cluster_arn'],
        'CLOUDRUN_TASK_ROLE_ARN': resources['task_role_arn'],
        'CLOUDRUN_REPOSITORY_URI': resources['repository_uri'],
        'CLOUDRUN_IMAGE_URI': resources['image_uri'],
        'CLOUDRUN_VPC_ID': resources['vpc_id'],
        'CLOUDRUN_SUBNET_ID': resources['subnet_id'],
        'CLOUDRUN_TASK_DEFINITION_ARN': resources['task_definition_arn'],
        'CLOUDRUN_REGION': region,
        'CLOUDRUN_INITIALIZED': 'true'
    }
    
    env_file = Path('.env')
    if not env_file.exists():
        env_file.touch()
    
    for key, value in env_vars.items():
        set_key(env_file, key, value)
    
    print("\nConfiguration saved to .env file") 