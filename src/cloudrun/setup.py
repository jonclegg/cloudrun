import os
import boto3
import json
from typing import Dict, Any, Tuple
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

def create_infrastructure(region: str = None) -> Dict[str, Any]:
    """
    Initialize AWS infrastructure for CloudRun.
    Creates all necessary resources and saves configuration.
    
    Args:
        region: AWS region to use. If None, uses AWS_DEFAULT_REGION or us-east-1
    
    Returns:
        Dict[str, Any]: Dictionary containing created resource information
    """
    load_dotenv()
    
    # Set region
    if region:
        os.environ['AWS_DEFAULT_REGION'] = region
    elif 'AWS_DEFAULT_REGION' not in os.environ:
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    
    region = os.getenv('AWS_DEFAULT_REGION')
    
    # Create VPC and subnet if needed
    ec2 = boto3.client('ec2', region_name=region)
    
    # Check for existing VPC tagged for CloudRun
    vpcs = ec2.describe_vpcs(
        Filters=[{'Name': 'tag:Name', 'Values': ['cloudrun-vpc']}]
    )['Vpcs']
    
    if vpcs:
        vpc_id = vpcs[0]['VpcId']
    else:
        # Create VPC
        vpc = ec2.create_vpc(CidrBlock='10.0.0.0/16')
        vpc_id = vpc['Vpc']['VpcId']
        
        # Add name tag
        ec2.create_tags(
            Resources=[vpc_id],
            Tags=[{'Key': 'Name', 'Value': 'cloudrun-vpc'}]
        )
        
        # Enable DNS hostnames
        ec2.modify_vpc_attribute(
            VpcId=vpc_id,
            EnableDnsHostnames={'Value': True}
        )
    
    # Check for existing subnet
    subnets = ec2.describe_subnets(
        Filters=[
            {'Name': 'vpc-id', 'Values': [vpc_id]},
            {'Name': 'tag:Name', 'Values': ['cloudrun-subnet']}
        ]
    )['Subnets']
    
    if subnets:
        subnet_id = subnets[0]['SubnetId']
    else:
        # Create subnet
        subnet = ec2.create_subnet(
            VpcId=vpc_id,
            CidrBlock='10.0.1.0/24'
        )
        subnet_id = subnet['Subnet']['SubnetId']
        
        # Add name tag
        ec2.create_tags(
            Resources=[subnet_id],
            Tags=[{'Key': 'Name', 'Value': 'cloudrun-subnet'}]
        )
        
        # Create and attach internet gateway
        igw = ec2.create_internet_gateway()
        igw_id = igw['InternetGateway']['InternetGatewayId']
        
        ec2.attach_internet_gateway(
            InternetGatewayId=igw_id,
            VpcId=vpc_id
        )
        
        # Create route table
        route_table = ec2.create_route_table(VpcId=vpc_id)
        route_table_id = route_table['RouteTable']['RouteTableId']
        
        # Create route to internet
        ec2.create_route(
            RouteTableId=route_table_id,
            DestinationCidrBlock='0.0.0.0/0',
            GatewayId=igw_id
        )
        
        # Associate route table with subnet
        ec2.associate_route_table(
            RouteTableId=route_table_id,
            SubnetId=subnet_id
        )
    
    # Create other infrastructure
    bucket_name, task_role_arn = ensure_infrastructure()
    
    # Create task definition
    ecs = boto3.client('ecs', region_name=region)
    task_definition = ecs.register_task_definition(
        family='cloudrun-task',
        networkMode='awsvpc',
        requiresCompatibilities=['FARGATE'],
        cpu='256',
        memory='512',
        executionRoleArn=task_role_arn,
        taskRoleArn=task_role_arn,
        containerDefinitions=[{
            'name': 'cloudrun-executor',
            'image': f'{os.getenv("AWS_ACCOUNT_ID")}.dkr.ecr.{region}.amazonaws.com/cloudrun-executor:latest',
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
    
    # Save configuration
    with open('.env', 'a') as f:
        f.write(f'\nCLOUDRUN_REGION={region}\n')
        f.write(f'CLOUDRUN_BUCKET_NAME={bucket_name}\n')
        f.write(f'CLOUDRUN_SUBNET_ID={subnet_id}\n')
        f.write(f'CLOUDRUN_TASK_DEFINITION_ARN={task_definition["taskDefinition"]["taskDefinitionArn"]}\n')
        f.write('CLOUDRUN_INITIALIZED=true\n')
    
    return {
        'region': region,
        'vpc_id': vpc_id,
        'subnet_id': subnet_id,
        'bucket_name': bucket_name,
        'task_role_arn': task_role_arn,
        'task_definition_arn': task_definition['taskDefinition']['taskDefinitionArn']
    } 