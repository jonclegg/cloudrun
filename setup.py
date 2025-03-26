from setuptools import setup
import os
import boto3
import json
from typing import Tuple, Dict, Any
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
    create_dynamo_table
)

setup(
    name="cloudrun",
    version="0.1.0",
    packages=["cloudrun"],
    package_dir={"": "src"},
    install_requires=[
        "boto3>=1.26.0",
        "click>=8.0.0",
    ],
    entry_points={
        "console_scripts": [
            "cloudrun=cloudrun.cli:main",
        ],
    },
)

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
    logs = boto3.client('logs', region_name=region)

    # Create S3 bucket
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME', f'cloudrun-{region}-{os.getenv("USER", "default")}')
    try:
        s3.create_bucket(Bucket=bucket_name)
    except s3.exceptions.BucketAlreadyExists:
        pass

    # Create CloudWatch log group
    log_group = '/ecs/cloudrun'
    try:
        logs.create_log_group(logGroupName=log_group)
    except logs.exceptions.ResourceAlreadyExistsException:
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
    Create the AWS infrastructure required for CloudRun.
    
    Args:
        region: AWS region to create resources in
        
    Returns:
        Dict[str, Any]: Dictionary containing created resource information
    """
    print("\n=== Starting CloudRun Infrastructure Creation ===")
    
    # Initialize AWS clients
    aws_clients = _initialize_aws_clients(region)
    
    # Create DynamoDB table for environment configs
    print("\nCreating DynamoDB table for environment configurations...")
    create_dynamo_table()
    
    # Rest of the existing infrastructure creation code...

def destroy_infrastructure() -> None:
    """
    Destroy all AWS infrastructure created by CloudRun.
    This includes ECS cluster, IAM roles, S3 bucket, and ECR repository.
    Also cleans up the .env file by removing all CLOUDRUN_ environment variables.
    """
    load_dotenv()
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    # Initialize AWS clients
    ecs = boto3.client('ecs', region_name=region)
    iam = boto3.client('iam', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    ecr = boto3.client('ecr', region_name=region)
    
    # Delete ECS cluster and tasks
    cluster_name = 'cloudrun-cluster'
    try:
        # Stop all running tasks
        tasks = ecs.list_tasks(cluster=cluster_name)
        if tasks.get('taskArns'):
            ecs.stop_task(
                cluster=cluster_name,
                task=tasks['taskArns'][0]
            )
            # Wait for tasks to stop
            waiter = ecs.get_waiter('tasks_stopped')
            waiter.wait(cluster=cluster_name, tasks=tasks['taskArns'])
        
        # Delete cluster
        ecs.delete_cluster(cluster=cluster_name)
    except ecs.exceptions.ClusterNotFoundException:
        pass
    
    # Delete task definition
    try:
        task_definitions = ecs.list_task_definitions(familyPrefix='cloudrun-task')
        for task_def in task_definitions.get('taskDefinitionArns', []):
            ecs.deregister_task_definition(taskDefinition=task_def)
    except ecs.exceptions.ClientException:
        pass
    
    # Delete IAM roles
    role_name = 'cloudrun-task-role'
    try:
        # Detach policies first
        policies = iam.list_attached_role_policies(RoleName=role_name)
        for policy in policies.get('AttachedPolicies', []):
            iam.detach_role_policy(
                RoleName=role_name,
                PolicyArn=policy['PolicyArn']
            )
        # Delete role
        iam.delete_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        pass
    
    # Delete S3 bucket
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME')
    if bucket_name:
        try:
            # Delete all objects first
            objects = s3.list_objects_v2(Bucket=bucket_name)
            if objects.get('Contents'):
                s3.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
                )
            s3.delete_bucket(Bucket=bucket_name)
        except s3.exceptions.NoSuchBucket:
            pass
    
    # Delete ECR repository
    repo_name = 'cloudrun-executor'
    try:
        ecr.delete_repository(repositoryName=repo_name, force=True)
    except ecr.exceptions.RepositoryNotFoundException:
        pass

    # Clean up .env file
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            lines = f.readlines()
        with open('.env', 'w') as f:
            for line in lines:
                if not line.startswith('CLOUDRUN_'):
                    f.write(line)
    