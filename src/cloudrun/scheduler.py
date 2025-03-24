import os
import boto3
import json
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Any
from botocore.exceptions import ClientError
from . import (
    get_region,
    get_bucket_name,
    get_subnet_id,
    get_task_definition_arn,
    get_scheduler_lambda_arn
)

###############################################################################

def _get_events_client(env_name: str = 'default'):
    """Get a boto3 EventBridge client with the configured region."""
    region = get_region(env_name)
    return boto3.client('events', region_name=region)

###############################################################################

def _get_s3_client(env_name: str = 'default'):
    """Get a boto3 S3 client with the configured region."""
    region = get_region(env_name)
    return boto3.client('s3', region_name=region)

###############################################################################

def _get_cloudrun_lambda_target(env_name: str = 'default'):
    """Get the ARN of the CloudRun executor Lambda function."""
    lambda_arn = get_scheduler_lambda_arn(env_name)
    if not lambda_arn:
        raise RuntimeError(
            f"CLOUDRUN_SCHEDULER_LAMBDA_ARN configuration value not set for environment '{env_name}'. "
            "Please run create_infrastructure() to set up the required resources."
        )
    return lambda_arn

###############################################################################

def _create_and_upload_zip(script_path: str, env_name: str = 'default') -> str:
    """
    Creates a zip file of the project and uploads it to S3.
    
    Args:
        script_path: Path to the script being run
        env_name: Name of the environment
    
    Returns:
        str: S3 key where the zip was uploaded
    """
    bucket_name = get_bucket_name(env_name)
    if not bucket_name:
        raise RuntimeError(f"CLOUDRUN_BUCKET_NAME not set for environment '{env_name}'. Please run create_infrastructure() first.")

    default_excludes = {'.venv/', 'venv/', '__pycache__/', '*.pyc', ".git/"}
    
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
        zip_path = Path(tmp.name)
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk('.'):
            if any(pattern in root for pattern in default_excludes):
                continue
                
            for file in files:
                if file != zip_path.name and not file.startswith('.'):
                    file_path = os.path.join(root, file)
                    if any(pattern in file_path for pattern in default_excludes):
                        continue
                    arcname = os.path.relpath(file_path, '.')
                    zipf.write(file_path, arcname)
    
    # Generate S3 key with a timestamp to ensure uniqueness
    s3_key = f"scheduled_jobs/{env_name}/{os.path.basename(script_path)}/{os.path.basename(zip_path.name)}"
    
    # Upload to S3
    s3 = _get_s3_client(env_name)
    s3.upload_file(str(zip_path), bucket_name, s3_key)
    
    # Clean up the temporary file
    zip_path.unlink()
    
    return s3_key

###############################################################################

def create_scheduled_job(
    name: str,
    script_path: str,
    schedule: str,
    env_name: str = 'default',
    vcpus: float = 0.25,
    memory: int = 512,
    use_spot: bool = False,
    method_name: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create a scheduled job that will run at the specified schedule.
    
    Args:
        name: Name of the scheduled job
        script_path: Path to the script to run
        schedule: Cron expression for the schedule
        env_name: Name of the environment to use (default: 'default')
        vcpus: Number of vCPUs to allocate (default: 0.25)
        memory: Amount of memory to allocate in MB (default: 512)
        use_spot: Whether to use spot instances (default: False)
        method_name: Optional name of a specific method to run in the script
        params: Optional dictionary of parameters to pass to the method
        
    Returns:
        Dict[str, Any]: Information about the created scheduled job
    """
    # Validate inputs
    if not name or not script_path or not schedule:
        raise ValueError("name, script_path, and schedule are required")
    
    # Get required configuration
    bucket_name = get_bucket_name(env_name)
    subnet_id = get_subnet_id(env_name)
    task_definition_arn = get_task_definition_arn(env_name)
    region = get_region(env_name)
    
    if not all([bucket_name, subnet_id, task_definition_arn]):
        raise RuntimeError(
            f"Missing required configuration values for environment '{env_name}'. "
            "Please run create_infrastructure() first"
        )
    
    # Upload script to S3
    s3_client = _get_s3_client(env_name)
    script_key = f'scripts/{env_name}/{name}/{os.path.basename(script_path)}'
    
    try:
        s3_client.upload_file(script_path, bucket_name, script_key)
    except ClientError as e:
        raise RuntimeError(f"Failed to upload script to S3: {str(e)}")
    
    # Create EventBridge rule
    events_client = _get_events_client(env_name)
    rule_name = f'cloudrun-{env_name}-{name}'
    
    # Prepare target input
    target_input = {
        'script_path': script_path,
        'vcpus': vcpus,
        'memory': memory,
        'use_spot': use_spot,
        's3_key': script_key,
        'env_name': env_name
    }
    
    if method_name:
        target_input['method_name'] = method_name
    if params:
        target_input['params'] = params
    
    try:
        response = events_client.put_rule(
            Name=rule_name,
            ScheduleExpression=f'cron({schedule})',
            State='ENABLED',
            Description=f'CloudRun scheduled job: {name} in environment {env_name}',
            Tags=[{'Key': 'cloudrun', 'Value': 'true'}, {'Key': 'environment', 'Value': env_name}]
        )
        
        rule_arn = response['RuleArn']
        
        # Add target to rule
        events_client.put_targets(
            Rule=rule_name,
            Targets=[{
                'Id': '1',
                'Arn': _get_cloudrun_lambda_target(env_name),
                'Input': json.dumps(target_input)
            }]
        )
        
        return {
            'name': name,
            'environment': env_name,
            'rule_arn': rule_arn,
            'schedule': schedule,
            'script_path': script_path,
            'script_key': script_key,
            'vcpus': vcpus,
            'memory': memory,
            'use_spot': use_spot,
            'method_name': method_name,
            'params': params
        }
        
    except ClientError as e:
        raise RuntimeError(f"Failed to create scheduled job: {str(e)}")

###############################################################################

def list_scheduled_jobs(env_name: str = 'default') -> List[Dict[str, Any]]:
    """
    List all scheduled jobs for a specific environment.
    
    Args:
        env_name: Name of the environment to list jobs for (default: 'default')
        
    Returns:
        List[Dict[str, Any]]: List of scheduled job information
    """
    events_client = _get_events_client(env_name)
    
    try:
        response = events_client.list_rules(
            NamePrefix=f'cloudrun-{env_name}-'
        )
        
        jobs = []
        for rule in response.get('Rules', []):
            # Get targets for the rule
            targets = events_client.list_targets_by_rule(
                Rule=rule['Name']
            ).get('Targets', [])
            
            if targets:
                target_input = json.loads(targets[0]['Input'])
                jobs.append({
                    'name': rule['Name'].replace(f'cloudrun-{env_name}-', ''),
                    'environment': env_name,
                    'rule_arn': rule['Arn'],
                    'schedule': rule['ScheduleExpression'].replace('cron(', '').replace(')', ''),
                    'script_path': target_input['script_path'],
                    'script_key': target_input['s3_key'],
                    'vcpus': target_input['vcpus'],
                    'memory': target_input['memory'],
                    'use_spot': target_input['use_spot'],
                    'method_name': target_input.get('method_name'),
                    'params': target_input.get('params')
                })
        
        return jobs
        
    except ClientError as e:
        raise RuntimeError(f"Failed to list scheduled jobs: {str(e)}")

###############################################################################

def delete_scheduled_job(name: str, env_name: str = 'default') -> None:
    """
    Delete a scheduled job.
    
    Args:
        name: Name of the scheduled job to delete
        env_name: Name of the environment (default: 'default')
    """
    events_client = _get_events_client(env_name)
    rule_name = f'cloudrun-{env_name}-{name}'
    
    try:
        # Remove targets first
        targets = events_client.list_targets_by_rule(
            Rule=rule_name
        ).get('Targets', [])
        
        if targets:
            events_client.remove_targets(
                Rule=rule_name,
                Ids=[target['Id'] for target in targets]
            )
        
        # Delete the rule
        events_client.delete_rule(
            Name=rule_name
        )
        
    except ClientError as e:
        raise RuntimeError(f"Failed to delete scheduled job: {str(e)}") 