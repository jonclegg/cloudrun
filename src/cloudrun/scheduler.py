import os
import boto3
import json
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Any
from botocore.exceptions import ClientError
from .dynamo_config import (
    get_region,
    get_bucket_name,
    get_subnet_id,
    get_task_definition_arn,
    get_scheduler_lambda_arn
)
from . import create_and_upload_zip

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

def _get_lambda_client(env_name: str = 'default'):
    """Get a boto3 Lambda client with the configured region."""
    region = get_region(env_name)
    return boto3.client('lambda', region_name=region)

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

def create_scheduled_job(
    name: str,
    method_name: str,
    schedule: str,
    env_name: str = 'default',
    vcpus: float = 0.25,
    memory: int = 512,
    use_spot: bool = False,
    params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create a scheduled job that will run at the specified schedule.
    
    Args:
        name: Name of the scheduled job
        method_name: Name of the method to run (module.method format)
        schedule: Cron expression for the schedule
        env_name: Name of the environment to use (default: 'default')
        vcpus: Number of vCPUs to allocate (default: 0.25)
        memory: Amount of memory to allocate in MB (default: 512)
        use_spot: Whether to use spot instances (default: False)
        params: Optional dictionary of parameters to pass to the method
        
    Returns:
        Dict[str, Any]: Information about the created scheduled job
    """
    # Validate inputs
    if not name or not method_name or not schedule:
        raise ValueError("name, method_name, and schedule are required")
    
    # Parse method name to get script path
    if not '.' in method_name:
        raise ValueError("method_name must be in module.method format (e.g. 'main.my_method')")
    
    module_path, function_name = method_name.rsplit('.', 1)
    script_path = f"{module_path}.py"
    
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Module not found: {script_path}")
    
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
    
    # Create zip file and upload it to S3
    zip_key = create_and_upload_zip(script_path, None, False, env_name)
    
    # Create EventBridge rule
    events_client = _get_events_client(env_name)
    rule_name = f'cloudrun-{env_name}-{name}'
    
    # Prepare target input
    target_input = {
        'script_path': script_path,
        'vcpus': vcpus,
        'memory': memory,
        'use_spot': use_spot,
        'zip_key': zip_key,
        'env_name': env_name,
        'method_name': method_name
    }
    
    if params:
        target_input['params'] = params
    
    # Format cron expression properly for AWS EventBridge
    # AWS EventBridge cron requires 6 fields: minutes hours day-of-month month day-of-week year
    schedule_expression = schedule.strip()
    
    # If user provided full cron expression with 'cron()' wrapper, extract just the expression
    if schedule_expression.startswith('cron(') and schedule_expression.endswith(')'):
        schedule_expression = schedule_expression[5:-1]
    
    # Validate that the expression has the right number of fields (6)
    fields = schedule_expression.split()
    if len(fields) != 6:
        raise ValueError(
            f"Invalid cron expression: '{schedule_expression}'. AWS EventBridge cron expressions must have 6 fields: "
            "minute hour day-of-month month day-of-week year. Example: '0 12 * * ? *'"
        )
    
    # Ensure day-of-month and day-of-week aren't both specified with values other than ?
    day_of_month = fields[2]
    day_of_week = fields[4]
    if day_of_month != '?' and day_of_week != '?':
        if day_of_month != '*' and day_of_week != '*':
            raise ValueError(
                f"Invalid cron expression: If day-of-month is specified, day-of-week must be '?' or vice versa. "
                f"Current values: day-of-month='{day_of_month}', day-of-week='{day_of_week}'"
            )

    try:
        response = events_client.put_rule(
            Name=rule_name,
            ScheduleExpression=f'cron({schedule_expression})',
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
            'zip_key': zip_key,
            'vcpus': vcpus,
            'memory': memory,
            'use_spot': use_spot,
            'method_name': function_name,
            'params': params
        }
        
    except ClientError as e:
        # Clean up if rule creation succeeded but target adding failed
        try:
            events_client.delete_rule(Name=rule_name)
        except ClientError:
             pass # Ignore error during cleanup
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
        paginator = events_client.get_paginator('list_rules')
        jobs = []
        for page in paginator.paginate(NamePrefix=f'cloudrun-{env_name}-'):
            for rule in page.get('Rules', []):
                # Get targets for the rule
                targets_response = events_client.list_targets_by_rule(Rule=rule['Name'])
                targets = targets_response.get('Targets', [])

                if targets:
                    target_input_str = targets[0].get('Input', '{}')
                    try:
                        target_input = json.loads(target_input_str)
                    except json.JSONDecodeError:
                        print(f"Warning: Could not decode JSON input for rule {rule['Name']}: {target_input_str}")
                        target_input = {} # Assign empty dict if JSON is invalid

                    job_info = {
                        'name': rule['Name'].replace(f'cloudrun-{env_name}-', ''),
                        'environment': env_name,
                        'rule_arn': rule['Arn'],
                        'schedule': rule.get('ScheduleExpression', '').replace('cron(', '').replace(')', ''),
                        'script_path': target_input.get('script_path'),
                        'vcpus': target_input.get('vcpus'),
                        'memory': target_input.get('memory'),
                        'use_spot': target_input.get('use_spot'),
                        'method_name': target_input.get('method_name'),
                        'params': target_input.get('params'),
                        'zip_key': target_input.get('zip_key')
                    }

                    jobs.append(job_info)

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
            target_ids = [target['Id'] for target in targets]
            if target_ids: # Ensure we have IDs before calling remove_targets
                 events_client.remove_targets(
                    Rule=rule_name,
                    Ids=target_ids,
                    Force=True # Use Force=True to allow deletion even if the rule is managed by another service
                )
        
        # Delete the rule
        events_client.delete_rule(
            Name=rule_name,
            Force=True # Use Force=True here as well for consistency
        )
        print(f"Successfully deleted scheduled job '{name}' in environment '{env_name}'.")
        
    except events_client.exceptions.ResourceNotFoundException:
         print(f"Scheduled job '{name}' not found in environment '{env_name}'.")
         # Don't raise an error if the job is already deleted or doesn't exist

    except ClientError as e:
        raise RuntimeError(f"Failed to delete scheduled job '{name}': {str(e)}")

###############################################################################

def run_scheduled_job_now(name: str, env_name: str = 'default') -> Dict[str, Any]:
    """
    Manually triggers the target (Lambda function) of a scheduled job immediately.

    Args:
        name: Name of the scheduled job to run.
        env_name: Name of the environment (default: 'default').

    Returns:
        Dict[str, Any]: Response from the Lambda invocation.

    Raises:
        RuntimeError: If the job or its target cannot be found, or if the Lambda invocation fails.
    """
    events_client = _get_events_client(env_name)
    lambda_client = _get_lambda_client(env_name)
    rule_name = f'cloudrun-{env_name}-{name}'

    try:
        # 1. Find the target associated with the rule
        targets_response = events_client.list_targets_by_rule(Rule=rule_name)
        targets = targets_response.get('Targets', [])

        if not targets:
            raise RuntimeError(f"No targets found for scheduled job rule '{rule_name}'. Cannot invoke.")

        # Assuming the first target is the correct one (as set up by create_scheduled_job)
        target = targets[0]
        target_arn = target.get('Arn')
        target_input_str = target.get('Input', '{}')

        if not target_arn:
             raise RuntimeError(f"Target for rule '{rule_name}' does not have an ARN.")

        # Ensure the target is the expected Lambda function
        expected_lambda_arn = _get_cloudrun_lambda_target(env_name)
        if target_arn != expected_lambda_arn:
             print(f"Warning: Target ARN '{target_arn}' does not match expected scheduler Lambda ARN '{expected_lambda_arn}'. Proceeding anyway.")
             # Decide if you want to raise an error here or just warn

        # 2. Invoke the Lambda function directly
        print(f"Invoking Lambda function '{target_arn}' with input: {target_input_str}")
        try:
            invocation_response = lambda_client.invoke(
                FunctionName=target_arn,
                InvocationType='RequestResponse',  # Synchronous invocation
                Payload=target_input_str.encode('utf-8') # Payload must be bytes
            )

            # Decode the response payload
            response_payload_bytes = invocation_response['Payload'].read()
            response_payload = json.loads(response_payload_bytes.decode('utf-8'))

            if invocation_response.get('FunctionError'):
                 # Handle Lambda execution errors (errors raised within the Lambda code)
                 error_details = response_payload # Payload contains error details in this case
                 raise RuntimeError(f"Lambda function execution failed for job '{name}'. Error: {json.dumps(error_details)}")

            print(f"Lambda invocation successful for job '{name}'. Response status: {invocation_response['StatusCode']}")
            return {
                'statusCode': invocation_response['StatusCode'],
                'payload': response_payload
            }

        except ClientError as e:
             raise RuntimeError(f"Failed to invoke Lambda function '{target_arn}' for job '{name}': {str(e)}")
        except json.JSONDecodeError as e:
             # Handle cases where the Lambda response isn't valid JSON
            raise RuntimeError(f"Failed to decode Lambda response payload for job '{name}': {str(e)}. Raw payload: {response_payload_bytes.decode('utf-8')}")


    except events_client.exceptions.ResourceNotFoundException:
         raise RuntimeError(f"Scheduled job rule '{rule_name}' not found in environment '{env_name}'.")
    except ClientError as e:
        # Catch other potential Boto3 errors during target listing
        raise RuntimeError(f"Failed to retrieve target for scheduled job '{name}': {str(e)}") 