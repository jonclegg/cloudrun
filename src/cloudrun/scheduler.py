import os
import boto3
import json
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from botocore.exceptions import ClientError

###############################################################################

def _get_events_client():
    """Get a boto3 EventBridge client with the configured region."""
    load_dotenv()
    region = os.getenv('CLOUDRUN_REGION', 'us-east-1')
    return boto3.client('events', region_name=region)

###############################################################################

def _get_s3_client():
    """Get a boto3 S3 client with the configured region."""
    load_dotenv()
    region = os.getenv('CLOUDRUN_REGION', 'us-east-1')
    return boto3.client('s3', region_name=region)

###############################################################################

def _get_cloudrun_lambda_target():
    """Get the ARN of the CloudRun executor Lambda function."""
    load_dotenv()
    # The Lambda function ARN should be stored in the environment file
    lambda_arn = os.getenv('CLOUDRUN_SCHEDULER_LAMBDA_ARN')
    if not lambda_arn:
        raise RuntimeError(
            "CLOUDRUN_SCHEDULER_LAMBDA_ARN environment variable not set. "
            "Please run create_infrastructure() to set up the required resources."
        )
    return lambda_arn

###############################################################################

def _create_and_upload_zip(script_path: str) -> str:
    """
    Creates a zip file of the project and uploads it to S3.
    
    Args:
        script_path: Path to the script being run
    
    Returns:
        str: S3 key where the zip was uploaded
    """
    load_dotenv()
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME')
    if not bucket_name:
        raise RuntimeError("CLOUDRUN_BUCKET_NAME not set. Please run create_infrastructure() first.")

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
    s3_key = f"scheduled_jobs/{os.path.basename(script_path)}/{os.path.basename(zip_path.name)}"
    
    # Upload to S3
    s3 = _get_s3_client()
    s3.upload_file(str(zip_path), bucket_name, s3_key)
    
    # Clean up the temporary file
    zip_path.unlink()
    
    return s3_key

###############################################################################

def create_scheduled_job(
    name: str,
    file_method_path: str,
    schedule_expression: str,
    description: str,
    vcpus: float = 0.25,
    memory: int = 512,
    use_spot: bool = False,
    params: Optional[Dict[str, Any]] = None
) -> str:
    """
    Create a scheduled job using EventBridge.
    
    Args:
        name: Name for the scheduled job
        file_method_path: Path to the script or module.method to run (e.g. "main.py" or "main.process_data")
        schedule_expression: Schedule expression (cron or rate expression)
        description: Description of the scheduled job
        vcpus: Number of vCPUs to allocate
        memory: Memory in MB to allocate
        use_spot: Whether to use spot instances
        params: Optional parameters to pass to the method
        
    Returns:
        str: ARN of the created EventBridge rule
    """
    events = _get_events_client()
    lambda_arn = _get_cloudrun_lambda_target()
    
    # Ensure the job name starts with "cloudrun-" to make cleanup easier
    if not name.startswith('cloudrun-'):
        name = f"cloudrun-{name}"
        print(f"Job name prefixed with 'cloudrun-' for easier management: {name}")
    
    # Parse file_method_path to extract script_path and method_name
    if '.' in file_method_path and not file_method_path.endswith('.py'):
        parts = file_method_path.split('.')
        script_path = '.'.join(parts[:-1])
        if not script_path.endswith('.py'):
            script_path += '.py'
        method_name = parts[-1]
    else:
        script_path = file_method_path
        method_name = None
    
    # Create and upload the zip file to S3
    s3_key = _create_and_upload_zip(script_path)
    
    # Create the CloudWatch Events rule
    rule_response = events.put_rule(
        Name=name,
        ScheduleExpression=schedule_expression,
        State='ENABLED',
        Description=description
    )
    
    # Prepare the input for the Lambda function
    lambda_input = {
        'script_path': script_path,
        'vcpus': vcpus,
        'memory': memory,
        'use_spot': use_spot,
        'method_name': method_name,
        'params': params,
        's3_key': s3_key
    }
    
    # Add the target to the rule
    events.put_targets(
        Rule=name,
        Targets=[{
            'Id': f'{name}-target',
            'Arn': lambda_arn,
            'Input': json.dumps(lambda_input)
        }]
    )
    
    # Return the ARN of the rule
    return rule_response['RuleArn']

###############################################################################

def list_scheduled_jobs() -> List[Dict[str, Any]]:
    """
    List all scheduled jobs.
    
    Returns:
        List[Dict[str, Any]]: List of scheduled jobs
    """
    events = _get_events_client()
    lambda_arn = _get_cloudrun_lambda_target()
    
    # List all rules with the cloudrun- prefix
    rules = events.list_rules(
        NamePrefix='cloudrun-'  # Filter to rules with our prefix
    )['Rules']
    
    # For each rule, add target information if available
    result = []
    for rule in rules:
        try:
            targets = events.list_targets_by_rule(Rule=rule['Name'])['Targets']
            rule['Targets'] = targets
            result.append(rule)
        except Exception as e:
            # Include the rule even if we can't get target information
            rule['Targets'] = []
            rule['TargetError'] = str(e)
            result.append(rule)
    
    return result

###############################################################################

def delete_scheduled_job(name: str) -> None:
    """
    Delete a scheduled job.
    
    Args:
        name: Name of the scheduled job to delete
    """
    events = _get_events_client()
    
    # Ensure name has the cloudrun- prefix
    if not name.startswith('cloudrun-'):
        name = f"cloudrun-{name}"
        print(f"Looking for job with name: {name}")
    
    try:
        # List targets for the rule
        targets = events.list_targets_by_rule(Rule=name)['Targets']
        
        # Remove targets from the rule
        if targets:
            target_ids = [target['Id'] for target in targets]
            events.remove_targets(
                Rule=name,
                Ids=target_ids
            )
            print(f"Removed {len(target_ids)} targets from rule {name}")
        
        # Delete the rule
        events.delete_rule(
            Name=name
        )
        print(f"Successfully deleted job: {name}")
    except events.exceptions.ResourceNotFoundException:
        print(f"Job {name} not found")
    except Exception as e:
        print(f"Error deleting job {name}: {str(e)}") 