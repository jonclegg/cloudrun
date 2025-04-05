import click
import os
import boto3
import sys
import time
import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from botocore.exceptions import ClientError
from .infrastructure import create_infrastructure
from .infrastructure import destroy_infrastructure
from .scheduler import create_scheduled_job, list_scheduled_jobs, delete_scheduled_job, run_scheduled_job_now
from cloudrun.dynamo_config import (
    get_cluster_name,
    list_environments,
)

def get_aws_session(profile: Optional[str] = None) -> boto3.Session:
    """Configure and return an AWS session with the given profile."""
    if profile:
        session = boto3.Session(profile_name=profile)
        # Set credentials for boto3 default session
        credentials = session.get_credentials()
        os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
        if session.region_name:
            os.environ['AWS_DEFAULT_REGION'] = session.region_name
        return session
    return boto3.Session()

def get_log_streams(logs_client: boto3.client, log_group: str, stream_prefix: Optional[str] = None) -> List[Dict]:
    """Get all log streams for a given log group, optionally filtered by prefix."""
    kwargs = {
        'logGroupName': log_group,
        'orderBy': 'LastEventTime',
        'descending': True
    }
    
    if stream_prefix:
        kwargs['logStreamNamePrefix'] = stream_prefix
        
    streams = logs_client.describe_log_streams(**kwargs)
    return streams['logStreams']

def fetch_historical_logs(
    logs_client: boto3.client,
    log_group: str,
    start_time: int,
    end_time: int,
    filter_pattern: Optional[str] = None,
    task_id: Optional[str] = None
) -> None:
    """Fetch and display historical logs from all streams in a log group."""
    
    # Build parameters for filter_log_events
    params = {
        'logGroupName': log_group,
        'startTime': start_time,
        'endTime': end_time,
        'interleaved': True  # Interleave events from different streams
    }
    
    if filter_pattern:
        params['filterPattern'] = filter_pattern
        
    # If task_id is provided, get matching stream names
    if task_id:
        try:
            streams = get_log_streams(logs_client, log_group, task_id)
            if not streams:
                click.echo(f"No streams found for task ID: {task_id}")
                return
            params['logStreamNames'] = [s['logStreamName'] for s in streams]
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                click.echo(f"Log group not found: {log_group}")
                return
            raise
            
    try:
        # Use paginator to handle possible multiple pages of results
        events = []
        paginator = logs_client.get_paginator('filter_log_events')
        
        for page in paginator.paginate(**params):
            events.extend(page.get('events', []))
            
        if not events:
            click.echo(f"No log events found in log group: {log_group}")
            return
            
        # Sort events by timestamp
        events.sort(key=lambda x: x['timestamp'])
        
        # Display events
        click.echo(f"\nFound {len(events)} log events:")
        click.echo("=" * 80)
        
        for event in events:
            timestamp = datetime.fromtimestamp(event['timestamp'] / 1000)
            stream_name = event.get('logStreamName', 'unknown')
            message = event['message'].strip()
            click.echo(f"{timestamp} - [{stream_name}] - {message}")
            
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            click.echo(f"Log group not found: {log_group}")
        else:
            click.echo(f"Error fetching logs: {str(e)}", err=True)

def tail_logs(
    logs_client: boto3.client,
    log_group: str,
    filter_pattern: Optional[str] = None,
    task_id: Optional[str] = None,
    start_time: Optional[int] = None,
    print_stream_name: bool = True
) -> None:
    """
    Continuously tail and display new logs from all streams in a log group.
    
    Args:
        logs_client: Boto3 CloudWatch logs client
        log_group: The CloudWatch log group name
        filter_pattern: Optional CloudWatch filter pattern
        task_id: Optional task ID to filter logs by (used as stream prefix)
        start_time: Optional start time in milliseconds since epoch
        print_stream_name: Whether to print the stream name in log output
    """
    click.echo("\nTailing logs... (Press Ctrl+C to stop)")
    
    # Set start time to now if not provided
    if start_time is None:
        start_time = int(time.time() * 1000)
    
    # Cache to track seen events and avoid duplicates
    seen_events = {}
    
    # Function to fetch and return new events
    def fetch_events():
        nonlocal start_time
        
        # Build parameters for filter_log_events
        params = {
            'logGroupName': log_group,
            'startTime': start_time,
            'interleaved': True  # Interleave events from different streams
        }
        
        if filter_pattern:
            params['filterPattern'] = filter_pattern
            
        # If task_id is provided, get matching stream names
        if task_id:
            try:
                streams = get_log_streams(logs_client, log_group, task_id)
                if not streams:
                    click.echo(f"No streams found for task ID: {task_id}")
                    return []
                params['logStreamNames'] = [s['logStreamName'] for s in streams]
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    click.echo(f"Log group not found: {log_group}")
                    return []
                raise
        
        # Collect new events
        new_events = []
        try:
            # Use paginator to handle possible multiple pages of results
            paginator = logs_client.get_paginator('filter_log_events')
            for page in paginator.paginate(**params):
                for event in page.get('events', []):
                    # Skip if we've seen this event before
                    event_id = event['eventId']
                    if event_id in seen_events:
                        continue
                        
                    # Update seen events and track
                    seen_events[event_id] = True
                    new_events.append(event)
                    
                    # Update start time for next iteration (add 1ms to avoid duplicates)
                    timestamp = event['timestamp']
                    if timestamp >= start_time:
                        start_time = timestamp + 1
        except ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                # If we hit API rate limits, wait and retry
                click.echo("Rate limit exceeded, retrying after short delay...", err=True)
                time.sleep(1)
                return fetch_events()
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                click.echo(f"Log group not found: {log_group}")
                return []
            else:
                # For other errors, report and continue
                click.echo(f"Error fetching logs: {str(e)}", err=True)
                return []
                
        return new_events
    
    # Function to format and display a log event
    def display_event(event):
        timestamp = datetime.fromtimestamp(event['timestamp'] / 1000)
        message = event['message'].strip()
        
        if print_stream_name:
            stream_name = event.get('logStreamName', 'unknown')
            click.echo(f"{timestamp} - [{stream_name}] - {message}")
        else:
            click.echo(f"{timestamp} - {message}")
    
    # Main polling loop
    has_displayed_waiting_message = False
    try:
        while True:
            events = fetch_events()
            
            if events:
                has_displayed_waiting_message = False
                # Sort events by timestamp for chronological order
                events.sort(key=lambda x: x['timestamp'])
                for event in events:
                    display_event(event)
            elif not has_displayed_waiting_message:
                # click.echo("Waiting for new logs...")
                has_displayed_waiting_message = True
                
            # Wait before polling again
            time.sleep(2)
            
    except KeyboardInterrupt:
        click.echo("\nStopped tailing logs")
    except Exception as e:
        click.echo(f"Error while tailing logs: {str(e)}", err=True)

@click.group()
@click.option('--profile', help='AWS profile to use')
@click.pass_context
def cli(ctx, profile):
    """CloudRun CLI tools"""
    ctx.ensure_object(dict)
    ctx.obj['profile'] = profile
    
    # Configure AWS session with profile if provided
    get_aws_session(profile)

@cli.command()
@click.option('--region', help='AWS region to use')
@click.option('--custom-docker-commands', help='Custom Docker commands to insert into the Dockerfile')
def setup(region, custom_docker_commands):
    """Initialize AWS infrastructure for CloudRun"""
    try:
        # Create infrastructure
        resources = create_infrastructure(
            region=region,
            custom_docker_commands=custom_docker_commands
        )
        
        click.echo("\nInfrastructure setup complete!")
        click.echo("\nResource Summary:")
        for key, value in resources.items():
            click.echo(f"{key}: {value}")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise

@cli.command()
def destroy():
    """Destroy all AWS infrastructure created by CloudRun"""
    try:
        if click.confirm('Are you sure you want to destroy all CloudRun infrastructure? This action cannot be undone.'):
            destroy_infrastructure()
            click.echo("\nInfrastructure destroyed successfully!")
        else:
            click.echo("\nOperation cancelled.")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise

@cli.command()
@click.option('--log-group', required=True, help='CloudWatch log group name')
@click.option('--hours', default=1, help='Number of hours of logs to fetch (default: 1)')
@click.option('--filter', help='Filter pattern to apply to the logs')
@click.option('--task-id', help='Filter logs by specific task ID')
@click.option('--tail', is_flag=True, help='Continuously tail the logs')
@click.option('--show-stream', is_flag=True, default=False, help='Show stream name in output')
def logs(log_group, hours, filter, task_id, tail, show_stream):
    """View logs from CloudWatch."""
    session = get_aws_session()
    logs_client = session.client('logs')
    
    # Calculate time range
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)
    
    if tail:
        click.echo(f"Tailing logs from group: {log_group}")
        if filter:
            click.echo(f"Using filter: {filter}")
        if task_id:
            click.echo(f"Filtering to task ID: {task_id}")
        
        tail_logs(logs_client, log_group, filter, task_id, start_time, show_stream)
    else:
        click.echo(f"Fetching logs from the last {hours} hour(s) from group: {log_group}")
        if filter:
            click.echo(f"Using filter: {filter}")
        if task_id:
            click.echo(f"Filtering to task ID: {task_id}")
        
        fetch_historical_logs(logs_client, log_group, start_time, end_time, filter, task_id)

@cli.group()
def schedule():
    """Manage scheduled jobs"""
    pass

@schedule.command(name='create')
@click.option('--file-method-path', required=True, help='Path to the script or module.method to run (e.g. "script.py" or "script.process_data")')
@click.option('--name', required=True, help='Name for the scheduled job')
@click.option('--schedule-expression', required=True, 
              help='Schedule expression (cron or rate expression, e.g. "cron(0 12 * * ? *)" or "rate(1 day)")')
@click.option('--description', help='Description of the scheduled job')
@click.option('--vcpus', type=float, default=0.25, help='Number of vCPUs (default: 0.25)')
@click.option('--memory', type=int, default=512, help='Memory in MB (default: 512)')
@click.option('--use-spot', is_flag=True, help='Use spot instances (cheaper but may be interrupted)')
@click.option('--params', type=str, help='JSON string of parameters to pass to the method')
def create_job(file_method_path, name, schedule_expression, description, vcpus, memory, use_spot, params):
    """Schedule a job to run at specified intervals."""
    try:
        # Parse params if provided
        params_dict = None
        if params:
            try:
                params_dict = json.loads(params)
            except json.JSONDecodeError:
                click.echo("Error: Params must be a valid JSON string", err=True)
                sys.exit(1)
        
        # Create the scheduled job
        job_rule_arn = create_scheduled_job(
            name=name,
            file_method_path=file_method_path,
            schedule_expression=schedule_expression,
            description=description or f"Scheduled job for {file_method_path}",
            vcpus=vcpus,
            memory=memory,
            use_spot=use_spot,
            params=params_dict
        )
        
        click.echo(f"\nJob scheduled successfully!")
        click.echo(f"Job name: {name}")
        click.echo(f"File/method: {file_method_path}")
        click.echo(f"Schedule: {schedule_expression}")
        click.echo(f"Rule ARN: {job_rule_arn}")
        
    except Exception as e:
        click.echo(f"Error scheduling job: {str(e)}", err=True)
        sys.exit(1)

@schedule.command(name='list')
@click.option('--env', default='default', help='Environment name (default: default)')
def list_jobs(env):
    """List all scheduled jobs."""
    try:
        jobs = list_scheduled_jobs(env_name=env)
        if not jobs:
            click.echo(f"No scheduled jobs found in environment '{env}'.")
            return
            
        click.echo(f"Scheduled jobs in environment '{env}':")
        # Simple print for now, consider more formatting (e.g., table)
        for job in jobs:
            click.echo(f"- Name: {job['name']}")
            click.echo(f"  Schedule: {job['schedule']}")
            click.echo(f"  State: {job.get('state', 'N/A')}") # Include state if available
            click.echo(f"  Method: {job.get('full_method_name', job.get('method_name', 'N/A'))}") # Show full path if possible
            click.echo(f"  Params: {json.dumps(job.get('params')) if job.get('params') else 'None'}")
            click.echo("-" * 10)
            
    except RuntimeError as e:
        click.echo(f"Error listing scheduled jobs: {e}", err=True)

@schedule.command(name='delete')
@click.option('--name', required=True, help='Name of the scheduled job to delete')
@click.option('--env', default='default', help='Environment name (default: default)')
def delete_scheduled_job_command(name, env):
    """Delete a scheduled job."""
    try:
        if click.confirm(f'Are you sure you want to delete the scheduled job "{name}" in environment "{env}"?'):
            # Pass env_name to the backend function
            delete_scheduled_job(name=name, env_name=env)
            click.echo(f"\nJob '{name}' in environment '{env}' deleted successfully!")
        else:
            click.echo("\nOperation cancelled.")
            
    except Exception as e:
        click.echo(f"Error deleting job '{name}' in environment '{env}': {str(e)}", err=True)
        sys.exit(1)

@schedule.command(name='invoke')
@click.argument('name', required=True)
@click.option('--env', default='default', help='Environment name (default: default)')
def invoke_job(name, env):
    """Manually invoke a scheduled job to run immediately."""
    try:
        click.echo(f"Invoking job '{name}' in environment '{env}'...")
        result = run_scheduled_job_now(name=name, env_name=env)
        click.echo("Invocation successful!")
        click.echo(f"Status Code: {result.get('statusCode')}")
        # Optionally print the payload, might be large
        click.echo(f"Response Payload: {json.dumps(result.get('payload'), indent=2)}")

    except (ValueError, RuntimeError) as e:
        click.echo(f"Error invoking job '{name}' in environment '{env}': {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"An unexpected error occurred: {e}", err=True)
        sys.exit(1)

@cli.group()
@click.option('--env', default='default', help='Environment name (default: default)')
@click.pass_context
def tasks(ctx, env):
    """Manage running tasks"""
    ctx.ensure_object(dict)
    ctx.obj['env'] = env

@tasks.command(name='list')
@click.pass_context
def list_tasks(ctx):
    """List all running tasks across all environments."""
    try:
        session = get_aws_session()
        ecs = session.client('ecs')
        
        # Get all environments
        environments = list_environments()
        if not environments:
            click.echo("No environments found. Run 'cloudrun setup' to create your first environment.")
            return
            
        found_tasks = False
        for env_name in environments:
            # Get cluster name from environment configuration
            cluster_name = get_cluster_name(env_name)
            if not cluster_name:
                continue
                
            # List tasks in the cluster
            response = ecs.list_tasks(
                cluster=cluster_name,
                desiredStatus='RUNNING'
            )
            
            if not response.get('taskArns'):
                continue
                
            # Get detailed task information
            tasks = ecs.describe_tasks(
                cluster=cluster_name,
                tasks=response['taskArns']
            )
            
            # Get task definitions to filter by CloudRun prefix
            task_definitions = {}
            for task in tasks['tasks']:
                task_def_arn = task['taskDefinitionArn']
                if task_def_arn not in task_definitions:
                    task_def = ecs.describe_task_definition(taskDefinition=task_def_arn)
                    task_definitions[task_def_arn] = task_def['taskDefinition']
            
            # Process tasks for this environment
            for task in tasks['tasks']:
                task_def = task_definitions[task['taskDefinitionArn']]
                
                # Only show tasks with CloudRun prefix in family name
                if not task_def['family'].startswith('cloudrun'):
                    continue
                    
                if not found_tasks:
                    click.echo("\nRunning CloudRun Tasks:")
                    click.echo("=" * 80)
                    found_tasks = True
                
                # Get task ID from environment variables
                task_id = next(
                    (env['value'] for env in task['overrides']['containerOverrides'][0]['environment']
                     if env['name'] == 'CLOUDRUN_TASK_ID'),
                    'unknown'
                )
                
                click.echo(f"Environment: {env_name}")
                click.echo(f"Task ID: {task_id}")
                click.echo(f"Task ARN: {task['taskArn']}")
                click.echo(f"Status: {task['lastStatus']}")
                click.echo(f"Started At: {task['startedAt']}")
                click.echo("=" * 80)
            
        if not found_tasks:
            click.echo("No running CloudRun tasks found in any environment.")
            
    except Exception as e:
        click.echo(f"Error listing tasks: {str(e)}", err=True)
        sys.exit(1)

@tasks.command(name='cancel')
@click.option('--task-id', required=True, help='Task ID to cancel')
@click.pass_context
def cancel_task(ctx, task_id):
    """Cancel a running task."""
    try:
        session = get_aws_session()
        ecs = session.client('ecs')
        
        # Get all environments
        environments = list_environments()
        if not environments:
            click.echo("No environments found. Run 'cloudrun setup' to create your first environment.")
            return
            
        # Search for task across all environments
        target_task = None
        target_cluster = None
        
        for env_name in environments:
            # Get cluster name from environment configuration
            cluster_name = get_cluster_name(env_name)
            if not cluster_name:
                continue
                
            # List tasks in the cluster
            response = ecs.list_tasks(
                cluster=cluster_name,
                desiredStatus='RUNNING'
            )
            
            if not response.get('taskArns'):
                continue
                
            # Get detailed task information
            tasks = ecs.describe_tasks(
                cluster=cluster_name,
                tasks=response['taskArns']
            )
            
            # Find task with matching task ID
            for task in tasks['tasks']:
                task_env_id = next(
                    (env['value'] for env in task['overrides']['containerOverrides'][0]['environment']
                     if env['name'] == 'CLOUDRUN_TASK_ID'),
                    None
                )
                if task_env_id == task_id:
                    target_task = task
                    target_cluster = cluster_name
                    break
                    
            if target_task:
                break
                
        if not target_task:
            click.echo(f"Task with ID {task_id} not found in any environment.")
            return
            
        # Stop the task
        ecs.stop_task(
            cluster=target_cluster,
            task=target_task['taskArn']
        )
        
        click.echo(f"Successfully cancelled task {task_id}")
        
    except Exception as e:
        click.echo(f"Error cancelling task: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
def environments():
    """List all available environments."""
    environments = list_environments()
    
    if not environments:
        click.echo("No environments found. Run 'cloudrun setup' to create your first environment.")
        return
        
    click.echo("\nAvailable environments:")
    click.echo("=" * 80)
    for env_name in environments:
        click.echo(f"  - {env_name}")
    click.echo("=" * 80)

def main():
    cli(obj={})

if __name__ == '__main__':
    main() 