import argparse
import sys
import time
import boto3
import json
from typing import Optional, List, Dict, Any

import click
import cloudrun._infrastructure as _infrastructure

def tail_logs(
    task_id: Optional[str] = None,
    start_time: Optional[int] = None
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
        log_group = _infrastructure.get_log_group()
        params = {
            'logGroupName': log_group,
            'startTime': start_time,
            'interleaved': True  # Interleave events from different streams
        }
        
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



def format_table(headers, rows):
    """
    Format data as a simple ASCII table.
    
    Args:
        headers: List of column headers
        rows: List of data rows (each row is a list)
        
    Returns:
        str: Formatted table as string
    """
    if not rows:
        return "No data available"
        
    # Calculate column widths based on headers and data
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    
    # Create the header row with padding
    header_row = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * w for w in col_widths)
    
    # Create data rows
    data_rows = []
    for row in rows:
        data_rows.append(" | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)))
    
    # Combine everything
    table = [header_row, separator] + data_rows
    return "\n".join(table)


def get_tasks(region: str) -> List[Dict[str, Any]]:
    """
    Get all running tasks in the CloudRun cluster.
    
    Args:
        region: AWS region
        
    Returns:
        List of task dictionaries with details
    """
    ecs = boto3.client('ecs', region_name=region)
    cluster_name = _infrastructure.get_cluster_name()
    
    try:
        # Get all tasks (running and stopped)
        task_arns = []
        
        # Get running tasks
        running_tasks = ecs.list_tasks(cluster=cluster_name, desiredStatus='RUNNING')
        if running_tasks.get('taskArns'):
            task_arns.extend(running_tasks['taskArns'])
            
        # Get stopped tasks
        stopped_tasks = ecs.list_tasks(cluster=cluster_name, desiredStatus='STOPPED')
        if stopped_tasks.get('taskArns'):
            task_arns.extend(stopped_tasks['taskArns'])
        
        if not task_arns:
            return []
            
        # Get detailed information about these tasks
        tasks_details = ecs.describe_tasks(cluster=cluster_name, tasks=task_arns)
        
        # Format the task details into a more usable structure
        formatted_tasks = []
        for task in tasks_details['tasks']:
            task_id = task['taskArn'].split('/')[-1]
            status = task['lastStatus']
            
            # Extract creation time
            created_at = task.get('createdAt', None)
            
            # Get the command that was used to run this task
            command = None
            for override in task.get('overrides', {}).get('containerOverrides', []):
                if override.get('name') == 'cloudrun-executor' and 'command' in override:
                    command = override['command']
                    break

            # Extract script name from command
            script = command[2] if command and len(command) > 2 else "Unknown"
            
            formatted_tasks.append({
                'id': task_id,
                'status': status,
                'script': script,
                'created_at': created_at,
                'taskArn': task['taskArn'],
                'task': task  # Include the full task object for reference
            })
            
        return formatted_tasks
    except Exception as e:
        print(f"Error retrieving tasks: {str(e)}")
        return []


def delete_task(task_id: str, region: str) -> bool:
    """
    Delete (stop) a running ECS task by ID.
    
    Args:
        task_id: The ID of the task to stop
        region: AWS region
        
    Returns:
        bool: Whether the task was successfully stopped
    """
    if not task_id:
        print("Error: Task ID is required")
        return False
        
    ecs = boto3.client('ecs', region_name=region)
    cluster_name = _infrastructure.get_cluster_name()
    
    try:
        # First check if this task exists and get its ARN
        task_arn = None
        tasks = get_tasks(region)
        
        for task in tasks:
            if task['id'] == task_id:
                task_arn = task['taskArn']
                break
                
        if not task_arn:
            print(f"Error: Task with ID {task_id} not found")
            return False
            
        # Stop the task
        response = ecs.stop_task(
            cluster=cluster_name,
            task=task_arn,
            reason="Stopped by CloudRun CLI"
        )
        
        print(f"Task {task_id} has been stopped")
        return True
    except Exception as e:
        print(f"Error stopping task: {str(e)}")
        return False


def list_tasks_command(args):
    """Handler for list-tasks command"""
    tasks = get_tasks(args.region)
    
    if not tasks:
        print("No tasks found in the CloudRun cluster")
        return
        
    # Format the tasks as a table
    table_data = []
    for task in tasks:
        created_at = task['created_at'].strftime('%Y-%m-%d %H:%M:%S') if task['created_at'] else "Unknown"
        table_data.append([
            task['id'],
            task['status'],
            task['script'],
            created_at
        ])
    
    headers = ["Task ID", "Status", "Script", "Created At"]
    print(format_table(headers, table_data))


def delete_task_command(args):
    """Handler for delete-task command"""
    if not args.task_id:
        print("Error: Task ID is required")
        return
        
    delete_task(args.task_id, args.region)


def main():
    parser = argparse.ArgumentParser(description="CloudRun Command Line Interface")
    parser.add_argument('--region', default='us-east-1', help="AWS region to use")
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # List tasks command
    list_parser = subparsers.add_parser('list-tasks', help='List all tasks')
    list_parser.set_defaults(func=list_tasks_command)
    
    # Delete task command
    delete_parser = subparsers.add_parser('delete-task', help='Delete a running task')
    delete_parser.add_argument('task_id', help='ID of the task to delete')
    delete_parser.set_defaults(func=delete_task_command)

    args = parser.parse_args()
    
    if not hasattr(args, 'func'):
        parser.print_help()
        return
        
    args.func(args)


if __name__ == "__main__":
    main()
