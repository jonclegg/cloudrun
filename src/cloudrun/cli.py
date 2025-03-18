import click
import os
import boto3
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

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

def get_log_streams(logs_client: boto3.client, log_group: str) -> List[Dict]:
    """Get all log streams for a given log group."""
    streams = logs_client.describe_log_streams(
        logGroupName=log_group,
        orderBy='LastEventTime',
        descending=True
    )
    return streams['logStreams']

def fetch_historical_logs(
    logs_client: boto3.client,
    log_group: str,
    start_time: int,
    end_time: int,
    filter_pattern: Optional[str] = None
) -> None:
    """Fetch and display historical logs from all streams in a log group."""
    streams = get_log_streams(logs_client, log_group)
    
    if not streams:
        click.echo(f"No log streams found in log group: {log_group}")
        return
        
    for stream in streams:
        click.echo(f"\nLog Stream: {stream['logStreamName']}")
        click.echo("-" * 80)
        
        kwargs = {
            'logGroupName': log_group,
            'logStreamName': stream['logStreamName'],
            'startTime': start_time,
            'endTime': end_time
        }
        
        if filter_pattern:
            kwargs['filterPattern'] = filter_pattern
            
        while True:
            response = logs_client.get_log_events(**kwargs)
            
            for event in response['events']:
                timestamp = datetime.fromtimestamp(event['timestamp'] / 1000)
                click.echo(f"{timestamp} - {event['message'].strip()}")
            
            if not response.get('nextForwardToken'):
                break
                
            kwargs['nextToken'] = response['nextForwardToken']

def tail_logs(
    logs_client: boto3.client,
    log_group: str,
    filter_pattern: Optional[str] = None
) -> None:
    """Continuously tail and display new logs from a log group."""
    click.echo("\nTailing logs... (Press Ctrl+C to stop)")
    last_token = None
    
    while True:
        try:
            kwargs = {
                'logGroupName': log_group,
                'startFromHead': False,
                'limit': 50
            }
            
            if filter_pattern:
                kwargs['filterPattern'] = filter_pattern
                
            if last_token:
                kwargs['nextToken'] = last_token
                
            response = logs_client.get_log_events(**kwargs)
            
            for event in response['events']:
                timestamp = datetime.fromtimestamp(event['timestamp'] / 1000)
                click.echo(f"{timestamp} - {event['message'].strip()}")
            
            last_token = response.get('nextForwardToken')
            time.sleep(1)  # Wait 1 second before checking for new logs
            
        except KeyboardInterrupt:
            click.echo("\nStopped tailing logs")
            break
        except Exception as e:
            click.echo(f"Error while tailing logs: {str(e)}", err=True)
            time.sleep(5)  # Wait longer on error before retrying

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
def setup(region):
    """Initialize AWS infrastructure for CloudRun"""
    try:
        # Import here to avoid circular imports
        from .setup import create_infrastructure
        
        # Create infrastructure
        resources = create_infrastructure(region)
        
        click.echo("\nInfrastructure setup complete!")
        click.echo("\nResource Summary:")
        for key, value in resources.items():
            click.echo(f"{key}: {value}")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise

@cli.command()
@click.option('--log-group', required=True, help='CloudWatch log group name')
@click.option('--hours', default=1, help='Number of hours of logs to fetch (default: 1)')
@click.option('--filter', help='Filter pattern to apply to the logs')
@click.option('--tail', is_flag=True, help='Continuously tail the logs')
def logs(log_group, hours, filter, tail):
    """View logs from a specific CloudWatch log group"""
    try:
        # Create CloudWatch Logs client
        logs_client = boto3.client('logs')
        
        # Calculate time range
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)
        
        # Fetch historical logs
        fetch_historical_logs(logs_client, log_group, start_time, end_time, filter)
        
        # If tailing is enabled, continue monitoring for new logs
        if tail:
            tail_logs(logs_client, log_group, filter)
                
    except Exception as e:
        click.echo(f"Error fetching logs: {str(e)}", err=True)
        raise

def main():
    cli(obj={})

if __name__ == '__main__':
    main() 