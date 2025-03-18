import click
import os
import boto3
from .setup import create_infrastructure

@click.group()
def cli():
    """CloudRun CLI tools 1.0"""
    pass

@cli.command()
@click.option('--region', help='AWS region to use')
@click.option('--profile', help='AWS profile to use')
def setup(region, profile):
    """Initialize AWS infrastructure for CloudRun"""
    try:
        # Configure AWS session with profile if provided
        if profile:
            session = boto3.Session(profile_name=profile)
            # Set credentials for boto3 default session
            credentials = session.get_credentials()
            os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
            if session.region_name:
                os.environ['AWS_DEFAULT_REGION'] = session.region_name
        
        # Create infrastructure
        resources = create_infrastructure(region)
        
        click.echo("\nInfrastructure setup complete!")
        click.echo("\nResource Summary:")
        for key, value in resources.items():
            click.echo(f"{key}: {value}")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise 