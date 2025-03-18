import click
import os
from .setup import create_infrastructure

@click.group()
def cli():
    """CloudRun CLI tools"""
    pass

@cli.command()
@click.option('--region', help='AWS region to use')
def setup(region):
    """Initialize AWS infrastructure for CloudRun"""
    if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
        click.echo("Error: AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        return
    
    try:
        resources = create_infrastructure(region)
        
        # Create or update .env file with resource information
        env_vars = {
            'CLOUDRUN_BUCKET_NAME': resources['bucket_name'],
            'CLOUDRUN_CLUSTER_ARN': resources['cluster_arn'],
            'CLOUDRUN_TASK_ROLE_ARN': resources['task_role_arn'],
            'CLOUDRUN_REPOSITORY_URI': resources['repository_uri']
        }
        
        with open('.env', 'a') as f:
            f.write('\n# CloudRun Resources\n')
            for key, value in env_vars.items():
                f.write(f'{key}={value}\n')
        
        click.echo("\nEnvironment variables have been added to .env file")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}") 