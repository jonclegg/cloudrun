import os
import pytest
import boto3
from cloudrun.infrastructure import create_infrastructure, destroy_infrastructure
from cloudrun.config import load_config, clear_config

###############################################################################

@pytest.fixture(scope="module")
def aws_client():
    """Create an AWS client for testing."""
    return boto3.client('cloudformation')

###############################################################################

@pytest.fixture(scope="module")
def test_env():
    """Set up test environment variables"""
    os.environ['CLOUDRUN_STACK_NAME'] = 'cloudrun-test'
    return os.environ['CLOUDRUN_STACK_NAME']

###############################################################################

def test_create_infrastructure():
    """Test infrastructure creation"""
    # Create infrastructure
    result = create_infrastructure()
    
    # Verify result contains all expected keys
    expected_keys = {'region', 'vpc_id', 'subnet_id', 'bucket_name', 'task_role_arn', 'task_definition_arn'}
    assert all(key in result for key in expected_keys)
    
    # Verify configuration was saved with expected values
    config = load_config()
    assert config.get('CLOUDRUN_REGION') == result['region']
    assert config.get('CLOUDRUN_BUCKET_NAME') == result['bucket_name']
    assert config.get('CLOUDRUN_SUBNET_ID') == result['subnet_id']
    assert config.get('CLOUDRUN_TASK_DEFINITION_ARN') == result['task_definition_arn']
    assert config.get('CLOUDRUN_INITIALIZED') == 'true'

###############################################################################

def test_destroy_infrastructure():
    """Test infrastructure destruction"""
    # Destroy infrastructure
    destroy_infrastructure()
    
    # Verify configuration was cleared
    config = load_config()
    assert not any(key.startswith('CLOUDRUN_') for key in config)

###############################################################################

def test_cleanup(aws_client, test_env):
    """Clean up test infrastructure"""
    # Delete the stack
    aws_client.delete_stack(StackName=test_env)
    
    # Wait for stack deletion to complete
    waiter = aws_client.get_waiter('stack_delete_complete')
    waiter.wait(StackName=test_env)
    
    # Verify stack is deleted
    with pytest.raises(aws_client.exceptions.ClientError) as exc_info:
        aws_client.describe_stacks(StackName=test_env)
    assert exc_info.value.response['Error']['Code'] == 'ValidationError'
    
    # Clean up configuration
    clear_config() 