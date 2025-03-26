import os
import pytest
import boto3
from cloudrun.dynamo_config import get_config_value, clear_environment

###############################################################################

@pytest.fixture(autouse=True)
def setup_teardown():
    """Setup and teardown for each test."""
    # Setup
    yield
    
    # Teardown
    clear_environment('default')
    # Clean up DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('cloudrun-environments')
    table.delete()

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
    assert get_config_value('CLOUDRUN_REGION', 'default') == result['region']
    assert get_config_value('CLOUDRUN_BUCKET_NAME', 'default') == result['bucket_name']
    assert get_config_value('CLOUDRUN_SUBNET_ID', 'default') == result['subnet_id']
    assert get_config_value('CLOUDRUN_TASK_DEFINITION_ARN', 'default') == result['task_definition_arn']
    assert get_config_value('CLOUDRUN_INITIALIZED', 'default') == True

###############################################################################

def test_destroy_infrastructure():
    """Test infrastructure destruction"""
    # Destroy infrastructure
    destroy_infrastructure()
    
    # Verify configuration was cleared
    config = get_config_value('CLOUDRUN_CONFIG', 'default')
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
    clear_environment('default') 