import os
import pytest
import boto3
from cloudrun.setup import create_infrastructure, ensure_infrastructure

@pytest.fixture(scope="module")
def aws_client():
    """Verify we can create an AWS client using default credentials"""
    try:
        client = boto3.client('cloudformation')
        # Try a simple API call to verify credentials work
        client.list_stacks()
        return client
    except Exception as e:
        pytest.skip(f"Could not create AWS client: {str(e)}")

@pytest.fixture(scope="module")
def test_env():
    """Set up test environment variables"""
    os.environ['CLOUDRUN_STACK_NAME'] = 'cloudrun-test'
    return os.environ['CLOUDRUN_STACK_NAME']

def test_create_infrastructure():
    """Test infrastructure creation"""
    # Create infrastructure
    result = create_infrastructure()
    
    # Verify result contains all expected keys
    expected_keys = {'region', 'vpc_id', 'subnet_id', 'bucket_name', 'task_role_arn', 'task_definition_arn'}
    assert all(key in result for key in expected_keys)
    
    # Verify .env file was created with expected values
    with open('.env', 'r') as f:
        env_contents = f.read()
        assert f'CLOUDRUN_REGION={result["region"]}' in env_contents
        assert f'CLOUDRUN_BUCKET_NAME={result["bucket_name"]}' in env_contents
        assert f'CLOUDRUN_SUBNET_ID={result["subnet_id"]}' in env_contents
        assert f'CLOUDRUN_TASK_DEFINITION_ARN={result["task_definition_arn"]}' in env_contents
        assert 'CLOUDRUN_INITIALIZED=true' in env_contents

def test_ensure_infrastructure(aws_client, test_env):
    """Test infrastructure verification"""
    # This should not raise an exception if infrastructure exists
    bucket_name, task_role_arn = ensure_infrastructure()
    assert bucket_name
    assert task_role_arn

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
    
    # Clean up .env file
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            lines = f.readlines()
        with open('.env', 'w') as f:
            for line in lines:
                if not any(key in line for key in ['CLOUDRUN_']):
                    f.write(line) 