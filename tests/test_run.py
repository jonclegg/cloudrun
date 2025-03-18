import os
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from cloudrun import run

@pytest.fixture
def mock_aws():
    """Mock AWS services and environment setup"""
    with patch('boto3.client') as mock_boto, \
         patch('cloudrun.check_initialization', return_value=True), \
         patch('cloudrun.ensure_infrastructure', return_value=('test-bucket', 'test-role-arn')), \
         patch.dict(os.environ, {
             'CLOUDRUN_REGION': 'us-east-1',
             'CLOUDRUN_SUBNET_ID': 'subnet-123',
             'CLOUDRUN_TASK_DEFINITION_ARN': 'task-def-arn',
             'CLOUDRUN_BUCKET_NAME': 'test-bucket'
         }):
        # Mock S3 and ECS clients
        mock_s3 = MagicMock()
        mock_ecs = MagicMock()
        mock_ecs.run_task.return_value = {'tasks': [{'taskArn': 'arn:aws:ecs:region:account:task/cluster/task-id'}]}
        
        def get_client(service, **kwargs):
            if service == 's3':
                return mock_s3
            elif service == 'ecs':
                return mock_ecs
        
        mock_boto.side_effect = get_client
        yield {'s3': mock_s3, 'ecs': mock_ecs}

@pytest.fixture
def temp_script():
    """Create a temporary script file"""
    script_path = Path('test_script.py')
    script_path.write_text('print("test")')
    yield script_path
    script_path.unlink(missing_ok=True)

def test_default_configuration(mock_aws, temp_script):
    """Test run with default configuration (0.25 vCPU, 512MB)"""
    job_id = run(str(temp_script))
    
    # Verify ECS task parameters
    mock_aws['ecs'].run_task.assert_called_once()
    task_params = mock_aws['ecs'].run_task.call_args[1]
    
    assert task_params['overrides']['cpu'] == '256'  # 0.25 vCPU = 256 units
    assert task_params['overrides']['memory'] == '512'
    assert task_params['launchType'] == 'FARGATE'
    assert not task_params.get('capacityProviderStrategy')  # No spot instances by default

def test_maximum_configuration(mock_aws, temp_script):
    """Test run with maximum configuration (16 vCPU, 120GB)"""
    job_id = run(str(temp_script), vcpus=16.0, memory=122880, use_spot=True)
    
    task_params = mock_aws['ecs'].run_task.call_args[1]
    assert task_params['overrides']['cpu'] == '16384'  # 16 vCPU = 16384 units
    assert task_params['overrides']['memory'] == '122880'
    assert task_params['capacityProviderStrategy'] == [{'capacityProvider': 'FARGATE_SPOT', 'weight': 1}]

def test_common_configurations(mock_aws, temp_script):
    """Test various valid CPU/memory combinations"""
    configurations = [
        (0.25, 512),    # Minimum
        (0.5, 2048),    # 0.5 vCPU with 2GB
        (1.0, 4096),    # 1 vCPU with 4GB
        (2.0, 8192),    # 2 vCPU with 8GB
        (4.0, 16384),   # 4 vCPU with 16GB
    ]
    
    for vcpus, memory in configurations:
        job_id = run(str(temp_script), vcpus=vcpus, memory=memory)
        task_params = mock_aws['ecs'].run_task.call_args[1]
        
        assert task_params['overrides']['cpu'] == str(int(vcpus * 1024))
        assert task_params['overrides']['memory'] == str(memory)

def test_invalid_cpu_values(mock_aws, temp_script):
    """Test invalid CPU values"""
    invalid_cpus = [0.1, 0.3, 3.0, 32.0]
    
    for vcpus in invalid_cpus:
        with pytest.raises(ValueError) as exc:
            run(str(temp_script), vcpus=vcpus)
        assert "vcpus must be one of" in str(exc.value)

def test_invalid_memory_combinations(mock_aws, temp_script):
    """Test invalid memory combinations for different CPU values"""
    invalid_combinations = [
        (0.25, 4096),   # Too much memory for 0.25 vCPU
        (0.5, 8192),    # Too much memory for 0.5 vCPU
        (1.0, 16384),   # Too much memory for 1 vCPU
        (2.0, 2048),    # Too little memory for 2 vCPU
        (4.0, 4096),    # Too little memory for 4 vCPU
    ]
    
    for vcpus, memory in invalid_combinations:
        with pytest.raises(ValueError) as exc:
            run(str(temp_script), vcpus=vcpus, memory=memory)
        assert "must be one of these values" in str(exc.value)

def test_nonexistent_script(mock_aws):
    """Test running a non-existent script"""
    with pytest.raises(FileNotFoundError):
        run('nonexistent_script.py')

def test_spot_instance_configuration(mock_aws, temp_script):
    """Test spot instance configuration"""
    job_id = run(str(temp_script), use_spot=True)
    
    task_params = mock_aws['ecs'].run_task.call_args[1]
    assert task_params['capacityProviderStrategy'] == [{'capacityProvider': 'FARGATE_SPOT', 'weight': 1}]
    assert 'launchType' in task_params  # Verify launchType is still present

def test_task_networking(mock_aws, temp_script):
    """Test task networking configuration"""
    job_id = run(str(temp_script))
    
    task_params = mock_aws['ecs'].run_task.call_args[1]
    assert task_params['networkConfiguration']['awsvpcConfiguration']['subnets'] == ['subnet-123']
    assert task_params['networkConfiguration']['awsvpcConfiguration']['assignPublicIp'] == 'ENABLED'

def test_file_packaging(mock_aws, temp_script):
    """Test that files are properly packaged and uploaded to S3"""
    job_id = run(str(temp_script))
    
    # Verify S3 upload was called
    mock_aws['s3'].upload_file.assert_called_once()
    
    # Verify the uploaded file path and key
    args = mock_aws['s3'].upload_file.call_args[0]  # Positional arguments
    assert len(args) == 3  # Should have filename, bucket, and key
    assert args[1] == 'test-bucket'  # Second argument is bucket name
    assert 'jobs/test_script.py/temp.zip' in args[2]  # Third argument is the key
    
    # Verify temp file cleanup
    assert not Path('temp.zip').exists() 