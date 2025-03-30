import os
import boto3
import json
import uuid
import time
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to execute CloudRun jobs by launching ECS tasks directly.
    
    Args:
        event: Event data containing job configuration
        context: Lambda context
        
    Returns:
        Dict[str, Any]: Job execution information
    """
    logger.info(f"CloudRun Scheduler Lambda invoked with event: {json.dumps(event)}")
    
    # Get job configuration from event
    vcpus = event.get('vcpus', 0.25)
    memory = event.get('memory', 512)
    use_spot = event.get('use_spot', False)
    method_name_str = event.get('method_name') # Renamed to avoid conflict
    params = event.get('params')
    zip_key = event.get('zip_key')
    
    logger.info(f"Job configuration: vcpus={vcpus}, memory={memory}, "
                f"use_spot={use_spot}, method_name={method_name_str}, zip_key={zip_key}")
    
    # Get environment variables
    bucket_name = os.environ.get('CLOUDRUN_BUCKET_NAME')
    subnet_id = os.environ.get('CLOUDRUN_SUBNET_ID')
    task_definition_arn = os.environ.get('CLOUDRUN_TASK_DEFINITION_ARN')
    region = os.environ.get('CLOUDRUN_REGION', 'us-east-1')
    cluster_name = os.environ.get('CLOUDRUN_CLUSTER_NAME')
    
    logger.info(f"Environment configuration: bucket_name={bucket_name}, subnet_id={subnet_id}, "
                f"region={region}, task_definition_arn={task_definition_arn}, cluster_name={cluster_name}")
    
    # Validate required parameters
    if not all([bucket_name, subnet_id, task_definition_arn, zip_key, cluster_name, method_name_str]): # Added method_name_str check
        error_msg = "Missing required parameters. Ensure all environment variables and required event fields (including method_name) are set."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Calculate CPU units
    cpu_units = str(int(vcpus * 1024))
    logger.info(f"Calculated CPU units: {cpu_units}")
    
    # Generate a custom task ID with timestamp for uniqueness
    custom_task_id = f"scheduled-task-{int(time.time())}-{str(uuid.uuid4())[:8]}"
    logger.info(f"Generated task ID: {custom_task_id}")

    # Split method_name into module and function
    try:
        module_name, function_name = method_name_str.rsplit('.', 1)
    except ValueError:
        error_msg = f"Invalid method_name format: '{method_name_str}'. Expected 'module.function'."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Prepare command for the container - pass module and function separately
    command = [bucket_name, zip_key, module_name, function_name]
    if params:
        command.append(json.dumps(params))
    else:
        command.append("{}") # Pass empty JSON object if no params
    
    logger.info(f"Container command prepared: {command}")
    
    # Prepare task parameters
    task_params = {
        'cluster': cluster_name, # Use cluster name from env var
        'taskDefinition': task_definition_arn,
        'networkConfiguration': {
            'awsvpcConfiguration': {
                'subnets': [subnet_id],
                'assignPublicIp': 'ENABLED'
            }
        },
        'overrides': {
            'cpu': cpu_units,
            'memory': str(memory),
            'containerOverrides': [{
                'name': 'cloudrun-executor',
                'command': command, # Use the new command structure
                'environment': [
                    {
                        'name': 'CLOUDRUN_TASK_ID',
                        'value': custom_task_id
                    },
                    {
                        'name': 'CLOUDRUN_USE_ZIP',
                        'value': 'true'
                    }
                ]
            }]
        }
    }
    
    # Add spot configuration if requested
    if use_spot:
        task_params['capacityProviderStrategy'] = [{
            'capacityProvider': 'FARGATE_SPOT',
            'weight': 1
        }]
        logger.info("Using FARGATE_SPOT capacity provider")
    else:
        task_params['launchType'] = 'FARGATE'
        logger.info("Using standard FARGATE launch type")
    
    # Run the task
    try:
        logger.info(f"Attempting to run ECS task with parameters: {json.dumps(task_params)}")
        ecs = boto3.client('ecs', region_name=region)
        response = ecs.run_task(**task_params)
        
        if response.get('tasks'):
            task_arn = response['tasks'][0]['taskArn']
            logger.info(f"Successfully started ECS task: {task_arn}")
        else:
            logger.warning(f"Task started but no task ARN returned. Full response: {json.dumps(response)}")
            task_arn = None
            
        if response.get('failures'):
            logger.error(f"Received failures when starting task: {json.dumps(response['failures'])}")
    except Exception as e:
        error_msg = f"Error running ECS task: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    result = {
        'job_id': custom_task_id,
        'task_arn': task_arn
    }
    
    logger.info(f"Lambda execution completed successfully. Result: {json.dumps(result)}")
    return result 