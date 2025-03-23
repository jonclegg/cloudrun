import os
import logging
import boto3
from botocore.exceptions import ClientError
from typing import Optional

class CloudWatchHandler(logging.Handler):
    """A logging handler that sends logs to CloudWatch."""
    
    def __init__(self, log_group: str, region: Optional[str] = None):
        super().__init__()
        self.log_group = log_group
        self.region = region or os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.logs_client = boto3.client('logs', region_name=self.region)
        
        # Use custom task ID from environment if available, otherwise fall back to process ID
        self.log_stream = os.getenv('CLOUDRUN_TASK_ID', str(os.getpid()))
        
        # Create log group if it doesn't exist
        try:
            self.logs_client.create_log_group(logGroupName=log_group)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                raise
        
        # Create log stream if it doesn't exist
        try:
            self.logs_client.create_log_stream(
                logGroupName=log_group,
                logStreamName=self.log_stream
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                raise
    
    def emit(self, record):
        try:
            msg = self.format(record)
            self.logs_client.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self.log_stream,
                logEvents=[{
                    'timestamp': int(record.created * 1000),
                    'message': msg
                }]
            )
        except Exception:
            self.handleError(record)

def get_logger(log_group: Optional[str] = None) -> logging.Logger:
    """
    Get a logger configured to send logs to both CloudWatch and console.
    
    Args:
        log_group: Optional CloudWatch log group name. If not provided, uses CLOUDRUN_LOG_GROUP or defaults to /ecs/cloudrun
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger()
    
    # Only add handlers if logger doesn't already have handlers
    if not logger.handlers:
        # Get log group from environment or use default
        log_group = log_group or os.getenv('CLOUDRUN_LOG_GROUP', '/ecs/cloudrun')
        
        # Create formatters
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Create CloudWatch handler
        cloudwatch_handler = CloudWatchHandler(log_group)
        cloudwatch_handler.setFormatter(formatter)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(cloudwatch_handler)
        logger.addHandler(console_handler)
        logger.setLevel(logging.INFO)
    
    return logger 