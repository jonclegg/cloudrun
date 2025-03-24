from setuptools import setup

setup(
    name="cloudrun",
    version="0.1.0",
    packages=["cloudrun"],
    package_dir={"": "src"},
    install_requires=[
        "boto3>=1.26.0",
        "click>=8.0.0",
    ],
    entry_points={
        "console_scripts": [
            "cloudrun=cloudrun.cli:main",
        ],
    },
)

def ensure_infrastructure() -> Tuple[str, str]:
    """
    Ensures all required AWS infrastructure exists.
    Creates resources if they don't exist.
    
    Returns:
        Tuple[str, str]: Bucket name and task role ARN
    """
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    iam = boto3.client('iam', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    ecs = boto3.client('ecs', region_name=region)
    ecr = boto3.client('ecr', region_name=region)
    logs = boto3.client('logs', region_name=region)

    # Create S3 bucket
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME', f'cloudrun-{region}-{os.getenv("USER", "default")}')
    try:
        s3.create_bucket(Bucket=bucket_name)
    except s3.exceptions.BucketAlreadyExists:
        pass

    # Create CloudWatch log group
    log_group = '/ecs/cloudrun'
    try:
        logs.create_log_group(logGroupName=log_group)
    except logs.exceptions.ResourceAlreadyExistsException:
        pass

    # Create ECS task execution role
    task_role_name = 'cloudrun-task-role'
    try:
        task_role = iam.create_role(
            RoleName=task_role_name,
            AssumeRolePolicyDocument=json.dumps({
                'Version': '2012-10-17',
                'Statement': [{
                    'Effect': 'Allow',
                    'Principal': {'Service': 'ecs-tasks.amazonaws.com'},
                    'Action': 'sts:AssumeRole'
                }]
            })
        )
        
        # Attach policies
        policies = [
            'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy',
            'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        ]
        for policy in policies:
            iam.attach_role_policy(
                RoleName=task_role_name,
                PolicyArn=policy
            )
    except iam.exceptions.EntityAlreadyExistsException:
        task_role = iam.get_role(RoleName=task_role_name)

    # Create ECS cluster
    cluster_name = 'cloudrun-cluster'
    try:
        ecs.create_cluster(
            clusterName=cluster_name,
            capacityProviders=['FARGATE']
        )
    except ecs.exceptions.ClusterExists:
        pass

    # Create ECR repository
    repo_name = 'cloudrun-executor'
    try:
        ecr.create_repository(repositoryName=repo_name)
    except ecr.exceptions.RepositoryAlreadyExistsException:
        pass

    return bucket_name, task_role['Role']['Arn']

def create_infrastructure(region: str = None) -> Dict[str, Any]:
    """
    Initialize AWS infrastructure for CloudRun.
    Creates all necessary resources and saves configuration.
    
    Args:
        region: AWS region to use. If None, uses AWS_DEFAULT_REGION or us-east-1
    
    Returns:
        Dict[str, Any]: Dictionary containing created resource information
    """
    load_dotenv()
    
    # Set region
    if region:
        os.environ['AWS_DEFAULT_REGION'] = region
    elif 'AWS_DEFAULT_REGION' not in os.environ:
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    
    region = os.getenv('AWS_DEFAULT_REGION')
    
    # Create VPC and subnet if needed
    ec2 = boto3.client('ec2', region_name=region)
    
    # Get default VPC and subnet
    vpcs = ec2.describe_vpcs(
        Filters=[{'Name': 'isDefault', 'Values': ['true']}]
    )['Vpcs']
    
    if not vpcs:
        raise Exception("No default VPC found. Please ensure your AWS account has a default VPC.")
    
    vpc_id = vpcs[0]['VpcId']
    
    # Get first available subnet in the default VPC
    subnets = ec2.describe_subnets(
        Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
    )['Subnets']
    
    if not subnets:
        raise Exception("No subnets found in default VPC.")
    
    subnet_id = subnets[0]['SubnetId']
    
    # Create other infrastructure
    bucket_name, task_role_arn = ensure_infrastructure()
    
    # Get AWS account ID
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    os.environ['AWS_ACCOUNT_ID'] = account_id
    
    # Build and push Docker image
    import subprocess
    import shutil
    
    # Get the directory containing this file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    docker_dir_path = os.path.join(current_dir, 'docker')
    
    # Create a temporary build context with the CloudRun package
    build_context = os.path.join(docker_dir_path, 'build_context')
    os.makedirs(build_context, exist_ok=True)
    
    # Copy only the necessary package files to the build context
    package_dir = os.path.dirname(current_dir)  # Go up one level from cloudrun to src
    cloudrun_dir = os.path.join(build_context, 'cloudrun')
    os.makedirs(cloudrun_dir, exist_ok=True)
    
    # Copy all necessary files and directories
    files_to_copy = [
        '__init__.py',
        'cli.py',
        'logger.py',
        'setup.py',
        'cloudformation.yml',
        'pyproject.toml'
    ]
    
    for file in files_to_copy:
        src = os.path.join(current_dir, file)
        if os.path.exists(src):
            shutil.copy2(src, cloudrun_dir)
    
    # Copy Dockerfile and other files to build context
    shutil.copy2(os.path.join(docker_dir_path, 'Dockerfile'), build_context)
    shutil.copy2(os.path.join(docker_dir_path, 'requirements.txt'), build_context)
    shutil.copy2(os.path.join(docker_dir_path, 'entrypoint.sh'), build_context)
    
    # Get AWS account ID and construct ECR repository URI
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    os.environ['AWS_ACCOUNT_ID'] = account_id
    
    ecr_repo = f'{account_id}.dkr.ecr.{region}.amazonaws.com/cloudrun-executor'
    
    # Login to ECR using AWS CLI with password-stdin
    password = subprocess.check_output([
        'aws', 'ecr', 'get-login-password',
        '--region', region
    ]).decode('utf-8').strip()
    
    subprocess.run([
        'docker', 'login',
        '--username', 'AWS',
        '--password-stdin',
        ecr_repo
    ], check=True, input=password.encode('utf-8'))
    
    # Build Docker image with platform specification
    subprocess.run([
        'docker', 'build',
        '--platform', 'linux/amd64',
        '-t', ecr_repo,
        build_context
    ], check=True)
    
    # Push image to ECR
    subprocess.run(['docker', 'push', ecr_repo], check=True)
    
    # Clean up build context
    shutil.rmtree(build_context)
    
    # Create task definition
    ecs = boto3.client('ecs', region_name=region)
    task_definition = ecs.register_task_definition(
        family='cloudrun-task',
        networkMode='awsvpc',
        requiresCompatibilities=['FARGATE'],
        cpu='256',
        memory='512',
        executionRoleArn=task_role_arn,
        taskRoleArn=task_role_arn,
        containerDefinitions=[{
            'name': 'cloudrun-executor',
            'image': ecr_repo,
            'essential': True,
            'logConfiguration': {
                'logDriver': 'awslogs',
                'options': {
                    'awslogs-group': '/ecs/cloudrun',
                    'awslogs-region': region,
                    'awslogs-stream-prefix': 'ecs'
                }
            }
        }]
    )
    
    # Save configuration
    with open('.env', 'a') as f:
        f.write(f'\nCLOUDRUN_REGION={region}\n')
        f.write(f'CLOUDRUN_BUCKET_NAME={bucket_name}\n')
        f.write(f'CLOUDRUN_SUBNET_ID={subnet_id}\n')
        f.write(f'CLOUDRUN_TASK_DEFINITION_ARN={task_definition["taskDefinition"]["taskDefinitionArn"]}\n')
        f.write('CLOUDRUN_INITIALIZED=true\n')
    
    return {
        'region': region,
        'vpc_id': vpc_id,
        'subnet_id': subnet_id,
        'bucket_name': bucket_name,
        'task_role_arn': task_role_arn,
        'task_definition_arn': task_definition['taskDefinition']['taskDefinitionArn']
    }

def destroy_infrastructure() -> None:
    """
    Destroy all AWS infrastructure created by CloudRun.
    This includes ECS cluster, IAM roles, S3 bucket, and ECR repository.
    Also cleans up the .env file by removing all CLOUDRUN_ environment variables.
    """
    load_dotenv()
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    # Initialize AWS clients
    ecs = boto3.client('ecs', region_name=region)
    iam = boto3.client('iam', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    ecr = boto3.client('ecr', region_name=region)
    
    # Delete ECS cluster and tasks
    cluster_name = 'cloudrun-cluster'
    try:
        # Stop all running tasks
        tasks = ecs.list_tasks(cluster=cluster_name)
        if tasks.get('taskArns'):
            ecs.stop_task(
                cluster=cluster_name,
                task=tasks['taskArns'][0]
            )
            # Wait for tasks to stop
            waiter = ecs.get_waiter('tasks_stopped')
            waiter.wait(cluster=cluster_name, tasks=tasks['taskArns'])
        
        # Delete cluster
        ecs.delete_cluster(cluster=cluster_name)
    except ecs.exceptions.ClusterNotFoundException:
        pass
    
    # Delete task definition
    try:
        task_definitions = ecs.list_task_definitions(familyPrefix='cloudrun-task')
        for task_def in task_definitions.get('taskDefinitionArns', []):
            ecs.deregister_task_definition(taskDefinition=task_def)
    except ecs.exceptions.ClientException:
        pass
    
    # Delete IAM roles
    role_name = 'cloudrun-task-role'
    try:
        # Detach policies first
        policies = iam.list_attached_role_policies(RoleName=role_name)
        for policy in policies.get('AttachedPolicies', []):
            iam.detach_role_policy(
                RoleName=role_name,
                PolicyArn=policy['PolicyArn']
            )
        # Delete role
        iam.delete_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        pass
    
    # Delete S3 bucket
    bucket_name = os.getenv('CLOUDRUN_BUCKET_NAME')
    if bucket_name:
        try:
            # Delete all objects first
            objects = s3.list_objects_v2(Bucket=bucket_name)
            if objects.get('Contents'):
                s3.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
                )
            s3.delete_bucket(Bucket=bucket_name)
        except s3.exceptions.NoSuchBucket:
            pass
    
    # Delete ECR repository
    repo_name = 'cloudrun-executor'
    try:
        ecr.delete_repository(repositoryName=repo_name, force=True)
    except ecr.exceptions.RepositoryNotFoundException:
        pass

    # Clean up .env file
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            lines = f.readlines()
        with open('.env', 'w') as f:
            for line in lines:
                if not line.startswith('CLOUDRUN_'):
                    f.write(line)
    