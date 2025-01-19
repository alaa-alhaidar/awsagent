import time


def fetchS3():
    # Initialize an S3 client
    s3_client = boto3.client('s3')

    # Specify your bucket name
    bucket_name = 'alaa-bucket'

    # List objects in the bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    # Check if the bucket has objects
    if 'Contents' in response:
        print(f"Files in {bucket_name}:")
        for obj in response['Contents']:
            print(f"- {obj['Key']}")
    else:
        print(f"No files found in {bucket_name}.")


import boto3


def create_emr_cluster(cluster_name, log_uri, release_label, instance_type, instance_count, key_name):
    """
    Creates an EMR cluster using boto3.

    Parameters:
    - cluster_name: Name of the cluster.
    - log_uri: S3 path for EMR logs (e.g., 's3://your-log-bucket/').
    - release_label: EMR release label (e.g., 'emr-6.10.0').
    - instance_type: Instance type for the cluster (e.g., 'm5.xlarge').
    - instance_count: Number of core and task nodes in the cluster.
    - key_name: Name of the EC2 key pair for SSH access.
    """
    client = boto3.client('emr')

    response = client.run_job_flow(
        Name=cluster_name,
        LogUri=log_uri,
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master node',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': instance_type,
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': instance_count,
                },
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2KeyName': key_name,
        },
        Applications=[
            {'Name': 'Hadoop'},
            {'Name': 'Spark'},  # Include Spark for running Spark jobs
        ],
        Configurations=[
            {
                'Classification': 'spark-env',
                'Configurations': [
                    {
                        'Classification': 'export',
                        'Properties': {
                            'PYSPARK_PYTHON': '/usr/bin/python3',
                        },
                    }
                ],
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_for_EC2_role',
        ServiceRole='EMR_ROLE',
    )

    return response
# Function to list instance groups
def list_instance_groups(cluster_id):
    emr_client = boto3.client('emr')
    response = emr_client.list_instance_groups(ClusterId=cluster_id)

    return response['InstanceGroups']

# Function to modify EMR cluster
def modify_emr_cluster(cluster_id, instance_group_id, instance_count, step_config=None):
    emr_client = boto3.client('emr')

    # Resize an instance group
    if instance_group_id and instance_count:
        response_resize = emr_client.modify_instance_groups(
            ClusterId=cluster_id,
            InstanceGroups=[
                {
                    'InstanceGroupId': instance_group_id,
                    'InstanceCount': instance_count,
                }
            ]
        )
        return response_resize


import boto3


import boto3

def add_spark_step(cluster_id, step_name, jar_path):
    """
    Add a Spark step to an existing EMR cluster.

    Parameters:
    - cluster_id: The ID of the EMR cluster.
    - step_name: A descriptive name for the step.
    - jar_path: The S3 path to the JAR file to run.
    """
    emr_client = boto3.client('emr')

    # Define the Spark step configuration
    step_config = {
        'Name': step_name,
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--class', 'com.Join',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--driver-memory', '5g',
                '--driver-cores', '4',
                '--num-executors', '4',
                '--executor-memory', '4g',
                '--executor-cores', '2',
                '--conf', 'spark.memory.fraction=0.4',
                '--conf', 'spark.shuffle.memoryFraction=0.5',
                '--conf', 'spark.shuffle.file.buffer=64k',
                jar_path
            ]
        }
    }

    # Add the step to the cluster
    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[step_config]
    )

    # Extract the step_id as a string (ensure it's not a list)
    step_id = response['StepIds'][0] if response['StepIds'] else None
    return step_id


def get_step_status(cluster_id, step_id):
    client = boto3.client('emr')

    # Describe the step to get its status
    response = client.describe_step(
        ClusterId=cluster_id,
        StepId=step_id
    )

    # Extract the step status from the response
    step_info = response['Step']
    status = step_info['Status']['State']

    return status, response


def wait_for_step_completion(cluster_id, step_id, wait_interval=30, max_attempts=60):
    # Poll the status of the step until it's completed or failed
    attempt = 0
    while attempt < max_attempts:
        status, response = get_step_status(cluster_id, step_id)

        print(f"Attempt {attempt + 1}: Step status is {status}")

        if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
            # Step has finished (either completed or failed)
            return status, response

        # If the step hasn't finished, wait for the next attempt
        time.sleep(wait_interval)
        attempt += 1

    # If the maximum attempts were exceeded, return a timeout status
    return 'TIMEOUT', None


def get_step_metrics(cluster_id, step_id):
    # Wait for the step to complete
    status, response = wait_for_step_completion(cluster_id, step_id)

    if status == 'TIMEOUT':
        print("Step did not complete in the expected time frame.")
        return None

    # If the step is completed, extract the metrics
    step_info = response['Step']
    status = step_info['Status']['State']

    # Use .get() method to avoid KeyError for missing keys
    start_time = step_info['Status']['Timeline'].get('StartDateTime', 'Start time not available')
    end_time = step_info['Status']['Timeline'].get('EndDateTime', 'End time not available')
    logs = step_info.get('Logs', 'No logs available')

    return {
        'Status': status,
        'Start Time': start_time,
        'End Time': end_time,
        'Logs': logs
    }




if __name__ == '__main__':
    #fetchS3()

    # Define your parameters if you want to create new cluster
    cluster_name = "MyEMRCluster"
    log_uri = "s3://alaa-bucket/"
    release_label = "emr-6.10.0"
    instance_type = "m5.xlarge"
    instance_count = 2
    key_name = "alaa.alhaidar_eu"
    #create_emr_cluster(cluster_name,log_uri,release_label,instance_type,instance_count,key_name)

    # Fetch instance groups and modify the CORE group
    # provide always cluster ID
    instance_groups = list_instance_groups('j-1RLZ3X860G5TP')

    # Find the CORE group and modify it
    for group in instance_groups:
        if group['InstanceGroupType'] == 'CORE':
            modify_emr_cluster('j-1RLZ3X860G5TP', group['Id'], 1, None)
            print(f"Resized CORE instance group {group['Id']} to 1 instance.")
            break
    else:
        print("No CORE instance group found in the cluster.")

    # Cluster ID of the running EMR cluster
    cluster_id = 'j-1RLZ3X860G5TP'

    # Define the step parameters
    step_name = "Run Word Count"
    script_s3_path = 's3://your-bucket/scripts/wordcount.py'
    script_args = ['s3://your-bucket/input/', 's3://alaa-bucket/output/']

    # Define your EMR cluster ID and step parameters
    cluster_id = 'j-1RLZ3X860G5TP'
    step_name = "Run Custom Spark Job"
    jar_path = 's3://alaa-bucket/custom-jar-name_scala2.12-0.1.jar'

    # Example usage:
    step_id = add_spark_step(cluster_id, step_name, jar_path)
    metrics = get_step_metrics(cluster_id, step_id)
    print(metrics)
