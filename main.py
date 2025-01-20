
import boto3

def create_emr_cluster(cluster_name, log_uri, release_label, instance_type, instance_count, key_name):

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


def describe_emr_cluster(cluster_id):
    """
    Fetches and prints details of the EMR cluster using the ClusterId.

    Parameters:
    - cluster_id: ID of the EMR cluster to describe.
    """
    client = boto3.client('emr')
    response = client.describe_cluster(ClusterId=cluster_id)

    cluster_info = response.get('Cluster', {})
    print("Cluster Info:")
    print(f"Name: {cluster_info.get('Name')}")
    print(f"Cluster ID: {cluster_info.get('Id')}")
    print(f"Status: {cluster_info.get('Status', {}).get('State')}")
    print(f"Master Public DNS: {cluster_info.get('MasterPublicDnsName')}")
    print(f"Applications: {[app['Name'] for app in cluster_info.get('Applications', [])]}")
    print(f"Log URI: {cluster_info.get('LogUri')}")
def stopEMR(clusterID):
    # Terminate the EMR cluster
    client = boto3.client('emr')
    response = client.terminate_job_flows(
        JobFlowIds=[cluster_id]
    )
    print(response, "with ", clusterID," terminated")

if __name__ == '__main__':
    cluster_name = "MyEMRCluster"
    log_uri = "s3://alaa-bucket/"
    release_label = "emr-6.10.0"
    instance_type = "m5.xlarge"
    instance_count = 2
    key_name = "alaa.alhaidar_eu"

    # Create the EMR cluster
    response = create_emr_cluster(cluster_name, log_uri, release_label, instance_type, instance_count, key_name)
    cluster_id = response['JobFlowId']
    print(f"Cluster created with ID: {cluster_id}")

    # Fetch and print cluster details
    describe_emr_cluster(cluster_id)


#git push -u origin main
