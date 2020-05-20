import boto3
import datetime

now = datetime.datetime.now()
ETL_HIVE_SCRIPT_LOCATION = '<HIVE SCRIPT S3 LOCATION>'
CORE_INSTANCE_TYPE = '<CORE NODES INSTANCE TYPE>'
MASTER_INSTANCE_TYPE = '<MASTER NODE INSTANCE TYPE>'
CLUSTER_SIZE = <NUMBER OF NODES>

# Initialise EMR client
client = boto3.client('emr', region_name='eu-west-1')


# Arguments for Hive step to load global events. Script is store in S3 location
hive_args = ['hive-script', '--run-hive-script', '--args', '-f', ETL_HIVE_SCRIPT_LOCATION]
emr_instances = {
    'MasterInstanceType': '<INSTANCE_TYPE>',
    'SlaveInstanceType': SLAVE_INSTANCE_TYPE,
    'InstanceCount': CLUSTER_SIZE,
    'KeepJobFlowAliveWhenNoSteps': False,
    'TerminationProtected': False,
    'Ec2SubnetId': '<subnet-0763bd61>',
    'Ec2KeyName': '<your_ec2_key_name>',
}
#Describe emr step
emr_step = [
    {
        'Name': 'Hive program',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Properties': [],
            'Jar': 'command-runner.jar',
            'Args': ['hive-script', '--run-hive-script', '--args', '-f',
                     ETL_HIVE_SCRIPT_LOCATION]
        }
    }
]

#Config to spin up emr with Glue catalog
glue_catalog_config = [
    {
        "Classification": "hive-site",
        "Properties": {
            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"},
        "Configurations": []
    }
]

#Create transient EMR cluster 
response = client.run_job_flow(
    Name="Cluster Name [" + now.strftime("%a-%d-%b-%Y") + "]",
    ReleaseLabel='<EMR VERSION>',
    Instances=emr_instances,
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    Configurations=glue_catalog_config,
    Applications=[{'Name': 'Hadoop'}, {'Name': 'Hive'}, {'Name': 'Tez'}],
    Steps=emr_step
)

def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200
    }

