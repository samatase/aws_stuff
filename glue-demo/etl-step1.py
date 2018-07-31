import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw_db = "raw-database"
table_persons = "raw-examples-persons_json"
table_memberships = "raw-examples-memberships_json"
table_organization = "raw-examples-organizations_json"
datalake_name = "datalake-master-builder-demo"
environment = "dev"
datalake_bucket = datalake_name + "-" + environment


persons = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name=table_persons)
memberships = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name=table_memberships)
orgs = glueContext.create_dynamic_frame.from_catalog(database=raw_db, table_name=table_organization)

#remane some fields ;)

orgs = orgs.drop_fields(['other_names', 'identifiers']).rename_field('id', 'org_id').rename_field('name', 'org_name')

#write tables to parquet files
print "Writing processed data to /legislators ..."
glueContext.write_dynamic_frame.from_options(frame = orgs, connection_type = "s3", connection_options = {"path": "s3://" + datalake_bucket + "/datalake/legislators-processed"}, format = "parquet")
glueContext.write_dynamic_frame.from_options(frame = memberships, connection_type = "s3", connection_options = {"path": "s3://" + datalake_bucket + "/datalake/legislators-processed"}, format = "parquet")
glueContext.write_dynamic_frame.from_options(frame = persons, connection_type = "s3", connection_options = {"path": "s3://" + datalake_bucket + "/datalake/legislators-processed"}, format = "parquet")

job.commit()
