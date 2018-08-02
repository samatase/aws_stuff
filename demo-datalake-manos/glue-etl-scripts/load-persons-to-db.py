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
## @type: DataSource
## @args: [database = "processed-database", table_name = "persons", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "processed-database", table_name = "persons", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("family_name", "string", "family_name", "string"), ("name", "string", "name", "string"), ("links", "array", "links", "string"), ("gender", "string", "gender", "string"), ("image", "string", "image", "string"), ("identifiers", "array", "identifiers", "string"), ("other_names", "array", "other_names", "string"), ("sort_name", "string", "sort_name", "string"), ("images", "array", "images", "string"), ("given_name", "string", "given_name", "string"), ("birth_date", "string", "birth_date", "string"), ("id", "string", "id", "string"), ("contact_details", "array", "contact_details", "string"), ("death_date", "string", "death_date", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("family_name", "string", "family_name", "string"), ("name", "string", "name", "string"), ("links", "array", "links", "string"), ("gender", "string", "gender", "string"), ("image", "string", "image", "string"), ("identifiers", "array", "identifiers", "string"), ("other_names", "array", "other_names", "string"), ("sort_name", "string", "sort_name", "string"), ("images", "array", "images", "string"), ("given_name", "string", "given_name", "string"), ("birth_date", "string", "birth_date", "string"), ("id", "string", "id", "string"), ("contact_details", "array", "contact_details", "string"), ("death_date", "string", "death_date", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "RDS", connection_options = {"dbtable": "persons", "database": "datalake"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "RDS", connection_options = {"dbtable": "persons", "database": "datalake"}, transformation_ctx = "datasink4")
job.commit()
