import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "datalake-submissions", table_name = "nao_csv", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("fare paid", "double", "fare", "double"), ("departure date", "string", "departure_date", "string"), ("origin city", "string", "origin_city", "string"), ("destination airport code", "string", "destination_airport_code", "string"), ("destination city", "string", "destination_city", "string"), ("origin country", "string", "origin_country", "string"), ("destination country", "string", "destination_country", "string"), ("class", "string", "class", "string"), ("trip type", "string", "trip_type", "string"), ("txn type", "string", "txn_type", "string")], transformation_ctx = "applymapping1")

resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")

dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
raw_dataframe = dropnullfields3.toDF()
original = raw_dataframe.filter(raw_dataframe["txn_type"] == "ORIGINAL")
refund = raw_dataframe.filter(raw_dataframe["txn_type"] == "REFUND")

originals_ddf = glueContext.create_dynamic_frame_from_rdd(original.rdd,"originals_ddf")
refunds_ddf = glueContext.create_dynamic_frame_from_rdd(refund.rdd,"originals_ddf")

datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = originals_ddf, catalog_connection = "Data Warehouse", connection_options = {"dbtable": "original", "database": "quickstart"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "load_originals")
datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = refunds_ddf, catalog_connection = "Data Warehouse", connection_options = {"dbtable": "refunds", "database": "quickstart"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "load_refunds")

job.commit()
