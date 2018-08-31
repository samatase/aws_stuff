import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

sensors = glueContext.create_dynamic_frame.from_catalog(database = "tfl", table_name = "sensors", transformation_ctx = "sensors")
traffic = glueContext.create_dynamic_frame.from_catalog(database = "tfl", table_name = "traffic_raw", transformation_ctx = "traffic")
trafficDF = traffic.toDF()
sensorsDF = sensors.toDF()
enrichedTrafficDF = trafficDF.join(sensorsDF,col("sensorid") == col("id"), "leftouter")
enrichedTraffic = DynamicFrame.fromDF(enrichedTrafficDF, glueContext, "enrichedTraffic")

datasink = glueContext.write_dynamic_frame.from_options(frame = enrichedTraffic, connection_type = "s3", connection_options = {"path": "s3://tfl-roads-poc/enriched-traffic"}, format = "csv", transformation_ctx = "datasink")
job.commit()
