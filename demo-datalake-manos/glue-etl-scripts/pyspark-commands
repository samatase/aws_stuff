Spark shell

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job;

glueContext = GlueContext(sc)
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "raw-database", table_name = "taxi-raw-data", transformation_ctx = "datasource0")
