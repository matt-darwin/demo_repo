from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", True)\
        .option("quote", "\"")\
        .option("sep", "NO")\
        .mode("append")\
        .option("separator", "NO")\
        .option("header", True)\
        .csv("path")
