from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MDM_CUSm_MDM_TCHPT_XML_MDM_sc_CPCR_TOUCH_POINT_MDMX(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("jdbc")\
        .option("url", "jdbcUrl")\
        .option("user", "admin")\
        .option("password", "password")\
        .option("query", None)\
        .option("pushDownPredicate", True)\
        .option("driver", "notFound")\
        .load()
