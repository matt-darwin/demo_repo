from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("prophecy_sk", monotonically_increasing_id())
