from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def EXP_TCHPT_XML_add_missing_column(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("VNATouchPointLastUpdateDate", lit(None).cast(StringType()))\
        .withColumn("VNATouchPointLastUpdateUser", lit(None).cast(StringType()))\
        .withColumn("VNATouchPointLastUpdateTxId", lit(None).cast(StringType()))
