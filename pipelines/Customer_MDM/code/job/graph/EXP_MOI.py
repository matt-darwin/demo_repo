from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def EXP_MOI(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("FK_TCRMService"), 
        col("VNATouchPointId"), 
        lit("").alias("DUMMY"), 
        col("VNAStartDate"), 
        col("VNAEndDate"), 
        col("VNAModelYear"), 
        col("VNAVehicleMakeValue"), 
        concat(ltrim(rtrim(col("VNAVehicleMakeValue"))), lit("-"), ltrim(rtrim(col("VNAMarketingModelValue"))))\
          .alias("o_VNAMarketingModelValue"), 
        concat(
            ltrim(rtrim(col("VNAVehicleMakeValue"))), 
            lit("-"), 
            ltrim(rtrim(col("VNAMarketingModelValue"))), 
            lit("-"), 
            ltrim(rtrim(col("VNAMarketingSubModelValue")))
          )\
          .alias("o_VNAMarketingSubModelValue")
    )
