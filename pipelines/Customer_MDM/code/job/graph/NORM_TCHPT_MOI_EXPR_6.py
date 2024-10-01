from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def NORM_TCHPT_MOI_EXPR_6(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("ETL_REQ_NUM").alias("FK_TCRMService"), 
        col("MOI_MRKT_CNTC_IDENT").alias("VNATouchPointId"), 
        col("MOI_ADD_DTTM").alias("VNAStartDate"), 
        col("MDL_YR").alias("VNAModelYear"), 
        col("VEH_MAKE_CDE").alias("VNAVehicleMakeValue"), 
        col("VEH_MDL_CDE").alias("VNAMarketingModelValue"), 
        col("VEH_SUB_MDL_CDE").alias("VNAMarketingSubModelValue")
    )
