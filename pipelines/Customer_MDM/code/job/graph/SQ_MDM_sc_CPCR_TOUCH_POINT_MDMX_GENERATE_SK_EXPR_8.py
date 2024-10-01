from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK_EXPR_8(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MDL_YR_1").alias("MDL_YR_in1"), 
        col("MDL_YR_2").alias("MDL_YR_in2"), 
        col("MDL_YR_3").alias("MDL_YR_in3"), 
        col("MDL_YR_4").alias("MDL_YR_in4"), 
        col("MDL_YR_5").alias("MDL_YR_in5"), 
        col("MDL_YR_6").alias("MDL_YR_in6"), 
        col("MDL_YR_7").alias("MDL_YR_in7"), 
        col("MDL_YR_8").alias("MDL_YR_in8"), 
        col("VEH_MAKE_CDE_1").alias("VEH_MAKE_CDE_in1"), 
        col("VEH_MAKE_CDE_2").alias("VEH_MAKE_CDE_in2"), 
        col("VEH_MAKE_CDE_3").alias("VEH_MAKE_CDE_in3"), 
        col("VEH_MAKE_CDE_4").alias("VEH_MAKE_CDE_in4"), 
        col("VEH_MAKE_CDE_5").alias("VEH_MAKE_CDE_in5"), 
        col("VEH_MAKE_CDE_6").alias("VEH_MAKE_CDE_in6"), 
        col("VEH_MAKE_CDE_7").alias("VEH_MAKE_CDE_in7"), 
        col("VEH_MAKE_CDE_8").alias("VEH_MAKE_CDE_in8"), 
        col("VEH_MDL_CDE_1").alias("VEH_MDL_CDE_in1"), 
        col("VEH_MDL_CDE_2").alias("VEH_MDL_CDE_in2"), 
        col("VEH_MDL_CDE_3").alias("VEH_MDL_CDE_in3"), 
        col("VEH_MDL_CDE_4").alias("VEH_MDL_CDE_in4"), 
        col("VEH_MDL_CDE_5").alias("VEH_MDL_CDE_in5"), 
        col("VEH_MDL_CDE_6").alias("VEH_MDL_CDE_in6"), 
        col("VEH_MDL_CDE_7").alias("VEH_MDL_CDE_in7"), 
        col("VEH_MDL_CDE_8").alias("VEH_MDL_CDE_in8"), 
        col("VEH_SUB_MDL_CDE_1").alias("VEH_SUB_MDL_CDE_in1"), 
        col("VEH_SUB_MDL_CDE_8").alias("VEH_SUB_MDL_CDE_in8"), 
        col("VEH_SUB_MDL_CDE_7").alias("VEH_SUB_MDL_CDE_in7"), 
        col("VEH_SUB_MDL_CDE_2").alias("VEH_SUB_MDL_CDE_in2"), 
        col("VEH_SUB_MDL_CDE_3").alias("VEH_SUB_MDL_CDE_in3"), 
        col("VEH_SUB_MDL_CDE_4").alias("VEH_SUB_MDL_CDE_in4"), 
        col("VEH_SUB_MDL_CDE_5").alias("VEH_SUB_MDL_CDE_in5"), 
        col("VEH_SUB_MDL_CDE_6").alias("VEH_SUB_MDL_CDE_in6"), 
        col("MOI_SRC_ADD_DTTM_8").alias("MOI_ADD_DTTM_in8"), 
        col("MOI_SRC_ADD_DTTM_7").alias("MOI_ADD_DTTM_in7"), 
        col("MOI_SRC_ADD_DTTM_6").alias("MOI_ADD_DTTM_in6"), 
        col("MOI_SRC_ADD_DTTM_5").alias("MOI_ADD_DTTM_in5"), 
        col("MOI_SRC_ADD_DTTM_4").alias("MOI_ADD_DTTM_in4"), 
        col("MOI_SRC_ADD_DTTM_3").alias("MOI_ADD_DTTM_in3"), 
        col("MOI_SRC_ADD_DTTM_2").alias("MOI_ADD_DTTM_in2"), 
        col("MOI_SRC_ADD_DTTM_1").alias("MOI_ADD_DTTM_in1"), 
        col("ETL_REQ_NUM").alias("ETL_REQ_NUM_in"), 
        col("MRKT_CNTC_IDENT").alias("MOI_MRKT_CNTC_IDENT_in")
    )
