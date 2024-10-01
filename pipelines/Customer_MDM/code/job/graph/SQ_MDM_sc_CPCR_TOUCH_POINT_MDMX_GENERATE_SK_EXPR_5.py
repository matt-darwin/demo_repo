from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK_EXPR_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("CNTC_DISP_DTE").alias("VNATouchPointDispDate"), 
        col("CNTC_DISP_CDE").alias("VNATouchPointDispCode"), 
        col("MRKTG_REC_TYP_CDE").alias("VNAMarketingRecValue"), 
        col("VEH_ID").alias("VNAVinNumber"), 
        col("MRKTG_SRC_ADD_DTTM").alias("VNAStartDate"), 
        col("MRKTG_SRC_UPD_DTTM").alias("VNASourceUpdateDate"), 
        col("ETL_REQ_NUM").alias("requestID"), 
        col("MRKT_CNTC_IDENT").alias("VNASourceContactId"), 
        col("CUST_IDENT").alias("VNACustomerId"), 
        col("DEL_REC_DTE").alias("VNAEndDate"), 
        col("ETL_REQ_NUM").alias("XPK_TCRMService"), 
        col("RESP_IND").alias("VNAResponseInd"), 
        col("PRMO_EVNT_CDE").alias("VNAPromoEvntCode")
    )
