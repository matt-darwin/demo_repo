from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def EXP_TCHPT_XML(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("XPK_TCRMService"), 
        col("requestID"), 
        lit(Config.PMRepositoryUserName).alias("requesterName"), 
        lit("100").alias("requesterLanguage"), 
        lit("VNAMaintainTouchPoint").alias("TCRMTxType"), 
        lit("VNATouchPointBObj").alias("TCRMTxObject"), 
        lit("").alias("ObjectReferenceId"), 
        lit("").alias("VNATouchPointIdPK"), 
        col("VNASourceContactId"), 
        col("VNAStartDate"), 
        col("VNAEndDate"), 
        col("VNACustomerId"), 
        lit("").alias("VNAPartyId"), 
        col("VNAResponseInd"), 
        col("VNAPromoEvntCode"), 
        col("VNATouchPointDispCode"), 
        col("VNATouchPointDispDate"), 
        lit("").alias("VNAMarketingRecType"), 
        col("VNAMarketingRecValue"), 
        col("VNAVinNumber"), 
        lit("").alias("VNAVehicleId"), 
        lit("").alias("VNASourceIdentifierType"), 
        lit("CPCR").alias("VNASourceIdentifierValue"), 
        col("VNASourceUpdateDate"), 
        col("VNATouchPointLastUpdateDate"), 
        col("VNATouchPointLastUpdateUser"), 
        col("VNATouchPointLastUpdateTxId"), 
        concat(lit("VNAMaintainTouchPoint"), lit("_"), ltrim(rtrim(col("requestID"))), lit(".xml")).alias("FileName")
    )
