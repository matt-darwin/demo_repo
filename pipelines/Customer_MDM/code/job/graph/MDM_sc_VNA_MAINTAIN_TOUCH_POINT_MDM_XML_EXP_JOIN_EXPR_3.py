from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP_JOIN_EXPR_3(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("XPK_TCRMService"), 
        col("requestID"), 
        col("requesterName"), 
        col("requesterLanguage"), 
        col("TCRMTxType"), 
        col("TCRMTxObject"), 
        col("ObjectReferenceId"), 
        col("VNATouchPointIdPK"), 
        col("VNASourceContactId"), 
        col("VNAStartDate"), 
        col("VNAEndDate"), 
        col("VNACustomerId"), 
        col("VNAPartyId"), 
        col("VNAResponseInd"), 
        col("VNAPromoEvntCode"), 
        col("VNATouchPointDispCode"), 
        col("VNATouchPointDispDate"), 
        col("VNAMarketingRecType"), 
        col("VNAMarketingRecValue"), 
        col("VNAVinNumber").alias("VNAVINNumber"), 
        col("VNAVehicleId"), 
        col("VNASourceIdentifierType"), 
        col("VNASourceIdentifierValue"), 
        col("VNASourceUpdateDate"), 
        col("VNATouchPointLastUpdateDate"), 
        col("VNATouchPointLastUpdateUser"), 
        col("VNATouchPointLastUpdateTxId"), 
        col("FileName"), 
        col("FK_TCRMService"), 
        col("DUMMY").alias("VNATouchPointId"), 
        col("VNAModelYear"), 
        col("DUMMY").alias("VNAVehicleMakeType"), 
        col("VNAVehicleMakeValue"), 
        col("DUMMY").alias("VNAMarketingModelType"), 
        col("o_VNAMarketingModelValue").alias("VNAMarketingModelValue"), 
        col("DUMMY").alias("VNAMarketingSubModelType"), 
        col("o_VNAMarketingSubModelValue").alias("VNAMarketingSubModelValue"), 
        col("VNAStartDate").alias("VNAStartDate0")
    )
