from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP(spark: SparkSession, in0: DataFrame) -> DataFrame:
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
        col("VNAVINNumber"), 
        col("VNAVehicleId"), 
        col("VNASourceIdentifierType"), 
        col("VNASourceIdentifierValue"), 
        col("VNASourceUpdateDate"), 
        col("VNATouchPointLastUpdateDate"), 
        col("VNATouchPointLastUpdateUser"), 
        col("VNATouchPointLastUpdateTxId"), 
        col("FileName"), 
        lit(None).alias("ObjectReferenceId0"), 
        col("FK_TCRMService"), 
        lit(None).alias("VNATouchPointMOIIdPK"), 
        col("VNATouchPointId"), 
        col("VNAModelYear"), 
        col("VNAVehicleMakeType"), 
        col("VNAVehicleMakeValue"), 
        col("VNAMarketingModelType"), 
        col("VNAMarketingModelValue"), 
        col("VNAMarketingSubModelType"), 
        col("VNAMarketingSubModelValue"), 
        col("VNAStartDate0"), 
        lit(None).alias("VNAEndDate0"), 
        lit(None).alias("VNATouchPointMOILastUpdateDate"), 
        lit(None).alias("VNATouchPointMOILastUpdateUser"), 
        lit(None).alias("VNATouchPointMOILastUpdateTxId")
    )
