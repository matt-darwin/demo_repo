from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP_JOIN(
        spark: SparkSession,
        in0: DataFrame,
        in1: DataFrame,
) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.VNAStartDate") == col("in1.VNAStartDate")), "inner")\
        .select(col("in0.VNATouchPointLastUpdateTxId").alias("VNATouchPointLastUpdateTxId"), col("in0.VNAEndDate").alias("VNAEndDate"), col("in0.VNATouchPointDispDate").alias("VNATouchPointDispDate"), col("in0.VNAMarketingRecType").alias("VNAMarketingRecType"), col("in1.VNAModelYear").alias("VNAModelYear"), col("in0.TCRMTxObject").alias("TCRMTxObject"), col("in1.DUMMY").alias("DUMMY"), col("in1.o_VNAMarketingSubModelValue").alias("o_VNAMarketingSubModelValue"), col("in0.VNAStartDate").alias("VNAStartDate"), col("in0.TCRMTxType").alias("TCRMTxType"), col("in0.VNAPartyId").alias("VNAPartyId"), col("in0.VNATouchPointDispCode").alias("VNATouchPointDispCode"), col("in0.VNASourceUpdateDate").alias("VNASourceUpdateDate"), col("in0.XPK_TCRMService").alias("XPK_TCRMService"), col("in0.VNATouchPointLastUpdateDate").alias("VNATouchPointLastUpdateDate"), col("in0.requesterName").alias("requesterName"), col("in1.VNAVehicleMakeValue").alias("VNAVehicleMakeValue"), col("in0.requesterLanguage").alias("requesterLanguage"), col("in0.ObjectReferenceId").alias("ObjectReferenceId"), col("in0.VNASourceIdentifierType").alias("VNASourceIdentifierType"), col("in0.VNACustomerId").alias("VNACustomerId"), col("in0.FileName").alias("FileName"), col("in0.VNAMarketingRecValue").alias("VNAMarketingRecValue"), col("in0.VNAPromoEvntCode").alias("VNAPromoEvntCode"), col("in1.o_VNAMarketingModelValue").alias("o_VNAMarketingModelValue"), col("in0.requestID").alias("requestID"), col("in0.VNAVinNumber").alias("VNAVinNumber"), col("in0.VNASourceContactId").alias("VNASourceContactId"), col("in0.VNATouchPointIdPK").alias("VNATouchPointIdPK"), col("in0.VNATouchPointLastUpdateUser").alias("VNATouchPointLastUpdateUser"), col("in1.FK_TCRMService").alias("FK_TCRMService"), col("in0.VNASourceIdentifierValue").alias("VNASourceIdentifierValue"), col("in0.VNAVehicleId").alias("VNAVehicleId"), col("in0.VNAResponseInd").alias("VNAResponseInd"))
