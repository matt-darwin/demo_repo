from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_MDM_CUSm_MDM_TCHPT_XML_MDM_sc_CPCR_TOUCH_POINT_MDMX = MDM_CUSm_MDM_TCHPT_XML_MDM_sc_CPCR_TOUCH_POINT_MDMX(spark)
    df_SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX = SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX(
        spark, 
        df_MDM_CUSm_MDM_TCHPT_XML_MDM_sc_CPCR_TOUCH_POINT_MDMX
    )
    df_SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK = SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK(
        spark, 
        df_SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX
    )
    df_SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK_EXPR_8 = SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK_EXPR_8(
        spark, 
        df_SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK
    )
    df_SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK_EXPR_5 = SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK_EXPR_5(
        spark, 
        df_SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK
    )
    df_EXP_TCHPT_XML_add_missing_column = EXP_TCHPT_XML_add_missing_column(
        spark, 
        df_SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK_EXPR_5
    )
    df_EXP_TCHPT_XML = EXP_TCHPT_XML(spark, df_EXP_TCHPT_XML_add_missing_column)
    df_NORM_TCHPT_MOI = NORM_TCHPT_MOI(spark, df_SQ_MDM_sc_CPCR_TOUCH_POINT_MDMX_GENERATE_SK_EXPR_8)
    df_NORM_TCHPT_MOI_EXPR_6 = NORM_TCHPT_MOI_EXPR_6(spark, df_NORM_TCHPT_MOI)
    df_FLT_MOI_add_missing_column = FLT_MOI_add_missing_column(spark, df_NORM_TCHPT_MOI_EXPR_6)
    df_FLT_MOI = FLT_MOI(spark, df_FLT_MOI_add_missing_column)
    df_EXP_MOI = EXP_MOI(spark, df_FLT_MOI)
    df_MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP_JOIN = MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP_JOIN(
        spark, 
        df_EXP_TCHPT_XML, 
        df_EXP_MOI
    )
    df_MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP_JOIN_EXPR_3 = MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP_JOIN_EXPR_3(
        spark, 
        df_MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP_JOIN
    )
    df_MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP = MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP(
        spark, 
        df_MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP_JOIN_EXPR_3
    )
    MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML(spark, df_MDM_sc_VNA_MAINTAIN_TOUCH_POINT_MDM_XML_EXP)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Customer_MDM")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Customer_MDM", config = Config)(pipeline)

if __name__ == "__main__":
    main()
