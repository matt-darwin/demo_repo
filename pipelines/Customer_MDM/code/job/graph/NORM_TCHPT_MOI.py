from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def NORM_TCHPT_MOI(spark: SparkSession, in0: DataFrame) -> DataFrame:
    
    keyDf = in0.withColumn(
        "GK_MDL_YR",
        row_number().over(Window.partitionBy(lit(1)).orderBy(monotonically_increasing_id()))
    )
    out0 = keyDf\
               .select(
                 col("MOI_MRKT_CNTC_IDENT_in").alias("MOI_MRKT_CNTC_IDENT"),
                 col("ETL_REQ_NUM_in").alias("ETL_REQ_NUM"),
                 col("VEH_MDL_CDE_in1").alias("VEH_MDL_CDE"),
                 col("MOI_ADD_DTTM_in1").alias("MOI_ADD_DTTM"),
                 col("VEH_SUB_MDL_CDE_in1").alias("VEH_SUB_MDL_CDE"),
                 col("MDL_YR_in1").alias("MDL_YR"),
                 col("VEH_MAKE_CDE_in1").alias("VEH_MAKE_CDE"),
                 lit(1).alias("GCID_MDL_YR"),
                 lit(1).alias("GCID_VEH_MAKE_CDE"),
                 lit(1).alias("GCID_VEH_MDL_CDE"),
                 lit(1).alias("GCID_VEH_SUB_MDL_CDE"),
                 lit(1).alias("GCID_MOI_ADD_DTTM")
               )\
               .union(
                 keyDf.select(
                   col("MOI_MRKT_CNTC_IDENT_in").alias("MOI_MRKT_CNTC_IDENT"),
                   col("ETL_REQ_NUM_in").alias("ETL_REQ_NUM"),
                   col("VEH_MDL_CDE_in2").alias("VEH_MDL_CDE"),
                   col("MOI_ADD_DTTM_in2").alias("MOI_ADD_DTTM"),
                   col("VEH_SUB_MDL_CDE_in2").alias("VEH_SUB_MDL_CDE"),
                   col("MDL_YR_in2").alias("MDL_YR"),
                   col("VEH_MAKE_CDE_in2").alias("VEH_MAKE_CDE"),
                   lit(2).alias("GCID_MDL_YR"),
                   lit(2).alias("GCID_VEH_MAKE_CDE"),
                   lit(2).alias("GCID_VEH_MDL_CDE"),
                   lit(2).alias("GCID_VEH_SUB_MDL_CDE"),
                   lit(2).alias("GCID_MOI_ADD_DTTM")
                 )
               )\
               .union(
                 keyDf.select(
                   col("MOI_MRKT_CNTC_IDENT_in").alias("MOI_MRKT_CNTC_IDENT"),
                   col("ETL_REQ_NUM_in").alias("ETL_REQ_NUM"),
                   col("VEH_MDL_CDE_in3").alias("VEH_MDL_CDE"),
                   col("MOI_ADD_DTTM_in3").alias("MOI_ADD_DTTM"),
                   col("VEH_SUB_MDL_CDE_in3").alias("VEH_SUB_MDL_CDE"),
                   col("MDL_YR_in3").alias("MDL_YR"),
                   col("VEH_MAKE_CDE_in3").alias("VEH_MAKE_CDE"),
                   lit(3).alias("GCID_MDL_YR"),
                   lit(3).alias("GCID_VEH_MAKE_CDE"),
                   lit(3).alias("GCID_VEH_MDL_CDE"),
                   lit(3).alias("GCID_VEH_SUB_MDL_CDE"),
                   lit(3).alias("GCID_MOI_ADD_DTTM")
                 )
               )\
               .union(
                 keyDf.select(
                   col("MOI_MRKT_CNTC_IDENT_in").alias("MOI_MRKT_CNTC_IDENT"),
                   col("ETL_REQ_NUM_in").alias("ETL_REQ_NUM"),
                   col("VEH_MDL_CDE_in4").alias("VEH_MDL_CDE"),
                   col("MOI_ADD_DTTM_in4").alias("MOI_ADD_DTTM"),
                   col("VEH_SUB_MDL_CDE_in4").alias("VEH_SUB_MDL_CDE"),
                   col("MDL_YR_in4").alias("MDL_YR"),
                   col("VEH_MAKE_CDE_in4").alias("VEH_MAKE_CDE"),
                   lit(4).alias("GCID_MDL_YR"),
                   lit(4).alias("GCID_VEH_MAKE_CDE"),
                   lit(4).alias("GCID_VEH_MDL_CDE"),
                   lit(4).alias("GCID_VEH_SUB_MDL_CDE"),
                   lit(4).alias("GCID_MOI_ADD_DTTM")
                 )
               )\
               .union(
                 keyDf.select(
                   col("MOI_MRKT_CNTC_IDENT_in").alias("MOI_MRKT_CNTC_IDENT"),
                   col("ETL_REQ_NUM_in").alias("ETL_REQ_NUM"),
                   col("VEH_MDL_CDE_in5").alias("VEH_MDL_CDE"),
                   col("MOI_ADD_DTTM_in5").alias("MOI_ADD_DTTM"),
                   col("VEH_SUB_MDL_CDE_in5").alias("VEH_SUB_MDL_CDE"),
                   col("MDL_YR_in5").alias("MDL_YR"),
                   col("VEH_MAKE_CDE_in5").alias("VEH_MAKE_CDE"),
                   lit(5).alias("GCID_MDL_YR"),
                   lit(5).alias("GCID_VEH_MAKE_CDE"),
                   lit(5).alias("GCID_VEH_MDL_CDE"),
                   lit(5).alias("GCID_VEH_SUB_MDL_CDE"),
                   lit(5).alias("GCID_MOI_ADD_DTTM")
                 )
               )\
               .union(
                 keyDf.select(
                   col("MOI_MRKT_CNTC_IDENT_in").alias("MOI_MRKT_CNTC_IDENT"),
                   col("ETL_REQ_NUM_in").alias("ETL_REQ_NUM"),
                   col("VEH_MDL_CDE_in6").alias("VEH_MDL_CDE"),
                   col("MOI_ADD_DTTM_in6").alias("MOI_ADD_DTTM"),
                   col("VEH_SUB_MDL_CDE_in6").alias("VEH_SUB_MDL_CDE"),
                   col("MDL_YR_in6").alias("MDL_YR"),
                   col("VEH_MAKE_CDE_in6").alias("VEH_MAKE_CDE"),
                   lit(6).alias("GCID_MDL_YR"),
                   lit(6).alias("GCID_VEH_MAKE_CDE"),
                   lit(6).alias("GCID_VEH_MDL_CDE"),
                   lit(6).alias("GCID_VEH_SUB_MDL_CDE"),
                   lit(6).alias("GCID_MOI_ADD_DTTM")
                 )
               )\
               .union(
                 keyDf.select(
                   col("MOI_MRKT_CNTC_IDENT_in").alias("MOI_MRKT_CNTC_IDENT"),
                   col("ETL_REQ_NUM_in").alias("ETL_REQ_NUM"),
                   col("VEH_MDL_CDE_in7").alias("VEH_MDL_CDE"),
                   col("MOI_ADD_DTTM_in7").alias("MOI_ADD_DTTM"),
                   col("VEH_SUB_MDL_CDE_in7").alias("VEH_SUB_MDL_CDE"),
                   col("MDL_YR_in7").alias("MDL_YR"),
                   col("VEH_MAKE_CDE_in7").alias("VEH_MAKE_CDE"),
                   lit(7).alias("GCID_MDL_YR"),
                   lit(7).alias("GCID_VEH_MAKE_CDE"),
                   lit(7).alias("GCID_VEH_MDL_CDE"),
                   lit(7).alias("GCID_VEH_SUB_MDL_CDE"),
                   lit(7).alias("GCID_MOI_ADD_DTTM")
                 )
               )\
               .union(keyDf.select(
        col("MOI_MRKT_CNTC_IDENT_in").alias("MOI_MRKT_CNTC_IDENT"),
        col("ETL_REQ_NUM_in").alias("ETL_REQ_NUM"),
        col("VEH_MDL_CDE_in8").alias("VEH_MDL_CDE"),
        col("MOI_ADD_DTTM_in8").alias("MOI_ADD_DTTM"),
        col("VEH_SUB_MDL_CDE_in8").alias("VEH_SUB_MDL_CDE"),
        col("MDL_YR_in8").alias("MDL_YR"),
        col("VEH_MAKE_CDE_in8").alias("VEH_MAKE_CDE"),
        lit(8).alias("GCID_MDL_YR"),
        lit(8).alias("GCID_VEH_MAKE_CDE"),
        lit(8).alias("GCID_VEH_MDL_CDE"),
        lit(8).alias("GCID_VEH_SUB_MDL_CDE"),
        lit(8).alias("GCID_MOI_ADD_DTTM")
               ))

    return out0
