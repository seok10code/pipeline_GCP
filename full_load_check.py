from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import sys
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.cloud import storage

ip = "ip"
port = 5432
db = "postgres"
user = "user"
password = "password"
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://ip_address:5432/dbname"

db_table_list = [
'DBF0.MVIEW_INSA_NEW'
,'DDB0.DDBBT_BDGT'
,'DDB0.DDBBT_BDGT_DTIL'
,'DDB0.DDBBT_BDGT_DTIL_MST'
,'DDB0.DDBBT_BDGT_RFCT'
,'DDB0.DDBET_CST_COST_IVST'
,'DDB0.DDBGT_BIZ_INOT_INFO'
,'DDB0.DDBGT_UDTG_CHG_CTRT_ASCT'
,'DDB0.DDBGT_UDTG_CTRT_ASCT'
,'DDB0.DDBGT_UDTG_CTRT_CNTS'
,'DDB0.DDBGT_UDTG_CTRT_DMND'
,'DDB0.DDBGT_UDTG_CTRT_INFO'
,'DDB0.DDBGT_UDTG_CTRT_ORDR'
,'DDB0.DDBHT_SITE'
,'DDC1.DDCET_CWP_ACPT_EVDC'
,'DDC1.DDCCT_HADOAPPREQ'
,'DDC1.DDCCT_HADOAPPREQAMT'
,'DDC1.DDCCT_STOCK_REQDOC'
,'DDC1.DDCCT_SUGUB'
,'DDC1.DDCBT_BALPLN'
,'DDC1.DDCBT_BALPLNRSTAMT'
,'DDC1.DDCBT_ESTAMTVS'
,'DDC1.DDCBT_UPCCHSREQ'
,'DDC1.DDCBT_UPCCHSREQAMT'
,'DDC1.DDCCT_GEYCUR'
,'DDC1.DDCCT_GEYCURAMT'
,'DDC1.DDCCT_STOCK'
,'DDC1.DDCBT_BDGTCOMP_EST'
,'DDC1.DDCBT_EHADOSILHENG_EST'
,'DDC1.DDCET_CSTLAW_CTYPCD'
,'DDC1.DDCCT_ORDRACPTCNTS'
,'DDG1.CD_DTIL'
,'DDG1.KIND_CD'
,'DDT0.DDTAT_ASCT_NM'
,'DDT0.DDTAT_ASCT_NM_CHG_HSTR'
,'DDT0.DDTAT_CTRT_PRSN_CHG_HSTR'
,'DDT0.DDTAT_CTRT_PRSN_INFO'
,'DDT0.DDTAT_CTRT_PRSN_RCTM'
,'DDT0.DDTAT_DONG_INFO'
,'DDT0.DDTAT_DONG_NOHS_CHG_HSTR'
,'DDT0.DDTAT_DONG_NOHS_INFO'
,'DDT0.DDTAT_FMLY_DTIL'
,'DDT0.DDTAT_HOSE_FORM_AREA'
,'DDT0.DDTAT_NIPT_CNSL_INFO'
,'DDT0.DDTAT_NIPT_CNTS'
,'DDT0.DDTAT_NIPT_CTRT'
,'DDT0.DDTAT_NIPT_CTRT_CNTS'
,'DDT0.DDTAT_NIPT_ITEM'
,'DDT0.DDTAT_NIPT_RCTM'
,'DDT0.DDTAT_PMTP_ITRT'
,'DDT0.DDTAT_PMTP_ITRT_RPMT'
,'DDT0.DDTAT_SITE_INFO'
,'DDT0.DDTBT_ALCT_CNTS'
,'DDT0.DDTBT_ALCT_ITEM_TT'
,'DDT0.DDTBT_ALCT_SITE_TT'
,'DDT0.DDTBT_AS_CPNY_POOL'
,'DDT0.DDTBT_BDGT_INFO_SITE'
,'DDT0.DDTBT_BIZ_DARY'
,'DDT0.DDTBT_BIZ_DARY_CNTS'
,'DDT0.DDTBT_BIZ_DARY_RCHNDL_CRST'
,'DDT0.DDTBT_BIZ_DARY_STTS'
,'DDT0.DDTBT_COOP_CPNY_LMGT_PLN'
,'DDT0.DDTBT_CPNY_DFCT_CTYP'
,'DDT0.DDTBT_CPNY_NOHS_QOTA'
,'DDT0.DDTBT_CSTN'
,'DDT0.DDTBT_CSTN_ITEM'
,'DDT0.DDTBT_CTRT_ITEM'
,'DDT0.DDTBT_CTRT_OWN'
,'DDT0.DDTBT_CWP'
,'DDT0.DDTBT_CWP_ITEM'
,'DDT0.DDTBT_DFCT'
,'DDT0.DDTBT_DFCT_ADMN_STD'
,'DDT0.DDTBT_DFCT_ASRC'
,'DDT0.DDTBT_DFCT_CAUS'
,'DDT0.DDTBT_DFCT_CL'
,'DDT0.DDTBT_DFCT_CTYP'
,'DDT0.DDTBT_DFCT_L_CTYP'
,'DDT0.DDTBT_DFCT_TYPE'
,'DDT0.DDTBT_DONG'
,'DDT0.DDTBT_HAPY_CALL'
,'DDT0.DDTBT_HNDL_PSTM'
,'DDT0.DDTBT_HOSH'
,'DDT0.DDTBT_HOSH_NIPT'
,'DDT0.DDTBT_HOSH_SCAB_MTTR'
,'DDT0.DDTBT_HP_TYPE_CHK'
,'DDT0.DDTBT_LIN'
,'DDT0.DDTBT_LMGT'
,'DDT0.DDTBT_LMGT_PLN'
,'DDT0.DDTBT_LMGT_RSVT'
,'DDT0.DDTBT_LOC'
,'DDT0.DDTBT_MY_ANSS_TT'
,'DDT0.DDTBT_OFDC_NTFC'
,'DDT0.DDTBT_QT_CHK_SCHD'
,'DDT0.DDTBT_QUIK_CLSF'
,'DDT0.DDTBT_RE_SBCN_CPNY'
,'DDT0.DDTBT_RGON'
,'DDT0.DDTBT_SITE'
,'DDT0.DDTBT_SITE_CPNY'
,'DDT0.DDTBT_SITE_QUIK_CLSF'
,'DDT0.DDTBT_TPPG'
,'DDT0.DDTBT_TT_CSTM_APMT'
,'DDT0.DDTBT_TT_DFCT_ACPT'
,'DDT0.DDTBT_TT_DFCT_HNDL_DR'
,'DDT0.DDTBT_TT_DFCT_OUTC'
,'DDT0.DDTBT_TT_DFCT_RCPT'
,'DDT0.DDTBT_TT_END_DFCT_ACPT'
,'DDT0.DDTBT_TT_END_DFCT_OUTC'
,'DDT0.DDTBT_TT_HAPY_CALL_OUTC'
,'DDT0.DDTBT_TT_WHOL_DFCT_ACPT'
,'DDT0.DDTBT_TT_WHOL_DFCT_OUTC'
,'DDT0.DDTBT_TT_WHOL_END_DFCT_ACPT'
,'DDT0.DDTBT_TT_WHOL_END_DFCT_OUTC'
,'DDT0.DDTBT_VOC'
,'DDT0.DDTBT_VOC_ASR'
,'DDT0.DDTCT_CCC_ORG'
,'DDT0.DDTCT_CCC_STD_CD'
,'DDT0.DDTCT_CNSL_CNTS'
,'DDT0.DDTCT_CNSL_DTIL_CNTS'
,'DDT0.DDTCT_CNSL_TYPE_CD'
,'DDT0.DDTCT_CNSR_INFO'
,'DDT0.DDTCT_CSTM_MST'
,'DDT0.DDTCT_CSTM_THNG'
,'DDT0.DDTCT_STT_RSLT'
,'DDT0.DDTCT_VOC_ORG'
,'DDT0.DDTDT_APVL_INFO'
,'DDT0.DDTDT_SITE_INFO'
,'DDT0.DDTBT_AS_MTRL'
,'DDT0.DDTBT_ACC'
,'DDT0.DDTBT_BDGT_INFO_TGT_SITE'
,'DDT2.DDTRT_CSTM_CNSL'
,'DDT2.DDTRT_CSTM_CNSL_DTIL'
,'DDT2.DDTRT_CTRT_BSC'
,'DDT2.DDTRT_CTRT_PRSN_CNTS'
,'DDT2.DDTRT_DTLS_SITE_BSC'
,'DDT2.DDTRT_FMLY_CNTS'
,'DDT2.DDTRT_LEAS_BIZ_CSTM'
,'DDT2.DDTRT_LEAS_COMN_DTIL_CD'
,'DDT2.DDTRT_SITE_BSC'
,'DDT4.TB_AGENT'
,'DDT4.TB_CONSULT'
,'DDT4.TB_CONSULT_CODE'
,'DDT4.TB_CUSTCARD'
,'DDT4.TB_GROUP'
,'DDT4.TB_LCODE'
,'DDT4.TB_SCODE'
,'DDT4.TB_SURV'
,'DGW51.DGWET_BIZ_INFO'
,'DGW51.DGWET_CSTM_BIZLND_INFO'
,'DGW51.DGWET_PBRL_AGRT'
,'DGW51.DGWET_MBR_PAUS'
,'DGW51.DGWET_MEMBER'
,'DGW52.DGWKT_BIZ_INFO'
,'DGW52.DGWKT_CSTM_BIZLND_INFO'
,'DGW52.DGWKT_PBRL_AGRT'
,'DGW52.DGWKT_MBR_PAUS'
,'DGW52.DGWKT_INFO_CSTM'
,'DGW82.DGWRT_MDLE_CNTS'
,'DGW82.DGWRT_LCT'
,'DGW82.DGWRT_RSVT'
,'DBD0.DBCAT_VENDOR'
,'DBD0.DBCBT_SLIPDTIL'
,'DAM1.MDM_PROJ_INSA'
,'DAM1.MDM_PROJ_MAST'
]
change_name_path = "gs://backet_name/컬럼변경.csv"

project_id = "project-workshop-2022"

spark = SparkSession.builder \
    .appName("PySpark PostgreSQL") \
    .config("spark.jars", "/postgresql-42.5.0.jar") \
    .master("yarn") \
    .getOrCreate()

bucket = "dp-files"
spark.conf.set('temporaryGcsBucket', bucket)
dbtable = 'DDB0.DDBGT_UDTG_CTRT_ORDR'

test = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", "jdbc:postgresql://20.214.216.108:{0}/{1}.format( port, db )")\
  .option("dbtable", dbtable)\
  .option("user", user)\
  .option("password", password)\
  .load()

dataset_id = "DDB0"
table_id = "DDBGT_UDTG_CTRT_ORDR"

test.createOrReplaceGlobalTempView("df")
select_df = spark.sql(f"SELECT * FROM df")
select_df.write\
  .mode("overwrite")\
  .format("bigquery")\
  .option("table","{}:{}.{}".format(project_id, dataset_id, table_id))
# col_name = spark.read\
#   .option("sep", ",")\
#   .option("header", True)\
#   .option("inferSchema", True)\
#   .csv("gs://cloocus-swkim-src/컬럼변경.csv")

# for idx, table in enumerate(db_table_list):
#   dataset_id, table_id = table.split(".")
#   print(dataset_id, table_id)
#   checker = col_name.filter((col_name.OWNER == dataset_id) & (col_name.TABLE_NAME == table_id))
#   remote_table = (
#   spark.read.format("jdbc") \
#     .option("driver", driver)
#     .option("url", "jdbc:postgresql://20.214.216.108:{0}/{1}".format( port, db ))
#     .option("dbtable", table.lower())
#     .option("user", user)
#     .option("password", password)
#     .load()
#   )
  
  # remote_table.select([col(item.OLD_COL_NM).alias(item.NEW_COL_NM) for item in checker.collect()])
  # remote_table.createOrReplaceTempView(f"df{idx}")
  # select_df = spark.sql(f"SELECT * FROM df{idx}")
  # select_df.write\
  # .mode("overwrite")\
  # .format("bigquery")\
  # .option('table', "{}:{}.{}".format(project_id, dataset_id, table_id))\
  # .save()
  
  # checker.unpersist()