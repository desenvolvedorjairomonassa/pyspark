# 
#como rodar via shell spark-submit --name detalhe --master yarn --deploy-mode client --queue depFinanceiro --executor-memory 12g --executor-cores 8 --total-executor-cores 8 detalhe_fatura.py
# ---------------------------------- Detalhe_Fatura -----------------------------------------
#libraries
import os
import sys
import json
from pyspark.sql.functions import when, col, lit, upper,count
from pyspark import SparkContext, SparkConf, SQLContext, HiveContext
from collections import namedtuple
import locale;
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


os.environ["PYTHONIOENCODING"] = "utf-8";
reload(sys)
sys.setdefaultencoding('UTF-8')


sc = SparkContext()
sql = SQLContext(sc)
hc = HiveContext(sc)

hc.setConf("spark.sql.shuffle.partitions", "1")
hc.setConf("spark.files.maxPartitionBytes", "134217728")
hc.setConf("spark.sql.adaptive.enabled", "true")
hc.setConf("spark.sql.files.maxPartitionBytes", "128MB")


dt_data = '2022-01-05 00:00:00'
dt_data = sys.argv[1]


bill_t = hc.read.table(dl_fibra+'.stg_brm_bill_t')\
            .select('ds_name','nu_poid_id0','nu_account_obj_id0','nu_billinfo_obj_id0','dh_mod_t')\          
            .where("dh_mod_t >= '{dt_data}'".format(dt_data))

windowSpec  = Window.partitionBy("nu_poid_id0").orderBy(col("dh_mod_t").desc())
                  
bill_t = bill_t\
             .withColumn("row_number",row_number().over(windowSpec))\
             .where(col('row_number')== 1)\
             .drop(col('dh_mod_t'))\
             .drop(col('row_number'))


ac_bill_t = hc.read.table(dl_fibra+'.stg_brm_ac_bill_t')\
               .select('nu_bill_obj_id0','dh_mod_t','nu_total_due','dh_created_t',\
                'dh_actg_last_t','dh_actg_next_t','nu_pay_type','dh_due_t','nu_status')
               
              
windowSpec  = Window.partitionBy("nu_bill_obj_id0").orderBy(col("dh_mod_t").desc())

ac_bill_t = ac_bill_t\
             .withColumn("row_number",row_number().over(windowSpec))\
             .where(col('row_number')== 1)\
             .drop(col('dh_mod_t'))\
             .drop(col('row_number'))
                    
#account_t
account_t = hc.read.table(dl_fibra+'.stg_brm_account_t')\
            .select('nu_poid_id0','dh_mod_t')

windowSpec  = Window.partitionBy("nu_poid_id0").orderBy(col("dh_mod_t").desc())

account_t = account_t\
             .withColumn("row_number",row_number().over(windowSpec))\
             .where(col('row_number')== 1)\
             .drop(col('dh_mod_t'))\
             .drop(col('row_number'))


#billing 
billinfo_t = hc.read.table(dl_fibra+'.stg_brm_bill_info_t')\
             .select('cd_poid_id0','dh_mod_t','nu_actg_cycle_dom')

windowSpec  = Window.partitionBy("cd_poid_id0").orderBy(col("dh_mod_t").desc())

billinfo_t = billinfo_t\
             .withColumn("row_number",row_number().over(windowSpec))\
             .where(col('row_number')== 1)\
             .drop(col('dh_mod_t'))\
             .drop(col('row_number'))
             
#ac_billinfo_t
ac_billinfo_t = hc.read.table(dl_fibra+'.stg_brm_ac_billinfo_t')\
                .select('nu_poid_id0','nu_billinfo_obj_id0','dh_mod_t','in_isencao')
                
windowSpec  = Window.partitionBy("nu_poid_id0").orderBy(col("dh_mod_t").desc())

ac_billinfo_t = ac_billinfo_t\
             .withColumn("row_number",row_number().over(windowSpec))\
             .where(col('row_number')== 1)\
             .drop(col('dh_mod_t'))\
             .drop(col('row_number'))


#ac_par_t 
ac_par_t = hc.read.table(dl_fibra+'.stg_brm_ac_par_t')\
           .select('cd_poid_id0','dh_mod_t','cd_par_id',\
            'nu_bill_obj_id0','dh_par_due_date', 'nu_total_value')


windowSpec  = Window.partitionBy("cd_poid_id0").orderBy(col("dh_mod_t").desc())

ac_par_t = ac_par_t\
            .withColumn("row_number",row_number().over(windowSpec))\
            .where(col('row_number')== 1)\
            .drop(col('dh_mod_t'))\
            .drop(col('row_number'))
           
#item_t
item_t = hc.read.table(dl_fibra+'.stg_brm_item_t')\
           .select('dh_mod_t','cd_item_no','nu_item_total','cd_poid_id0',\
           'cd_bill_obj_id0','cd_service_obj_id0')          

windowSpec  = Window.partitionBy("cd_poid_id0").orderBy(col("dh_mod_t").desc())

item_t = item_t\
            .withColumn("row_number",row_number().over(windowSpec))\
            .where(col('row_number')== 1)\
            .drop(col('dh_mod_t'))\
            .drop(col('row_number'))

#service_t
service_t = hc.read.table(dl_fibra+'.stg_brm_service_t')\
           .select('dh_mod_t','no_poid_type','cd_poid_id0')
           
windowSpec  = Window.partitionBy("cd_poid_id0").orderBy(col("dh_mod_t").desc())

service_t = service_t\
            .withColumn("row_number",row_number().over(windowSpec))\
            .where(col('row_number')== 1)\
            .drop(col('dh_mod_t'))\
            .drop(col('row_number'))

#purchased_product_t
purchased_product_t = hc.read.table(dl_fibra+'.stg_brm_purchased_product_t')\
           .select('dh_mod_t','dh_created_t','nu_service_obj_id0','nu_poid_id0')
           
windowSpec  = Window.partitionBy("nu_poid_id0").orderBy(col("dh_mod_t").desc())

purchased_product_t = purchased_product_t\
            .withColumn("row_number",row_number().over(windowSpec))\
            .where(col('row_number')== 1)\
            .drop(col('dh_mod_t'))\
            .drop(col('row_number'))

#ac_purchased_product_t
ac_purchased_product_t = hc.read.table(dl_fibra+'.stg_brm_ac_purchased_product_t')\
           .select('dh_mod_t','nu_poid_id0','ds_crm_product_descr',\
           'nu_crm_product_id','nu_cat_product_id','nu_purchased_product_obj_id0')          
           
windowSpec  = Window.partitionBy("nu_poid_id0").orderBy(col("dh_mod_t").desc())

ac_purchased_product_t = ac_purchased_product_t\
            .withColumn("row_number",row_number().over(windowSpec))\
            .where(col('row_number')== 1)\
            .drop(col('dh_mod_t'))\
            .drop(col('row_number'))


#ac_protocol_members_t
ac_protocol_members_t = hc.read.table(dl_fibra+'.stg_brm_ac_protocol_members_t')\
           .select('dh_extracao','cd_obj_id0', 'cd_rec_id','no_poid_type',\
           'cd_poid_id0','cd_obj_id0')\
           .where(col('no_poid_type') == '/purchased_product')
           
windowSpec  = Window.partitionBy("cd_obj_id0", "cd_rec_id").orderBy(col("dh_extracao").desc())

ac_protocol_members_t = ac_protocol_members_t\
            .withColumn("row_number",row_number().over(windowSpec))\
            .where(col('row_number')== 1)\
            .drop(col('dh_mod_t'))\
            .drop(col('row_number'))


#ac_protocol_t
ac_protocol_t = hc.read.table(dl_fibra+'.stg_brm_ac_protocol_t')\
           .select('dh_mod_t','cd_poid_id0','ds_ac_protocol_id')\
           
           
windowSpec  = Window.partitionBy("cd_poid_id0").orderBy(col("dh_mod_t").desc())

ac_protocol_t = ac_protocol_t\
            .withColumn("row_number",row_number().over(windowSpec))\
            .where(col('row_number')== 1)\
            .drop(col('dh_mod_t'))\
            .drop(col('row_number'))

nf_detalhe_fatura = bill_t\
                    .join(ac_bill_t,ac_bill_t.nu_bill_obj_id0 == bill_t.nu_poid_id0)\
                    .join(account_t,bill_t.nu_account_obj_id0 == account_t.nu_poid_id0)\
                    .join(billinfo_t,bill_t.nu_billinfo_obj_id0 == billinfo_t.cd_poid_id0)\
                    .join(ac_billinfo_t,billinfo_t.cd_poid_id0 == ac_billinfo_t.nu_billinfo_obj_id0)\
                    .join(item_t,bill_t.nu_poid_id0 == item_t.cd_bill_obj_id0)\
                    .join(ac_par_t, bill_t.nu_poid_id0 == ac_par_t.nu_bill_obj_id0,"left")\
                    .join(service_t,item_t.cd_service_obj_id0 == service_t.cd_poid_id0, "left")\
                    .join(purchased_product_t,service_t.cd_poid_id0 == purchased_product_t.nu_service_obj_id0,"left")\
                    .join(ac_purchased_product_t,purchased_product_t.nu_poid_id0 == ac_purchased_product_t.nu_purchased_product_obj_id0,"left")\
                    .join(ac_protocol_members_t,purchased_product_t.nu_poid_id0 == ac_protocol_members_t.cd_poid_id0,"left")\
                    .join(ac_protocol_t,ac_protocol_members_t.cd_obj_id0 == ac_protocol_t.cd_poid_id0,"left")\
                    .select(bill_t.nu_poid_id0,\
                        ac_par_t.cd_par_id,\
                        billinfo_t.cd_poid_id0,\
                        ac_bill_t.nu_total_due,\
                        ac_bill_t.dh_created_t,\
                        billinfo_t.nu_actg_cycle_dom,\
                        ac_bill_t.dh_actg_last_t,\
                        ac_bill_t.dh_actg_next_t,\
                        ac_bill_t.nu_pay_type,\
                        ac_bill_t.nu_status,\
                        ac_protocol_t.ds_ac_protocol_id,\
                        item_t.cd_item_no,\
                        item_t.nu_item_total,\
                        service_t.no_poid_type,\
                        purchased_product_t.dh_created_t.alias('dh_ativacao_produto'),\
                        ac_purchased_product_t.ds_crm_product_descr,\
                        ac_purchased_product_t.nu_crm_product_id,\
                        ac_purchased_product_t.nu_cat_product_id,\
                        ac_bill_t.dh_due_t,\
                        bill_t.ds_name,\
                        bill_t.nu_billinfo_obj_id0.alias('billinfo'),\
                        ac_par_t.dh_par_due_date,\
                        ac_par_t.nu_total_value,\
                        ac_billinfo_t.in_isencao)\
                    .withColumnRenamed('ds_name','ds_bill_now')\
                    .withColumnRenamed('nu_total_due', 'vl_fatura')\
                    .withColumnRenamed('dh_created_t','dh_emissao_fatura')\
                    .withColumnRenamed('dh_actg_last_t','dh_periodo_de')\
                    .withColumnRenamed('dh_actg_next_t','dh_periodo_ate')\
                    .withColumnRenamed('nu_pay_type','cd_meio_pagamento')\
                    .withColumnRenamed('dh_due_t','dh_vencimento')\
                    .withColumnRenamed('nu_status','nu_status_fatura')\
                    .withColumnRenamed('cd_poid_id0','cd_conta_fatura')\
                    .withColumnRenamed('nu_actg_cycle_dom','nu_dom')\
                    .withColumnRenamed('cd_par_id','cd_fatura')\
                    .withColumnRenamed('cd_item_no','cd_item_fatura')\
                    .withColumnRenamed('nu_item_total','vl_item_fatura')\
                    .withColumnRenamed('no_poid_type','cd_tipo_servico')\                    
                    .withColumnRenamed('ds_crm_product_descr','ds_produto')\
                    .withColumnRenamed('nu_crm_product_id','id_produto')\
                    .withColumnRenamed('nu_cat_product_id','cd_produto')\
                    .withColumnRenamed('ds_ac_protocol_id','nu_pedido')

#nf_detalhe_fatura.printSchema()

nf_detalhe_fatura.write\
  .mode("overwrite")\
  .option("encoding", "utf-8")\
  .parquet("/fatura")
  



