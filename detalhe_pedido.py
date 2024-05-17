# rodando via shell spark-submit --name fatura --master yarn --deploy-mode client --queue reportGerencial --executor-memory 12g --executor-cores 8 --total-executor-cores 8 detalhe_fatura.py

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



sc = SparkContext()
sql = SQLContext(sc)
hc = HiveContext(sc)

hc.setConf("spark.sql.shuffle.partitions", "1")
hc.setConf("spark.files.maxPartitionBytes", "134217728")
hc.setConf("spark.sql.adaptive.enabled", "true")
hc.setConf("spark.sql.files.maxPartitionBytes", "128MB")


dt_data = '2022-01-05 00:00:00'
#dt_data = sys.argv[1]
dl_fibra = config.db_raw

windowSpec  = Window.partitionBy("nu_id").orderBy(col("dh_systemmodstamp").desc())

#stg_sf_order
stg_sf_order = hc.read.table(dl_fibra+'.stg_sf_order')\
           .where( (col('ds_status').isin('Em Aprovisionamento','Pendência Técnica',        'Pendência Cliente',\
           'Pedido Enviado com Sucesso', 'Concluído')) &\
                  (~col('ds_t2r_sub_type_c').isin('Retirada','Down','Alt_Pagamento','Up') ))\
           .select('nu_id','dh_systemmodstamp',\
           'nu_accountid','nu_CONTRACTID','cd_idautorizacaopm_c',\
           'nu_ordernumber','dh_createddate','ds_status',\
           'ds_vl_cmt_accountrecordtype_c','ds_shippingstate',\
           'nu_dom_c','ds_fidelizacao_c','dh_activateddate',\
           'ds_l2c_pdv_c','dt_l2c_dataagendamentoinicio_c',\
           'ds_t2r_sub_type_c','dt_l2c_dataagendamentofim_c',\
           'ds_shippingcity' )
                 
stg_sf_order = stg_sf_order\
             .withColumn("row_number",row_number().over(windowSpec))\
             .where(col('row_number')== 1)\
             .drop(col('dh_systemmodstamp'))\
             .drop(col('row_number'))

#stg_sf_order.show(4)

#stg_sf_orderitem
stg_sf_orderitem = hc.read.table(dl_fibra+'.stg_sf_orderitem')\
           .select('nu_id','dh_systemmodstamp',\
                   'nu_orderid',\
                   'nu_vlocity_cmt_product2id_c',\
                   'nu_vlocity_cmt_recurringtotal_c' )
                   
stg_sf_orderitem = stg_sf_orderitem\
                 .withColumn("row_number",row_number().over(windowSpec))\
                 .where(col('row_number')== 1)\
                 .drop(col('dh_systemmodstamp'))\
                 .drop(col('row_number'))

#stg_sf_orderitem.show(4)

#stg_sf_asset
stg_sf_asset = hc.read.table(dl_fibra+'.stg_sf_asset')\
                  .select('nu_id','dh_systemmodstamp',\
                  'nu_vl_cmt_orderproductid_c',\
                   'ds_status','ds_vl_cmt_provisioningstatus_c')
                  
stg_sf_asset = stg_sf_asset\
                 .withColumn("row_number",row_number().over(windowSpec))\
                 .where((col('row_number')== 1))\
                 .drop(col('dh_systemmodstamp'))\
                 .drop(col('row_number'))
                 

#stg_sf_asset.show(4)           

#stg_sf_product2
stg_sf_product2 = hc.read.table(dl_fibra+'.stg_sf_product2')\
                  .select('nu_id','dh_systemmodstamp',\
                   'ds_tiposva_c','ds_incidencia_c','nu_productcode',\
                   'ds_vl_cmt_specificationtype_c','ds_name')

stg_sf_product2 = stg_sf_product2\
                 .withColumn("row_number",row_number().over(windowSpec))\
                 .where(col('row_number')== 1)\
                 .drop(col('dh_systemmodstamp'))\
                 .drop(col('row_number'))
#stg_sf_product2.show(4)

#stg_sf_account
stg_sf_account = hc.read.table(dl_fibra+'.stg_sf_account')\
           .select('nu_id','dh_systemmodstamp',\
           'ds_cpf_c','ds_l2c_nomecompleto_c')

stg_sf_account = stg_sf_account\
             .withColumn("row_number",row_number().over(windowSpec))\
             .where(col('row_number')== 1)\
             .drop(col('dh_systemmodstamp'))\
             .drop(col('row_number'))
stg_sf_account.show(4)

#stg_sf_contract
stg_sf_contract = hc.read.table(dl_fibra+'.stg_sf_contract')\
           .select('nu_id','dh_systemmodstamp',\
           'ds_contractnumber','ds_status','dh_activateddate')           
                 
stg_sf_contract = stg_sf_contract\
             .withColumn("row_number",row_number().over(windowSpec))\
             .where(col('row_number')== 1)\
             .drop(col('dh_systemmodstamp'))\
             .drop(col('row_number'))

#stg_sf_contract.show(4)

#stg_sf_payment_method
stg_sf_payment_method = hc.read.table(dl_fibra+'.stg_sf_payment_method')\
           .select('nu_id','dh_systemmodstamp',\
           'ds_vl_cmt_methodtype_c','ds_vl_cmt_bankaccounttype_c',\
           'ds_vl_cmt_bankaccountholdernm_c','ds_vl_cmt_bankaccountnumber_c')
                  
stg_sf_payment_method = stg_sf_payment_method\
             .withColumn("row_number",row_number().over(windowSpec))\
             .where(col('row_number')== 1)\
             .drop(col('dh_systemmodstamp'))\
             .drop(col('row_number'))

detalhe_pedido = stg_sf_order\
         .join(stg_sf_orderitem,stg_sf_order.nu_id == stg_sf_orderitem.nu_orderid, "left")\
         .join(stg_sf_asset,stg_sf_orderitem.nu_id == stg_sf_asset.nu_vl_cmt_orderproductid_c, "left")\
         .join(stg_sf_product2,stg_sf_orderitem.nu_vlocity_cmt_product2id_c == stg_sf_product2.nu_id,"left")\
         .join(stg_sf_account,stg_sf_order.nu_accountid == stg_sf_account.nu_id, "left")\
         .join(stg_sf_contract,stg_sf_order.nu_CONTRACTID == stg_sf_contract.nu_id,"left")\
         .join(stg_sf_payment_method,stg_sf_order.cd_idautorizacaopm_c == stg_sf_payment_method.nu_id, "left")\
         .withColumn('dt',stg_sf_order.dh_activateddate.substr(1,10))\
         .withColumn('ds_meio_pagamento',when(col('ds_vl_cmt_methodtype_c')==1 , '1 - Cartão de Crédito')\
                        .when(col('ds_vl_cmt_methodtype_c')== 2, '2 - Boleto')\
                        .when(col('ds_vl_cmt_methodtype_c')== 3, '3 - Débito Direto')\
                        .when(col('ds_vl_cmt_methodtype_c')== 4, '4 - Cartão Presente')\
                        .when(col('ds_vl_cmt_methodtype_c')== 5, '5 - INAPP')\
                        .when(col('ds_vl_cmt_methodtype_c')== 6, '6 - DC')\
                        .otherwise(col('ds_vl_cmt_methodtype_c')))\
        .select (stg_sf_account.nu_id.alias('nu_conta'),\
                stg_sf_order.nu_ordernumber.alias('nu_pedido'),\
                stg_sf_order.dh_createddate.alias('dt_pedido'),\
                stg_sf_order.ds_status.alias('ds_status_pedido'),\
                stg_sf_contract.ds_contractnumber.alias('nu_contrato'),\
                stg_sf_contract.ds_status.alias('ds_status_contrato'),\
                stg_sf_contract.dh_activateddate.alias('dt_ativacao_contrato'),\
                stg_sf_order.ds_vl_cmt_accountrecordtype_c.alias('ds_segmento'),\
                stg_sf_account.ds_cpf_c.alias('cd_documento'),\
                stg_sf_account.ds_l2c_nomecompleto_c.alias('ds_nome'),\
                stg_sf_order.ds_shippingstate.alias('cd_uf_ativacao'),\
                stg_sf_payment_method.ds_vl_cmt_methodtype_c.alias('cd_meio_pagamento'),\
                'ds_meio_pagamento',\
                stg_sf_order.nu_dom_c.alias('nu_dia_aniversario'),\
                stg_sf_order.ds_fidelizacao_c.alias('ds_fidelizacao'),\
                stg_sf_orderitem.nu_id.alias('nu_id_produto'),\
                stg_sf_asset.nu_id.alias('nu_asset_id'),\
                stg_sf_asset.ds_status.alias('ds_asset_status'),\
                stg_sf_asset.ds_vl_cmt_provisioningstatus_c.alias('ds_asset_provisioning_status'),\
                stg_sf_order.dh_activateddate.alias('dh_ativacao_produto'),\
                stg_sf_product2.nu_productcode.alias('cd_produto'),\
                stg_sf_product2.ds_name.alias('ds_produto'),\
                stg_sf_product2.ds_vl_cmt_specificationtype_c.alias('ds_tipo_especificacao'),\
                stg_sf_orderitem.nu_vlocity_cmt_recurringtotal_c.alias('vl_produto'),\
                stg_sf_order.ds_l2c_pdv_c.alias('cd_pdv'),\
                col('dt'),\
                stg_sf_payment_method.ds_vl_cmt_bankaccounttype_c.alias('banco'),\
                stg_sf_payment_method.ds_vl_cmt_bankaccountholdernm_c.alias('agencia'),\
                stg_sf_payment_method.ds_vl_cmt_bankaccountnumber_c.alias('nu_conta_banco'),\
                stg_sf_product2.ds_tiposva_c.alias('ds_tipo_sva'),\
                stg_sf_product2.ds_incidencia_c.alias('ds_incidencia_sva'),\
                stg_sf_order.dt_l2c_dataagendamentoinicio_c.alias('dt_agendamento_inicio'),\
                stg_sf_order.dt_l2c_dataagendamentofim_c.alias('dt_agendamento_fim'),\
                stg_sf_order.ds_t2r_sub_type_c.alias('subtipo'),\
                stg_sf_order.ds_shippingcity.alias('localidade'))
detalhe_pedido.write\
  .mode("overwrite")\
  .option("encoding", "utf-8")\
  .parquet("/pedido")                