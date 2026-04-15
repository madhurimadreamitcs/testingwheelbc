#!/usr/bin/env python
# coding: utf-8

# ## Config_BusinessCentral
# 
# null

# In[1]:

import json
import time
import os
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from delta.tables import DeltaTable
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from notebookutils import mssparkutils as notebookutils
from pyspark.sql.types import *
import re




# In[ ]:

def config_BusinessCentral(input_config, spark):
    # input_config = spark.conf.get("input_config", None)

    if not input_config:
        raise Exception("input_config not passed from pipeline")

        # Remove non-breaking spaces
    # input_config_fixed = input_config.replace('\xa0', ' ').strip()
    

    if isinstance(input_config, dict):
     config = input_config
    else:
        input_config_fixed = input_config.replace('\xa0', ' ').strip()
        try:
            config = json.loads(input_config_fixed)
        except json.JSONDecodeError as e:
            raise Exception(f"✗ Error: {e}")


    # In[6]:

    def create_lakehouses(names):

        for name in names:
            try:
                notebookutils.lakehouse.create(name=name)
                print(f"Created: {name}")
            except Exception as e:
                # The two possible error messages Fabric returns
                error_str = str(e).lower()
                if ("itemdisplaynamealreadyinuse" in error_str or 
                    "already in use" in error_str or 
                    "already exists" in error_str or "errorcode" in error_str):
                    print(f"Already exists: {name}")
                else:
                    print(f"Unexpected error for {name}: {e}")
                    raise

    lakehouse_names = ["Bronze_Lakehouse", "Staging_Lakehouse", "Silver_Lakehouse", "Gold_Lakehouse"]
    create_lakehouses(lakehouse_names)


    # In[6]:


    def get_lakehouse_path(lakehouse_name: str) -> str:
        try:
            lakehouse_details = notebookutils.lakehouse.get(lakehouse_name)
        except:
            raise Exception(f"A LAKEHOUSE WITH NAME '{lakehouse_name}' DOES NOT EXIST IN THE WORKSPACE")
        
        return lakehouse_details.get('properties', {}).get('abfsPath') + '/Tables'

    workspace_info = notebookutils.lakehouse.list() 
    if not workspace_info:
        raise Exception("No lakehouses found in the workspace.")

    WORKSPACE_ID = workspace_info[0].get('workspaceId') 

    STAGING_LAKEHOUSE_PATH = get_lakehouse_path('Staging_Lakehouse')
    BRONZE_LAKEHOUSE_PATH = get_lakehouse_path('Bronze_Lakehouse')
    SILVER_LAKEHOUSE_PATH = get_lakehouse_path ('Silver_Lakehouse')


    # # Bronze Config Table Setup 

    # In[ ]:


    # initialize the config table with the encrypted access token using the stored private key
    schema = StructType([
        # StructField("company_id", StringType(), True),
        StructField("table", StringType(), True),
        StructField("source", StringType(), True),
        StructField("watermark_column", StringType(), True),
        StructField("watermark_format", StringType(), True),
        StructField("key", StringType(), True),
        StructField("last_sync", TimestampType(), True),
        StructField("isActive", BooleanType(), True),
        StructField("URL", StringType(), True),
        StructField("URLtype", StringType(), True),
        StructField("filter_query", StringType(), True)
    ])

    last_sync = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")
    # last_sync = datetime.now() - timedelta(weeks=2)
    data = []
    
    if "sources" not in config:
        raise Exception("Missing 'sources' in input_config")
    sources = config["sources"]

    if not sources:
        raise Exception("No 'sources' found in config. Check your input_config.")
    for source_name, source_config in sources.items():
        table_name = f"br_bc_{source_name}"
        watermark_column = source_config["watermark_column"]
        watermark_format = source_config["watermark_format"]
        key = source_config["key"]
        # active_flag = source_config["active_flag"]
        active_flag = bool(source_config["active_flag"])
        URL = source_config["URL"]
        URLtype = source_config["URLtype"]
        filter_query = source_config["filter_query"]
        
        data.append((
            # company_id,
            table_name,
            source_name,
            watermark_column,
            watermark_format,
            key,
            last_sync,
            active_flag,
            URL,
            URLtype,
            filter_query
        ))


    new_df = spark.createDataFrame(data, schema)

    table_path = f"{BRONZE_LAKEHOUSE_PATH}/br_bc_config"

    try:
        target_df = spark.read.format("delta").load(table_path)
    except:
        print("Table doesn't exist yet.")
        (new_df.write
        .format("delta")
        .mode("overwrite")
        .save(table_path))
        target_df = spark.read.format("delta").load(table_path)

    delta_table = DeltaTable.forPath(spark, table_path)

    (delta_table.alias("target")
        .merge(
            new_df.alias("source"),
            "target.table = source.table AND target.source = source.source"    
            )
        .whenMatchedUpdate(set = {
            "isActive": "source.isActive"
            # ,"last_sync":"source.last_sync",
            # "watermark_column":"source.watermark_column",
            # "watermark_format":"source.watermark_format",
            # "filter_query":"source.filter_query"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )


    # # Stagging Config

    # In[ ]:


    BRONZE_CONFIG_PATH = f"{BRONZE_LAKEHOUSE_PATH}/br_bc_config"
    bronze_config = spark.read.format("delta").load(BRONZE_CONFIG_PATH)

    default_sync_date = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")

    new_processing_rows = bronze_config.select(
        col("table").alias("source"),
        col("isActive").alias("isActive"),
        col("source").alias("bronze_source"),
        col("key").alias("key")
    ).distinct()
    new_processing_rows = new_processing_rows.withColumn("table",concat(lit("sil.bc."), col("bronze_source"))).withColumn(
        "last_sync",
        lit(default_sync_date)
    )

    new_processing_rows = new_processing_rows.select("table", "last_sync", "source","isActive","key")
    new_processing_rows = new_processing_rows.filter(col("table").isNotNull())


    PROCESSING_CONFIG_PATH = f"{STAGING_LAKEHOUSE_PATH}/Staging_config"

    try:
        existing_df = spark.read.format("delta").load(PROCESSING_CONFIG_PATH)
        print("Existing Staging_config found will preserve last_sync and upsert isActive")
    except:
        existing_df = None
        print("No existing Staging_config creating from scratch")

    if existing_df is not None:
        delta_table = DeltaTable.forPath(spark, PROCESSING_CONFIG_PATH)

        (delta_table.alias("target")
        .merge(
            new_processing_rows.alias("source"),
            "target.table = source.table AND target.source = source.source" 
        )
        .whenMatchedUpdate(set={
            "isActive": col("source.isActive")
        })
        .whenNotMatchedInsertAll() 
        .execute())

        print("MERGE completed: isActive updated, new sources added, last_sync preserved")

    else:
        (new_processing_rows.write
        .format("delta")
        .mode("overwrite")
        .save(PROCESSING_CONFIG_PATH))
        print("Staging_config created for the first time")


    # # Silver Config 

    # In[9]:


    schema1 = StructType([
        StructField("table", StringType(), True),
        StructField("primary_key", StringType(), True),
        StructField("last_sync", TimestampType(), True)
    ])
    data1 = []
    df = spark.createDataFrame(data1, schema1)

    sil_lakehouse_path = f"{SILVER_LAKEHOUSE_PATH}/Sil_config"
    try:
        spark.read.format("delta").load(sil_lakehouse_path)
        pass
    except:
        df.write.format("delta").mode("overwrite").save(sil_lakehouse_path)
    
    return config 

# In[ ]:
# try:
#     config = config_BusinessCentral(input_config, spark)
# except Exception as e:
#     print(f"Error loading configuration: {e}")

# try:
#     create_lakehouses(lakehouse_names)
# except Exception as e:
#     print(f"Error creating lakehouses: {e}")

# Continue with other parts of the code...


