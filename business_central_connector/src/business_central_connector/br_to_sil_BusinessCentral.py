#!/usr/bin/env python
# coding: utf-8

# ## br_to_sil_BusinessCentral
# 
# New notebook

# In[1]:


from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from notebookutils import mssparkutils as notebookutils
from pyspark.sql.functions import col, when, struct, length, split, lit, sum as spark_sum, count as spark_count,max as spark_max
from functools import reduce
from collections import deque
import re
import os
import time
from datetime import datetime, timedelta, timezone

def br_to_sil_BusinessCentral(spark):   
# In[2]:

    def setup_spark_configs(spark):
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")


    # In[3]:
    def get_lakehouse_paths():
        workspace_info = notebookutils.lakehouse.list()
        if not workspace_info:
            raise Exception("No lakehouses found in the workspace.")

        def get_path(name):
            return notebookutils.lakehouse.get(name).get('properties', {}).get('abfsPath') + '/Tables'

        return {
            "STAGING": get_path('Staging_Lakehouse'),
            "BRONZE": get_path('Bronze_Lakehouse'),
            "SILVER": get_path('Silver_Lakehouse')
        }
    # # In[4]:


    # SILVER_CONFIG_PATH = f"{SILVER_LAKEHOUSE_PATH}/Sil_config"
    # STAGING_CONFIG_PATH = f"{STAGING_LAKEHOUSE_PATH}/Staging_config"


    # In[5]:


    def update_config_date(spark, table, source, new_last_sync, path):
        delta_table = DeltaTable.forName(spark, f"delta.`{path}`")
        if source:
            condition = f"table = '{table}' AND source = '{source}'"
        else:
            condition = f"table = '{table}'"
        delta_table.update(
            condition=condition,
            set={"last_sync": lit(new_last_sync)} 
        )
        print(f"Config table updated for {table} with date {new_last_sync}")


    # In[6]:


    def list_schema_tables(spark,LAKEHOUSE_PATH):
        table_paths = notebookutils.fs.ls(f'{LAKEHOUSE_PATH}/')
        table_names = []
        for table in table_paths:
            table_path = table.path
            if DeltaTable.isDeltaTable(spark, table_path):
                table_name = os.path.basename(table_path.rstrip('/'))
                if ('sil.bc' in table_name or 'br_bc' in table_name) and 'config' not in table_name:
                    table_names.append(table_name)
        return table_names


    # In[7]:


    def get_name_from_path(path):
        return path.split('/')[-1]



    setup_spark_configs(spark)

    paths = get_lakehouse_paths()
    STAGING = paths["STAGING"]
    BRONZE = paths["BRONZE"]
    SILVER = paths["SILVER"]

    STAGING_CONFIG_PATH = f"{STAGING}/Staging_config"
    SILVER_CONFIG_PATH = f"{SILVER}/Sil_config"

    Staging_config = spark.read.format("delta").load(STAGING_CONFIG_PATH).filter(col('isActive') == lit(True))
    Sil_config = spark.read.format("delta").load(SILVER_CONFIG_PATH)

    # In[8]:


    def replace_null_equivalents(df):
        null_equivalents = ["null", "NULL", "None", "none", "NaN", "nan", "", " ", "NA", "N/A", "n/a", "?", "-", "--"]
        for col_name in df.columns:
            if df.schema[col_name].dataType == StringType():
                df = df.withColumn(
                    col_name,
                    when(col(f"`{col_name}`").isin(null_equivalents), None).otherwise(col(f"`{col_name}`"))
                )
        return df


    # In[9]:


    _TYPE_INFO = [
        (DoubleType(),    "double"),
        (ShortType(),     "short"),
        (LongType(),      "long"),
        (TimestampType(), "timestamp"),
        (DateType(),      "date")
    ]

    def infer_schema_spark_optimized(df, sample_size=500):
        """
        Optimized schema inference with single-pass validation.
        Returns: (schema_map, mixed_columns)
        """
        if df.rdd.isEmpty():
            return {}, []
        
        all_columns = df.columns
        schema_map = {}
        mixed_columns = []
        
        print(f'Inferring schema for {len(all_columns)} columns (sample_size={sample_size})...')
        
        # Step 1: Take a single sample with all columns cast to string
        sample_df = (
            df.select([col(f"`{c}`").cast("string").alias(c) for c in all_columns])
            .filter(
                # At least one column is not null
                reduce(lambda a, b: a | b, [col(f"`{c}`").isNotNull() for c in all_columns])
            )
            .limit(sample_size)
            .cache()  # Cache since we'll use it multiple times
        )
        
        sample_count = sample_df.count()
        
        if sample_count == 0:
            sample_df.unpersist()
            return {c: "string" for c in all_columns}, []
        
        # Step 2: For each column, determine candidate type from sample
        candidate_types = {}
        
        for col_name in all_columns:
            candidate_type = _infer_from_sample(sample_df, col_name)
            candidate_types[col_name] = candidate_type
        
        sample_df.unpersist()
        
        # Step 3: Batch validation - validate all columns in a SINGLE pass
        columns_to_validate = {
            col_name: dtype for col_name, dtype in candidate_types.items() 
            if dtype != "string"
        }
        
        if columns_to_validate:
            validation_results = _validate_types_single_pass(df, columns_to_validate)
            
            # Apply validation results
            for col_name, (final_type, is_mixed, null_count) in validation_results.items():
                schema_map[col_name] = final_type
                if is_mixed:
                    mixed_columns.append((col_name, null_count))
        
        # Add string columns (no validation needed)
        for col_name, dtype in candidate_types.items():
            if dtype == "string" and col_name not in schema_map:
                schema_map[col_name] = "string"
        
        return schema_map, mixed_columns


    def _infer_from_sample(sample_df, col_name):
        """
        Infer type from sample data only (no full table scan).
        """
        # Check if column is all null in sample
        non_null_count = sample_df.filter(col(f"`{col_name}`").isNotNull()).count()
        if non_null_count == 0:
            return "string"
        
        # Try each type on sample
        for spark_type_cls, sql_name in _TYPE_INFO:
            nulls_in_sample = (
                sample_df
                .select(col(f"`{col_name}`").cast(spark_type_cls).alias("_cast"))
                .filter(col("_cast").isNull() & col(f"`{col_name}`").isNotNull())
                .count()
            )
            
            if nulls_in_sample == 0:
                return sql_name
        
        # Try decimal
        decimal_info = _try_decimal_on_sample(sample_df, col_name)
        if decimal_info is not None:
            return decimal_info
        
        return "string"


    def _validate_types_single_pass(df, columns_to_validate):
        """
        Validate multiple columns in a SINGLE pass over the data.
        Returns: {col_name: (final_type, is_mixed, null_count)}
        """
        if not columns_to_validate:
            return {}
        
        # Build a single select with all validations
        validation_exprs = []
        col_mapping = {}
        
        for col_name, dtype in columns_to_validate.items():
            # Original value is not null
            original_not_null = col(f"`{col_name}`").isNotNull()
            
            # Cast to target type
            if dtype.startswith("decimal"):
                cast_type = dtype
            else:
                cast_type = dtype
            
            casted = col(f"`{col_name}`").cast(cast_type)
            
            # Count where original is not null but cast is null (= failed cast)
            failed_cast = (original_not_null & casted.isNull()).cast("int")
            
            alias_name = f"_failed_{col_name.replace('`', '').replace(' ', '_')}"
            validation_exprs.append(spark_sum(failed_cast).alias(alias_name))
            col_mapping[alias_name] = col_name
        
        # Single aggregation query for ALL columns
        result = df.select(validation_exprs).collect()[0]
        
        # Process results
        validation_results = {}
        
        for alias_name, col_name in col_mapping.items():
            null_count = result[alias_name] or 0
            original_type = columns_to_validate[col_name]
            
            if null_count > 0:
                # Mixed type detected - fall back to string
                # Calculate percentage for logging
                total_count = df.select(col(f"`{col_name}`")).filter(
                    col(f"`{col_name}`").isNotNull()
                ).count()
                
                percentage = (null_count / total_count) * 100 if total_count > 0 else 0
                
                # print(f"WARNING: Column '{col_name}' has mixed types!")
                # print(f"   - Sample suggested: {original_type}")
                # print(f"   - But {null_count}/{total_count} ({percentage:.2f}%) values cannot be cast to {original_type}")
                # print(f"   - Falling back to STRING to preserve all data")
                
                validation_results[col_name] = ("string", True, null_count)
            else:
                validation_results[col_name] = (original_type, False, 0)
        
        return validation_results


    def _try_decimal_on_sample(sample_df, col_name):
        """
        Try to infer decimal type from sample.
        """
        # First check if all values can be cast to double
        double_cast = sample_df.select(col(f"`{col_name}`").cast("double").alias("_d"))
        if double_cast.filter(col("_d").isNull() & col(f"`{col_name}`").isNotNull()).count() > 0:
            return None
        
        # Calculate precision and scale
        int_frac = sample_df.select(
            when(
                col(f"`{col_name}`").contains("."),
                struct(
                    length(split(col(f"`{col_name}`"), "[.]").getItem(0).cast("string")).alias("int"),
                    length(split(col(f"`{col_name}`"), "[.]").getItem(1).cast("string")).alias("frac")
                )
            ).otherwise(
                struct(
                    length(col(f"`{col_name}`").cast("string")).alias("int"),
                    lit(0).alias("frac")
                )
            ).alias("parts")
        ).agg(
            spark_max(col("parts.int")).alias("max_int"),
            spark_max(col("parts.frac")).alias("max_frac")
        ).collect()[0]
        
        max_int  = int_frac["max_int"]  or 0
        max_frac = int_frac["max_frac"] or 0
        
        # Check for negative numbers in sample
        has_negative = sample_df.filter(col(f"`{col_name}`").startswith("-")).count() > 0
        if has_negative:
            max_int += 1
        
        precision = max_int + max_frac
        if precision == 0 or precision > 38:
            return None
        
        return f"decimal({precision},{max_frac})"


    def infer_and_cast_table(spark, source, sample_size=500):
        """
        Infer and cast table with optimized validation and reporting.
        """
        main_df = spark.read.format("delta").load(source)
        main_df = replace_null_equivalents(main_df)
        table_name = get_name_from_path(source)
        
        if not main_df.limit(1).count():
            print(f'During infer casting no data found in {table_name}')
            return
        
        print(f'Inferring schema for {table_name} (sample_size={sample_size})...')
        schema_map, mixed_columns = infer_schema_spark_optimized(main_df, sample_size)
        
        # Report findings
        if mixed_columns:
            print(f'\nMIXED TYPE COLUMNS DETECTED IN {table_name}:')
            for col_name, null_count in mixed_columns:
                print(f'   - {col_name}: {null_count} values would become NULL on cast → kept as STRING')
            print()
        
        # Cast all columns in a SINGLE select operation
        cast_exprs = [
            col(f"`{col_name}`").cast(data_type).alias(col_name)
            for col_name, data_type in schema_map.items()
        ]
        
        df_casted = main_df.select(cast_exprs)
        
        print(f'Saving table {table_name} after inferring schema')
        df_casted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(source)
        
        # Final summary
        type_counts = {}
        for dt in schema_map.values():
            type_counts[dt] = type_counts.get(dt, 0) + 1
        
        print(f'{table_name} schema inference complete:')
        for dt, count in sorted(type_counts.items()):
            print(f'   - {dt}: {count} columns')
        print()


    # In[10]:


    staging_config = spark.read.format("delta").load(STAGING_CONFIG_PATH).filter(col('isActive') == lit(True))
    silver_config = spark.read.format("delta").load(SILVER_CONFIG_PATH)


    # In[11]:


    pairs_df = (
        staging_config
        .filter(col("table").contains("sil.bc"))
        .select("table", "source")
        .distinct()
    )
    rows = pairs_df.collect()

    def _make_path(name: str, LAKEHOUSE_PATH: str) -> str:
        if name is None:
            return None
        if name.startswith(LAKEHOUSE_PATH) or name.startswith("/") or ("/" in name) or ("\\" in name):
            return name
        return f"{LAKEHOUSE_PATH}/{name}"

    staging_paths = []
    bronze_paths = []

    for r in rows:
        proc_name = r["table"]
        src_name = r["source"]
        staging_paths.append(_make_path(proc_name, STAGING))
        bronze_paths.append(_make_path(src_name, BRONZE))


    # In[ ]:


    tables = list_schema_tables(spark, STAGING)
    if not tables:
        print(f"No tables found")
    else:
        print(f"Found {len(tables)} table(s)")
        for table_name in tables:
            full_table_name = f"delta.`{STAGING}/{table_name}`"
            print("dropping", table_name)
            drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
            spark.sql(drop_sql)


    # In[ ]:


    for bronze_table_path, staging_table_path in zip(bronze_paths, staging_paths):
        table = get_name_from_path(staging_table_path)
        last_sync = staging_config.filter(col("table") == table).collect()[0]['last_sync']
        staging_new_last_sync = datetime.now()

        if not DeltaTable.isDeltaTable(spark, bronze_table_path):
            continue

        df = spark.read.format("delta").load(bronze_table_path)
        df = df.filter(col('record_timestamp') >= last_sync)
        df.write.mode("overwrite").option("overwriteSchema", "true").save(staging_table_path)
        print(f"Moved {get_name_from_path(bronze_table_path)} to {table}")

        df = spark.read.format("delta").load(staging_table_path)
        infer_and_cast_table(spark,staging_table_path)
        
        silver_new_last_sync = datetime.now()

        df = spark.read.format("delta").load(staging_table_path)
        if not df.limit(1).count():
            print("no data to process")
            continue

        df = df.withColumn("record_timestamp", current_timestamp())
        table_path = f"{SILVER}/{table}"

        primary_key = silver_config.filter(col("table") == table).collect()
        if not len(primary_key):
            last_sync = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")
            primary_key = staging_config.filter(col("table") == table).collect()[0]['key']
            new_row = Row(table=table, last_sync=last_sync, primary_key=primary_key)
            new_df = spark.createDataFrame([new_row])
            new_df.write.mode("append").save(SILVER_CONFIG_PATH)
        else:
            primary_key = primary_key[0]['primary_key']
        primary_key_vals = [val.strip() for val in primary_key.split(',')]
        primary_key = [f"target.{val} = source.{val}" for val in primary_key_vals]
        primary_key  = ' AND '.join(primary_key)

        if "companies" not in table.lower() and "company" not in table.lower():
            primary_key = f"{primary_key} AND target.company_id = source.company_id"
        
        col_set = {f"`{col}`": f"source.`{col}`" for col in df.columns}

        if DeltaTable.isDeltaTable(spark, table_path):
            delta_table = DeltaTable.forPath(spark, table_path)
            delta_table.alias("target").merge(
                df.alias("source"),
                primary_key
            ).whenMatchedUpdate(set=col_set).whenNotMatchedInsert(values=col_set).execute()
        else:
            print(f'overwriting the table silver.{table}')
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)

        update_config_date(spark, table, get_name_from_path(bronze_table_path), staging_new_last_sync, STAGING_CONFIG_PATH)
        update_config_date(spark, table, None, silver_new_last_sync, SILVER_CONFIG_PATH)
        


# In[ ]:
   



