from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from notebookutils import mssparkutils as notebookutils
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def get_lakehouse_path(lakehouse_name: str) -> str:
    try:
        lakehouse_details = notebookutils.lakehouse.get(lakehouse_name)
    except:
        raise Exception(f"A LAKEHOUSE WITH NAME '{lakehouse_name}' DOES NOT EXIST IN THE WORKSPACE")
    
    return lakehouse_details.get('properties', {}).get('abfsPath') + '/Tables'


def get_access_token(TENANT_ID,CLIENT_ID,CLIENT_SECRET):
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://api.businesscentral.dynamics.com/.default"
    }
    token_response = requests.post(token_url, data=token_data)
    access_token = token_response.json().get("access_token")

    return access_token


def update_config_date(spark,CONFIG_PATH,table, source, new_last_sync):
    delta_table = DeltaTable.forName(spark, f"delta.`{CONFIG_PATH}`")
    delta_table.update(
        condition=f"table = '{table}' AND source = '{source}'",
        set={"last_sync": lit(new_last_sync)} 
    )
    print(f"Config table updated for {table} {source} with date {new_last_sync}")


def get_date_chunks(object, min_date, max_date, format, date_col, access_token,URL,filter_query):
    HEADERS = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    end_date = max_date
    start_date = min_date
    modified_start_date = min_date

    yearly_chunks = []
    current_year = start_date.year
    year_group = [
        (
            object,
            HEADERS, 
            URL,
            filter_query,
            date_col,
            "0001-01-01T00:00:00.000000Z",
            "0001-01-01T00:00:00.000000Z"
        )
    ]
    
    while start_date < end_date:
        chunk_start = modified_start_date
        chunk_end = start_date + relativedelta(months=1) - timedelta(seconds=1)
        
        chunk = (
            object,
            HEADERS, 
            URL,
            filter_query,
            date_col,
            chunk_start.strftime(format),
            chunk_end.strftime(format)
        )
        
        if start_date.year != current_year:
            yearly_chunks.append(year_group)
            year_group = []
            current_year = start_date.year
        
        year_group.append(chunk)
        modified_start_date = start_date + relativedelta(months=1) - timedelta(days=2)
        start_date = start_date + relativedelta(months=1)
    
    if year_group:
        yearly_chunks.append(year_group)
    
    return yearly_chunks


def fetch_object_from_business_central(chunk, max_retries=5, backoff_factor=1):
    object_name, headers, base_url, filter_query, date_col, start_time, end_time = chunk
    url_string = f"{base_url}/{object_name}?$filter={date_col} ge {start_time} and {date_col} le {end_time}{filter_query}"
    results = []
    while url_string:
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(url_string, headers=headers)
                response.raise_for_status()
                break
            except requests.RequestException as e:
                retries += 1
                if retries >= max_retries:
                    # Gracefully fail after max retries
                    print(f"Failed after {max_retries} attempts: {e}")
                    return results
                # Exponential backoff before retrying
                sleep_time = backoff_factor * (2 ** (retries - 1))
                time.sleep(sleep_time)

        body = response.json()
        page = body.get("value", [])
        if page:
            results.extend(page)
        url_string = body.get("@odata.nextLink")

    return results 


def fetch_company_data(spark,CONFIG_PATH,BRONZE_LAKEHOUSE_PATH, TENANT_ID, CLIENT_ID, CLIENT_SECRET,ENVIRONMENT):

    df_config = spark.read.format("delta").load(CONFIG_PATH)
    df_config = df_config.filter(col('isactive') == lit(True))
    df_config = df_config.filter(lower(col("source")).isin("company", "companies"))
    display(df_config)

    for row in df_config.collect():
        table_name = row['table']
        source_object = row['source']
        # url = f"{row['URL']}"
        url = row['URL'].format(
            Teanant_id=TENANT_ID,
            environment=ENVIRONMENT
)
        filter_query = row['filter_query']
        last_sync = row['last_sync'] - relativedelta(hours=6)
        date_col = row['watermark_column']
        date_format = row['watermark_format']
        key = row['key']

        print(f"\nstarting ingestion for {table_name}")

        table_path = f"{BRONZE_LAKEHOUSE_PATH}/{table_name}"
        print(f"target Delta table path: {table_path}")

        new_last_sync = datetime.now()
        ACCESS_TOKEN = get_access_token(TENANT_ID,CLIENT_ID,CLIENT_SECRET)
        chunks = get_date_chunks(source_object, last_sync, new_last_sync, date_format, date_col, ACCESS_TOKEN,url,filter_query)
        print(f"generated date chunks for source: {source_object}")

        # for year_chunk in reversed(chunks):
        #     rdd = spark.sparkContext.parallelize(year_chunk)
        #     print(f"fetching records in parallel for {source_object} from {year_chunk[-1][-2]} to {year_chunk[-1][-1]} and new records")
        #     record_chunks = rdd.map(fetch_object_from_business_central).collect()
        #     print(f"completed fetch")
        for year_chunk in reversed(chunks):
            print(f"fetching records in parallel for {source_object} from {year_chunk[-1][-2]} to {year_chunk[-1][-1]} and new records")

            record_chunks = []

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(fetch_object_from_business_central, chunk)
                    for chunk in year_chunk
                ]

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        record_chunks.append(result)
                    except Exception as e:
                        print(f"Error in parallel fetch: {str(e)}")

            print(f"completed fetch")

            record_chunks_data_existence_map = []
            for i in range(len(record_chunks)):
                if record_chunks[i]:
                    record_chunks_data_existence_map.append(1)
                else: record_chunks_data_existence_map.append(0)

            record_existence_sum = 0
            for val in record_chunks_data_existence_map: record_existence_sum += val

            if record_existence_sum:
                record_chunks = [item for item, flag in zip(record_chunks, record_chunks_data_existence_map) if flag]

                sample_record = record_chunks[0][0]
                string_schema = StructType([
                    StructField(key, StringType(), True) for key in sample_record.keys()
                ])
                print(f"inferred schema with {len(sample_record.keys())} fields from sample record.")

                for i, records in enumerate(record_chunks):
                    print(f"writing chunk {i+1}/{len(record_chunks)} with {len(records)} records...")
                    df = spark.createDataFrame(records, schema=string_schema)
                    df = df.withColumn("record_timestamp", current_timestamp())

                    if DeltaTable.isDeltaTable(spark, table_path):
                        delta_table = DeltaTable.forPath(spark, table_path)
                        delta_table.alias("target").merge(
                            df.alias("source"),
                            f"target.{key} = source.{key}"
                        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                    else:
                        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
                        

                print(f"finished writing all chunks to {table_path}")
            else: print("No data was fetched")

        update_config_date(spark,CONFIG_PATH,table_name, source_object, new_last_sync)

def update_comapny_config(spark,CONFIG_PATH,BRONZE_LAKEHOUSE_PATH,COMPANIES_CONFIG_PATH):

    df_config = spark.read.format("delta").load(CONFIG_PATH)
    df_config = df_config.filter(col('isactive') == lit(True))
    df_config = df_config.filter(lower(col("source")).isin("company", "companies"))

    for row in df_config.collect():

        table_name = row['table']
        table_path = f"{BRONZE_LAKEHOUSE_PATH}/{table_name}"
        df_company_config = spark.read.format("delta").load(table_path)
        last_sync = datetime.now() - timedelta(weeks=2)
        config_insert_df = (
            df_company_config
                .select(
                    col("Id").alias("company_id")
                )
                .withColumn("isActive", lit(True))
                .withColumn("last_sync", lit(last_sync).cast(TimestampType()))
        ).distinct()

        if DeltaTable.isDeltaTable(spark, COMPANIES_CONFIG_PATH):
            delta_table_com = DeltaTable.forPath(spark, COMPANIES_CONFIG_PATH)
            delta_table_com.alias("target").merge(
                config_insert_df.alias("source"),
                f"target.company_id = source.company_id"
            ).whenNotMatchedInsertAll().execute()
        else:
            config_insert_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(COMPANIES_CONFIG_PATH)

# def insert_company_config(spark,BRONZE_LAKEHOUSE_PATH,COMPANIES_CONFIG_PATH):
#     table_name = row['table']
#     table_path = f"{BRONZE_LAKEHOUSE_PATH}/{table_name}"
#     df_company_config = spark.read.format("delta").load(table_path)
#     last_sync = datetime.now() - timedelta(weeks=2)
#     config_insert_df = (
#             df_company_config
#                 .select(
#                 col("Id").alias("company_id")
#             )
#             .withColumn("isActive", lit(True))
#             .withColumn("last_sync", lit(last_sync).cast(TimestampType()))
#     ).distinct()

#     if DeltaTable.isDeltaTable(spark, COMPANIES_CONFIG_PATH):
#         delta_table_com = DeltaTable.forPath(spark, COMPANIES_CONFIG_PATH)
#         delta_table_com.alias("target").merge(
#             config_insert_df.alias("source"),
#             f"target.company_id = source.company_id"
#         ).whenNotMatchedInsertAll().execute()
#     else:
#         config_insert_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(COMPANIES_CONFIG_PATH)

def process_source_for_company(spark, config_row, company_id, access_token, company_last_sync, BRONZE_LAKEHOUSE_PATH):
    table_name = config_row['table']
    source_object = config_row['source']
    last_sync = company_last_sync if company_last_sync < config_row['last_sync'] else config_row['last_sync']
    url = config_row['URL']
    urltype = config_row['URLtype']
    filter_query = config_row['filter_query']
    date_col = config_row['watermark_column']
    date_format = config_row['watermark_format']
    primary_key = config_row['key']

    primary_key_vals = [val.strip() for val in primary_key.split(',')]
    primary_key_condition = ' AND '.join([f"target.{val} = source.{val}" for val in primary_key_vals])

    df = None

    print(f"\nStarting ingestion for {table_name} (source: {source_object}) for company {company_id}")
    table_path = f"{BRONZE_LAKEHOUSE_PATH}/{table_name}"
    new_last_sync = datetime.now()
    prefix_reformed_source_object = "companies" if urltype == "APIv2" else "company"

    reformed_source_object = f"{prefix_reformed_source_object}(id={company_id})/{source_object}"
    chunks = get_date_chunks(reformed_source_object, last_sync, new_last_sync, date_format, date_col, access_token,url,filter_query)

    is_delta_table = DeltaTable.isDeltaTable(spark, table_path)
    if is_delta_table:
        try:
            spark.sql(f"""
                ALTER TABLE delta.`{table_path}` 
                SET TBLPROPERTIES (
                    'delta.columnMapping.mode' = 'name',
                    'delta.minReaderVersion' = '2',
                    'delta.minWriterVersion' = '5'
                )
            """)
            print("column mapping enabled")
        except Exception as e:
            print(f"column mapping already enabled or error: {e}")

    records_to_write = []

    # for year_chunk in reversed(chunks):
    #     rdd = spark.sparkContext.parallelize(year_chunk)
    #     print(f"fetching records in parallel for {source_object} from {year_chunk[-1][-2]} to {year_chunk[-1][-1]} and new records")
    #     record_chunks = rdd.map(fetch_object_from_business_central).collect()
    #     record_chunks = [chunk for chunk in record_chunks if chunk]
    #     if not record_chunks:
    #         print(f"No data for {reformed_source_object}")
    #         continue
        
    #     records_to_write.extend(record_chunks)
    #     print(f"{len(records_to_write)}test1")

    for year_chunk in reversed(chunks):
        print(f"fetching records in parallel for {source_object} from {year_chunk[-1][-2]} to {year_chunk[-1][-1]} and new records")

        record_chunks = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(fetch_object_from_business_central, chunk)
                for chunk in year_chunk
            ]

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        record_chunks.append(result)
                except Exception as e:
                    print(f"Error in parallel fetch: {str(e)}")

        record_chunks = [chunk for chunk in record_chunks if chunk]
        if not record_chunks:
            print(f"No data for {reformed_source_object}")
            continue
        
        records_to_write.extend(record_chunks)
        print(f"{len(records_to_write)}test1")

    if records_to_write:
        sample_record = records_to_write[0][0]
        string_schema = StructType([StructField(key, StringType(), True) for key in sample_record.keys()])

        # l = len(records_to_write)
        # print(f"{l}test1")

        for i, records in enumerate(records_to_write):
            df = spark.createDataFrame(records, schema=string_schema)
            df = df.withColumn("company_id", lit(company_id)).withColumn("record_timestamp", current_timestamp())
            col_set = {f"`{col}`": f"source.`{col}`" for col in df.columns}
            if DeltaTable.isDeltaTable(spark, table_path):
                delta_table = DeltaTable.forPath(spark, table_path)
                delta_table.alias("target").merge(
                    df.alias("source"),
                    f"{primary_key_condition} AND target.company_id = source.company_id"
                ).whenMatchedUpdate(set=col_set).whenNotMatchedInsert(values=col_set).execute()
            else:
                df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("delta.columnMapping.mode","name").option("delta.minReaderVersion","2").option("delta.minWriterVersion","5").save(table_path)
                is_delta_table = True
                try:
                    spark.sql(f"""
                        ALTER TABLE delta.`{table_path}` 
                        SET TBLPROPERTIES (
                            'delta.columnMapping.mode' = 'name',
                            'delta.minReaderVersion' = '2',
                            'delta.minWriterVersion' = '5'
                        )
                    """)
                    print("column mapping enabled")
                except Exception as e:
                    print(f"column mapping already enabled or error: {e}")

    return {'company_id': company_id, 'table_name': table_name, 'source_object': source_object, 'status': 'completed', 'new_last_sync': new_last_sync, 'last_sync': last_sync}

def process_company(spark,company_row, config_rows, TENANT_ID, CLIENT_ID, CLIENT_SECRET, COMPANIES_CONFIG_PATH, CONFIG_PATH, BRONZE_LAKEHOUSE_PATH):
    company_id = company_row['company_id']
    company_last_sync = company_row['last_sync']

    company_new_sync = datetime.now()

    print(f"\nProcessing company {company_id}...")

    results = []
    ACCESS_TOKEN = get_access_token(TENANT_ID,CLIENT_ID,CLIENT_SECRET)

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_source = {
            executor.submit(process_source_for_company,spark, config_row, company_id, ACCESS_TOKEN, company_last_sync, BRONZE_LAKEHOUSE_PATH): config_row
            for config_row in config_rows
        }

        for future in as_completed(future_to_source):
            config_row = future_to_source[future]

            try:
                result = future.result()
                print(result['last_sync'], company_id)
                update_config_date(spark,CONFIG_PATH,result['table_name'], result['source_object'], result['new_last_sync'])
                results.append(result)

            except Exception as e:
                print(f"Error processing {config_row['source']} for company {company_id}: {str(e)}")
                results.append({
                    'company_id': company_id,
                    'table_name': config_row['table'],
                    'source_object': config_row['source'],
                    'status': 'failed',
                    'error': str(e)
                })
    
    delta_table = DeltaTable.forName(spark, f"delta.`{COMPANIES_CONFIG_PATH}`")
    delta_table.update(
        condition = f"company_id = '{company_id}'",
        set = {"last_sync": lit(datetime.now())} 
    )

    print(f"\nSummary for company {company_id}:")
    for r in results:
        print(f" {r['table_name']} ({r['source_object']}): {r['status']}")


def br_ingestion_BusinessCentral(spark,TENANT_ID ,CLIENT_ID , CLIENT_SECRET, ENVIRONMENT):
    workspace_info = notebookutils.lakehouse.list() 
    if not workspace_info:
        raise Exception("No lakehouses found in the workspace.")

    WORKSPACE_ID = workspace_info[0].get('workspaceId') 
    BRONZE_LAKEHOUSE_PATH = get_lakehouse_path('Bronze_Lakehouse')

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.delta.merge.enableSchemaEvolution", "true")

    ACCESS_TOKEN = get_access_token(TENANT_ID,CLIENT_ID,CLIENT_SECRET)
    HEADERS = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    CONFIG_PATH = f"{BRONZE_LAKEHOUSE_PATH}/br_bc_config"
    COMPANIES_CONFIG_PATH = f"{BRONZE_LAKEHOUSE_PATH}/br_bc_companies_config"

    fetch_company_data(spark,CONFIG_PATH,BRONZE_LAKEHOUSE_PATH, TENANT_ID, CLIENT_ID, CLIENT_SECRET,ENVIRONMENT)

    update_comapny_config(spark,CONFIG_PATH,BRONZE_LAKEHOUSE_PATH,COMPANIES_CONFIG_PATH)
    
    # insert_company_config(spark,BRONZE_LAKEHOUSE_PATH,COMPANIES_CONFIG_PATH) 

    df_config = spark.read.format("delta").load(CONFIG_PATH)
    table_name_companies = df_config.filter(lower(col("source")).isin("company", "companies")).collect()[0]['table']

    df_config = df_config.filter((col('isactive') == lit(True)) & (~lower(col("source")).isin("company", "companies")))
    display(df_config)
    config_rows = df_config.collect()

    br_bc_companies_config = spark.read.format("delta").load(COMPANIES_CONFIG_PATH)
    display(br_bc_companies_config)
    br_bc_companies_config_rows = br_bc_companies_config.collect()

    print(f"\n{'='*80}")
    print(f"Starting ingestion for {len(br_bc_companies_config_rows)} companies")
    print(f"{'='*80}\n")

    for br_bc_companies_config_row in br_bc_companies_config_rows:
        process_company(spark,br_bc_companies_config_row, config_rows, TENANT_ID, CLIENT_ID, CLIENT_SECRET, COMPANIES_CONFIG_PATH, CONFIG_PATH, BRONZE_LAKEHOUSE_PATH)

