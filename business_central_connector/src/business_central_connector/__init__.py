from .config_BusinessCentral import *
from pyspark.sql import SparkSession
from .br_ingestion_BusinessCentral import *
from .br_to_sil_BusinessCentral import *
from datetime import datetime
__version__ = "1.0.0"
__all__ = ['config_BusinessCentral', 'br_ingestion_BusinessCentral', 'br_to_sil_BusinessCentral', 'run_pipeline']


def run_pipeline(input_config ,spark):
    
    print("\n" + "="*60)
    print(" Business Central ETL PIPELINE STARTED")
    print("="*60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # config_BusinessCentral(input_config,spark)
        config = config_BusinessCentral(input_config, spark)

        creds = config.get("credentials", {})

        TENANT_ID = creds.get("TENANT_ID")
        CLIENT_ID = creds.get("CLIENT_ID")
        CLIENT_SECRET = creds.get("CLIENT_SECRET")
        ENVIRONMENT = creds.get("ENVIRONMENT")

        br_ingestion_BusinessCentral(spark, TENANT_ID ,CLIENT_ID , CLIENT_SECRET, ENVIRONMENT)
        br_to_sil_BusinessCentral(spark)        
        
        result = {
            'status': 'success',
            'source': 'Business Central',
        }
        
        print("\n" + "="*60)
        print(" PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return result
        
    except Exception as e:
        
        error_result = {
            'status': 'failed',
            'error': str(e),
            'source': 'Business Central',
        }
        
        print("\n" + "="*60)
        print(" PIPELINE FAILED")
        print("="*60)
        print(f"Error: {str(e)}")
        
        return error_result