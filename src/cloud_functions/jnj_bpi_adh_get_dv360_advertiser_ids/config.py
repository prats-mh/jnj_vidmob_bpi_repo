import json
from os import environ
from dotenv import load_dotenv


load_dotenv(dotenv_path='env/.env')

def get_env_var(env_var):
    return environ.get(env_var)



configurations = {
        'gcp_project_id' : get_env_var('GCP_PROJECT_ID'),
        'adh_account_id' : get_env_var('ADH_ACCOUNT_ID'),
        'customer_name' : get_env_var('CUSTOMER_NAME'),
        'adh_svc_acct_secret' : json.loads(get_env_var('ADH_SVC_ACCT')),
        'adh_api_key' : get_env_var('ADH_API_KEY'),
        'bq_dataset_name': get_env_var('BQ_DATASET_NAME'),
        'bq_table_name_template': get_env_var('BQ_TABLE_NAME_TEMPLATE'),
        'datalinks_filter_clause' :  get_env_var('DATALINKS_FILTER_CLAUSE'),
        'pubsub_topic_path' : get_env_var('PUBSUB_TOPIC_PATH')

    }

