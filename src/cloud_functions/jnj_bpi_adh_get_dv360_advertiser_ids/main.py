from config import configurations
import os
import functions_framework
from dotenv import load_dotenv

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


from utility_functions import (
    get_dv360_ads_data_links,
    send_start_transient_query_request,
    poll_operation,
    bq_table_list,
    send_pubsub_msg,
    write_json_file,
    read_json_file,
)
import json

load_dotenv(dotenv_path="env/.env")
# svc_acct_path = "env/jnj_bpi_service_account_key.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = svc_acct_path

SCOPES = [
    "https://www.googleapis.com/auth/adsdatahub",
    "https://www.googleapis.com/auth/bigquery",
]
DISCOVERY_URL = "https://adsdatahub.googleapis.com/$discovery/rest?version=v1"

config_properties = configurations


creds = Credentials.from_service_account_info(
    config_properties["adh_svc_acct_secret"]
).with_scopes(SCOPES)

adh_service = build(
    "AdsDataHub",
    "v1",
    credentials=creds,
    developerKey=config_properties["adh_api_key"],
    discoveryServiceUrl=DISCOVERY_URL,
)


@functions_framework.http
def get_dv360_advertiser_ids(request):
    dv360_data_links = get_dv360_ads_data_links(adh_service)
    print(dv360_data_links)
    write_json_file("dv360_data_links", dv360_data_links)

    operations_state_list = []
    for data_link in dv360_data_links:
        response = send_start_transient_query_request(data_link, adh_service)
        operations_state_list.append(response)

    print(operations_state_list)
    write_json_file("operations_state_list", operations_state_list)

    operations_complete_list = []
    for op in operations_state_list:
        result = poll_operation(op["name"], adh_service)
        operations_complete_list.append(result)

    print(operations_complete_list)
    write_json_file("operations_complete_list", operations_complete_list)

    table_lists = bq_table_list(operations_complete_list)
    write_json_file("table_lists", table_lists)

    # with open('cloud_functions/testing/jnj_bpi_adh_get_dv360_advertiser_ids/temp_files/table_lists.json', 'r') as file:
    #         # Load the contents of the file into a Python object
    #         data = json.load(file)

    # tl_list = list(data)
    # print(tl_list)

    send_pubsub_msg(table_lists)  # table_lists
    # write_json_file('pub_res', pub_res)

    print("Function completed")
    return "it is done!"
