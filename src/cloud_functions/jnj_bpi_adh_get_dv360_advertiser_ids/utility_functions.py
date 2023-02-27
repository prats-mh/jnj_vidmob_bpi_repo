from datetime import date, timedelta
from config import configurations
# import logging
# import google.cloud.logging
import json
import base64
from google.cloud import pubsub_v1

# Instantiates a client
# client = google.cloud.logging.Client()

publisher = pubsub_v1.PublisherClient()

# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
# client.setup_logging()

# logger = logging.getLogger(__name__)

con_prop = configurations


def get_dv360_ads_data_links(service):
    try:
        response = service.customers().adsDataLinks().list(
            parent=con_prop['customer_name'], filter=con_prop['datalinks_filter_clause'], pageSize=500).execute()
        return curate_ads_data_links_response(response)
    except Exception as e:
        # logger.exception('Exception when calling the ADH API', extra={"json_fields": e})
        print(e)


def send_start_transient_query_request(data_link, service):
    date_range = date_range_dict()
    if 'partner_ids' in data_link:
        query_parameter_name = 'partner_ids'
        query_text = {"queryText": "with\ndv360_dt_advertiser as (\nSELECT DISTINCT\ndv360_advertiser as advertiser_name,\ndv360_advertiser_id as advertiser_id,\ndv360_partner_id as partner_id, \nfrom adh.dv360_dt_advertiser\nWHERE dv360_partner_id in UNNEST(@partner_ids)\n),\n\nfinal as (\nSELECT \n0 as adh_account_id,\npartner_id,\nadvertiser_id,\nadvertiser_name\nfrom dv360_dt_advertiser\norder by\n2,3\n)\n\nselect * from final", "parameterTypes": {
            "partner_ids": {"type": {"arrayType": {"type": "INT64"}}, "defaultValue": {"value": ""}}}}
        param_values = data_link['partner_ids']
    elif 'advertiser_ids' in data_link:
        query_parameter_name = 'advertiser_ids'
        query_text = {"queryText": "with\ndv360_dt_advertiser as (\nSELECT DISTINCT\ndv360_advertiser as advertiser_name,\ndv360_advertiser_id as advertiser_id,\ndv360_partner_id as partner_id, \nfrom adh.dv360_dt_advertiser\nWHERE dv360_advertiser_id in UNNEST(@advertiser_ids)\n),\n\nfinal as (\nSELECT \n0 as adh_account_id,\npartner_id,\nadvertiser_id,\nadvertiser_name\nfrom dv360_dt_advertiser\norder by\n2,3\n)\n\nselect * from final", "parameterTypes": {
            "advertiser_ids": {"type": {"arrayType": {"type": "INT64"}}, "defaultValue": {"value": ""}}}}
        param_values = data_link['advertiser_ids']

    spec = {"adsDataCustomerId": data_link['adsDataCustomerId'], "startDate": {"year": date_range['start_date_year'], "month": date_range['start_date_month'], "day": date_range['start_date_day']}, "endDate": {
        "year": date_range['end_date_year'], "month": date_range['end_date_month'], "day": date_range['end_date_day']}, "parameterValues": {query_parameter_name: {"arrayValue": {"values": param_values}}}}

    body = {
        "query": query_text,
        "spec": spec,
        "destTable": f"jjt-consumerdatalake-bigquery.adh_testing_layer.{con_prop['bq_table_name_template']}_{data_link['adsDataCustomerId']}"
    }

    try:
        response = service.customers().analysisQueries().startTransient(
            parent=con_prop['customer_name'], body=body).execute()
        return response
    except Exception as e:
        # logger.exception('Exception when calling the ADH API', extra={"json_fields": e})
        print(e)


def poll_operation(operation_name, service):
    while True:
        operation_state = service.operations().wait(
            name=operation_name, body={'timeout': '120s'}).execute()
        if 'done' in operation_state:
            if operation_state['done'] == True:
                return operation_state


def bq_table_list(operation_list):
    bq_table_list = []
    for op in operation_list:
        if 'error' in op:
            # logger.error('Error in ADH API', extra={"json_fields": op['error']})
            print(op['error'])
        elif 'response' in op:
            bq_table_list.append({
                'adsDataCustomerId': op['metadata']['adsDataCustomerId'],
                'destTable': op['metadata']['destTable']
            })

    return bq_table_list


def send_pubsub_msg(bq_table_list):
    request = {
        'table_modification_type': 'update_adh_column',
        'table_lists': bq_table_list

    }
    message_byte = json.dumps(request).encode("utf-8")
    future = publisher.publish(con_prop['pubsub_topic_path'], message_byte)
    print(future.result())
    # return future.result()  # Change in deployment

# Helper Functions


def curate_ads_data_links_response(response):
    result = {}

    for link in response['links']:
        adsDataCustomerId = link['customerLink']['customerId']
        entityid = link['linkedEntity']['entityId']
        link_type = link['linkedEntity']['type']

        if adsDataCustomerId not in result:
            result[adsDataCustomerId] = {
                'adsDataCustomerId': adsDataCustomerId
            }

        if link_type == 'GMP_DV360_PARTNER':
            if 'partner_ids' not in result[adsDataCustomerId]:
                result[adsDataCustomerId]['partner_ids'] = [
                    {'value': entityid}]
            else:
                result[adsDataCustomerId]['partner_ids'].append(
                    {'value': entityid})
        elif link_type == 'GMP_DV360_ADVERTISER':
            if 'advertiser_ids' not in result[adsDataCustomerId]:
                result[adsDataCustomerId]['advertiser_ids'] = [
                    {'value': entityid}]
            else:
                result[adsDataCustomerId]['advertiser_ids'].append(
                    {'value': entityid})

    final = list(result.values())
    both_ids = [x for x in final if 'partner_ids' in x and 'advertiser_ids' in x]
    final_mins_both_ids = [x for x in final if not (
        'partner_ids' in x and 'advertiser_ids' in x)]
    for x in both_ids:
        partner_ids = {
            'adsDataCustomerId': x['adsDataCustomerId'], 'partner_ids': x['partner_ids']}
        advertiser_ids = {
            'adsDataCustomerId': x['adsDataCustomerId'], 'advertiser_ids': x['advertiser_ids']}
        final_mins_both_ids.append(partner_ids)
        final_mins_both_ids.append(advertiser_ids)

    return final_mins_both_ids


def date_range_dict():
    today = date.today()
    first_date_of_current_month = today.replace(day=1)
    last_date_of_last_month = first_date_of_current_month - timedelta(days=1)
    first_date_of_last_month = last_date_of_last_month.replace(day=1)
    last_date_of_last_2_months = first_date_of_last_month - timedelta(days=1)
    first_date_of_last_2_months = last_date_of_last_2_months.replace(day=1)

    start_date = date(2022, 1, 1)  # first_date_of_last_2_months
    end_date = date(2023, 1, 31)  # last_date_of_last_month

    return {'start_date_year': start_date.year,
            'start_date_month': start_date.month,
            'start_date_day': start_date.day,
            'end_date_year': end_date.year,
            'end_date_month': end_date.month,
            'end_date_day': end_date.day}


def write_json_file(file_name, data):
    with open(f"cloud_functions/testing/jnj_bpi_adh_get_dv360_advertiser_ids/temp_files/{file_name}.json", "w") as write_file:
        json.dump(data, write_file)


def read_json_file(file_name):
    with open(file_name, 'r') as file:
        # Load the contents of the file into a Python object
        data = json.load(file)
        return dict(data)
