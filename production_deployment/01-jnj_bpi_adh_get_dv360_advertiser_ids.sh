#!/usr/bin/zsh

FUNCTION_NAME="get_dv360_advertiser_ids"
REGION="us-central1"
RUNTIME="python310"
SOURCE="src/cloud_functions/jnj_bpi_adh_get_dv360_advertiser_ids"
ENTRY_POINT="src/cloud_functions/jnj_bpi_adh_get_dv360_advertiser_ids"
TRIGGER_TOPIC="jnj_bpi_get_dv360_advertiser_ids"
SERVICE_ACCOUNT="jnj-adh-audience-builder@jjt-consumerdatalake-bigquery.iam.gserviceaccount.com"
TIMEOUT="540s"
ENV_VARS_FILE="src/cloud_functions/jnj_bpi_adh_get_dv360_advertiser_ids/env/.env.yaml"
MEMORY="512MB"
STAGE_BUCKET="gs://jnj_bpi_cloud_functions"
SET_SECRETS="ADH_API_KEY=adh-api-key:1,ADH_SVC_ACCT=adh-svc-acct-secret:1"

gcloud functions deploy $FUNCTION_NAME \
--region=$REGION \
--runtime=$RUNTIME \
--source=$SOURCE \
--entry-point=$ENTRY_POINT \
--trigger-topic=$TRIGGER_TOPIC \
--service-account=$SERVICE_ACCOUNT \
--timeout=$TIMEOUT \
--env-vars-file=$ENV_VARS_FILE \
--memory=$MEMORY \
--stage-bucket=$STAGE_BUCKET \
--set-secrets=$SET_SECRETS