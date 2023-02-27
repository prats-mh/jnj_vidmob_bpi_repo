export PUBSUB_PROJECT_ID=jjt-consumerdatalake-bigquery
export TOPIC_ID=jnj_bpi_get_dv360_advertiser_ids
export PUSH_SUBSCRIPTION_ID=jnj_bpi_get_dv360_advertiser_ids-sub

python pubsub_emulator/01_create_topic.py $PUBSUB_PROJECT_ID create $TOPIC_ID
# python pubsub_emulator/subscriber.py $PUBSUB_PROJECT_ID create-push $TOPIC_ID $PUSH_SUBSCRIPTION_ID http://localhost:8080
# python pubsub_emulator/01_create_topic.py $PUBSUB_PROJECT_ID publish $TOPIC_ID