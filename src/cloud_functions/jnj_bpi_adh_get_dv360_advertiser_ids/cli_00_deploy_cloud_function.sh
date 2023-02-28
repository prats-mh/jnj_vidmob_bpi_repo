#!/bin/zsh

TARGET="get_dv360_advertiser_ids"
SIGNATURE_TYPE="http"
DEBUG="--debug"
PORT=8081

functions-framework --target="$TARGET" --signature-type="$SIGNATURE_TYPE" "$DEBUG" --port="$PORT"