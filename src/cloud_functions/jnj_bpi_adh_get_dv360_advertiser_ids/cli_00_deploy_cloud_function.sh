#!/usr/bin/zsh

TARGET="get_dv360_advertiser_ids"
SIGNATURE_TYPE="event"
DEBUG="--debug"
PORT=8080

functions-framework --target="$TARGET" --signature-type="$SIGNATURE_TYPE" "$DEBUG" --port="$PORT"