#!/usr/bin/zsh

TARGET="hello"
SIGNATURE_TYPE="event"
DEBUG="--debug"
PORT=8080

functions-framework --target="$TARGET" --signature-type="$SIGNATURE_TYPE" "$DEBUG" --port="$PORT"