#!/usr/bin/zsh

HOST_PORT="localhost:8085"
gcloud beta emulators pubsub start  --host-port=$HOST_PORT