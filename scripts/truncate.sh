#!/bin/bash

set -x -e

# IMPORTANT: THIS IS FOR DEBUGGING ONLY - NEVER USE IN PRODUCTION

aws dynamodb scan --table-name "$DYNAMO_CARS_TABLE" | jq -r -c '.Items[].path.S' | xargs -L1 -IID aws dynamodb delete-item --table-name "$DYNAMO_CARS_TABLE" --key '{"path": {"S": "ID"}}'