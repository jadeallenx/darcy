#!/usr/bin/env bash
set -e

SCRIPT_PATH=`dirname $0`

source "$SCRIPT_PATH/dynamodb-install.sh"

DYNAMODB_LOCAL_DIR="$SCRIPT_PATH/dynamodb_local"

echo "Starting DynamoDB local"
java -Djava.library.path="$DYNAMODB_LOCAL_DIR/DynamoDBLocal_lib" \
     -jar "$DYNAMODB_LOCAL_DIR/DynamoDBLocal.jar" \
     -sharedDb \
     -inMemory \
     -port 12000 &
