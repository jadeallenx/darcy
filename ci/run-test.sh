#!/usr/bin/env bash

set -e

SCRIPT_PATH=`dirname $0`

source "$SCRIPT_PATH/start-dynamodb.sh"

rebar3 eunit
