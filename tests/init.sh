#!/usr/bin/env bash

set -e

# Create state store table.
aws dynamodb create-table \
    --table-name kinesumer-state-store \
    --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
    AttributeName=last_update,AttributeType=S \
    --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
    --provisioned-throughput \
    ReadCapacityUnits=10,WriteCapacityUnits=10 \
    --local-secondary-indexes file://schema/ddb-lsi.json \
    --endpoint-url http://localhost:14566 \
    --region ap-northeast-2 || true | cat

# Create kinesis stream.
aws kinesis create-stream \
    --stream-name events \
    --shard-count 5 \
    --endpoint-url http://localhost:14566 \
    --cli-connect-timeout 6000 \
    --region ap-northeast-2 || true | cat
