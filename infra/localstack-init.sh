#!/usr/bin/env bash
# Creates the SQS FIFO queues used by the wagering processor.
# Mounted as /etc/localstack/init/ready.d/init.sh (runs once, when ready).
set -euo pipefail

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# DLQ first: the main queue's RedrivePolicy needs its ARN.
DLQ_URL="$(awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true \
  --query 'QueueUrl' --output text)"
DLQ_ARN="$(awslocal sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)"

# JSON form covers the whole attribute map (the comma-separated form
# would split the nested RedrivePolicy JSON).
MAIN_ATTRS="$(printf '{"FifoQueue":"true","ContentBasedDeduplication":"true","VisibilityTimeout":"30","RedrivePolicy":"{\\"deadLetterTargetArn\\":\\"%s\\",\\"maxReceiveCount\\":\\"5\\"}"}' "$DLQ_ARN")"

awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "$MAIN_ATTRS" > /dev/null

awslocal sqs create-queue \
  --queue-name wager-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true > /dev/null

echo "sqs queues ready: wager-transactions.fifo, wager-transactions-dlq.fifo, wager-events.fifo"
