#!/bin/bash
ENV_DIR="../../.env"

if [ ! -f $ENV_DIR ]; then
    echo "Error: .env file not found at " $ENV_DIR
    exit 1
fi

# Load environment variables
source $ENV_DIR

# Create an s3 bucket
aws s3 mb s3://$AWS_S3_BUCKET --region ${AWS_REGION:-"eu-north-1"}



# aws s3 mb s3://$AWS_S3_BUCKET --region ${AWS_REGION:-"eu-north-1"}