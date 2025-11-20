#!/bin/bash

# Spark Job Submission Helper Script
# Usage: ./submit-spark-job.sh <path-to-python-file>

if [ $# -eq 0 ]; then
    echo "Usage: $0 <path-to-python-file>"
    echo "Example: $0 /opt/spark/apps/wordcount.py"
    exit 1
fi

APP_FILE=$1

echo "Submitting Spark job: $APP_FILE"
echo "Master URL: spark://spark-master:7077"
echo "----------------------------------------"

docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    "$APP_FILE"

echo "----------------------------------------"
echo "Job completed!"