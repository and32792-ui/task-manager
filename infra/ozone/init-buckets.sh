#!/bin/bash
# Initialize Ozone volume and buckets for the task manager data lake
# Run after Ozone cluster is healthy

set -e

OZONE_OM="${OZONE_OM_HOST:-ozone-om}"

echo "Waiting for Ozone Manager to be ready..."
until curl -sf "http://${OZONE_OM}:9874" > /dev/null 2>&1; do
  echo "  Ozone OM not ready, retrying in 5s..."
  sleep 5
done

echo "Creating volume: taskmanager"
ozone sh volume create "/${VOLUME:-taskmanager}" --user root 2>/dev/null || echo "  Volume already exists"

echo "Creating buckets..."

# Raw events from Kafka streaming ingestion
ozone sh bucket create "/${VOLUME:-taskmanager}/raw-events" 2>/dev/null || echo "  raw-events already exists"

# Curated data after Spark ETL (Parquet format)
ozone sh bucket create "/${VOLUME:-taskmanager}/curated-data" 2>/dev/null || echo "  curated-data already exists"

# Analytics output from Spark batch jobs
ozone sh bucket create "/${VOLUME:-taskmanager}/analytics-output" 2>/dev/null || echo "  analytics-output already exists"

# File attachments uploaded via API
ozone sh bucket create "/${VOLUME:-taskmanager}/attachments" 2>/dev/null || echo "  attachments already exists"

# Spark streaming checkpoints
ozone sh bucket create "/${VOLUME:-taskmanager}/checkpoints" 2>/dev/null || echo "  checkpoints already exists"

echo ""
echo "Ozone buckets initialized:"
ozone sh bucket list "/${VOLUME:-taskmanager}"
