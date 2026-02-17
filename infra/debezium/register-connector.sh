#!/bin/bash
# Register the Debezium PostgreSQL CDC connector with Kafka Connect
# Run after docker compose is up and Kafka Connect is healthy

set -e

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Waiting for Kafka Connect to be ready..."
until curl -s "$CONNECT_URL/connectors" > /dev/null 2>&1; do
  sleep 2
done

echo "Registering Debezium PostgreSQL connector..."
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d @"$SCRIPT_DIR/register-connector.json" \
  "$CONNECT_URL/connectors" | jq .

echo ""
echo "Connector status:"
curl -s "$CONNECT_URL/connectors/taskmanager-postgres-connector/status" | jq .
