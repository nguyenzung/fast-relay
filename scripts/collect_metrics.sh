#!/bin/bash

# Configuration
URL="http://localhost:8080/metrics"
OUTPUT_FILE="churn_test_benchmark.md"
INTERVAL=5 # 3 minutes (180 seconds)

echo "Collecting metrics from $URL every $INTERVAL seconds..."
echo "Output file: $OUTPUT_FILE"

# Add header if file doesn't exist
if [ ! -f "$OUTPUT_FILE" ]; then
    echo "# Churn Test Benchmark Metrics" > "$OUTPUT_FILE"
    echo "Collected at: $(date)" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    echo "| Timestamp | Metrics (JSON) |" >> "$OUTPUT_FILE"
    echo "|---|---|" >> "$OUTPUT_FILE"
fi

while true; do
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    METRICS=$(curl -s "$URL")
    
    if [ $? -eq 0 ] && [ ! -z "$METRICS" ]; then
        # Compact JSON to a single line for the table
        COMPACT_METRICS=$(echo "$METRICS" | jq -c '.')
        echo "| $TIMESTAMP | \`$COMPACT_METRICS\` |" >> "$OUTPUT_FILE"
        echo "[$TIMESTAMP] Metrics saved."
    else
        echo "| $TIMESTAMP | ERROR: Could not fetch metrics |" >> "$OUTPUT_FILE"
        echo "[$TIMESTAMP] Error: Could not fetch metrics from $URL"
    fi
    
    sleep $INTERVAL
done
