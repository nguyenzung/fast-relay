#!/usr/bin/env bash

# ============================================================
# Relay Server Metrics Collector
# - Fetches JSON from /metrics
# - Adds real process memory from /proc/<pid>
# - Writes compact JSON into Markdown table
#
# Usage:
#   ./scripts/collect_metrics.sh
#
# Better:
#   SERVER_PID=$(pgrep -f "bin/relayer" | head -n 1) ./scripts/collect_metrics.sh
#
# Optional env:
#   URL="http://localhost:8080/metrics"
#   OUTPUT_FILE="churn_test_benchmark.md"
#   INTERVAL=5
#   SERVER_PID=12345
#   PROCESS_PATTERN="bin/relayer"
# ============================================================

URL="${URL:-http://localhost:8080/metrics}"
OUTPUT_FILE="${OUTPUT_FILE:-churn_test_benchmark.md}"
INTERVAL="${INTERVAL:-5}"
PROCESS_PATTERN="${PROCESS_PATTERN:-bin/relayer}"

echo "Collecting metrics from $URL every $INTERVAL seconds..."
echo "Output file: $OUTPUT_FILE"
echo "Process pattern: $PROCESS_PATTERN"

if ! command -v curl >/dev/null 2>&1; then
    echo "ERROR: curl is required"
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "ERROR: jq is required"
    echo "Install: sudo apt install jq"
    exit 1
fi

get_server_pid() {
    # If SERVER_PID is provided and alive, use it.
    if [ -n "${SERVER_PID:-}" ] && [ -d "/proc/$SERVER_PID" ]; then
        echo "$SERVER_PID"
        return
    fi

    # Otherwise find by pattern.
    # Default pattern is bin/relayer because your server starts as ./bin/relayer.
    pgrep -f "$PROCESS_PATTERN" | while read -r pid; do
        # Avoid this collector process itself.
        if [ "$pid" != "$$" ]; then
            echo "$pid"
            break
        fi
    done
}

kb_to_bytes() {
    local kb="${1:-0}"

    if [ -z "$kb" ]; then
        echo 0
    else
        echo $((kb * 1024))
    fi
}

read_status_kb() {
    local pid="$1"
    local key="$2"

    if [ ! -r "/proc/$pid/status" ]; then
        echo 0
        return
    fi

    awk -v k="$key" '$1 == k {print $2}' "/proc/$pid/status"
}

read_smaps_kb() {
    local pid="$1"
    local key="$2"

    if [ ! -r "/proc/$pid/smaps_rollup" ]; then
        echo 0
        return
    fi

    awk -v k="$key" '$1 == k {print $2}' "/proc/$pid/smaps_rollup"
}

read_process_memory_json() {
    local pid="$1"

    if [ -z "$pid" ] || [ ! -d "/proc/$pid" ]; then
        jq -n \
            --argjson process_pid 0 \
            --argjson process_rss_bytes 0 \
            --argjson process_rss_anon_bytes 0 \
            --argjson process_vm_size_bytes 0 \
            --argjson process_pss_bytes 0 \
            --argjson process_private_bytes 0 \
            '{
                process_pid: $process_pid,
                process_rss_bytes: $process_rss_bytes,
                process_rss_anon_bytes: $process_rss_anon_bytes,
                process_vm_size_bytes: $process_vm_size_bytes,
                process_pss_bytes: $process_pss_bytes,
                process_private_bytes: $process_private_bytes
            }'
        return
    fi

    # /proc/<pid>/status
    local vm_rss_kb
    local rss_anon_kb
    local vm_size_kb

    vm_rss_kb=$(read_status_kb "$pid" "VmRSS:")
    rss_anon_kb=$(read_status_kb "$pid" "RssAnon:")
    vm_size_kb=$(read_status_kb "$pid" "VmSize:")

    local process_rss_bytes
    local process_rss_anon_bytes
    local process_vm_size_bytes

    process_rss_bytes=$(kb_to_bytes "$vm_rss_kb")
    process_rss_anon_bytes=$(kb_to_bytes "$rss_anon_kb")
    process_vm_size_bytes=$(kb_to_bytes "$vm_size_kb")

    # /proc/<pid>/smaps_rollup
    # More useful than raw RSS because it provides PSS and private memory.
    local pss_kb
    local private_clean_kb
    local private_dirty_kb

    pss_kb=$(read_smaps_kb "$pid" "Pss:")
    private_clean_kb=$(read_smaps_kb "$pid" "Private_Clean:")
    private_dirty_kb=$(read_smaps_kb "$pid" "Private_Dirty:")

    [ -z "$pss_kb" ] && pss_kb=0
    [ -z "$private_clean_kb" ] && private_clean_kb=0
    [ -z "$private_dirty_kb" ] && private_dirty_kb=0

    local process_pss_bytes
    local process_private_bytes

    process_pss_bytes=$(kb_to_bytes "$pss_kb")
    process_private_bytes=$(((private_clean_kb + private_dirty_kb) * 1024))

    jq -n \
        --argjson process_pid "$pid" \
        --argjson process_rss_bytes "$process_rss_bytes" \
        --argjson process_rss_anon_bytes "$process_rss_anon_bytes" \
        --argjson process_vm_size_bytes "$process_vm_size_bytes" \
        --argjson process_pss_bytes "$process_pss_bytes" \
        --argjson process_private_bytes "$process_private_bytes" \
        '{
            process_pid: $process_pid,
            process_rss_bytes: $process_rss_bytes,
            process_rss_anon_bytes: $process_rss_anon_bytes,
            process_vm_size_bytes: $process_vm_size_bytes,
            process_pss_bytes: $process_pss_bytes,
            process_private_bytes: $process_private_bytes
        }'
}

# Add header if file does not exist
if [ ! -f "$OUTPUT_FILE" ]; then
    echo "# Churn Test Benchmark Metrics" > "$OUTPUT_FILE"
    echo "Collected at: $(date)" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    echo "| Timestamp | Metrics (JSON) |" >> "$OUTPUT_FILE"
    echo "|---|---|" >> "$OUTPUT_FILE"
fi

while true; do
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

    METRICS_RAW=$(curl -fsS --max-time 3 "$URL" 2>/dev/null)

    if [ $? -eq 0 ] && [ -n "$METRICS_RAW" ]; then
        # Validate and compact server JSON first
        SERVER_JSON=$(echo "$METRICS_RAW" | jq -c '.' 2>/dev/null)

        if [ $? -ne 0 ] || [ -z "$SERVER_JSON" ]; then
            echo "| $TIMESTAMP | ERROR: Invalid JSON from metrics endpoint |" >> "$OUTPUT_FILE"
            echo "[$TIMESTAMP] Error: Invalid JSON from $URL"
            sleep "$INTERVAL"
            continue
        fi

        PID=$(get_server_pid)
        PROC_MEM_JSON=$(read_process_memory_json "$PID")

        # Merge:
        # - server /metrics JSON
        # - process memory JSON from /proc
        #
        # Add derived metrics:
        # - process_rss_mb
        # - process_pss_mb
        # - process_private_mb
        # - process_rss_anon_mb
        # - rss_per_conn_kb
        # - pss_per_conn_kb
        # - private_per_conn_kb
        # - go_sys_per_conn_kb
        # - go_alloc_per_conn_kb
        # - cpu_cores_recent
        # - cpu_cores_avg
        COMPACT_METRICS=$(jq -c -n \
            --argjson metrics "$SERVER_JSON" \
            --argjson proc "$PROC_MEM_JSON" \
            '
            ($metrics + $proc)
            | .process_rss_mb = (.process_rss_bytes / 1024 / 1024)
            | .process_pss_mb = (.process_pss_bytes / 1024 / 1024)
            | .process_private_mb = (.process_private_bytes / 1024 / 1024)
            | .process_rss_anon_mb = (.process_rss_anon_bytes / 1024 / 1024)
            | .rss_per_conn_kb =
                (if (.active_connections // 0) > 0
                 then (.process_rss_bytes / .active_connections / 1024)
                 else 0 end)
            | .pss_per_conn_kb =
                (if (.active_connections // 0) > 0
                 then (.process_pss_bytes / .active_connections / 1024)
                 else 0 end)
            | .private_per_conn_kb =
                (if (.active_connections // 0) > 0
                 then (.process_private_bytes / .active_connections / 1024)
                 else 0 end)
            | .go_sys_per_conn_kb =
                (if (.active_connections // 0) > 0
                 then (.sys_bytes / .active_connections / 1024)
                 else 0 end)
            | .go_alloc_per_conn_kb =
                (if (.active_connections // 0) > 0
                 then (.alloc_bytes / .active_connections / 1024)
                 else 0 end)
            | .cpu_cores_recent = ((.cpu_recent_percent // 0) / 100)
            | .cpu_cores_avg = ((.cpu_percent // 0) / 100)
            ')

        echo "| $TIMESTAMP | \`$COMPACT_METRICS\` |" >> "$OUTPUT_FILE"

        ACTIVE=$(echo "$COMPACT_METRICS" | jq -r '.active_connections // 0')
        RSS_MB=$(echo "$COMPACT_METRICS" | jq -r '.process_rss_mb')
        RSS_PER_CONN=$(echo "$COMPACT_METRICS" | jq -r '.rss_per_conn_kb')
        GO_SYS_PER_CONN=$(echo "$COMPACT_METRICS" | jq -r '.go_sys_per_conn_kb')
        P99=$(echo "$COMPACT_METRICS" | jq -r '.latency_p99_ms // 0')
        CPU_RECENT=$(echo "$COMPACT_METRICS" | jq -r '.cpu_cores_recent')

        echo "[$TIMESTAMP] saved | pid=${PID:-0} active=$ACTIVE rss=${RSS_MB}MB rss/conn=${RSS_PER_CONN}KB go_sys/conn=${GO_SYS_PER_CONN}KB p99=${P99}ms cpu_recent=${CPU_RECENT} cores"
    else
        echo "| $TIMESTAMP | ERROR: Could not fetch metrics |" >> "$OUTPUT_FILE"
        echo "[$TIMESTAMP] Error: Could not fetch metrics from $URL"
    fi

    sleep "$INTERVAL"
done