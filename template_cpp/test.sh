#!/bin/bash

set -e  # Exit on error

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if required arguments are provided
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <hosts_file> <config_file> [output_dir] [--valgrind]"
    exit 1
fi

# Convert to absolute paths
HOSTS_FILE="$(realpath "$1")"
CONFIG_FILE="$(realpath "$2")"
OUTPUT_DIR="${3:-$SCRIPT_DIR/output}"

# Check for valgrind flag
USE_VALGRIND=0
for arg in "$@"; do
    if [ "$arg" = "--valgrind" ]; then
        USE_VALGRIND=1
        echo "Valgrind mode enabled"
        break
    fi
done

# Also make output dir absolute
OUTPUT_DIR="$(realpath -m "$OUTPUT_DIR")"
# Call the binary directly instead of through run.sh
BINARY="$SCRIPT_DIR/bin/da_proc"

# Check if valgrind is available when requested
if [ $USE_VALGRIND -eq 1 ]; then
    if ! command -v valgrind &> /dev/null; then
        echo "Error: valgrind requested but not found in PATH"
        echo "Install with: sudo apt-get install valgrind"
        exit 1
    fi
    echo "Valgrind found: $(which valgrind)"
fi

# Verify that the binary exists and is executable
if [ ! -f "$BINARY" ]; then
    echo "Error: Binary $BINARY not found"
    exit 1
fi

if [ ! -x "$BINARY" ]; then
    echo "Error: Binary $BINARY is not executable"
    echo "Run: chmod +x $BINARY"
    exit 1
fi

# Verify input files exist
if [ ! -f "$HOSTS_FILE" ]; then
    echo "Error: Hosts file $HOSTS_FILE not found"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file $CONFIG_FILE not found"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Create valgrind subdirectory if needed
if [ $USE_VALGRIND -eq 1 ]; then
    mkdir -p "$OUTPUT_DIR/valgrind"
fi

# Array to store PIDs
declare -a PIDS

# Read number of processes from hosts file (count non-empty, non-comment lines)
NUM_PROCESSES=$(awk 'NF && $1 !~ /^#/' "$HOSTS_FILE" | wc -l)
if ! [[ "$NUM_PROCESSES" =~ ^[0-9]+$ ]] || [ "$NUM_PROCESSES" -eq 0 ]; then
    echo "Error: Could not determine number of processes from hosts file $HOSTS_FILE"
    exit 1
fi

echo "Starting $NUM_PROCESSES processes..."
echo "Output directory: $OUTPUT_DIR"
echo "Hosts file: $HOSTS_FILE"
echo "Config file: $CONFIG_FILE"
echo ""

# Test that files are readable
if [ ! -r "$HOSTS_FILE" ]; then
    echo "Error: Cannot read hosts file: $HOSTS_FILE"
    exit 1
fi

if [ ! -r "$CONFIG_FILE" ]; then
    echo "Error: Cannot read config file: $CONFIG_FILE"
    exit 1
fi

# Print first few lines of config for verification
echo "Config file contents:"
head -n 3 "$CONFIG_FILE"
echo ""

# Start each process
for ((i=1; i<=NUM_PROCESSES; i++)); do
    OUTPUT_FILE="$OUTPUT_DIR/proc_$i.output"
    STDERR_FILE="$OUTPUT_DIR/proc_$i.stderr"
    
    echo "Starting process $i (output: $OUTPUT_FILE)..."
    
    if [ $USE_VALGRIND -eq 1 ]; then
        VALGRIND_LOG="$OUTPUT_DIR/valgrind/proc_$i.valgrind"
        echo "  Valgrind log: $VALGRIND_LOG"
        
        # Run with valgrind
        valgrind \
            --leak-check=full \
            --show-leak-kinds=all \
            --track-origins=yes \
            --verbose \
            --log-file="$VALGRIND_LOG" \
            "$BINARY" --id "$i" --hosts "$HOSTS_FILE" --output "$OUTPUT_FILE" "$CONFIG_FILE" \
            > "$STDERR_FILE" 2>&1 &
    else
        # Run normally without valgrind
        "$BINARY" --id "$i" --hosts "$HOSTS_FILE" --output "$OUTPUT_FILE" "$CONFIG_FILE" \
            > "$STDERR_FILE" 2>&1 &
    fi
    
    PID=$!
    PIDS+=($PID)
    echo "  Process $i started with PID $PID"
    
    # Small delay to avoid race conditions
    sleep 0.1
done

# Record start time (with fallback for systems without millisecond support)
if date +%s%3N &>/dev/null; then
    START_TIME=$(date +%s%3N)
    USE_MS=1
else
    START_TIME=$(date +%s)000
    USE_MS=0
fi

echo ""
echo "All processes started. PIDs: ${PIDS[@]}"
if [ $USE_VALGRIND -eq 1 ]; then
    echo "NOTE: Valgrind will make execution MUCH slower"
    echo "Valgrind logs will be in: $OUTPUT_DIR/valgrind/"
fi
echo "Press Ctrl+C to stop all processes gracefully..."
echo ""

# Function to handle cleanup
cleanup() {
    echo ""
    echo "Stopping all processes gracefully..."
    
    for PID in "${PIDS[@]}"; do
        if ps -p $PID > /dev/null 2>&1; then
            echo "  Sending SIGTERM to PID $PID"
            kill -TERM $PID 2>/dev/null || true
        fi
    done
    
    # Wait for all processes to terminate (with timeout)
    echo "Waiting for processes to terminate..."
    TIMEOUT=10  # Increased timeout for valgrind
    for PID in "${PIDS[@]}"; do
        COUNT=0
        while ps -p $PID > /dev/null 2>&1 && [ $COUNT -lt $TIMEOUT ]; do
            sleep 1
            COUNT=$((COUNT + 1))
        done
        
        # Force kill if still running
        if ps -p $PID > /dev/null 2>&1; then
            echo "  Force killing PID $PID"
            kill -9 $PID 2>/dev/null || true
        fi
    done
    
    # Calculate execution time
    if [ $USE_MS -eq 1 ]; then
        END_TIME=$(date +%s%3N)
    else
        END_TIME=$(date +%s)000
    fi
    EXECUTION_TIME_MS=$((END_TIME - START_TIME))
    EXECUTION_TIME_S=$(awk "BEGIN {printf \"%.3f\", $EXECUTION_TIME_MS/1000}")
    
    echo "All processes stopped."
    echo "Execution time: ${EXECUTION_TIME_S}s (${EXECUTION_TIME_MS}ms)"
    echo "Output files are in: $OUTPUT_DIR"
    
    if [ $USE_VALGRIND -eq 1 ]; then
        echo ""
        echo "Valgrind Summary:"
        echo "================="
        for ((i=1; i<=NUM_PROCESSES; i++)); do
            VALGRIND_LOG="$OUTPUT_DIR/valgrind/proc_$i.valgrind"
            if [ -f "$VALGRIND_LOG" ]; then
                echo ""
                echo "Process $i:"
                grep -E "ERROR SUMMARY|definitely lost|indirectly lost|possibly lost" "$VALGRIND_LOG" | head -n 5
            fi
        done
        echo ""
        echo "Full valgrind logs in: $OUTPUT_DIR/valgrind/"
    fi
    
    exit 0
}

# Trap SIGINT and SIGTERM
trap cleanup SIGINT SIGTERM

# Wait for all background processes
wait

# Calculate execution time
if [ $USE_MS -eq 1 ]; then
    END_TIME=$(date +%s%3N)
else
    END_TIME=$(date +%s)000
fi
EXECUTION_TIME_MS=$((END_TIME - START_TIME))
EXECUTION_TIME_S=$(awk "BEGIN {printf \"%.3f\", $EXECUTION_TIME_MS/1000}")

# If we get here, all processes exited normally
echo ""
echo "All processes completed."
echo "Execution time: ${EXECUTION_TIME_S}s (${EXECUTION_TIME_MS}ms)"
echo "Output files are in: $OUTPUT_DIR"

if [ $USE_VALGRIND -eq 1 ]; then
    echo ""
    echo "Valgrind Summary:"
    echo "================="
    for ((i=1; i<=NUM_PROCESSES; i++)); do
        VALGRIND_LOG="$OUTPUT_DIR/valgrind/proc_$i.valgrind"
        if [ -f "$VALGRIND_LOG" ]; then
            echo ""
            echo "Process $i:"
            grep -E "ERROR SUMMARY|definitely lost|indirectly lost|possibly lost" "$VALGRIND_LOG" | head -n 5
        fi
    done
    echo ""
    echo "Full valgrind logs in: $OUTPUT_DIR/valgrind/"
fi