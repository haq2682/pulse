#!/bin/bash
set -e

usage() {
    echo "Usage: $0 <python_file> [additional_args...]"
    echo ""
    echo "Options:"
    echo "  -l, --log         Save output to logs directory"
    echo "  -h, --help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 scripts/my_script.py"
    echo "  $0 scripts/data_processor.py --input data.csv --output results.json"
    echo "  $0 --log scripts/kafka_consumer.py --topic ecom.orders"
    exit 1
}

# Parse options
SAVE_LOG=false
while [[ $# -gt 0 ]]; do
    case $1 in
        -l|--log)
            SAVE_LOG=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            break
            ;;
    esac
done

# Check if python file is provided
if [ $# -eq 0 ]; then
    echo "Error: No Python file specified"
    usage
fi

PYTHON_FILE="$1"
shift  # Remove first argument, leaving additional args

# Check if file exists
if [ ! -f "$PYTHON_FILE" ]; then
    echo "Error: File '$PYTHON_FILE' not found"
    exit 1
fi

# Check if python container is running
if ! docker ps --format '{{.Names}}' | grep -q '^python$'; then
    echo "Error: 'python' container is not running"
    echo "Start it with: docker-compose up -d python"
    exit 1
fi

SCRIPT_NAME=$(basename "$PYTHON_FILE")
SCRIPT_DIR=$(dirname "$PYTHON_FILE")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Setup logging
if [ "$SAVE_LOG" = true ]; then
    mkdir -p logs
    LOG_FILE="logs/${SCRIPT_NAME%.py}_${TIMESTAMP}.log"
    echo "Logging to: $LOG_FILE"
fi

echo "================================================"
echo "Running: $SCRIPT_NAME"
echo "Container: python"
echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "================================================"
echo ""

# Copy the Python file to container
# docker cp "$PYTHON_FILE" python:/app/"$SCRIPT_NAME"

# Execute the Python file in the container
if [ "$SAVE_LOG" = true ]; then
    docker exec python python3.10 "/app/$PYTHON_FILE" "$@" 2>&1 | tee "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}
else
    docker exec python python3.10 "/app/$PYTHON_FILE" "$@"
    EXIT_CODE=$?
fi

# Cleanup: remove the copied file from container
docker exec python rm "/app/$PYTHON_FILE" 2>/dev/null || true

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Execution completed successfully"
else
    echo "✗ Execution failed with exit code: $EXIT_CODE"
fi
echo "================================================"

exit $EXIT_CODE