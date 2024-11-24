#!/bin/bash

# Set up logging
LOG_FILE="table_processor_$(date '+%Y%m%d_%H%M%S').log"
exec 1> >(tee -a "$LOG_FILE") 2>&1

# Log start time
echo "Starting processing at $(date)"
echo "Logging to: $LOG_FILE"

# Get base_dir from config.yaml
CONFIG_BASE_DIR=$(grep "base_dir:" config.yaml | cut -d':' -f2 | xargs)
echo "Using base directory: $CONFIG_BASE_DIR"

# Check if tables file exists
if [ ! -f "tables" ]; then
    echo "Error: tables file not found"
    exit 1
fi

# Process each table name
while IFS= read -r table_name || [ -n "$table_name" ]; do
    # Skip empty lines and comments
    [[ -z "$table_name" || "$table_name" =~ ^[[:space:]]*# ]] && continue

    # Remove leading/trailing whitespace
    table_name=$(echo "$table_name" | xargs)

    echo "$(date '+%Y-%m-%d %H:%M:%S') Processing table: $table_name"

    # Delete the folder
    echo "$(date '+%Y-%m-%d %H:%M:%S') Cleaning up directory for $table_name"
    rm -rf "${CONFIG_BASE_DIR:?}/$table_name"*

    # Delete the folder and wait for completion
    echo "$(date '+%Y-%m-%d %H:%M:%S') Cleaning up directory for $table_name"
    rm -rf "${CONFIG_BASE_DIR:?}/$table_name"*
    while [ -d "${CONFIG_BASE_DIR:?}/$table_name"* ] || [ -f "${CONFIG_BASE_DIR:?}/$table_name"* ]; do
        sleep 10
        echo "Waiting for deletion to complete..."
    done

    # Run the processor
    echo "$(date '+%Y-%m-%d %H:%M:%S') Running db2_processor for $table_name"
    python3 db2_processor.py --table_name "$table_name" --label "$table_name"

    if [ $? -ne 0 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') Error processing table: $table_name"
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') Completed processing $table_name"
    echo "----------------------------------------"
done < "tables"

echo "$(date '+%Y-%m-%d %H:%M:%S') All tables processed"
echo "Log file: $LOG_FILE"