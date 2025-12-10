#!/bin/bash

# Directory that contains incoming .gz trace log files
GZ_DIR="/root/MCI_ONEDAY_TRACELOG_20251115/20251115"

# Directory where extracted files will be written
PROCESS_DIR="/root/MCI_ONEDAY_TRACELOG_20251115/my_file/gz_file"

# Continuously monitor and process incoming .gz files
while true; do

    # Get the first available .gz file (if any)
    gz_file=$(ls "$GZ_DIR"/*.gz 2>/dev/null | head -n 1)

    # If no files exist, wait and check again
    if [ -z "$gz_file" ]; then
        echo "No new .gz files. Sleeping..."
        sleep 10
        continue
    fi

    echo "Processing: $gz_file"

    # Extract base file name without .gz
    base=$(basename -s .gz "$gz_file")

    echo "Extracting to $extracted_file"

    # Decompress .gz into PROCESS_DIR using stream extraction
    gunzip -c "$gz_file" > "$PROCESS_DIR/$base"

    # Move processed archive to a subfolder to avoid reprocessing
    mv "$gz_file" "$GZ_DIR/processed"

    echo "sleeping"
    sleep 8
done

