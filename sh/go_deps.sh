#!/usr/bin/env bash

# Set up directories and files
LOG_DIR="logs"
COVERAGE_FILE="$LOG_DIR/coverage.txt"
PROFILE_FILE="profile.out"

# Ensure the logs directory exists
mkdir -p "$LOG_DIR"

# Exit immediately if a command exits with a non-zero status
set -e

# Initialize the coverage file
>"$COVERAGE_FILE"

# Run tests with race detection and coverage for each package
for package in $(go list ./... | grep -v vendor); do
    go test -race -coverprofile="$PROFILE_FILE" -covermode=atomic "$package"

    # Append profile output to the coverage file if it exists
    if [ -f "$PROFILE_FILE" ]; then
        cat "$PROFILE_FILE" >>"$COVERAGE_FILE"
        rm "$PROFILE_FILE"
    fi
done

echo "🟢 Coverage results written to $COVERAGE_FILE"
