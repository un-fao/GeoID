#!/bin/bash

#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

# Get the mode from the first argument, default to "prod"
MODE=${1:-prod}

# Resolve the project root relative to this script's location (docker/scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$DOCKER_DIR/.." && pwd)"

# Helper function for pretty printing
print_header() {
    echo "========================================================"
    echo "$1"
    echo "========================================================"
}

# Helper to prepare log directories
prepare_logs() {
    echo "Preparing log directories..."
    mkdir -p "$PROJECT_ROOT/logs/api"
    mkdir -p "$PROJECT_ROOT/logs/worker"
    # Attempt to set permissions so the container user (uid 1001) can write.
    chmod -R 777 "$PROJECT_ROOT/logs" 2>/dev/null || echo "   (Warning: Could not chmod logs, ensure user 1001 has write access)"
}

# Compose file names (relative to DOCKER_DIR)
PROD_FILE="docker-compose.yml"
DEV_FILE="docker-compose.dev.yml"
ENV_FILE=".env"

# --- 1. PREPARATION ---
prepare_logs

# --- 2. ENTER DOCKER CONTEXT ---
# Use trap to guarantee we return to the original folder on exit (clean up)
trap "popd > /dev/null 2>&1" EXIT

echo "Entering $DOCKER_DIR directory..."
pushd "$DOCKER_DIR" > /dev/null || exit 1

# --- 3. EXECUTION ---
case $MODE in
    "dev")
        print_header "Starting Local Development Mode (Debug Enabled)"

        echo "   Target: $DEV_FILE"
        echo "   Context: $(pwd)"
        echo "   Env File: $ENV_FILE"
        echo "   Features: Hot-reload, Debugpy, Console Logs"
        echo "   (Use CTRL+C to exit)"

        docker compose --env-file "$ENV_FILE" -f "$DEV_FILE" -p dynastore up --build
        ;;

    "prod")
        print_header "Starting On-Premise Production Mode"

        echo "   Target: $PROD_FILE"
        echo "   Context: $(pwd)"
        echo "   Env File: $ENV_FILE"
        echo "   Features: Detached (-d), Logs in $PROJECT_ROOT/logs/"

        docker compose --env-file "$ENV_FILE" -f "$PROD_FILE" -p dynastore up -d --build
        ;;

    "down")
        print_header "Stopping Production Services"

        echo "   Target Profile: Production"
        echo "   Env File: $ENV_FILE"

        docker compose --env-file "$ENV_FILE" -f "$PROD_FILE" -p dynastore down
        ;;

    *)
        echo "Usage: $0 [dev|prod|down]"
        echo "  dev       : Runs with debugpy, hot-reload, console logs (Foreground)"
        echo "  prod      : Runs optimized production image, file logs (Detached)"
        echo "  down      : Stops production services"
        exit 1
        ;;

esac

# The trap will fire here automatically
