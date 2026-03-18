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

# Get the mode from the first argument, default to "dev"
MODE=${1:-prod}

# Define config paths pointing to the docker/ folder
DOCKER_DIR="docker"

# Ensure we can find the docker directory
if [ ! -d "$DOCKER_DIR" ]; then
    echo "Error: Directory '$DOCKER_DIR' not found. Please run this script from the project root."
    exit 1
fi

# Helper function for pretty printing
print_header() {
    echo "========================================================"
    echo "$1"
    echo "========================================================"
}

# Helper to prepare log directories (Runs in Project Root)
prepare_logs() {
    echo "📁 Preparing log directories..."
    mkdir -p logs/api
    mkdir -p logs/worker
    # Attempt to set permissions so the container user (uid 1001) can write.
    chmod -R 777 logs 2>/dev/null || echo "   (Warning: Could not chmod logs, ensure user 1001 has write access)"
}

# Filenames are relative to the DOCKER_DIR
PROD_FILE="docker-compose.yml"
DEV_FILE="docker-compose.dev.yml"
ENV_FILE=".env"

# --- 1. PREPARATION ---
prepare_logs

# --- 2. ENTER DOCKER CONTEXT ---
# Use trap to guarantee we return to the original folder on exit (clean up)
trap "popd > /dev/null 2>&1" EXIT

# Enter the directory
echo "📂 Entering $DOCKER_DIR directory..."
pushd "$DOCKER_DIR" > /dev/null || exit 1

# --- 3. EXECUTION ---
case $MODE in
    "dev")
        print_header "🛠️  Starting Local Development Mode (Debug Enabled)"
        
        echo "   Target: $DEV_FILE"
        echo "   Context: $(pwd)"
        echo "   Env File: $ENV_FILE"
        echo "   Features: Hot-reload, Debugpy, Console Logs"
        echo "   (Use CTRL+C to exit)"
        
        # NOTE: Removed 'exec' so the script stays alive to run the trap at the end.
        # Explicitly loading base + dev file is often safer than relying solely on extends,
        # but relying on your extends configuration is fine here too.
        docker compose --env-file "$ENV_FILE" -f "$DEV_FILE" -p dynastore up --build
        ;;

    "prod")
        print_header "🚀 Starting On-Premise Production Mode"

        echo "   Target: $PROD_FILE"
        echo "   Context: $(pwd)"
        echo "   Env File: $ENV_FILE"
        echo "   Features: Detached (-d), Logs in ../logs/"
        
        docker compose --env-file "$ENV_FILE" -f "$PROD_FILE" -p dynastore up -d --build
        ;;

    "down")
        print_header "🛑 Stopping Production Services"
        
        echo "   Target Profile: Production"
        echo "   Env File: $ENV_FILE"
        
        docker compose --env-file "$ENV_FILE" -f "$PROD_FILE" -p dynastore down
        ;;

    *)
        echo "Usage: ./manage.sh [dev|prod|down]"
        echo "  dev       : Runs with debugpy, hot-reload, console logs (Foreground)"
        echo "  prod      : Runs optimized production image, file logs (Detached)"
        echo "  down      : Stops production services"
        exit 1
        ;;
        
esac

# The trap will fire here automatically