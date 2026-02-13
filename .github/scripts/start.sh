#!/bin/bash
#
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

# start.sh: Gunicorn/Uvicorn entrypoint script

# --- Default Configuration ---
: "${APP:=dynastore}"
: "${GUNICORN_WORKERS:=4}"
: "${GUNICORN_THREADS:=1}"
: "${GUNICORN_TIMEOUT:=30}"
: "${KEEP_ALIVE:=5}"
: "${LOG_LEVEL:=info}"
: "${ACCESS_LOG:=-}"
: "${ERROR_LOG:=-}"
# -----------------------------

echo "Starting Gunicorn for application: ${APP}.main:app"

# Ensure Log Directories Exist if we are logging to files
if [ "$ACCESS_LOG" != "-" ] && [ -n "$ACCESS_LOG" ]; then
    echo "   Access Log: $ACCESS_LOG"
    mkdir -p "$(dirname "$ACCESS_LOG")"
fi

if [ "$ERROR_LOG" != "-" ] && [ -n "$ERROR_LOG" ]; then
    echo "   Error Log: $ERROR_LOG"
    mkdir -p "$(dirname "$ERROR_LOG")"
fi

# Load env vars unless ignored (e.g., during tests). 
# We use a loop to ensure we DO NOT overwrite variables already set by the environment (Cloud Run).
if [ "$IGNORE_BAKED_ENV" != "true" ] && [ -f "/${APP}/env/.env" ]; then
    echo "Loading baked environment variables from /${APP}/env/.env (respecting existing vars)..."
    while IFS='=' read -r key value || [ -n "$key" ]; do
        # Skip comments and empty lines
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        # Remove potential 'export ' prefix
        key="${key#export }"
        # Only set if not already present in the environment
        if [ -z "${!key}" ]; then
            export "$key=$value"
        fi
    done < "/${APP}/env/.env"
fi

# If arguments are provided, execute them instead of the default Gunicorn command.
if [ "$#" -gt 0 ]; then
    echo "Executing custom command: $@"
    exec "$@"
fi

# The 'exec' command ensures that the gunicorn process replaces the script 
# process, allowing Docker to correctly handle signals (like graceful shutdown).
exec gunicorn \
  --worker-class "uvicorn.workers.UvicornWorker" \
  "${APP}.main:app" \
  --bind "0.0.0.0:${TCP_PORT}" \
  --workers "${GUNICORN_WORKERS}" \
  --threads "${GUNICORN_THREADS}" \
  --timeout "${GUNICORN_TIMEOUT}" \
  --keep-alive "${KEEP_ALIVE}" \
  --log-level "${LOG_LEVEL}" \
  --access-logfile "${ACCESS_LOG}" \
  --error-logfile "${ERROR_LOG}"