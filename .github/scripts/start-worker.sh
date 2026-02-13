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

# .github/scripts/start-worker.sh

: "${APP:=dynastore}"
echo "Starting Worker for: ${APP}"

# Source secrets and venv
# Source secrets and venv safely (respecting existing vars)
if [ -f "/${APP}/env/.env" ]; then
    echo "Loading baked environment variables from /${APP}/env/.env (respecting existing vars)..."
    while read -r line || [[ -n "$line" ]]; do
        [[ "$line" =~ ^#.*$ ]] && continue
        [[ -z "$line" ]] && continue
        key=$(echo "$line" | cut -d'=' -f1)
        value=$(echo "$line" | cut -d'=' -f2-)
        key="${key#export }"
        if [ -z "${!key}" ]; then
            export "$key=$value"
        fi
    done < "/${APP}/env/.env"
fi
export VENV_PATH="/opt/venv"
source "${VENV_PATH}/bin/activate"

# 1. Apply Procrastinate database schema
# This ensures all necessary tables, functions, and indexes for Procrastinate
# are present and up-to-date in the database.
# echo "Applying Procrastinate database schema if it doesn't exist..."
# if ! python -m procrastinate --app dynastore.modules.procrastinate.module.app schema --read >/dev/null 2>&1; then
#   echo "Schema not found or incomplete. Applying now."
#   python -m procrastinate --app dynastore.modules.procrastinate.module.app schema
# else
#   echo "Schema already exists."
# fi

python -m http.server $TCP_PORT &> /dev/null &

# If arguments are provided, execute them instead of the default worker command.
if [ "$#" -gt 0 ]; then
    echo "Executing custom command: $@"
    exec "$@"
fi

# 2. Run the worker
echo "Starting Procrastinate worker via main_worker.py..."
exec python -m dynastore.main_worker