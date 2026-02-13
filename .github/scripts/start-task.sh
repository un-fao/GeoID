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

# start-task.sh: Entrypoint for Cloud Run Jobs

# Default APP name if not set
: "${APP:=dynastore}"

echo "Starting Task Runner for application: ${APP}"

# source secret environment variables
# source secret environment variables safely (respecting existing vars)
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

# source the python virtual environment
export VENV_PATH="/opt/venv"
source "${VENV_PATH}/bin/activate"

# The 'exec' command ensures the Python process replaces this script,
# allowing Cloud Run to manage its lifecycle and signals correctly.
# It directly calls main_task.py, passing along any arguments ($@)
# received by this script (which will be the payload from the gcloud command).
exec python /${APP}/src/dynastore/main_task.py "$@"
