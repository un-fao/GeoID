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

import logging
import requests
import os
try:
    from google.auth import default
    from google.auth.exceptions import DefaultCredentialsError
except ImportError:
    default = None
    DefaultCredentialsError = Exception # Fallback to generic Exception if error type missing
from pathlib import Path

logger = logging.getLogger(__name__)

def resolve_gcp_credentials():
    """
    Map a container-style ``GOOGLE_APPLICATION_CREDENTIALS`` path to the real
    file on whatever source tree this code runs from.

    Local development carries a container path in ``.env`` (e.g.
    ``/dynastore/src/dynastore/modules/gcp/<key>.json``) while the key actually
    lives under the source tree at ``packages/core/src/dynastore/modules/gcp/``.
    We strip the ``/dynastore/`` prefix and walk up from this file trying each
    ancestor as the root the relative path hangs off, so resolution works
    across both the pre-monorepo (``src/...``) and monorepo
    (``packages/core/src/...``) layouts AND inside the container — instead of
    assuming a fixed ancestor depth (which silently broke after the monorepo
    move).
    """
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not creds_path.startswith("/dynastore/"):
        return
    if os.path.exists(creds_path):
        return  # already a valid path (e.g. the container mount makes it real)

    relative_path = creds_path.replace("/dynastore/", "", 1)
    for ancestor in Path(__file__).resolve().parents:
        candidate = ancestor / relative_path
        if candidate.exists():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(candidate)
            return
    logger.warning(
        "Detected container GCP credentials path '%s' but could not resolve it "
        "to a file on disk.", creds_path,
    )

def get_credentials() -> tuple:
    """
    Retrieves and identifies the active Google Cloud Platform (GCP) identity.

    This function uses the Application Default Credentials (ADC) strategy to
    find the credentials and determines if they belong to a service account or a
    user account.

    Returns:
        A tuple containing the credentials object and a dictionary with identity
        details ('account_email', 'project_id', 'region'), or None if not found.
    """
    try:
        resolve_gcp_credentials()
        logger.debug(f"GCP GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS','Not Set')}")
        if default is None:
            raise ImportError("google-auth is not installed; cannot resolve GCP credentials.")
        credentials, project_id = default()
        
        # Initialize defaults to avoid UnboundLocalError
        project_id_meta = project_id
        account_email = getattr(credentials, 'service_account_email', 'N/A (user credentials)')
        account_type = "Service Account" if hasattr(credentials, 'service_account_email') else "User Account"
        region = os.getenv("REGION")
        project_number = os.getenv("GCP_PROJECT_NUMBER")

        metadata_url = "http://metadata.google.internal/computeMetadata/v1/"
        headers = {"Metadata-Flavor": "Google"}
        timeout = 1.0 # Significant reduction from 60s

        try:
            # Fetch project ID from metadata server for consistency.
            project_id_meta_response = requests.get(f"{metadata_url}project/project-id", headers=headers, timeout=timeout)
            if project_id_meta_response.status_code == 200:
                project_id_meta = project_id_meta_response.text

            # Fetch service account email.
            email_response = requests.get(f"{metadata_url}instance/service-accounts/default/email", headers=headers, timeout=timeout)
            if email_response.status_code == 200:
                account_email = email_response.text
                account_type = "Service Account"

            project_number_resp = requests.get(f"{metadata_url}project/numeric-project-id", headers=headers, timeout=timeout)
            if project_number_resp.status_code == 200:
                project_number = project_number_resp.text

            # Fetch region from zone.
            zone_response = requests.get(f"{metadata_url}instance/zone", headers=headers, timeout=timeout)
            if zone_response.status_code == 200:
                zone = zone_response.text.split('/')[-1]
                region = '-'.join(zone.split('-')[:-1])

        except (requests.exceptions.RequestException, IndexError) as e:
            # Fallback for environments without a metadata server (e.g., local dev with user creds)
            # or if metadata server is not reachable.
            logger.debug(f"Metadata server not available: {e}")

        # Use project_id_meta in the log message for consistency.
        logging.info(f"Successfully identified GCP identity. Account email: {account_email}, Project: {project_id_meta}, Region: {region or 'Not Detected'}")
        identity_info = {
            "account_type": account_type,
            "account_email": account_email,
            "project_id": project_id_meta,
            "project_number": project_number,
            "region": region,
        }
        return credentials, identity_info

    except DefaultCredentialsError:
        logging.error(
            "Could not find Application Default Credentials. Ensure your environment "
            "is configured correctly (e.g., GOOGLE_APPLICATION_CREDENTIALS is set, "
            "you are running on a GCP resource with a service account, or you have "
            "run 'gcloud auth application-default login')."
        )
        raise
