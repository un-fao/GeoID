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
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

# File: dynastore/extensions/iam/middleware.py

import time
import jwt
import logging
from typing import Optional, Callable, Awaitable

from dynastore.tools.discovery import get_protocol

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from dynastore.modules.iam.conditions import condition_manager, EvaluationContext
from dynastore.models.protocols.stats import StatsProtocol
from dynastore.models.protocols.policies import PermissionProtocol
from dynastore.models.protocols.iam import IamProtocol
from dynastore.modules.iam.models import PolicyBundle

logger = logging.getLogger(__name__)


class IamMiddleware(BaseHTTPMiddleware):
    _iam_manager: Optional[IamProtocol] = None
    _policy_service: Optional[PermissionProtocol] = None

    def __init__(self, app, **kwargs):
        super().__init__(app)
        self._iam_manager: Optional[IamProtocol] = None
        self._policy_service: Optional[PermissionProtocol] = None

    def _emit_audit(
        self, event_type: str, principal_id: str, ip: str, schema: str,
        detail: Optional[dict] = None,
    ) -> None:
        """Fire-and-forget audit event (non-blocking)."""
        import asyncio
        storage = getattr(self._iam_manager, "storage", None)
        if storage and hasattr(storage, "log_audit_event"):
            try:
                asyncio.get_running_loop().create_task(
                    storage.log_audit_event(
                        event_type=event_type,
                        principal_id=principal_id,
                        ip_address=ip,
                        detail=detail,
                        schema=schema,
                    )
                )
            except RuntimeError:
                pass

    def lazy_init_manager(self) -> bool:
        """Returns True if IAM is available, False if this scope runs without IAM."""
        if self._iam_manager is None:
            iam_protocol = get_protocol(IamProtocol)
            if not iam_protocol:
                logger.debug(
                    "IamProtocol not registered — IamMiddleware running in pass-through mode."
                )
                return False

            self._iam_manager = iam_protocol
            self._policy_service = iam_protocol.get_policy_service()

            if not self._policy_service:
                logger.warning(
                    "PolicyService not available in IamProtocol — IamMiddleware running in pass-through mode."
                )
                self._iam_manager = None
                return False

        return True

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        logger.warning(
            f"DEBUG: IamMiddleware.dispatch: {request.method} {request.url.path}"
        )
        start_time = time.time()
        method = request.method
        raw_path = request.url.path
        # Strip API_ROOT_PATH so policy patterns are relative to the service root
        root_path = request.scope.get("root_path", "") or ""
        if root_path and root_path != "/" and raw_path.startswith(root_path):
            path = raw_path[len(root_path):] or "/"
        else:
            path = raw_path

        query_params = dict(request.query_params)
        query_params_tuple = tuple(sorted(query_params.items()))

        client_ip = request.client.host if request.client else "unknown"

        # Initialize identity variables
        principal_role = None
        principal_obj = None

        context_extras = {
            "client_ip": client_ip,
            "user_agent": request.headers.get("user-agent", ""),
            "referer": request.headers.get("referer", ""),
            "principal_obj": principal_obj,
        }

        if self._iam_manager is None:
            if not self.lazy_init_manager():
                return await call_next(request)

        catalog_id = getattr(request.state, "catalog_id", None)
        if not catalog_id:
            # Simple path-based extraction heuristic for common patterns
            if "/catalogs/" in path:
                parts = path.split("/")
                try:
                    idx = parts.index("catalogs")
                    if idx + 1 < len(parts):
                        catalog_id = parts[idx + 1]
                        request.state.catalog_id = catalog_id
                except ValueError:
                    pass

        # Resolve physical schema
        schema = await self._iam_manager.resolve_schema(catalog_id)

        # 1. Authenticate and get Principal
        (
            principal_role,
            principal_obj,
        ) = await self._iam_manager.authenticate_and_get_role(request)

        request.state.principal_role = principal_role
        request.state.principal = principal_obj

        # 2. Extract Identity Metadata from JWT
        token_str = self._iam_manager.extract_token_from_request(request)

        token_identifier = None
        source = "unauthenticated"

        if token_str:
            source = "JWT"
            try:
                unverified_payload = jwt.decode(
                    token_str, options={"verify_signature": False}
                )
                token_identifier = unverified_payload.get("jti")
            except jwt.InvalidTokenError:
                pass

        if principal_role and "sysadmin" in principal_role:
            source = "sysadmin"

        effective_principal_id = (
            (principal_obj.display_name or principal_obj.subject_id)
            if principal_obj
            else (principal_role[0] if principal_role else "anonymous")
        )
        request.state.principal_id = effective_principal_id

        # 3. Build Evaluation Context
        ctx = EvaluationContext(
            request=request,
            storage=self._iam_manager.storage,
            manager=self._iam_manager,
            token_identifier=token_identifier,
            principal_id=effective_principal_id,
            path=path,
            method=method,
            query_params=query_params,
            schema=schema,
            catalog_id=catalog_id,
            extras=context_extras,
        )

        # 4. Aggregating Policies (The Hierarchy)
        all_conditions = []

        # A. Token/Principal Policy (The "User")
        if principal_obj and principal_obj.custom_policies:
            p_policy = PolicyBundle(statements=principal_obj.custom_policies)
            if not self._policy_service.evaluate_policy_statements(
                p_policy, method, path
            ):
                return JSONResponse(
                    {"detail": "Access denied by Principal policy."}, status_code=403
                )
            # Add Conditions
            for p in principal_obj.custom_policies:
                if p.conditions:
                    all_conditions.extend(p.conditions)

        # C. Global System Policies
        # Include both the Identifier and the Roles for policy matching
        principals_to_check = [effective_principal_id] + (
            principal_role
            if isinstance(principal_role, list)
            else ([principal_role] if principal_role else [])
        )

        allowed_by_global, reason = await self._policy_service.evaluate_access(
            principals=principals_to_check,
            path=path,
            method=method,
            request_context=ctx,
            catalog_id=catalog_id,
        )
        if not allowed_by_global:
            logger.debug(f"Access denied by Global Security policy: {reason}")
            self._emit_audit(
                "authz_denied", effective_principal_id, client_ip, schema,
                {"path": path, "method": method, "reason": reason},
            )
            return JSONResponse(
                {"detail": f"Access denied by Global Security policy: {reason}"},
                status_code=403,
            )

        request.state.policy_allowed = True

        # 5. Evaluate All Accumulated Conditions (from Key and Principal)
        if all_conditions:
            from dynastore.modules.iam.conditions import evaluate_conditions

            if not await evaluate_conditions(all_conditions, ctx):
                logger.warning(
                    f"Rate Limit/Quota Exceeded for '{effective_principal_id}' via '{source}'"
                )
                return JSONResponse(
                    {"detail": "Rate limit or Quota exceeded."}, status_code=429
                )

        response = await call_next(request)

        # Stats Logging (Best Effort)
        try:
            processing_time_ms = (time.time() - start_time) * 1000
            details = {
                "auth_user": effective_principal_id,
                "auth_source": source,
                "api_key": api_key_hash,
            }
            # 6. Log completion metrics
            stats_service = get_protocol(StatsProtocol)
            if stats_service:
                stats_service.log_request_completion(
                    request=request,
                    status_code=response.status_code,
                    processing_time_ms=processing_time_ms,
                    details=details,
                    catalog_id=catalog_id,
                )
        except Exception as e:
            logger.error(f"Stats error: {e}")

        return response
