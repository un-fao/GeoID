#    Copyright 2026 FAO
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

class IamError(Exception):
    """Base exception for IAM module."""
    status_code = 500

class PrincipalNotFoundError(IamError):
    status_code = 404

class RateLimitExceededError(IamError):
    status_code = 429

class QuotaExceededError(IamError):
    status_code = 403

class ConflictingResourceError(IamError):
    status_code = 409

class InvalidAuthTokenError(IamError):
    """Raised when an Authorization header is present but no provider
    (OIDC nor HS256 fallback) can validate it. Distinguishes "invalid
    token" from "no token" — the latter still degrades to anonymous."""
    status_code = 401
