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

class ApiKeyError(Exception):
    """Base exception for ApiKey module."""
    status_code = 500

class PrincipalNotFoundError(ApiKeyError):
    status_code = 404

class ApiKeyNotFoundError(ApiKeyError):
    status_code = 404

class ApiKeyInvalidError(ApiKeyError):
    status_code = 401

class ApiKeyExpiredError(ApiKeyError):
    status_code = 401

class RateLimitExceededError(ApiKeyError):
    status_code = 429

class QuotaExceededError(ApiKeyError):
    status_code = 403

class ConflictingResourceError(ApiKeyError):
    status_code = 409
