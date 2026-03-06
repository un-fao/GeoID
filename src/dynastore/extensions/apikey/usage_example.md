# DynaStore API Key Usage Example

## Complete Guide: Restricted Access with Rate and Quota Limits

This guide demonstrates how to use the DynaStore API Key system to create a principal with highly restricted access:

- **Rate Limit**: 100 read/write requests per minute
- **Quota Limit**: Total request quota
- **Restricted Operations**: Only POST to add items to a specific collection and GET specific items by ID
- **Denied Operations**: Cannot list collections, cannot access other catalogs, cannot delete or update

---

## Table of Contents

1. [Authentication Flow](#authentication-flow)
2. [Creating Principals and Roles](#creating-principals-and-roles)
3. [Creating API Keys with Limits](#creating-api-keys-with-limits)
4. [Defining Restrictive Policies](#defining-restrictive-policies)
5. [Making API Calls](#making-api-calls)
6. [Monitoring Usage](#monitoring-usage)
7. [Troubleshooting](#troubleshooting)

---

## 1. Authentication Flow

### Overview

DynaStore supports multiple authentication methods:
- **API Keys**: Long-lived credentials for service accounts
- **OAuth2 Tokens**: Short-lived JWT tokens exchanged from API keys
- **External IdP**: Keycloak, Google, Azure AD (for cloud deployments)

### Basic Flow

```
1. Create Principal → 2. Create API Key → 3. Use API Key or Exchange for JWT → 4. Make API Calls
```

---

## 2. Creating Principals and Roles

### Create a Principal with Restricted Access

```python
from dynastore.modules.apikey.models import Principal
from dynastore.modules.apikey.apikey_manager import ApiKeyManager

# Initialize the API Key Manager
manager = ApiKeyManager(storage, policy_manager, app_state)

# Create a principal with no default roles (we'll use custom policies)
principal = Principal(
    provider="local",
    subject_id="restricted_user_001",
    roles=[],  # No roles - using custom policies only
    attributes={
        "name": "Restricted Data Contributor",
        "department": "External Partners",
        "purpose": "Add items to demo collection only"
    }
)

created_principal = await manager.create_principal(principal)
print(f"Created Principal ID: {created_principal.id}")
```

### Understanding Roles vs Custom Policies

- **Roles**: Predefined sets of permissions (e.g., `admin`, `user`, `viewer`)
- **Custom Policies**: Fine-grained permissions attached directly to principals or API keys
- **Best Practice**: Use custom policies for highly restricted access scenarios

---

## 3. Creating API Keys with Limits

### API Key with Rate Limit (100 requests/minute)

```python
from dynastore.modules.apikey.models import ApiKeyCreate, Condition

# Define rate limit condition
rate_limit = Condition(
    type="rate_limit",
    config={
        "max_requests": 100,
        "period_seconds": 60,
        "scope": "principal"  # Limit applies to the entire principal
    }
)

# Create API key
key_create = ApiKeyCreate(
    principal_id=created_principal.id,
    name="Restricted Contributor Key",
    note="100 req/min, can only POST to demo_collection",
    conditions=[rate_limit]
)

api_key, raw_key = await manager.create_key(key_create)
print(f"API Key: {raw_key}")
print(f"Store this securely - it won't be shown again!")
```

### API Key with Quota Limit

```python
# Define quota limit condition
quota_limit = Condition(
    type="max_count",
    config={
        "max_count": 10000,  # Total 10,000 requests allowed
        "scope": "principal"
    }
)

# Create API key with both rate and quota limits
key_create = ApiKeyCreate(
    principal_id=created_principal.id,
    name="Limited Quota Key",
    note="100 req/min, 10K total quota",
    conditions=[rate_limit, quota_limit]
)

api_key, raw_key = await manager.create_key(key_create)
```

### API Key with Time Expiration

```python
from datetime import datetime, timezone, timedelta

# Expires in 30 days
expiry_time = datetime.now(timezone.utc) + timedelta(days=30)

expiry_condition = Condition(
    type="time_expiration",
    config={
        "expires_at": expiry_time.isoformat()
    }
)

key_create = ApiKeyCreate(
    principal_id=created_principal.id,
    name="Temporary Key",
    note="Expires in 30 days",
    conditions=[rate_limit, quota_limit, expiry_condition]
)

api_key, raw_key = await manager.create_key(key_create)
```

---

## 4. Defining Restrictive Policies

### Policy 1: Only POST to Specific Collection

```python
from dynastore.modules.apikey.models import Policy

# Allow POST to /features/catalogs/demo_catalog/collections/demo_collection/items
post_policy = Policy(
    id="allow_post_demo_collection",
    effect="allow",
    actions=["POST"],
    resources=[
        r"^/features/catalogs/demo_catalog/collections/demo_collection/items$"
    ],
    conditions=[]
)

# Create the policy
await manager.policy_manager.create_policy(post_policy)
```

### Policy 2: Only GET Specific Items by ID

```python
# Allow GET /features/catalogs/demo_catalog/collections/demo_collection/items/{item_id}
get_item_policy = Policy(
    id="allow_get_demo_items",
    effect="allow",
    actions=["GET"],
    resources=[
        r"^/features/catalogs/demo_catalog/collections/demo_collection/items/[^/]+$"
    ],
    conditions=[]
)

await manager.policy_manager.create_policy(get_item_policy)
```

### Policy 3: Enforce Query Parameters

```python
# Allow DELETE only with force=false (safe delete)
safe_delete_policy = Policy(
    id="allow_safe_delete",
    effect="allow",
    actions=["DELETE"],
    resources=[
        r"^/features/catalogs/demo_catalog/collections/demo_collection/items/[^/]+$"
    ],
    conditions=[
        Condition(
            type="match",
            config={
                "attribute": "query.force",
                "operator": "eq",
                "value": "false"
            }
        )
    ]
)

await manager.policy_manager.create_policy(safe_delete_policy)
```

### Attach Policies to API Key

```python
# Create API key with all restrictive policies
key_create = ApiKeyCreate(
    principal_id=created_principal.id,
    name="Fully Restricted Key",
    note="POST to collection, GET items only",
    conditions=[rate_limit, quota_limit],
    custom_policies=[post_policy, get_item_policy]
)

api_key, raw_key = await manager.create_key(key_create)
```

---

## 5. Making API Calls

### Using the API Key

```python
import httpx

# Base URL of your DynaStore instance
BASE_URL = "https://your-dynastore-instance.com"

# Create HTTP client with API key
headers = {
    "X-API-Key": raw_key
}

async with httpx.AsyncClient(base_url=BASE_URL, headers=headers) as client:
    # ✅ ALLOWED: POST item to demo_collection
    item_data = {
        "type": "Feature",
        "id": "item_001",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"name": "Test Item"}
    }
    
    response = await client.post(
        "/features/catalogs/demo_catalog/collections/demo_collection/items",
        json=item_data
    )
    print(f"POST Item: {response.status_code}")  # 201 Created
    
    # ✅ ALLOWED: GET specific item
    response = await client.get(
        "/features/catalogs/demo_catalog/collections/demo_collection/items/item_001"
    )
    print(f"GET Item: {response.status_code}")  # 200 OK
    
    # ❌ DENIED: List items (not allowed by policy)
    response = await client.get(
        "/features/catalogs/demo_catalog/collections/demo_collection/items"
    )
    print(f"List Items: {response.status_code}")  # 403 Forbidden
    
    # ❌ DENIED: Access other catalog
    response = await client.get(
        "/features/catalogs/other_catalog/collections"
    )
    print(f"Other Catalog: {response.status_code}")  # 403 Forbidden
    
    # ❌ DENIED: DELETE item
    response = await client.delete(
        "/features/catalogs/demo_catalog/collections/demo_collection/items/item_001"
    )
    print(f"DELETE Item: {response.status_code}")  # 403 Forbidden
```

### Exchanging API Key for JWT Token

```python
# Exchange API key for short-lived JWT
response = await client.post(
    "/apikey/token",
    json={"ttl_seconds": 3600}  # 1 hour
)

token_data = response.json()
jwt_token = token_data["access_token"]

# Use JWT for subsequent requests
jwt_headers = {
    "Authorization": f"Bearer {jwt_token}"
}

async with httpx.AsyncClient(base_url=BASE_URL, headers=jwt_headers) as jwt_client:
    response = await jwt_client.post(
        "/features/catalogs/demo_catalog/collections/demo_collection/items",
        json=item_data
    )
    print(f"POST with JWT: {response.status_code}")
```

---

## 6. Monitoring Usage

### Check Current Usage and Quota

```python
# Get usage statistics
response = await client.get("/apikey/usage")
usage_data = response.json()

print(f"Total Requests: {usage_data.get('total_requests', 0)}")
print(f"Quota Remaining: {usage_data.get('quota_remaining', 'N/A')}")
print(f"Rate Limit Window: {usage_data.get('rate_limit_window', 'N/A')}")
```

### Rate Limit Enforcement Example

```python
# Make 100 requests (at rate limit)
for i in range(100):
    response = await client.get(
        f"/features/catalogs/demo_catalog/collections/demo_collection/items/item_{i}"
    )
    print(f"Request {i+1}: {response.status_code}")

# 101st request will be rate limited
response = await client.get(
    "/features/catalogs/demo_catalog/collections/demo_collection/items/item_101"
)
print(f"Request 101: {response.status_code}")  # 429 Too Many Requests

# Wait for rate limit window to reset (60 seconds)
import asyncio
await asyncio.sleep(60)

# Now requests work again
response = await client.get(
    "/features/catalogs/demo_catalog/collections/demo_collection/items/item_102"
)
print(f"After Reset: {response.status_code}")  # 200 OK
```

---

## 7. Troubleshooting

### Common Issues

#### 403 Forbidden

**Cause**: Policy does not allow the requested operation

**Solution**: 
- Check the policy resources match the exact path
- Verify the action (GET, POST, DELETE) is allowed
- Check if conditions (query parameters) are met

```python
# Debug: Check what policies are attached
key_info = await manager.get_key_info(raw_key)
print(f"Policies: {key_info.custom_policies}")
```

#### 429 Too Many Requests

**Cause**: Rate limit exceeded

**Solution**:
- Wait for the rate limit window to reset
- Reduce request frequency
- Request a higher rate limit from administrator

#### 401 Unauthorized

**Cause**: Invalid or expired API key/token

**Solution**:
- Verify the API key is correct
- Check if the key has expired
- Regenerate the API key if necessary

### Policy Regex Debugging

```python
import re

# Test if your path matches the policy regex
policy_regex = r"^/features/catalogs/demo_catalog/collections/demo_collection/items$"
test_path = "/features/catalogs/demo_catalog/collections/demo_collection/items"

if re.match(policy_regex, test_path):
    print("✅ Path matches policy")
else:
    print("❌ Path does not match policy")
```

---

## Complete Example Script

See [`demo_restricted_access.py`](file:///Users/ccancellieri/work/code/dynastore/src/dynastore/extensions/apikey/demo_restricted_access.py) for a complete, executable example demonstrating all concepts in this guide.

---

## Summary

This guide demonstrated:

1. ✅ Creating principals with custom attributes
2. ✅ Creating API keys with rate limits (100 req/min)
3. ✅ Creating API keys with quota limits
4. ✅ Defining restrictive policies (POST to collection, GET items only)
5. ✅ Enforcing query parameter conditions
6. ✅ Denying access to other catalogs and operations
7. ✅ Monitoring usage and handling rate limits

For more information, see:
- [IAG Framework Documentation](file:///Users/ccancellieri/work/code/dynastore/src/dynastore/extensions/apikey/DynaStore%20Identity.TXT)
- [API Key Module README](file:///Users/ccancellieri/work/code/dynastore/src/dynastore/modules/apikey/readme.md)
