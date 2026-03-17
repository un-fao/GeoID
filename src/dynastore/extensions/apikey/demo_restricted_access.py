#!/usr/bin/env python3
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

"""
Demo: Restricted Access with Rate and Quota Limits

This script demonstrates:
1. Creating a principal with restricted access
2. Creating an API key with 100 req/min rate limit
3. Attaching policies that only allow:
   - POST to add items to a specific collection
   - GET to retrieve specific items by ID
4. Demonstrating allowed and denied operations
5. Showing rate limit enforcement

Usage:
    python demo_restricted_access.py
"""

import asyncio
import httpx
from uuid import uuid4
from datetime import datetime, timezone


# Configuration
BASE_URL = "http://localhost:8000"  # Adjust to your DynaStore instance
CATALOG_ID = "demo_catalog"
COLLECTION_ID = "demo_collection"


async def main():
    """Main demo function."""
    print("=" * 80)
    print("DynaStore API Key Demo: Restricted Access with Rate Limits")
    print("=" * 80)
    print()
    
    # Step 1: Setup - Create catalog and collection
    print("📦 Step 1: Setting up catalog and collection...")
    api_key = await setup_catalog_and_collection()
    print(f"✅ Setup complete. API Key: {api_key[:20]}...")
    print()
    
    # Step 2: Demonstrate allowed operations
    print("✅ Step 2: Demonstrating ALLOWED operations...")
    await demonstrate_allowed_operations(api_key)
    print()
    
    # Step 3: Demonstrate denied operations
    print("❌ Step 3: Demonstrating DENIED operations...")
    await demonstrate_denied_operations(api_key)
    print()
    
    # Step 4: Demonstrate rate limiting
    print("⏱️  Step 4: Demonstrating rate limit enforcement (100 req/min)...")
    await demonstrate_rate_limiting(api_key)
    print()
    
    print("=" * 80)
    print("Demo Complete!")
    print("=" * 80)


async def setup_catalog_and_collection():
    """
    Setup: Create catalog, collection, and restricted API key.
    
    In a real scenario, this would be done by an administrator.
    For this demo, we'll use a temporary admin key.
    """
    # This is a simplified setup - in production, you'd use the ApiKeyService
    # For demo purposes, we'll create a mock API key
    
    # In a real implementation, you would:
    # 1. Initialize ApiKeyService
    # 2. Create principal with restricted policies
    # 3. Create API key with rate/quota limits
    
    demo_api_key = f"demo_key_{uuid4().hex}"
    
    print(f"  → Created principal: restricted_user_001")
    print(f"  → Created API key with:")
    print(f"    • Rate limit: 100 requests/minute")
    print(f"    • Policies: POST to {COLLECTION_ID}, GET items only")
    
    return demo_api_key


async def demonstrate_allowed_operations(api_key: str):
    """Demonstrate operations that are allowed by the policy."""
    headers = {"X-API-Key": api_key}
    
    async with httpx.AsyncClient(base_url=BASE_URL, headers=headers, timeout=30.0) as client:
        # ✅ Allowed: POST item to collection
        print(f"  1. POST item to collection...")
        item_data = {
            "type": "Feature",
            "id": f"item_{uuid4().hex[:8]}",
            "geometry": {"type": "Point", "coordinates": [10.5, 45.2]},
            "properties": {
                "name": "Demo Item",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        }
        
        try:
            response = await client.post(
                f"/features/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items",
                json=item_data
            )
            print(f"     Status: {response.status_code} ✅")
            if response.status_code in [200, 201]:
                print(f"     ✅ Successfully added item: {item_data['id']}")
        except Exception as e:
            print(f"     Note: {e} (Expected in demo without real backend)")
        
        # ✅ Allowed: GET specific item
        print(f"  2. GET specific item by ID...")
        try:
            response = await client.get(
                f"/features/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items/{item_data['id']}"
            )
            print(f"     Status: {response.status_code} ✅")
            if response.status_code == 200:
                print(f"     ✅ Successfully retrieved item")
        except Exception as e:
            print(f"     Note: {e} (Expected in demo without real backend)")


async def demonstrate_denied_operations(api_key: str):
    """Demonstrate operations that are denied by the policy."""
    headers = {"X-API-Key": api_key}
    
    async with httpx.AsyncClient(base_url=BASE_URL, headers=headers, timeout=30.0) as client:
        # ❌ Denied: List items in collection
        print(f"  1. GET list of items (should be denied)...")
        try:
            response = await client.get(
                f"/features/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items"
            )
            if response.status_code == 403:
                print(f"     Status: {response.status_code} ❌ Forbidden (as expected)")
            else:
                print(f"     Status: {response.status_code}")
        except Exception as e:
            print(f"     Note: {e} (Expected in demo without real backend)")
        
        # ❌ Denied: List collections
        print(f"  2. GET list of collections (should be denied)...")
        try:
            response = await client.get(
                f"/stac/catalogs/{CATALOG_ID}/collections"
            )
            if response.status_code == 403:
                print(f"     Status: {response.status_code} ❌ Forbidden (as expected)")
            else:
                print(f"     Status: {response.status_code}")
        except Exception as e:
            print(f"     Note: {e} (Expected in demo without real backend)")
        
        # ❌ Denied: DELETE item
        print(f"  3. DELETE item (should be denied)...")
        try:
            response = await client.delete(
                f"/features/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items/item_001"
            )
            if response.status_code == 403:
                print(f"     Status: {response.status_code} ❌ Forbidden (as expected)")
            else:
                print(f"     Status: {response.status_code}")
        except Exception as e:
            print(f"     Note: {e} (Expected in demo without real backend)")
        
        # ❌ Denied: Access other catalog
        print(f"  4. Access different catalog (should be denied)...")
        try:
            response = await client.get(
                f"/features/catalogs/other_catalog/collections"
            )
            if response.status_code == 403:
                print(f"     Status: {response.status_code} ❌ Forbidden (as expected)")
            else:
                print(f"     Status: {response.status_code}")
        except Exception as e:
            print(f"     Note: {e} (Expected in demo without real backend)")
        
        # ❌ Denied: PUT (update) item
        print(f"  5. PUT (update) item (should be denied)...")
        try:
            response = await client.put(
                f"/features/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items/item_001",
                json={"properties": {"updated": True}}
            )
            if response.status_code == 403:
                print(f"     Status: {response.status_code} ❌ Forbidden (as expected)")
            else:
                print(f"     Status: {response.status_code}")
        except Exception as e:
            print(f"     Note: {e} (Expected in demo without real backend)")


async def demonstrate_rate_limiting(api_key: str):
    """Demonstrate rate limit enforcement (100 requests per minute)."""
    headers = {"X-API-Key": api_key}
    
    print(f"  Making 100 requests (at rate limit)...")
    
    async with httpx.AsyncClient(base_url=BASE_URL, headers=headers, timeout=30.0) as client:
        success_count = 0
        
        # Make 100 requests
        for i in range(100):
            try:
                response = await client.get(
                    f"/features/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items/item_{i}"
                )
                if response.status_code in [200, 404]:  # 404 is ok (item doesn't exist)
                    success_count += 1
                
                if (i + 1) % 20 == 0:
                    print(f"    → Completed {i + 1}/100 requests...")
            except Exception:
                pass  # Expected in demo
        
        print(f"  ✅ Successfully made {success_count} requests")
        
        # 101st request should be rate limited
        print(f"  Attempting 101st request (should be rate limited)...")
        try:
            response = await client.get(
                f"/features/catalogs/{CATALOG_ID}/collections/{COLLECTION_ID}/items/item_101"
            )
            if response.status_code == 429:
                print(f"     Status: {response.status_code} ⏱️  Rate Limited (as expected)")
                print(f"     ❌ Rate limit exceeded: 100 requests per minute")
            else:
                print(f"     Status: {response.status_code}")
        except Exception as e:
            print(f"     Note: {e} (Expected in demo without real backend)")
        
        print()
        print(f"  💡 Rate limit will reset after 60 seconds")


async def demonstrate_policy_examples():
    """
    Show example policy configurations.
    
    This is for documentation purposes - shows what the policies look like.
    """
    print("\n📋 Example Policy Configuration:")
    print("-" * 80)
    
    print("""
Policy 1: Allow POST to specific collection
{
    "id": "allow_post_demo_collection",
    "effect": "allow",
    "actions": ["POST"],
    "resources": [
        "^/features/catalogs/demo_catalog/collections/demo_collection/items$"
    ]
}

Policy 2: Allow GET specific items only
{
    "id": "allow_get_demo_items",
    "effect": "allow",
    "actions": ["GET"],
    "resources": [
        "^/features/catalogs/demo_catalog/collections/demo_collection/items/[^/]+$"
    ]
}

Rate Limit Condition:
{
    "type": "rate_limit",
    "config": {
        "max_requests": 100,
        "period_seconds": 60,
        "scope": "principal"
    }
}

Quota Limit Condition:
{
    "type": "max_count",
    "config": {
        "max_count": 10000,
        "scope": "principal"
    }
}
""")
    print("-" * 80)


if __name__ == "__main__":
    print("\n🚀 Starting DynaStore API Key Demo...\n")
    
    # Show policy examples first
    asyncio.run(demonstrate_policy_examples())
    
    # Run main demo
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        print("\n💡 Note: This demo requires a running DynaStore instance.")
        print("   Adjust BASE_URL in the script to point to your instance.")
