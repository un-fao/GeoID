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

"""
Integration tests for STAC Language Extension support.

Tests validate:
1. Localized input handling (single language strings wrapped into dicts)
2. Language-specific retrieval (lang=en, lang=fr, etc.)
3. Full multi-language response (lang=*)
4. Partial language updates (PUT with lang=en merges data)
5. Language deletion (PUT with lang=* replaces data)
6. Proper injection of language and languages metadata fields
"""

import pytest
import json
from dynastore.tools.identifiers import generate_id_hex
from dynastore.models.shared_models import Catalog, Collection, LocalizedText, LocalizedKeywords


@pytest.mark.asyncio
async def test_create_catalog_with_string_localization(sysadmin_in_process_client, db_engine, db_cleanup):
    """Test creating a catalog with simple string values that get localized to 'en'."""
    catalog_id = f"cat_lang_{generate_id_hex()[:8]}"
    payload = {
        "id": catalog_id,
        "description": "A test catalog in English"
    }
    
    response = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    
    assert response.status_code == 201
    data = response.json()
    
    # Check localized fields are properly wrapped
    assert data["id"] == catalog_id
    assert data["description"] == "A test catalog in English"


@pytest.mark.asyncio
async def test_create_catalog_with_multilanguage_dict(sysadmin_in_process_client):
    """Test creating a catalog with explicit multi-language dictionary."""
    catalog_id = f"cat_lang_multi_{generate_id_hex()[:8]}"
    payload = {
        "id": catalog_id,
        "description": {
            "en": "English description",
            "fr": "Description en français",
            "es": "Descripción en español"
        }
    }
    
    response = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    
    assert response.status_code == 201
    data = response.json()
    
    assert data["description"] == "English description"


@pytest.mark.asyncio
async def test_get_catalog_with_language_resolution(sysadmin_in_process_client):
    """Test retrieving catalog with language-specific content resolution."""
    catalog_id = f"cat_lang_res_{generate_id_hex()[:8]}"
    # First create a multi-language catalog
    payload = {
        "id": catalog_id,
        "description": {
            "en": "English description",
            "fr": "Description française",
            "es": "Descripción española"
        },
        "title": {
            "en": "English Title",
            "fr": "Titre Français"
        }
    }
    
    create_resp = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    assert create_resp.status_code == 201
    
    # Test retrieval with lang=en (should get English)
    resp_en = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}",
        params={"lang": "en"}
    )
    assert resp_en.status_code == 200
    data_en = resp_en.json()
    assert data_en["description"] == "English description"
    
    # Test retrieval with lang=fr (should get French)
    resp_fr = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}",
        params={"lang": "fr"}
    )
    assert resp_fr.status_code == 200
    data_fr = resp_fr.json()
    assert data_fr["description"] == "Description française"
    
    # Test retrieval with lang=* (should get all languages)
    resp_all = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}",
        params={"lang": "*"}
    )
    assert resp_all.status_code == 200
    data_all = resp_all.json()
    assert data_all["description"]["en"] == "English description"
    assert data_all["description"]["fr"] == "Description française"
    assert data_all["description"]["es"] == "Descripción española"


@pytest.mark.asyncio
async def test_language_metadata_injection(sysadmin_in_process_client):
    """Test that language and languages metadata fields are injected."""
    catalog_id = f"cat_lang_meta_{generate_id_hex()[:8]}"
    payload = {
        "id": catalog_id,
        "description": {
            "en": "English",
            "fr": "Français",
            "it": "Italiano"
        }
    }
    
    create_resp = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    assert create_resp.status_code == 201
    
    # Retrieve with lang=en
    resp = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}",
        params={"lang": "en"}
    )
    data = resp.json()
    
    # Check STAC Language Extension fields
    assert "language" in data or "languages" in data, \
        "STAC Language Extension fields (language/languages) missing"
    
    # If language field exists, it should match the requested lang
    if "language" in data:
        assert data["language"]["code"] == "en"
        assert "name" in data["language"]


@pytest.mark.asyncio
async def test_partial_language_update(sysadmin_in_process_client):
    """Test partial updates that merge new language data without overwriting others."""
    catalog_id = f"cat_lang_partial_{generate_id_hex()[:8]}"
    # Create initial catalog
    payload = {
        "id": catalog_id,
        "description": {
            "en": "English description",
            "fr": "Description française"
        }
    }
    
    create_resp = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    assert create_resp.status_code == 201
    
    # Update with new Spanish translation
    update_payload = {
        "description": "Descripción en español"
    }
    
    update_resp = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}",
        json=update_payload,
        params={"lang": "es"}
    )
    assert update_resp.status_code == 200
    
    # Verify: Spanish should be added, English and French preserved
    verify_resp = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}",
        params={"lang": "*"}
    )
    assert verify_resp.status_code == 200
    data = verify_resp.json()
    
    assert data["description"]["en"] == "English description"
    assert data["description"]["fr"] == "Description française"
    assert data["description"]["es"] == "Descripción en español"


@pytest.mark.asyncio
async def test_language_deletion_with_full_replacement(sysadmin_in_process_client):
    """Test that PUT with lang=* performs full replacement, allowing language deletion."""
    catalog_id = f"cat_lang_del_{generate_id_hex()[:8]}"
    # Create initial catalog
    payload = {
        "id": catalog_id,
        "description": {
            "en": "English",
            "fr": "Français",
            "es": "Español"
        }
    }
    
    create_resp = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    assert create_resp.status_code == 201
    
    # Full replacement with only English and French
    update_payload = {
        "description": {
            "en": "Updated English",
            "fr": "Français mis à jour"
        }
    }
    
    update_resp = await sysadmin_in_process_client.put(
        f"/stac/catalogs/{catalog_id}",
        json=update_payload,
        params={"lang": "*"}
    )
    assert update_resp.status_code == 200
    
    # Verify: Spanish should be removed
    verify_resp = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}",
        params={"lang": "*"}
    )
    assert verify_resp.status_code == 200
    data = verify_resp.json()
    
    assert data["description"]["en"] == "Updated English"
    assert data["description"]["fr"] == "Français mis à jour"
    assert "es" not in data["description"]


@pytest.mark.asyncio
async def test_collection_language_support(sysadmin_in_process_client):
    """Test that Collections also support STAC Language Extension."""
    catalog_id = f"cat_coll_lang_{generate_id_hex()[:8]}"
    # First create a catalog
    catalog_payload = {
        "id": catalog_id,
        "description": "Test catalog for collections"
    }
    
    cat_resp = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=catalog_payload,
        params={"lang": "en"}
    )
    assert cat_resp.status_code == 201
    
    # Create collection with multilingual metadata
    collection_payload = {
        "id": "test_collection",
        "description": {
            "en": "English collection description",
            "fr": "Description de collection en français"
        },
        "extent": {
            "spatial": {
                "bbox": [[-180, -90, 180, 90]]
            },
            "temporal": {
                "interval": [[None, None]]
            }
        }
    }
    
    coll_resp = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections",
        json=collection_payload,
        params={"lang": "en"}
    )
    assert coll_resp.status_code == 201
    coll_data = coll_resp.json()
    
    # Verify languages are preserved
    assert coll_data["description"] == "English collection description"


@pytest.mark.asyncio
async def test_keywords_language_support(sysadmin_in_process_client):
    """Test that keywords also support multilingual content."""
    catalog_id = f"cat_kw_lang_{generate_id_hex()[:8]}"
    payload = {
        "id": catalog_id,
        "description": "Test keywords localization",
        "keywords": {
            "en": ["satellite", "imagery", "remote sensing"],
            "fr": ["satellite", "imagerie", "télédétection"],
            "es": ["satélite", "imagería", "teledetección"]
        }
    }
    
    create_resp = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    assert create_resp.status_code == 201
    data = create_resp.json()
    
    # Verify keywords in English (requested lang)
    assert "keywords" in data
    assert "satellite" in data["keywords"]
    assert "imagery" in data["keywords"]
    assert "remote sensing" in data["keywords"]
    
    # Verify other languages are NOT in the top-level list
    assert "imagerie" not in data["keywords"]


@pytest.mark.asyncio
async def test_license_language_support(sysadmin_in_process_client):
    """Test that license information can be localized."""
    catalog_id = f"cat_lic_lang_{generate_id_hex()[:8]}"
    payload = {
        "id": catalog_id,
        "description": "Test license localization",
        "license": {
            "license_id": "CC-BY-4.0",
            "localized_content": {
                "en": {
                    "name": "Creative Commons Attribution 4.0 International",
                    "url": "https://creativecommons.org/licenses/by/4.0/"
                },
                "fr": {
                    "name": "Creative Commons Attribution 4.0 International",
                    "url": "https://creativecommons.org/licenses/by/4.0/deed.fr"
                }
            }
        }
    }
    
    create_resp = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    assert create_resp.status_code == 201
    data = create_resp.json()
    
    # Verify license structure
    assert "license" in data
    assert data["license"]["license_id"] == "CC-BY-4.0"


@pytest.mark.asyncio
async def test_language_fallback_resolution(sysadmin_in_process_client):
    """Test that language resolution falls back correctly when exact match not found."""
    catalog_id = f"cat_fall_lang_{generate_id_hex()[:8]}"
    payload = {
        "id": catalog_id,
        "description": {
            "en": "English description",
            "fr": "Description française"
        }
    }
    
    create_resp = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    assert create_resp.status_code == 201
    
    # Request a language that doesn't exist (should fallback)
    # The implementation should try base language, then default, then first available
    resp = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}",
        params={"lang": "de"}  # German not available
    )
    assert resp.status_code == 200
    data = resp.json()
    
    # Should still get some description (either EN or FR based on fallback logic)
    assert "description" in data


@pytest.mark.asyncio
async def test_stac_extensions_includes_language_extension(sysadmin_in_process_client):
    """Test that STAC Extensions list includes the Language Extension URI."""
    catalog_id = f"cat_ext_lang_{generate_id_hex()[:8]}"
    payload = {
        "id": catalog_id,
        "description": {
            "en": "Test",
            "fr": "Test"
        }
    }
    
    create_resp = await sysadmin_in_process_client.post(
        "/stac/catalogs",
        json=payload,
        params={"lang": "en"}
    )
    assert create_resp.status_code == 201
    data = create_resp.json()
    
    # Check stac_extensions includes language extension
    assert "stac_extensions" in data or "extensions" in data or "language" in data, \
        "STAC Language Extension not properly registered"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
