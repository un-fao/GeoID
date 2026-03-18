import pytest
from pydantic import ValidationError
from dynastore.models.shared_models import LocalizedText, LocalizedKeywords, BaseMetadata, Language, LicenseInfo

def test_language_enum():
    assert Language.EN == "en"
    assert Language.FR == "fr"
    assert "it" in [l.value for l in Language]

def test_localized_text_validation():
    # Valid
    lt = LocalizedText(en="Hello", fr="Bonjour")
    assert lt.en == "Hello"
    assert lt.fr == "Bonjour"
    
    # Extra languages are allowed (for flexibility with RFC 5646 ids)
    lt2 = LocalizedText(en="Hello", de="Hallo", ja="Konnichiwa")
    assert lt2.en == "Hello"
    assert lt2.de == "Hallo"
    assert lt2.ja == "Konnichiwa"

def test_localized_keywords_validation():
    # Valid
    lk = LocalizedKeywords(en=["a", "b"], it=["c"])
    assert lk.en == ["a", "b"]
    assert lk.it == ["c"]

def test_base_metadata_wrapping():
    # Test string wrapping for title/description
    bm = BaseMetadata(id="test", title="My Title", description="My Desc")
    assert isinstance(bm.title, LocalizedText)
    assert bm.title.en == "My Title"
    assert bm.description.en == "My Desc"
    
    # Test list wrapping for keywords
    bm2 = BaseMetadata(id="test2", keywords=["k1", "k2"])
    assert isinstance(bm2.keywords, LocalizedKeywords)
    assert bm2.keywords.en == ["k1", "k2"]
    
    # Test direct dict passing
    bm3 = BaseMetadata(id="test3", title={"fr": "Titre"})
    assert bm3.title.fr == "Titre"
    assert bm3.title.en is None

def test_license_wrapping():
    # Test string wrapping for license
    bm = BaseMetadata(id="test", license="MIT")
    assert bm.license.license_id == "MIT"
    
    # Test complex license
    lic_info = LicenseInfo(
        license_id="CC-BY-4.0",
        localized_content={
            "en": {"name": "Creative Commons", "url": "https://example.com"}
        }
    )
    bm2 = BaseMetadata(id="test2", license=lic_info)
    assert bm2.license.license_id == "CC-BY-4.0"
    assert bm2.license.localized_content.en.name == "Creative Commons"
