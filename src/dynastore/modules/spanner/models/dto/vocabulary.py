# Esempio: app/models/metadata.py
from typing import Optional
from pydantic import BaseModel, Field
from typing import Optional, Dict
import dynastore.modules.spanner.constants as _c
from dynastore.modules.spanner.models.business.asset import Term, TermTranslation, Asset


class TermDTO(BaseModel):
    translations: Optional[Dict[str, str]] = Field(
        default=None, description="Translations mapping language code to translated string"
    )


class AssetTermsDTO(BaseModel):
    name: Optional[Dict[str, str]] = Field(
        default=None, description="Asset name translations")
    description: Optional[Dict[str, str]] = Field(
        default=None, description="Asset description translations")
    terms: Optional[Dict[str, Dict[str, str]]] = Field(
        default=None,
        description="Additional terms: term key → language code → translated string",
    )


def term_dto_to_term(term_dto: TermDTO) -> Term:
    translations: Dict[str, TermTranslation] = {}
    if term_dto.translations:
        for lang_code, translation_dto in term_dto.translations.items():
            translations[lang_code] = TermTranslation(
                language_code=getattr(_c.LanguageCodes, lang_code.upper()),
                value=translation_dto.value
            )
    term = Term(
        translations=translations
    )
    return term


def term_dto_to_term(term_dto: Dict[str, str]) -> Term:
    translations: Dict[str, TermTranslation] = {}
    for lang_code, value in (term_dto or {}).items():
        translations[lang_code] = TermTranslation(
            language_code=getattr(_c.LanguageCodes, lang_code.upper()),
            value=value
        )
    term = Term(translations=translations)
    return term


def asset_terms_dto_from_asset(asset: Asset) -> AssetTermsDTO:
    # Initialize dictionaries for name, description and other terms
    name_translations = {}
    description_translations = {}
    other_terms = {}

    for term_code, term in asset.get_terms().items():
        translations = {}
        for lang_code, translation_dto in term.get_translations().items():
            translations[lang_code.name] = translation_dto.value

        if term_code == _c.NAME_TERM_CODE:
            name_translations = translations
        elif term_code == _c.DESCRIPTION_TERM_CODE:
            description_translations = translations
        else:
            other_terms[term_code] = translations

    return AssetTermsDTO(name=name_translations, description=description_translations, terms=other_terms)


def set_terms_to_asset(asset: Asset, asset_terms_dto: AssetTermsDTO):
    if not asset_terms_dto:
        return

    if asset_terms_dto.name:
        name_term = term_dto_to_term(asset_terms_dto.name)
        name_term.set_code(_c.NAME_TERM_CODE)
        asset.add_term(term_code=_c.NAME_TERM_CODE, term=name_term)

    if asset_terms_dto.description:
        description_term = term_dto_to_term(asset_terms_dto.description)
        description_term.set_code(_c.DESCRIPTION_TERM_CODE)
        asset.add_term(term_code=_c.DESCRIPTION_TERM_CODE, term=description_term)

    if asset_terms_dto.terms:
        for term_code, translations_dict in asset_terms_dto.terms.items():
            term = term_dto_to_term(translations_dict)
            term.set_code(term_code)
            asset.add_term(term_code=term_code, term=term)