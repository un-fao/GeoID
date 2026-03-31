"""Sample datasets for OGC Dimensions use case demonstrations.

Each dataset is a list of nodes for ``StaticTreeGenerator`` or
``LeveledTreeGenerator``. Every node carries at minimum ``code``, ``label``,
and ``parent_code`` (null for root members). Nodes in leveled datasets also
carry a ``level`` integer.

Reference specification:
  https://github.com/ccancellieri/ogc-dimensions/tree/main/spec
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# 1. INDICATOR_NODES — recursive hierarchy (Domain → Group → Indicator)
# ---------------------------------------------------------------------------

INDICATOR_NODES: list[dict[str, Any]] = [
    # Level 0 — Domains (root)
    {"code": "FS",   "label": "Food Security",              "parent_code": None, "level": 0},
    {"code": "PROD", "label": "Production",                 "parent_code": None, "level": 0},
    {"code": "TRD",  "label": "Trade",                      "parent_code": None, "level": 0},
    {"code": "ENV",  "label": "Environment",                "parent_code": None, "level": 0},
    {"code": "LND",  "label": "Land Use",                   "parent_code": None, "level": 0},
    {"code": "EMP",  "label": "Employment",                 "parent_code": None, "level": 0},

    # Level 1 — Groups under Food Security
    {"code": "FS-AVL", "label": "Availability",  "parent_code": "FS", "level": 1, "unit": "various"},
    {"code": "FS-ACC", "label": "Access",        "parent_code": "FS", "level": 1, "unit": "various"},
    {"code": "FS-UTL", "label": "Utilization",   "parent_code": "FS", "level": 1, "unit": "various"},
    {"code": "FS-STB", "label": "Stability",     "parent_code": "FS", "level": 1, "unit": "various"},

    # Level 2 — Indicators under Availability
    {"code": "FS-AVL-DES",  "label": "Dietary Energy Supply",             "parent_code": "FS-AVL", "level": 2, "unit": "kcal/person/day"},
    {"code": "FS-AVL-ADER", "label": "Average Dietary Energy Requirement", "parent_code": "FS-AVL", "level": 2, "unit": "kcal/person/day"},
    {"code": "FS-AVL-PROT", "label": "Protein Supply",                     "parent_code": "FS-AVL", "level": 2, "unit": "g/person/day"},
    {"code": "FS-AVL-FAT",  "label": "Fat Supply",                         "parent_code": "FS-AVL", "level": 2, "unit": "g/person/day"},

    # Level 2 — Indicators under Access
    {"code": "FS-ACC-FIES", "label": "Food Insecurity Experience Scale",  "parent_code": "FS-ACC", "level": 2, "unit": "%"},
    {"code": "FS-ACC-POUN", "label": "Prevalence of Undernourishment",    "parent_code": "FS-ACC", "level": 2, "unit": "%"},
    {"code": "FS-ACC-GDP",  "label": "GDP Per Capita",                    "parent_code": "FS-ACC", "level": 2, "unit": "USD"},

    # Level 1 — Groups under Production
    {"code": "PROD-CROP",  "label": "Crop Production",      "parent_code": "PROD", "level": 1},
    {"code": "PROD-LIVE",  "label": "Livestock Production",  "parent_code": "PROD", "level": 1},

    # Level 2 — Indicators under Crop Production
    {"code": "PROD-CROP-AREA",  "label": "Area Harvested",  "parent_code": "PROD-CROP", "level": 2, "unit": "ha"},
    {"code": "PROD-CROP-YIELD", "label": "Yield",           "parent_code": "PROD-CROP", "level": 2, "unit": "kg/ha"},
    {"code": "PROD-CROP-PROD",  "label": "Production",      "parent_code": "PROD-CROP", "level": 2, "unit": "tonnes"},

    # Level 1 — Groups under Trade
    {"code": "TRD-IMP", "label": "Imports", "parent_code": "TRD", "level": 1},
    {"code": "TRD-EXP", "label": "Exports", "parent_code": "TRD", "level": 1},
]


# ---------------------------------------------------------------------------
# 2. ADMIN_NODES — leveled hierarchy (Continent → Country → Region)
# ---------------------------------------------------------------------------

ADMIN_NODES: list[dict[str, Any]] = [
    # Level 0 — Continents
    {"code": "AFR", "label": "Africa",   "parent_code": None, "level": 0},
    {"code": "AMR", "label": "Americas", "parent_code": None, "level": 0},
    {"code": "ASI", "label": "Asia",     "parent_code": None, "level": 0},
    {"code": "EUR", "label": "Europe",   "parent_code": None, "level": 0},
    {"code": "OCE", "label": "Oceania",  "parent_code": None, "level": 0},

    # Level 1 — Countries: Africa
    {"code": "DZA", "label": "Algeria",      "parent_code": "AFR", "level": 1},
    {"code": "AGO", "label": "Angola",       "parent_code": "AFR", "level": 1},
    {"code": "EGY", "label": "Egypt",        "parent_code": "AFR", "level": 1},
    {"code": "ETH", "label": "Ethiopia",     "parent_code": "AFR", "level": 1},
    {"code": "KEN", "label": "Kenya",        "parent_code": "AFR", "level": 1},
    {"code": "NGA", "label": "Nigeria",      "parent_code": "AFR", "level": 1},
    {"code": "ZAF", "label": "South Africa", "parent_code": "AFR", "level": 1},
    {"code": "TZA", "label": "Tanzania",     "parent_code": "AFR", "level": 1},
    {"code": "MOZ", "label": "Mozambique",   "parent_code": "AFR", "level": 1},

    # Level 1 — Countries: Americas
    {"code": "ARG", "label": "Argentina",     "parent_code": "AMR", "level": 1},
    {"code": "BOL", "label": "Bolivia",       "parent_code": "AMR", "level": 1},
    {"code": "BRA", "label": "Brazil",        "parent_code": "AMR", "level": 1},
    {"code": "CAN", "label": "Canada",        "parent_code": "AMR", "level": 1},
    {"code": "COL", "label": "Colombia",      "parent_code": "AMR", "level": 1},
    {"code": "MEX", "label": "Mexico",        "parent_code": "AMR", "level": 1},
    {"code": "PER", "label": "Peru",          "parent_code": "AMR", "level": 1},
    {"code": "USA", "label": "United States", "parent_code": "AMR", "level": 1},

    # Level 1 — Countries: Asia
    {"code": "AFG", "label": "Afghanistan",   "parent_code": "ASI", "level": 1},
    {"code": "BGD", "label": "Bangladesh",    "parent_code": "ASI", "level": 1},
    {"code": "CHN", "label": "China",         "parent_code": "ASI", "level": 1},
    {"code": "IND", "label": "India",         "parent_code": "ASI", "level": 1},
    {"code": "IDN", "label": "Indonesia",     "parent_code": "ASI", "level": 1},
    {"code": "JPN", "label": "Japan",         "parent_code": "ASI", "level": 1},
    {"code": "PAK", "label": "Pakistan",      "parent_code": "ASI", "level": 1},

    # Level 1 — Countries: Europe
    {"code": "DEU", "label": "Germany",        "parent_code": "EUR", "level": 1},
    {"code": "ESP", "label": "Spain",          "parent_code": "EUR", "level": 1},
    {"code": "FRA", "label": "France",         "parent_code": "EUR", "level": 1},
    {"code": "GBR", "label": "United Kingdom", "parent_code": "EUR", "level": 1},
    {"code": "ITA", "label": "Italy",          "parent_code": "EUR", "level": 1},
    {"code": "POL", "label": "Poland",         "parent_code": "EUR", "level": 1},
    {"code": "ROU", "label": "Romania",        "parent_code": "EUR", "level": 1},

    # Level 1 — Countries: Oceania
    {"code": "AUS", "label": "Australia",   "parent_code": "OCE", "level": 1},
    {"code": "NZL", "label": "New Zealand", "parent_code": "OCE", "level": 1},

    # Level 2 — Regions: Ethiopia
    {"code": "ETH-TIG", "label": "Tigray",  "parent_code": "ETH", "level": 2},
    {"code": "ETH-AFA", "label": "Afar",    "parent_code": "ETH", "level": 2},
    {"code": "ETH-AMH", "label": "Amhara",  "parent_code": "ETH", "level": 2},
    {"code": "ETH-ORM", "label": "Oromia",  "parent_code": "ETH", "level": 2},
    {"code": "ETH-SNN", "label": "SNNPR",   "parent_code": "ETH", "level": 2},
    {"code": "ETH-SID", "label": "Sidama",  "parent_code": "ETH", "level": 2},

    # Level 2 — Regions: Kenya
    {"code": "KEN-NAI", "label": "Nairobi",  "parent_code": "KEN", "level": 2},
    {"code": "KEN-MOM", "label": "Mombasa",  "parent_code": "KEN", "level": 2},
    {"code": "KEN-KIS", "label": "Kisumu",   "parent_code": "KEN", "level": 2},
    {"code": "KEN-NAK", "label": "Nakuru",   "parent_code": "KEN", "level": 2},

    # Level 2 — Regions: Italy
    {"code": "ITA-LOM", "label": "Lombardia",      "parent_code": "ITA", "level": 2},
    {"code": "ITA-LAZ", "label": "Lazio",           "parent_code": "ITA", "level": 2},
    {"code": "ITA-CAM", "label": "Campania",        "parent_code": "ITA", "level": 2},
    {"code": "ITA-SIC", "label": "Sicilia",         "parent_code": "ITA", "level": 2},
    {"code": "ITA-VEN", "label": "Veneto",          "parent_code": "ITA", "level": 2},
    {"code": "ITA-PIE", "label": "Piemonte",        "parent_code": "ITA", "level": 2},
    {"code": "ITA-EMR", "label": "Emilia-Romagna",  "parent_code": "ITA", "level": 2},
    {"code": "ITA-TOS", "label": "Toscana",         "parent_code": "ITA", "level": 2},

    # Level 2 — Regions: Brazil
    {"code": "BRA-SP",  "label": "São Paulo",       "parent_code": "BRA", "level": 2},
    {"code": "BRA-RJ",  "label": "Rio de Janeiro",  "parent_code": "BRA", "level": 2},
    {"code": "BRA-MG",  "label": "Minas Gerais",    "parent_code": "BRA", "level": 2},
    {"code": "BRA-BA",  "label": "Bahia",           "parent_code": "BRA", "level": 2},
    {"code": "BRA-AM",  "label": "Amazonas",        "parent_code": "BRA", "level": 2},

    # Level 2 — Regions: India
    {"code": "IND-MH",  "label": "Maharashtra",     "parent_code": "IND", "level": 2},
    {"code": "IND-UP",  "label": "Uttar Pradesh",   "parent_code": "IND", "level": 2},
    {"code": "IND-KA",  "label": "Karnataka",       "parent_code": "IND", "level": 2},
    {"code": "IND-TN",  "label": "Tamil Nadu",      "parent_code": "IND", "level": 2},
    {"code": "IND-RJ",  "label": "Rajasthan",       "parent_code": "IND", "level": 2},
]


# ---------------------------------------------------------------------------
# 3. SPECIES_NODES — recursive hierarchy (Order → Family → Species)
# ---------------------------------------------------------------------------

SPECIES_NODES: list[dict[str, Any]] = [
    # Level 0 — Orders
    {"code": "PINALES",    "label": "Pinales",      "parent_code": None, "level": 0},
    {"code": "FAGALES",    "label": "Fagales",       "parent_code": None, "level": 0},
    {"code": "SAPINDALES", "label": "Sapindales",    "parent_code": None, "level": 0},
    {"code": "MYRTALES",   "label": "Myrtales",      "parent_code": None, "level": 0},

    # Level 1 — Families under Pinales
    {"code": "PINACEAE",     "label": "Pinaceae",      "parent_code": "PINALES", "level": 1},
    {"code": "CUPRESSACEAE", "label": "Cupressaceae",   "parent_code": "PINALES", "level": 1},

    # Level 1 — Families under Fagales
    {"code": "FAGACEAE",     "label": "Fagaceae",       "parent_code": "FAGALES", "level": 1},
    {"code": "BETULACEAE",   "label": "Betulaceae",     "parent_code": "FAGALES", "level": 1},

    # Level 1 — Families under Sapindales
    {"code": "MELIACEAE",    "label": "Meliaceae",      "parent_code": "SAPINDALES", "level": 1},

    # Level 1 — Families under Myrtales
    {"code": "MYRTACEAE",    "label": "Myrtaceae",      "parent_code": "MYRTALES", "level": 1},

    # Level 2 — Species under Pinaceae
    {"code": "Pinus sylvestris",   "label": "Scots Pine",         "parent_code": "PINACEAE", "level": 2},
    {"code": "Pinus pinaster",     "label": "Maritime Pine",      "parent_code": "PINACEAE", "level": 2},
    {"code": "Pinus halepensis",   "label": "Aleppo Pine",        "parent_code": "PINACEAE", "level": 2},
    {"code": "Pinus nigra",        "label": "Black Pine",         "parent_code": "PINACEAE", "level": 2},
    {"code": "Picea abies",        "label": "Norway Spruce",      "parent_code": "PINACEAE", "level": 2},
    {"code": "Abies alba",         "label": "Silver Fir",         "parent_code": "PINACEAE", "level": 2},

    # Level 2 — Species under Cupressaceae
    {"code": "Juniperus communis",  "label": "Common Juniper",    "parent_code": "CUPRESSACEAE", "level": 2},
    {"code": "Cupressus sempervirens", "label": "Italian Cypress", "parent_code": "CUPRESSACEAE", "level": 2},

    # Level 2 — Species under Fagaceae
    {"code": "Quercus robur",       "label": "Pedunculate Oak",   "parent_code": "FAGACEAE", "level": 2},
    {"code": "Quercus ilex",        "label": "Holm Oak",          "parent_code": "FAGACEAE", "level": 2},
    {"code": "Quercus suber",       "label": "Cork Oak",          "parent_code": "FAGACEAE", "level": 2},
    {"code": "Fagus sylvatica",     "label": "European Beech",    "parent_code": "FAGACEAE", "level": 2},
    {"code": "Castanea sativa",     "label": "Sweet Chestnut",    "parent_code": "FAGACEAE", "level": 2},

    # Level 2 — Species under Betulaceae
    {"code": "Betula pendula",      "label": "Silver Birch",      "parent_code": "BETULACEAE", "level": 2},

    # Level 2 — Species under Meliaceae
    {"code": "Swietenia mahagoni",  "label": "Mahogany",          "parent_code": "MELIACEAE", "level": 2},
    {"code": "Cedrela odorata",     "label": "Spanish Cedar",     "parent_code": "MELIACEAE", "level": 2},

    # Level 2 — Species under Myrtaceae
    {"code": "Eucalyptus globulus",  "label": "Blue Gum",         "parent_code": "MYRTACEAE", "level": 2},
    {"code": "Eucalyptus grandis",   "label": "Rose Gum",         "parent_code": "MYRTACEAE", "level": 2},
    {"code": "Eucalyptus camaldulensis", "label": "River Red Gum", "parent_code": "MYRTACEAE", "level": 2},
]
