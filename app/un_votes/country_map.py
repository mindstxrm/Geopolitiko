"""Map UN member state names to ISO3. Used for normalizing un_votes and alignment.
Covers ALL countries via ALL_COUNTRIES + UN variants + substring fallback."""
import re
from typing import Optional, List, Tuple, Dict

_NAME_TO_ISO3 = None
_ISO3_BY_SUBSTRING: List[Tuple[str, str]] = []
_ISO3_TO_NAME: Dict[str, str] = {}


def _build_map():
    global _NAME_TO_ISO3, _ISO3_BY_SUBSTRING, _ISO3_TO_NAME
    if _NAME_TO_ISO3 is not None:
        return _NAME_TO_ISO3
    from app.country_data import ALL_COUNTRIES
    m = {}
    iso3_to_name = {}
    by_substring = []
    for iso3, name, *_ in ALL_COUNTRIES:
        nl = name.lower()
        m[nl] = iso3
        m[re.sub(r"\s*\([^)]+\)\s*", " ", nl).strip()] = iso3
        by_substring.append((nl, iso3))
        iso3_to_name[iso3] = name
        if " " in nl:
            m[nl.replace(" ", "")] = iso3
    extras = {
        "united states of america": "USA",
        "united states": "USA",
        "united kingdom of great britain and northern ireland": "GBR",
        "united kingdom": "GBR",
        "syrian arab republic": "SYR",
        "syria": "SYR",
        "venezuela (bolivarian republic of)": "VEN",
        "bolivia (plurinational state of)": "BOL",
        "russian federation": "RUS",
        "union of soviet socialist republics": "RUS",
        "china": "CHN",
        "republic of china": "TWN",
        "taiwan": "TWN",
        "republic of korea": "KOR",
        "south korea": "KOR",
        "democratic people's republic of korea": "PRK",
        "north korea": "PRK",
        "iran (islamic republic of)": "IRN",
        "tanzania, united republic of": "TZA",
        "viet nam": "VNM",
        "vietnam": "VNM",
        "lao people's democratic republic": "LAO",
        "laos": "LAO",
        "republic of moldova": "MDA",
        "the former yugoslav republic of macedonia": "MKD",
        "north macedonia": "MKD",
        "brunei darussalam": "BRN",
        "brunei": "BRN",
        "côte d'ivoire": "CIV",
        "ivory coast": "CIV",
        "cabo verde": "CPV",
        "cape verde": "CPV",
        "democratic republic of the congo": "COD",
        "republic of the congo": "COG",
        "congo": "COG",
        "congo-brazzaville": "COG",
        "congo-kinshasa": "COD",
        "timor-leste": "TLS",
        "palestine state": "PSE",
        "state of palestine": "PSE",
        "palestine": "PSE",
        "holy see": "VAT",
        "vatican": "VAT",
        "vatican city": "VAT",
        "micronesia (federated states of)": "FSM",
        "trinidad and tobago": "TTO",
        "antigua and barbuda": "ATG",
        "saint kitts and nevis": "KNA",
        "saint lucia": "LCA",
        "saint vincent and the grenadines": "VCT",
        "eswatini": "SWZ",
        "swaziland": "SWZ",
        "czechia (czech republic)": "CZE",
        "czech republic": "CZE",
        "czechoslovakia": "CZE",
        "czechia": "CZE",
        "byelorussian soviet socialist republic": "BLR",
        "ukrainian soviet socialist republic": "UKR",
        "yemen": "YEM",
        "democratic yemen": "YEM",
        "israel": "ISR",
        "west germany": "DEU",
        "east germany": "DEU",  # historical; use DEU for alignment
        "german democratic republic": "DEU",
        "federal republic of germany": "DEU",
        "türkiye": "TUR",
        "turkey": "TUR",
        "germany": "DEU",
        "france": "FRA",
        "canada": "CAN",
        "italy": "ITA",
        "japan": "JPN",
        "australia": "AUS",
        "spain": "ESP",
        "netherlands": "NLD",
        "belgium": "BEL",
        "portugal": "PRT",
        "greece": "GRC",
        "sweden": "SWE",
        "norway": "NOR",
        "denmark": "DNK",
        "finland": "FIN",
        "austria": "AUT",
        "switzerland": "CHE",
        "ireland": "IRL",
        "new zealand": "NZL",
        "brazil": "BRA",
        "mexico": "MEX",
        "argentina": "ARG",
        "chile": "CHL",
        "colombia": "COL",
        "peru": "PER",
        "india": "IND",
        "indonesia": "IDN",
        "philippines": "PHL",
        "malaysia": "MYS",
        "singapore": "SGP",
        "thailand": "THA",
        "myanmar": "MMR",
        "burma": "MMR",
        "egypt": "EGY",
        "south africa": "ZAF",
        "nigeria": "NGA",
        "kenya": "KEN",
        "ethiopia": "ETH",
        "morocco": "MAR",
        "algeria": "DZA",
        "tunisia": "TUN",
        "saudi arabia": "SAU",
        "united arab emirates": "ARE",
        "iraq": "IRQ",
        "iran": "IRN",
        "pakistan": "PAK",
        "bangladesh": "BGD",
        "sri lanka": "LKA",
        "ukraine": "UKR",
        "poland": "POL",
        "romania": "ROU",
        "hungary": "HUN",
        "slovakia": "SVK",
        "croatia": "HRV",
        "serbia": "SRB",
        "bosnia and herzegovina": "BIH",
        "slovenia": "SVN",
        "bulgaria": "BGR",
        "georgia": "GEO",
        "armenia": "ARM",
        "azerbaijan": "AZE",
        "kazakhstan": "KAZ",
        "uzbekistan": "UZB",
        "kingdom of the netherlands": "NLD",
        "kingdom of norway": "NOR",
        "kingdom of denmark": "DNK",
        "kingdom of belgium": "BEL",
        "kingdom of spain": "ESP",
        "kingdom of sweden": "SWE",
        "french republic": "FRA",
        "republic of india": "IND",
        "republic of indonesia": "IDN",
        "republic of the philippines": "PHL",
        "socialist republic of viet nam": "VNM",
        "not applicable": None,
        # UN-specific variants (including historical / alternate names)
        "federal republic of yugoslavia": "YUG",
        "yugoslavia": "YUG",
        "serbia and montenegro": "SRB",
        "moldova": "MDA",
        "germany (federal republic of)": "DEU",
        "libyan arab jamahiriya": "LBY",
        "democratic kampuchea": "KHM",
        "islamic republic of iran": "IRN",
        "bolivia (plurinational state of)": "BOL",
        "gambia (republic of the)": "GMB",
        "cÃ´te d'ivoire": "CIV",  # common CSV encoding
    }
    for k, v in extras.items():
        if v:
            m[k] = v
    _NAME_TO_ISO3 = m
    _ISO3_TO_NAME = iso3_to_name
    by_substring.sort(key=lambda x: -len(x[0]))
    _ISO3_BY_SUBSTRING = by_substring
    return m


def get_iso3_to_canonical_name() -> Dict[str, str]:
    """Return mapping ISO3 -> canonical country name for display."""
    _build_map()
    return _ISO3_TO_NAME


def get_countries_for_autocomplete() -> List[Tuple[str, str]]:
    """Return [(name, iso3), ...] sorted by name for autocomplete."""
    _build_map()
    out = [(name, iso3) for iso3, name in _ISO3_TO_NAME.items()]
    out.sort(key=lambda x: x[0].lower())
    return out


# ISO 3166-3 withdrawn codes and other defunct state codes (no longer existing).
# These are archived and not shown in the default alignment view.
DEFUNCT_COUNTRY_CODES = frozenset({
    "CSK",   # Czechoslovakia
    "DDR",   # East Germany
    "SUN",   # Soviet Union / USSR
    "YUG",   # Yugoslavia
    "YMD",   # South Yemen (Democratic Yemen)
    "YAR",   # North Yemen (Yemen Arab Republic)
    "RHO",   # Rhodesia
    "TMP",   # East Timor (old)
})


def get_defunct_names() -> Dict[str, str]:
    """Names for defunct countries (for archive display)."""
    return {
        "CSK": "Czechoslovakia",
        "DDR": "East Germany",
        "SUN": "Soviet Union",
        "YUG": "Yugoslavia",
        "YMD": "South Yemen",
        "YAR": "North Yemen",
        "RHO": "Rhodesia",
        "TMP": "East Timor (pre-independence)",
    }


def is_defunct_country(code: str) -> bool:
    """True if code represents a defunct/historical state."""
    return (code or "").strip().upper() in DEFUNCT_COUNTRY_CODES


def normalize_country_to_iso3(raw: Optional[str]) -> Optional[str]:
    """
    Normalize a country code or name to ISO3 for ALL countries.
    Returns ISO3 or None if unmappable.
    Pass-through: if already 3 uppercase letters, return as-is.
    Uses explicit map first, then substring fallback for 'X of Y' style names.
    """
    if not raw or not str(raw).strip():
        return None
    s = str(raw).strip()
    if len(s) == 3 and s.isupper() and s.isalpha():
        return s
    n = s.lower().strip()
    m = _build_map()
    if n in m:
        return m[n]
    base = re.sub(r"\s*\([^)]+\)\s*", " ", n).strip()
    if base in m:
        return m[base]
    base_norm = re.sub(r"\s+", " ", base)
    if base_norm in m:
        return m[base_norm]
    for canonical, iso3 in _ISO3_BY_SUBSTRING:
        if len(canonical) < 4:
            continue
        if canonical in n or n in canonical:
            return iso3
    return None
