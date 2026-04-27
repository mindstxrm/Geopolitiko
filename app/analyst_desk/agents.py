"""Fixed regional coverage for desk analysts (ISO3 codes matching Geopolitiko filters)."""

# Desk agents are deterministic scopes (fixed country sets).
# We keep this layer separate from the main Geopolitiko synthesis/approval workflow.
AGENTS: dict[str, dict] = {
    # EAST ASIA
    "east_asia_i": {
        "label": "East Asia I",
        "description": "China, Taiwan, and Mongolia.",
        "countries": ["CHN", "TWN", "MNG"],
        "country_tiers": {"CHN": 1, "TWN": 2, "MNG": 3},
    },
    "east_asia_ii": {
        "label": "East Asia II",
        "description": "Japan, South Korea, and North Korea.",
        "countries": ["JPN", "KOR", "PRK"],
        "country_tiers": {"JPN": 1, "KOR": 2, "PRK": 2},
    },

    # SOUTHEAST ASIA
    "southeast_asia_i": {
        "label": "Southeast Asia I",
        "description": "Singapore, Malaysia, Indonesia, and Brunei.",
        "countries": ["SGP", "MYS", "IDN", "BRN"],
        "country_tiers": {"SGP": 2, "MYS": 2, "IDN": 2, "BRN": 3},
    },
    "southeast_asia_ii": {
        "label": "Southeast Asia II",
        "description": "Thailand, Vietnam, Philippines, and Myanmar.",
        "countries": ["THA", "VNM", "PHL", "MMR"],
        "country_tiers": {"THA": 2, "VNM": 2, "PHL": 2, "MMR": 3},
    },
    "southeast_asia_iii": {
        "label": "Southeast Asia III",
        "description": "Cambodia, Laos, and Timor-Leste.",
        "countries": ["KHM", "LAO", "TLS"],
        "country_tiers": {"KHM": 3, "LAO": 3, "TLS": 3},
    },

    # SOUTH ASIA
    "south_asia_i": {
        "label": "South Asia I",
        "description": "India, Pakistan, and Bangladesh.",
        "countries": ["IND", "PAK", "BGD"],
        "country_tiers": {"IND": 1, "PAK": 2, "BGD": 2},
    },
    "south_asia_ii": {
        "label": "South Asia II",
        "description": "Sri Lanka, Nepal, Bhutan, Maldives, and Afghanistan.",
        "countries": ["LKA", "NPL", "BTN", "MDV", "AFG"],
        "country_tiers": {"LKA": 2, "NPL": 3, "BTN": 3, "MDV": 3, "AFG": 2},
    },

    # CENTRAL ASIA & CAUCASUS
    "central_asia_i": {
        "label": "Central Asia I",
        "description": "Kazakhstan, Uzbekistan, Turkmenistan, Kyrgyzstan, and Tajikistan.",
        "countries": ["KAZ", "UZB", "TKM", "KGZ", "TJK"],
        "country_tiers": {"KAZ": 2, "UZB": 3, "TKM": 3, "KGZ": 3, "TJK": 3},
    },
    "caucasus_i": {
        "label": "Caucasus I",
        "description": "Armenia, Azerbaijan, and Georgia.",
        "countries": ["ARM", "AZE", "GEO"],
        "country_tiers": {"ARM": 2, "AZE": 2, "GEO": 2},
    },

    # MIDDLE EAST
    "gulf_i": {
        "label": "Gulf I",
        "description": "Saudi Arabia, UAE, Qatar, Kuwait, Bahrain, Oman.",
        "countries": ["SAU", "ARE", "QAT", "KWT", "BHR", "OMN"],
        "country_tiers": {"SAU": 1, "ARE": 2, "QAT": 2, "KWT": 2, "BHR": 3, "OMN": 3},
    },
    "levant_i": {
        "label": "Levant I",
        "description": "Israel, Jordan, Lebanon, Syria, Palestine.",
        "countries": ["ISR", "JOR", "LBN", "SYR", "PSE"],
        "country_tiers": {"ISR": 1, "JOR": 2, "LBN": 2, "SYR": 2, "PSE": 2},
    },
    "iran_iraq_i": {
        "label": "Iran-Iraq I",
        "description": "Iran, Iraq, and Yemen.",
        "countries": ["IRN", "IRQ", "YEM"],
        "country_tiers": {"IRN": 1, "IRQ": 2, "YEM": 2},
    },
    "turkey_east_med_i": {
        "label": "Turkey & East Med I",
        "description": "Turkey and Cyprus.",
        "countries": ["TUR", "CYP"],
        "country_tiers": {"TUR": 1, "CYP": 3},
    },

    # NORTH AFRICA
    "north_africa_i": {
        "label": "North Africa I",
        "description": "Egypt, Libya, Tunisia, Algeria, Morocco, Sudan.",
        "countries": ["EGY", "LBY", "TUN", "DZA", "MAR", "SDN"],
        "country_tiers": {"EGY": 1, "LBY": 2, "TUN": 3, "DZA": 2, "MAR": 2, "SDN": 2},
    },

    # WEST AFRICA
    "west_africa_i": {
        "label": "West Africa I",
        "description": "Nigeria, Ghana, Côte d’Ivoire, Senegal.",
        "countries": ["NGA", "GHA", "CIV", "SEN"],
        "country_tiers": {"NGA": 1, "GHA": 2, "CIV": 2, "SEN": 2},
    },
    "west_africa_ii": {
        "label": "West Africa II",
        "description": "Mali, Niger, Burkina Faso, Guinea, Sierra Leone, Liberia.",
        "countries": ["MLI", "NER", "BFA", "GIN", "SLE", "LBR"],
        "country_tiers": {"MLI": 2, "NER": 2, "BFA": 2, "GIN": 3, "SLE": 3, "LBR": 3},
    },

    # CENTRAL AFRICA
    "central_africa_i": {
        "label": "Central Africa I",
        "description": "DR Congo, Angola, Cameroon, Chad, Central African Republic.",
        "countries": ["COD", "AGO", "CMR", "TCD", "CAF"],
        "country_tiers": {"COD": 2, "AGO": 2, "CMR": 3, "TCD": 3, "CAF": 3},
    },

    # EAST AFRICA / HORN
    "east_africa_horn_i": {
        "label": "East Africa I",
        "description": "Ethiopia, Kenya, Somalia, Eritrea, Djibouti.",
        "countries": ["ETH", "KEN", "SOM", "ERI", "DJI"],
        "country_tiers": {"ETH": 2, "KEN": 2, "SOM": 2, "ERI": 3, "DJI": 3},
    },
    "east_africa_horn_ii": {
        "label": "East Africa II",
        "description": "Tanzania, Uganda, Rwanda, Burundi, South Sudan.",
        "countries": ["TZA", "UGA", "RWA", "BDI", "SSD"],
        "country_tiers": {"TZA": 2, "UGA": 2, "RWA": 3, "BDI": 3, "SSD": 2},
    },

    # SOUTHERN AFRICA
    "southern_africa_i": {
        "label": "Southern Africa I",
        "description": "South Africa, Namibia, Botswana, Zimbabwe, Zambia.",
        "countries": ["ZAF", "NAM", "BWA", "ZWE", "ZMB"],
        "country_tiers": {"ZAF": 1, "NAM": 3, "BWA": 3, "ZWE": 2, "ZMB": 2},
    },
    "southern_africa_ii": {
        "label": "Southern Africa II",
        "description": "Mozambique, Malawi, Lesotho, Eswatini, Madagascar.",
        "countries": ["MOZ", "MWI", "LSO", "SWZ", "MDG"],
        "country_tiers": {"MOZ": 2, "MWI": 3, "LSO": 3, "SWZ": 3, "MDG": 2},
    },

    # EASTERN EUROPE
    "eastern_europe_i": {
        "label": "Eastern Europe I",
        "description": "Russia, Ukraine, Belarus, Moldova.",
        "countries": ["RUS", "UKR", "BLR", "MDA"],
        "country_tiers": {"RUS": 1, "UKR": 1, "BLR": 2, "MDA": 2},
    },
    "eastern_europe_ii": {
        "label": "Eastern Europe II",
        "description": "Poland, Czech Republic, Slovakia, Hungary, Romania, Bulgaria.",
        "countries": ["POL", "CZE", "SVK", "HUN", "ROU", "BGR"],
        "country_tiers": {"POL": 2, "CZE": 2, "SVK": 2, "HUN": 2, "ROU": 2, "BGR": 2},
    },
    "balkans_i": {
        "label": "Balkans I",
        "description": "Serbia, Bosnia, Croatia, Slovenia, Montenegro, North Macedonia.",
        "countries": ["SRB", "BIH", "HRV", "SVN", "MNE", "MKD"],
        "country_tiers": {"SRB": 2, "BIH": 2, "HRV": 2, "SVN": 2, "MNE": 2, "MKD": 2},
    },
    "balkans_ii": {
        "label": "Balkans II",
        "description": "Albania, Kosovo, Greece.",
        "countries": ["ALB", "KOS", "GRC"],
        "country_tiers": {"ALB": 2, "KOS": 2, "GRC": 1},
    },

    # WESTERN EUROPE
    "western_europe_i": {
        "label": "Western Europe I",
        "description": "UK, France, Benelux (Belgium, Netherlands, Luxembourg).",
        "countries": ["GBR", "FRA", "BEL", "NLD", "LUX"],
        "country_tiers": {"GBR": 1, "FRA": 1, "BEL": 2, "NLD": 2, "LUX": 2},
    },
    "western_europe_ii": {
        "label": "Western Europe II",
        "description": "Germany, Switzerland, Austria.",
        "countries": ["DEU", "CHE", "AUT"],
        "country_tiers": {"DEU": 1, "CHE": 2, "AUT": 2},
    },
    "southern_europe_i": {
        "label": "Southern Europe I",
        "description": "Italy, Spain, Portugal, Malta.",
        "countries": ["ITA", "ESP", "PRT", "MLT"],
        "country_tiers": {"ITA": 1, "ESP": 1, "PRT": 2, "MLT": 3},
    },
    "nordics_i": {
        "label": "Nordics I",
        "description": "Sweden, Norway, Finland, Denmark, Iceland.",
        "countries": ["SWE", "NOR", "FIN", "DNK", "ISL"],
        "country_tiers": {"SWE": 2, "NOR": 2, "FIN": 2, "DNK": 2, "ISL": 3},
    },
    "baltics_i": {
        "label": "Baltics I",
        "description": "Estonia, Latvia, Lithuania.",
        "countries": ["EST", "LVA", "LTU"],
        "country_tiers": {"EST": 2, "LVA": 2, "LTU": 2},
    },

    # AMERICAS
    "north_america_i": {
        "label": "North America I",
        "description": "United States.",
        "countries": ["USA"],
        "country_tiers": {"USA": 1},
    },
    "north_america_ii": {
        "label": "North America II",
        "description": "Canada and Mexico.",
        "countries": ["CAN", "MEX"],
        "country_tiers": {"CAN": 1, "MEX": 2},
    },
    "central_america_i": {
        "label": "Central America I",
        "description": "Guatemala, Honduras, El Salvador, Nicaragua, Costa Rica, Panama.",
        "countries": ["GTM", "HND", "SLV", "NIC", "CRI", "PAN"],
        "country_tiers": {"GTM": 2, "HND": 2, "SLV": 2, "NIC": 2, "CRI": 2, "PAN": 2},
    },
    "caribbean_i": {
        "label": "Caribbean I",
        "description": "Cuba, Dominican Republic, Haiti, Jamaica, Bahamas, Trinidad and Tobago.",
        "countries": ["CUB", "DOM", "HTI", "JAM", "BHS", "TTO"],
        "country_tiers": {"CUB": 2, "DOM": 2, "HTI": 2, "JAM": 2, "BHS": 3, "TTO": 2},
    },
    "south_america_i": {
        "label": "South America I",
        "description": "Brazil, Argentina, Uruguay, Paraguay.",
        "countries": ["BRA", "ARG", "URY", "PRY"],
        "country_tiers": {"BRA": 1, "ARG": 2, "URY": 3, "PRY": 3},
    },
    "south_america_ii": {
        "label": "South America II",
        "description": "Chile, Peru, Bolivia, Ecuador.",
        "countries": ["CHL", "PER", "BOL", "ECU"],
        "country_tiers": {"CHL": 2, "PER": 2, "BOL": 2, "ECU": 2},
    },
    "south_america_iii": {
        "label": "South America III",
        "description": "Colombia, Venezuela, Guyana, Suriname.",
        "countries": ["COL", "VEN", "GUY", "SUR"],
        "country_tiers": {"COL": 2, "VEN": 2, "GUY": 3, "SUR": 3},
    },

    # OCEANIA
    "oceania_i": {
        "label": "Oceania I",
        "description": "Australia, New Zealand, Papua New Guinea, Fiji.",
        "countries": ["AUS", "NZL", "PNG", "FJI"],
        "country_tiers": {"AUS": 1, "NZL": 2, "PNG": 2, "FJI": 2},
    },
    "oceania_ii": {
        "label": "Oceania II",
        "description": "Solomon Islands, Vanuatu, Samoa, Tonga, Kiribati, Tuvalu, Nauru.",
        "countries": ["SLB", "VUT", "WSM", "TON", "KIR", "TUV", "NRU"],
        "country_tiers": {"SLB": 3, "VUT": 3, "WSM": 3, "TON": 3, "KIR": 3, "TUV": 3, "NRU": 3},
    },
}


def list_agent_ids() -> list[str]:
    return list(AGENTS.keys())


def get_agent(agent_id: str) -> dict | None:
    return AGENTS.get(agent_id)
