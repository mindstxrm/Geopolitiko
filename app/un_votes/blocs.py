"""Bloc membership for UN vote cohesion analysis. ISO3 codes."""

BLOCS = {
    # Asia / Indo-Pacific
    "ASEAN": ["BRN", "KHM", "IDN", "LAO", "MYS", "MMR", "PHL", "SGP", "THA", "VNM", "TLS"],
    "BRICS": ["BRA", "RUS", "IND", "CHN", "ZAF", "EGY", "ETH", "IRN", "ARE"],
    "SCO": ["CHN", "RUS", "KAZ", "KGZ", "TJK", "UZB", "IND", "PAK", "IRN", "BLR"],
    "CPTPP": ["AUS", "BRN", "CAN", "CHL", "JPN", "MYS", "MEX", "NZL", "PER", "SGP", "VNM", "GBR"],
    "RCEP": [
        "BRN", "KHM", "IDN", "LAO", "MYS", "MMR", "PHL", "SGP", "THA", "VNM", "TLS",
        "AUS", "CHN", "JPN", "KOR", "NZL",
    ],
    "Quad": ["USA", "JPN", "IND", "AUS"],
    "AUKUS": ["AUS", "GBR", "USA"],
    # Middle East & Islamic
    "OIC": [
        "AFG", "ALB", "DZA", "AZE", "BHR", "BGD", "BEN", "BRN", "BFA", "CMR", "TCD",
        "COM", "CIV", "DJI", "EGY", "GAB", "GMB", "GIN", "GNB", "GUY", "IDN", "IRN",
        "IRQ", "JOR", "KAZ", "KWT", "KGZ", "LBN", "LBY", "MYS", "MDV", "MLI", "MRT",
        "MAR", "MOZ", "NER", "NGA", "OMN", "PAK", "PSE", "QAT", "SAU", "SEN", "SLE",
        "SOM", "SDN", "SUR", "SYR", "TJK", "TGO", "TUN", "TUR", "TKM", "UGA", "ARE",
        "UZB", "YEM",
    ],
    "Arab League": [
        "DZA", "BHR", "COM", "DJI", "EGY", "IRQ", "JOR", "KWT", "LBN", "LBY",
        "MRT", "MAR", "OMN", "PSE", "QAT", "SAU", "SOM", "SDN", "SYR", "TUN", "ARE", "YEM",
    ],
    # Africa
    "AU": [
        "DZA", "AGO", "BEN", "BWA", "BFA", "BDI", "CPV", "CMR", "CAF", "TCD",
        "COM", "COG", "COD", "CIV", "DJI", "EGY", "GNQ", "ERI", "SWZ", "ETH",
        "GAB", "GMB", "GHA", "GIN", "GNB", "KEN", "LSO", "LBR", "LBY", "MDG",
        "MWI", "MLI", "MRT", "MUS", "MAR", "MOZ", "NAM", "NER", "NGA", "RWA",
        "STP", "SEN", "SYC", "SLE", "SOM", "ZAF", "SSD", "SDN", "TZA", "TGO",
        "TUN", "UGA", "ZMB", "ZWE",
    ],
    "ECOWAS": ["BEN", "BFA", "CPV", "GMB", "GHA", "GIN", "GNB", "CIV", "LBR", "MLI", "NER", "NGA", "SEN", "SLE", "TGO"],
    "SADC": ["AGO", "BWA", "COM", "COD", "SWZ", "LSO", "MDG", "MWI", "MUS", "MOZ", "NAM", "SYC", "ZAF", "TZA", "ZMB", "ZWE"],
    "EAC": ["BDI", "COD", "KEN", "RWA", "SSD", "TZA", "UGA"],
    # Americas
    "OAS": [
        "ARG", "BOL", "BRA", "CHL", "COL", "CRI", "CUB", "DOM", "ECU", "SLV",
        "GTM", "HTI", "HND", "MEX", "NIC", "PAN", "PRY", "PER", "USA", "URY", "VEN",
    ],
    "Mercosur": ["BRA", "ARG", "URY", "PRY", "BOL"],
    "Pacific Alliance": ["CHL", "COL", "MEX", "PER"],
    "CELAC": [
        "ARG", "BOL", "BRA", "CHL", "COL", "CRI", "CUB", "ECU", "SLV", "GTM",
        "GUY", "HTI", "HND", "JAM", "MEX", "NIC", "PAN", "PRY", "PER", "DOM",
        "SUR", "TTO", "URY", "VEN",
        "ATG", "BHS", "BRB", "DMA", "GRD", "LCA", "VCT",
    ],
    # Europe / Eurasia
    "EU": [
        "AUT", "BEL", "BGR", "HRV", "CYP", "CZE", "DNK", "EST", "FIN", "FRA",
        "DEU", "GRC", "HUN", "IRL", "ITA", "LVA", "LTU", "LUX", "MLT", "NLD",
        "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE",
    ],
    "EFTA": ["NOR", "CHE", "ISL", "LIE"],
    "Council of Europe": [
        "ALB", "AND", "ARM", "AUT", "AZE", "BGR", "BIH", "CYP", "CZE", "DEU",
        "DNK", "ESP", "EST", "FIN", "FRA", "GBR", "GEO", "GRC", "HRV", "HUN",
        "ISL", "IRL", "ITA", "LIE", "LTU", "LUX", "LVA", "MLT", "MDA", "MNE",
        "NLD", "MKD", "NOR", "POL", "PRT", "ROU", "SMR", "SRB", "SVK", "SVN",
        "SWE", "CHE", "TUR", "UKR",
    ],
    "NATO": [
        "ALB", "BEL", "BGR", "CAN", "HRV", "CZE", "DNK", "EST", "FIN", "FRA",
        "DEU", "GRC", "HUN", "ISL", "ITA", "LVA", "LTU", "LUX", "MNE", "NLD",
        "MKD", "NOR", "POL", "PRT", "ROU", "SVK", "SVN", "ESP", "SWE", "TUR",
        "GBR", "USA",
    ],
    "G7": ["CAN", "FRA", "DEU", "ITA", "JPN", "GBR", "USA"],
    "GCC": ["BHR", "KWT", "OMN", "QAT", "SAU", "ARE"],
}

# P5 for UNSC veto analysis
P5 = ["CHN", "FRA", "RUS", "GBR", "USA"]

# Key poles for alignment/shock reference
REFERENCE_POLES = ["USA", "CHN", "EU"]  # EU as bloc centroid

# Display names for blocs and organizations (for Countries & Regions page)
BLOC_DISPLAY_NAMES = {
    "ASEAN": "Association of Southeast Asian Nations",
    "BRICS": "BRICS (incl. expanded members)",
    "SCO": "Shanghai Cooperation Organisation",
    "CPTPP": "CPTPP (Trans-Pacific trade)",
    "RCEP": "RCEP (Regional trade)",
    "Quad": "Quad (Indo-Pacific)",
    "AUKUS": "AUKUS",
    "OIC": "Organisation of Islamic Cooperation",
    "Arab League": "Arab League",
    "AU": "African Union",
    "ECOWAS": "ECOWAS",
    "SADC": "SADC",
    "EAC": "East African Community",
    "OAS": "Organization of American States",
    "Mercosur": "Mercosur",
    "Pacific Alliance": "Pacific Alliance",
    "CELAC": "CELAC",
    "EU": "European Union",
    "EFTA": "EFTA",
    "Council of Europe": "Council of Europe",
    "NATO": "NATO",
    "G7": "Group of Seven",
    "GCC": "Gulf Cooperation Council",
    "P5": "UN Security Council (P5)",
}

# Group blocs by category for Countries & Regions page
BLOC_CATEGORIES = {
    "Asia / Indo-Pacific": ["ASEAN", "BRICS", "SCO", "CPTPP", "RCEP", "Quad", "AUKUS"],
    "Middle East & Islamic": ["OIC", "Arab League", "GCC"],
    "Africa": ["AU", "ECOWAS", "SADC", "EAC"],
    "Americas": ["OAS", "Mercosur", "Pacific Alliance", "CELAC"],
    "Europe / Eurasia": ["EU", "EFTA", "Council of Europe", "NATO", "G7", "P5"],
}
