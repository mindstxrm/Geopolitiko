# Geopolitical Terminal — Indicator Roadmap

Policy-oriented expansion from political → political-economy. Structured by priority and data source.

---

## 🏦 1️⃣ Macroeconomic Stress Indicators (CRITICAL)

**Policy people constantly ask: *"Can this country absorb shock?"***

| Indicator | Source | Status |
|-----------|--------|--------|
| GDP growth (quarterly + annual) | World Bank, IMF, OECD | Table: `macroeconomic_stress` |
| Inflation | IMF, FRED, Trading Economics | Table: `macroeconomic_stress` |
| FX reserves (months of imports) | IMF, BIS | Exists: `country_risk_integration.reserve_months_imports` |
| Current account balance | World Bank, IMF | Table: `macroeconomic_stress` |
| External debt | World Bank, BIS | Table: `macroeconomic_stress` |
| Debt-to-GDP | World Bank, IMF | Table: `macroeconomic_stress` |
| Sovereign credit rating | S&P, Moody's, Fitch | Table: `macroeconomic_stress` |
| Bond spreads | FRED, BIS | Table: `macroeconomic_stress` |

**Why this matters:** Sanctions impact + fragility risk + voting drift often correlate with macro stress.

**Sources:** World Bank, IMF, BIS, FRED, Trading Economics, OECD

---

## ⚡ 2️⃣ Energy & Commodity Exposure

**Energy moves geopolitics.**

| Indicator | Status |
|-----------|--------|
| Oil production | Table: `energy_commodity_exposure` |
| Gas exports | Table: `energy_commodity_exposure` |
| LNG capacity | Table: `energy_commodity_exposure` |
| Major pipeline routes | Overlay with chokepoints |
| Energy import dependency % | Exists: `country_risk_integration.energy_import_exposure_pct` |
| Rare earth production | Table: `energy_commodity_exposure` |
| Grain export/import exposure | Table: `energy_commodity_exposure` |

**Overlay with:** Sanctions, trade chokepoints, conflict zones → **Supply shock mapping.** That's huge.

**Sources:** World Bank, IEA, national statistics

---

## 🛡 3️⃣ Military Capability Snapshot

**Not classified stuff. Just structured open-source metrics.**

| Indicator | Status |
|-----------|--------|
| Military spending | Exists: `defense_spending` (SIPRI) |
| Active troops | Table: `military_capability_snapshot` |
| Naval tonnage | Table: `military_capability_snapshot` |
| Major weapons imports/exports | Exists: `arms_trade` |
| Defense alliances | Treaties + `military_capability_snapshot` |

**Why this matters:** Your escalation model has structural muscle behind it.

**Sources:** SIPRI, open-source defense databases

---

## 📈 4️⃣ Public Opinion & Legitimacy

**Policy shifts follow political survival.**

| Indicator | Status |
|-----------|--------|
| Leader approval ratings | Exists: `approval_ratings` |
| Protest frequency | Exists: `protest_tracking` |
| Coup attempts history | Table: `elite_institutional` or conflict layer |
| Election cycle timing | Exists: `election_calendar` |
| Fragile state indicators | Exists: GPI fragility, institutional models |

**Combine with:** UN vote drift + sanctions pressure. **Now you're modeling domestic constraint.**

**Sources:** National polls, ACLED, news archives

---

## 🌐 5️⃣ Trade Flow Granularity

**Not just RTAs.**

| Indicator | Status |
|-----------|--------|
| Top 5 export destinations | Table: `trade_flow_partners` |
| Top 5 imports | Table: `trade_flow_partners` |
| Sectoral trade breakdown | Table: `trade_flow_partners` |
| Strategic dependence (e.g. semiconductor imports from X%) | Table: `trade_flow_partners` |

**Why this matters:** Now sanctions friction becomes real.

**Sources:** UN Comtrade, WTO, national customs

---

## 🧠 6️⃣ Elite & Institutional Structure

**Policy researchers care about: *"Who actually controls decisions?"***

| Indicator | Status |
|-----------|--------|
| Governance model | Table: `elite_institutional` |
| Key political actors | Table: `elite_institutional` |
| Major state-owned enterprises | Table: `elite_institutional` |
| Central bank independence indicator | Table: `elite_institutional` |
| Party structure | Table: `elite_institutional` |

**Sources:** Academic datasets, governance indices, news analysis

---

## 🌍 7️⃣ Conflict & Event Datasets

**Use:**
- ACLED (protest + conflict events)
- Uppsala UCDP
- ICEWS dataset (if accessible)

| Dataset | Status |
|---------|--------|
| ACLED (protest + conflict) | Table: `conflict_event_imports` (source=ACLED) |
| Uppsala UCDP | Table: `conflict_event_imports` (source=UCDP) |
| ICEWS | Table: `conflict_event_imports` (source=ICEWS) |

**Overlay this with:** Fragility + escalation probability. **Now you're serious.**

**Sources:** ACLED, Uppsala UCDP, ICEWS

---

## 📊 8️⃣ Capital Flows & FDI

**Geopolitics is also capital politics.**

| Indicator | Status |
|-----------|--------|
| FDI inflow/outflow | Table: `capital_flows` |
| Portfolio flows | Table: `capital_flows` |
| Exposure by sector | Table: `capital_flows` |
| Sovereign wealth funds | Table: `capital_flows` |

**Sources:** IMF, UNCTAD, central banks

---

## 🌎 9️⃣ Multilateral Participation Layer

**Then you map: Institutional embeddedness.**

| Membership | Status |
|------------|--------|
| WTO | Table: `multilateral_participation` |
| IMF programs | Table: `multilateral_participation` |
| World Bank programs | Table: `multilateral_participation` |
| Development banks (AIIB, AfDB, etc.) | Table: `multilateral_participation` |
| NATO partnership | Table: `multilateral_participation` |
| AIIB | Table: `multilateral_participation` |
| BRI participation | Table: `multilateral_participation` |

**Sources:** WTO, IMF, World Bank, NATO, AIIB, BRI documentation

---

## 🛰 🔟 Geospatial Infrastructure

**Eventually:** That makes your map layer elite.

| Layer | Status |
|-------|--------|
| Ports | Table: `geospatial_infrastructure` (future) |
| Undersea cables | Table: `geospatial_infrastructure` (future) |
| Pipelines | Chokepoints + `geospatial_infrastructure` |
| Chokepoints | Exists: `chokepoints`, `chokepoint_flows` |
| Logistics hubs | Table: `geospatial_infrastructure` (future) |

**Sources:** Maritime databases, telecom maps, energy atlases

---

## 💡 1️⃣1️⃣ Legislative / Policy Tracker

**Early signal layer.**

| Tracked | Status |
|---------|--------|
| Sanction law proposals | Table: `legislative_policy_tracker` |
| Defense spending bills | Table: `legislative_policy_tracker` |
| Trade bill amendments | Table: `legislative_policy_tracker` |
| Export control updates | Exists: `export_restrictions` |

**Sources:** Congress.gov, EU legislation, national parliaments

---

## 🚨 1️⃣2️⃣ Technology & Semiconductor Layer

**Extremely relevant for US–China.**

| Indicator | Status |
|-----------|--------|
| Chip exports | Table: `technology_semiconductor` |
| Critical tech companies | Table: `technology_semiconductor` |
| Export restriction lists | Exists: `export_restrictions` |
| Advanced manufacturing capacity | Table: `technology_semiconductor` |

**Strategic chokepoint 2.0.**

**Sources:** SIA, national trade statistics, tech industry reports

---

## 🧬 1️⃣3️⃣ Climate & Resource Vulnerability

**Climate stress is geopolitics.**

| Indicator | Status |
|-----------|--------|
| Water stress index | Table: `climate_resource_vulnerability` |
| Food insecurity index | Table: `climate_resource_vulnerability` |
| Natural disaster frequency | Table: `climate_resource_vulnerability` |
| Climate risk scores | Table: `climate_resource_vulnerability` |

**Policy analysts care about:** Migration risk + fragility correlation.

**Sources:** WRI, FAO, ND-GAIN, IPCC

---

## Implementation Status

- **Tables created:** `macroeconomic_stress`, `energy_commodity_exposure`, `military_capability_snapshot`, `trade_flow_partners`, `multilateral_participation`, `capital_flows`, `elite_institutional`, `climate_resource_vulnerability`, `legislative_policy_tracker`, `technology_semiconductor`, `conflict_event_imports`, `geospatial_infrastructure`
- **Country dashboard:** All 13 categories have cards; populated when data available
- **Indicator Layers hub:** `/indicators` — links to all categories and where each appears
- **Legislative / Policy Tracker:** `/indicators/legislative` — sanction bills, defense bills, trade amendments
- **Data feeds:** To be wired (World Bank API, IMF, Trading Economics scrape, ACLED, etc.)
