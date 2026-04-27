# Geopolitical News

A news aggregator that collects world and geopolitical news from major outlets via RSS and provides **analysis**: topics, key takeaways, digests, trending topics, and story clusters.

## Setup

```bash
cd /Users/jasminetan/PycharmProjects/GeopoliticalNews
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Fetching news (scraper)

Run the scraper to pull the latest articles into the local SQLite database:

```bash
python scrape.py
```

Sources: Reuters, BBC World, Al Jazeera English, Foreign Policy, The Diplomat, DW World. Articles are deduplicated by URL.

## Analysis jobs

Run jobs to add **topics/entities**, **key takeaways**, **digests**, and **story clusters**:

```bash
# Run all jobs (recommended after each scrape)
python run_jobs.py --all

# Or run individually
python run_jobs.py --topics      # extract topics and entities
python run_jobs.py --analysis    # key takeaways + "why it matters"
python run_jobs.py --digest      # daily digest
python run_jobs.py --weekly      # weekly digest
python run_jobs.py --cluster     # group similar stories
```

- **Topics** are keyword-based (e.g. US-China, Russia-Ukraine, Middle East). No API key needed.
- **Analysis** uses extractive summaries by default. Set `OPENAI_API_KEY` for LLM-generated takeaways and “why it matters.”
- **Digests** are template-based without OpenAI; with `OPENAI_API_KEY` they use GPT for short analyses.

## Running the site

```bash
python run.py
```

Open [http://127.0.0.1:5003](http://127.0.0.1:5003) (or the port in `run.py`).

### Analyst Desk (separate approval console)

Human-in-the-loop drafts for a **fixed multi-region analyst registry** live in a **separate** Flask app and **separate SQLite file** (`data/analyst_desk.db` by default)—not the main Geopolitiko UI or `news.db` approval flow.

```bash
python run_analyst_desk.py
```

Open [http://127.0.0.1:5005](http://127.0.0.1:5005). Set `ANALYST_DESK_ADMIN_PASSWORD` to require login.

- **No AI / no API key:** set `ANALYST_DESK_HEURISTIC_ONLY=1` in `.env` or your shell (or simply omit `OPENAI_API_KEY`). Agents then produce **structured roll-ups** from headlines, summaries, and takeaways in `data/news.db` only.
- **With LLM:** add `OPENAI_API_KEY` in project-root `.env` and do **not** set heuristic-only; restart the desk.

Drafts **read** articles from the same `DATABASE_PATH` / `data/news.db` as the main app.

Analyst Desk now includes Phase-1 and Phase-2 pipeline lanes in the UI:
- `Ingest recent documents` (mirrors recent articles into desk `raw_documents`, emits `document_ingested`)
- `Enrich documents` (country/topic tagging, emits `document_enriched`)
- `Build events + scores` (normalizes to `events`/`event_scores`, emits `event_updated` and `alert_required` when thresholds trigger)
- `Materialize alerts` (consumes `alert_required`, writes `alerts`)
- `Generate daily brief` (writes `briefings` from latest events/alerts)
- `Topic synthesis` (topic briefings from normalized events)
- `Regional synthesis` (regional risk snapshots)
- `Country risk snapshots` (per-country risk dimensions + watch levels)
- `Watchlist movers` (delta detection vs previous snapshots)
- `Run full pipeline (1→9)` button to execute all stages in sequence

Optional background cadence:
- Set `ANALYST_DESK_PIPELINE_INTERVAL_SECONDS` (e.g. `900`) to auto-run ingest→enrich→events→alerts→daily-brief on that interval when `run_analyst_desk.py` is running.
- Set `ANALYST_DESK_PHASE2_ENABLED=0` to skip Phase-2 lanes in autorun.

Optional alert delivery channels:
- `ANALYST_DESK_ALERT_WEBHOOK_URL` for webhook POST delivery
- `ANALYST_DESK_ALERT_EMAIL_TO` for SMTP email fallback (comma-separated recipients; requires existing `SMTP_*` vars)
- Use **Delivery settings → Send test alert** in Analyst Desk UI to verify channel wiring and status.

The registry currently includes full East Asia / Southeast Asia / South Asia / Middle East / Africa / Europe / Americas / Oceania desk coverage (see `app/analyst_desk/agents.py`).

Query agent endpoint (MVP):
- `GET /api/query?q=...` on Analyst Desk app returns matching normalized events.

**Automatic refresh:** When you run `python run.py`, a background scheduler runs every **60 seconds**: it fetches new articles from all RSS sources, then runs all analysis jobs (topics, impact, analysis, daily digest, clustering). The first refresh runs 15 seconds after startup. You can still run `python scrape.py` and `python run_jobs.py --all` manually anytime.

### Features

- **News** – Latest articles with filters by **source** and **topic**. Per-article **key takeaways** and **why it matters** when analysis has been run.
- **Search** – Full-text search over titles and summaries (header search or `/search?q=...`).
- **Analysis** – Digests (daily/weekly), **trending topics** (last 7 days), and **story clusters** (multi-source coverage of the same story).

## Project layout

- `config.py` – Database path, scraper user-agent
- `run.py` – Main Flask app entry point
- `run_analyst_desk.py` – Analyst Desk (separate port, separate approval DB)
- `scrape.py` – Fetch news from all RSS sources
- `run_jobs.py` – Run analysis jobs (topics, analysis, digest, clustering)
- `app/` – Application package
  - `models.py` – SQLite schema, FTS search, articles, digests
  - `routes.py` – Flask routes (index, search, analysis, digest, cluster)
  - `jobs/` – Topic extraction, analysis generation, digest, clustering
  - `scrapers/` – RSS scrapers and source list
  - `templates/` – Jinja2 HTML
  - `static/` – CSS

## Adding sources

Edit `app/scrapers/sources.py` and add entries to `SOURCES` with `name`, `url` (RSS/Atom feed URL), and `homepage`.

## Data

The SQLite database is at `data/news.db` (override with `DATABASE_PATH`). If you get “database disk image is malformed,” remove `data/news.db` and re-run `python scrape.py` and `python run_jobs.py --all`.
