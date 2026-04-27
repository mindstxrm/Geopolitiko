#!/usr/bin/env python3
"""Run analysis jobs: topics, analysis, digest, clustering."""
import argparse
import sys
from config import DATABASE_PATH
from app.models import init_db, backfill_integration_countries


def main():
    parser = argparse.ArgumentParser(description="Run geopolitical news analysis jobs")
    parser.add_argument("--topics", action="store_true", help="Extract topics and entities for articles")
    parser.add_argument("--analysis", action="store_true", help="Generate key takeaways and why-it-matters for articles")
    parser.add_argument("--digest", action="store_true", help="Generate daily digest")
    parser.add_argument("--weekly", action="store_true", help="Generate weekly digest")
    parser.add_argument("--cluster", action="store_true", help="Cluster articles by story")
    parser.add_argument("--risk", action="store_true", help="Update Risk Engine (country risk, forward probability index)")
    parser.add_argument("--impact", action="store_true", help="Score article impact and domains")
    parser.add_argument("--integration", action="store_true", help="Fill Countries & Regions: ensure all countries have integration data (country pages)")
    parser.add_argument("--institutional", action="store_true", help="Run institutional models (GEPI, CDEI, SFI, GEG, alignment, fragility, escalation)")
    parser.add_argument("--un-votes-sync", nargs="?", const="", default=None, metavar="LIMIT", help="Sync UN votes from local UNGA-DM CSV or UNIGE (optional LIMIT)")
    parser.add_argument("--un-votes-normalize", action="store_true", help="Normalize un_votes to ISO3, recompute alignment (fixes duplicate country pairs)")
    parser.add_argument("--un-votes-gpi", action="store_true", help="Run all UN vote analytics: migrate, alignment, issues, blocs, shocks, volatility, polarization")
    parser.add_argument("--wto-rtas", nargs="?", const="", metavar="PATH", help="Import WTO RTAs from AllRTAs.xlsx (default: data/AllRTAs.xlsx or PATH)")
    parser.add_argument("--wto-scrape", type=int, nargs="?", const=50, metavar="N", help="Scrape WTO RTA detail pages for documents/annexes (optional limit N)")
    parser.add_argument("--all", action="store_true", help="Run all jobs (topics, analysis, impact, digest, cluster, risk, integration)")
    parser.add_argument("--limit", type=int, default=500, help="Limit articles for topics/analysis (default 500)")
    args = parser.parse_args()

    init_db(DATABASE_PATH)

    if args.all or args.topics:
        from app.jobs.topics import extract_topics_for_all
        n = extract_topics_for_all(limit=args.limit)
        print(f"Topics: processed {n} articles.")

    if args.all or args.analysis:
        from app.jobs.analysis import generate_analysis_for_all
        n = generate_analysis_for_all(limit=args.limit)
        print(f"Analysis: processed {n} articles.")

    if args.all or args.cluster:
        from app.jobs.clustering import cluster_articles
        num_clusters, num_articles = cluster_articles(lookback_days=7)
        print(f"Clustering: processed {num_articles} articles (last 7 days), {num_clusters} story clusters with 2+ articles.")

    if args.all or args.impact:
        from app.jobs.impact import score_all
        n = score_all(limit=args.limit)
        print(f"Impact: scored {n} articles.")

    if args.all or args.risk:
        from app.risk_engine import compute_country_risk_from_articles, compute_forward_risk_index
        n1 = compute_country_risk_from_articles(days=7)
        n2 = compute_forward_risk_index(days=7)
        print(f"Risk Engine: {n1} country/region scores, {n2} forward risk index rows.")

    if args.all or args.integration:
        n = backfill_integration_countries()
        print(f"Countries & Regions: {n} countries filled (integration data for each country page).")

    if args.all or args.institutional:
        from app.jobs.institutional import run_institutional_models
        out = run_institutional_models()
        print(f"Institutional models: GEPI={out.get('gepi', {})}, CDEI={out.get('cdei_count', 0)}, SFI={out.get('sfi_count', 0)}, GEG events={out.get('geg_events', 0)}, links={out.get('geg_links', 0)}, TBCS={out.get('tbcs_count', 0)}, fragility={out.get('fragility_count', 0)}.")

    if args.un_votes_sync is not None:
        from app.jobs.un_votes_sync import sync_un_votes_from_unige
        limit = int(args.un_votes_sync) if args.un_votes_sync else None
        count, err = sync_un_votes_from_unige(limit=limit)
        if err:
            print(f"UN votes sync error: {err}")
            return 1
        print(f"UN votes sync: imported {count} votes from UNIGE UNGA-DM.")
        from app.models import compute_voting_alignment
        pairs = compute_voting_alignment(min_votes=5)
        print(f"Voting alignment: computed {pairs} country pairs.")

    if args.wto_rtas is not None:
        from pathlib import Path
        from scripts.import_wto_rtas import run_import
        xlsx = args.wto_rtas if args.wto_rtas else str(Path(DATABASE_PATH).parent / "AllRTAs.xlsx")
        return run_import(xlsx)

    if args.wto_scrape is not None:
        from scripts.wto_rta_detail_scraper import run_scrape
        result = run_scrape(limit=args.wto_scrape)
        if result.get("error"):
            print(f"Error: {result['error']}")
            return 1
        print(f"WTO scrape: updated {result['updated']} treaties (of {result['total']} RTAs).")
        return 0

    if args.all or args.digest:
        from app.jobs.digest import generate_daily_digest
        digest_id = generate_daily_digest()
        print(f"Daily digest created (id={digest_id}).")

    if args.weekly:
        from app.jobs.digest import generate_weekly_digest
        digest_id = generate_weekly_digest()
        print(f"Weekly digest created (id={digest_id}).")

    if not any([args.topics, args.analysis, args.digest, args.weekly, args.cluster, args.risk, args.impact, args.integration, args.institutional, args.all, args.un_votes_gpi, args.un_votes_normalize, args.un_votes_sync is not None, args.wto_rtas is not None, args.wto_scrape is not None]):
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
