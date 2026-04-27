"""Cluster articles by similar topic/date for 'Coverage of this story' view."""
import re
from collections import defaultdict

from config import DATABASE_PATH
from app.models import (
    init_db,
    update_article_cluster,
    _connection,
)


def _normalize_title(title: str) -> str:
    """Lowercase, remove punctuation, collapse spaces."""
    if not title:
        return ""
    s = re.sub(r"[^\w\s]", " ", title.lower())
    return " ".join(s.split())


def _title_overlap(a: str, b: str, min_words: int = 2) -> bool:
    """True if the two titles share at least min_words. Use 2 so more stories cluster."""
    wa = set(_normalize_title(a).split())
    wb = set(_normalize_title(b).split())
    # Ignore very common words
    stop = {"the", "a", "an", "and", "or", "for", "in", "on", "at", "to", "of", "with", "as"}
    wa -= stop
    wb -= stop
    if len(wa) < min_words or len(wb) < min_words:
        return False
    common = wa & wb
    return len(common) >= min_words


def cluster_articles(lookback_days: int = 7, min_cluster_size: int = 2):
    """
    Assign cluster_id to articles. Articles with similar titles and close dates
    get the same cluster_id. Returns (num_clusters_kept, num_articles_processed).
    """
    init_db(DATABASE_PATH)
    # Use COALESCE so articles with null published_utc (use scraped_at) are included
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT id, title, published_utc, scraped_at
            FROM articles
            WHERE COALESCE(NULLIF(trim(published_utc), ''), scraped_at) >= date('now', ?)
            ORDER BY COALESCE(NULLIF(trim(published_utc), ''), scraped_at) DESC
            """,
            (f"-{lookback_days} days",),
        )
        rows = cur.fetchall()
    articles = [dict(row) for row in rows]

    if not articles:
        return (0, 0)

    # Clear existing cluster assignments for these articles
    with _connection() as conn:
        for a in articles:
            conn.execute("UPDATE articles SET cluster_id = NULL WHERE id = ?", (a["id"],))

    # Cluster across the whole window: if two articles have similar titles they share a cluster.
    # For each article, compare to all previous; if overlap with any, join that cluster.
    id_to_cid = {}
    next_cid = 1
    for i, a in enumerate(articles):
        assigned_cid = None
        for j in range(i):
            b = articles[j]
            if _title_overlap(a["title"], b["title"]):
                assigned_cid = id_to_cid.get(b["id"])
                if assigned_cid is not None:
                    break
        if assigned_cid is not None:
            update_article_cluster(a["id"], assigned_cid)
            id_to_cid[a["id"]] = assigned_cid
        else:
            update_article_cluster(a["id"], next_cid)
            id_to_cid[a["id"]] = next_cid
            next_cid += 1

    # Remove single-article clusters (set cluster_id to NULL)
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT cluster_id FROM articles
            WHERE cluster_id IS NOT NULL
            GROUP BY cluster_id
            HAVING COUNT(*) < ?
            """,
            (min_cluster_size,),
        )
        singles = [row[0] for row in cur.fetchall()]
        for cid in singles:
            conn.execute("UPDATE articles SET cluster_id = NULL WHERE cluster_id = ?", (cid,))

    # Count how many clusters remain (size >= min_cluster_size)
    with _connection() as conn:
        cur = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT cluster_id FROM articles
                WHERE cluster_id IS NOT NULL
                GROUP BY cluster_id
                HAVING COUNT(*) >= ?
            )
            """,
            (min_cluster_size,),
        )
        num_clusters = cur.fetchone()[0]

    return (num_clusters, len(articles))
