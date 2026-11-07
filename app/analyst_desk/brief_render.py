"""Human-friendly rendering of desk proposal markdown for Approvals review."""
from __future__ import annotations

import html
import re
from typing import Any

_HEADING_RE = re.compile(r"^(#{1,3})\s+(.+)$", re.MULTILINE)
_CITATION_RE = re.compile(r"\[S(\d+)\]", re.IGNORECASE)
_LABEL_RE = re.compile(r"\b(FACT|CLAIM|ASSESS|ASSESSMENT)\b")
_BULLET_RE = re.compile(r"^(\s*)([-*•]|\d+\.)\s+(.+)$")


def split_brief_sections(body: str) -> list[dict[str, str]]:
    """Split markdown into [{heading, level, body}] blocks for card layout."""
    text = (body or "").replace("\r\n", "\n").strip()
    if not text:
        return []

    matches = list(_HEADING_RE.finditer(text))
    if not matches:
        return [{"heading": "Brief", "level": 2, "body": text}]

    sections: list[dict[str, str]] = []
    # Lead-in before first heading
    if matches[0].start() > 0:
        lead = text[: matches[0].start()].strip()
        if lead:
            sections.append({"heading": "Overview", "level": 2, "body": lead})

    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        sections.append(
            {
                "heading": m.group(2).strip(),
                "level": len(m.group(1)),
                "body": text[start:end].strip(),
            }
        )
    return sections


def _inline_format(line: str) -> str:
    """Escape + bold/italic + citation pills + FACT/CLAIM/ASSESS badges."""
    s = html.escape(line)
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
    s = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<em>\1</em>", s)
    s = re.sub(r"`(.+?)`", r"<code>\1</code>", s)
    s = _LABEL_RE.sub(
        lambda m: f'<span class="brief-label brief-label-{m.group(1).lower()}">{m.group(1)}</span>',
        s,
    )
    s = _CITATION_RE.sub(r'<span class="brief-cite" title="Source \1">[S\1]</span>', s)
    # Autolink plain URLs
    s = re.sub(
        r"(https?://[^\s<]+)",
        r'<a href="\1" target="_blank" rel="noopener">\1</a>',
        s,
    )
    return s


def _block_to_html(block: str) -> str:
    """Convert a section body into paragraphs / lists."""
    lines = (block or "").split("\n")
    out: list[str] = []
    in_ul = False
    in_ol = False
    para: list[str] = []

    def flush_para() -> None:
        nonlocal para
        if para:
            out.append("<p>" + " ".join(_inline_format(p) for p in para) + "</p>")
            para = []

    def close_lists() -> None:
        nonlocal in_ul, in_ol
        if in_ul:
            out.append("</ul>")
            in_ul = False
        if in_ol:
            out.append("</ol>")
            in_ol = False

    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            flush_para()
            close_lists()
            continue
        m = _BULLET_RE.match(line)
        if m:
            flush_para()
            marker = m.group(2)
            content = m.group(3)
            is_ol = marker[-1] == "." and marker[:-1].isdigit()
            if is_ol:
                if in_ul:
                    out.append("</ul>")
                    in_ul = False
                if not in_ol:
                    out.append("<ol>")
                    in_ol = True
            else:
                if in_ol:
                    out.append("</ol>")
                    in_ol = False
                if not in_ul:
                    out.append("<ul>")
                    in_ul = True
            out.append(f"<li>{_inline_format(content)}</li>")
            continue
        # Horizontal rule
        if re.match(r"^(-{3,}|\*{3,}|_{3,})$", line.strip()):
            flush_para()
            close_lists()
            out.append("<hr>")
            continue
        close_lists()
        para.append(line.strip())

    flush_para()
    close_lists()
    return "\n".join(out)


def render_brief_html(body: str) -> str:
    """Full HTML document fragment for a proposal body."""
    sections = split_brief_sections(body)
    if not sections:
        return '<p class="muted">No content.</p>'
    parts: list[str] = []
    for sec in sections:
        h = html.escape(sec["heading"])
        level = min(max(int(sec["level"]), 1), 3)
        # Normalize Sources / Watch as secondary tone
        cls = "brief-section"
        key = sec["heading"].lower()
        if "source" in key:
            cls += " brief-section-sources"
        elif any(x in key for x in ("gap", "uncertain", "open question", "watch")):
            cls += " brief-section-watch"
        elif any(x in key for x in ("so what", "implication")):
            cls += " brief-section-so-what"
        parts.append(
            f'<section class="{cls}">'
            f"<h{level} class=\"brief-heading\">{h}</h{level}>"
            f'<div class="brief-body">{_block_to_html(sec["body"])}</div>'
            f"</section>"
        )
    return "\n".join(parts)


def brief_preview_plain(body: str, limit: int = 180) -> str:
    """One-line human preview for Approvals list (skip headings/markup noise)."""
    text = (body or "").replace("\r\n", "\n")
    lines = []
    for line in text.split("\n"):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        s = re.sub(r"\*+", "", s)
        s = re.sub(r"^[-*•]\s+", "", s)
        s = re.sub(r"^\d+\.\s+", "", s)
        if s:
            lines.append(s)
        if sum(len(x) for x in lines) >= limit:
            break
    joined = " ".join(lines).strip()
    if len(joined) > limit:
        return joined[: limit - 1].rstrip() + "…"
    return joined


def brief_toc(body: str) -> list[dict[str, Any]]:
    """Section titles for a quick jump list."""
    return [{"heading": s["heading"], "level": s["level"]} for s in split_brief_sections(body)]
