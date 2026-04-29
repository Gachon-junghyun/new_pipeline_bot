#!/usr/bin/env python3
"""
LLM-friendly news search/export tool.

Searches the local `news_alert.db` first, optionally supplements with Naver
News search, and writes a timestamped TXT report under `llm_outputs/`.
"""

from __future__ import annotations

import argparse
import html
import os
import re
import sqlite3
import sys
import textwrap
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "news_alert.db"
SCENARIO_DB_PATH = BASE_DIR / "scenario.db"
OUTPUT_DIR = BASE_DIR / "llm_outputs"
SCENARIO_DIR = BASE_DIR / "searched_scenario"
LOCAL_TZ = timezone(timedelta(hours=9))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}


@dataclass
class NewsItem:
    title: str
    url: str
    source: str
    date: str
    summary: str = ""


@dataclass
class ScenarioItem:
    title: str
    scenario: str
    file: str
    score: str
    source: str
    published_at: str
    url: str
    matched_terms: str
    significance: str
    summary: str
    category: str = ""
    keywords: str = ""
    description: str = ""
    node_id: str = ""
    scenario_id: str = ""


def now() -> datetime:
    return datetime.now(LOCAL_TZ)


def clean_text(value: str) -> str:
    value = html.unescape(value or "")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def terms_from_query(query: str) -> list[str]:
    return [part.strip() for part in re.split(r"\s+", query) if part.strip()]


def format_date_range(days: int | None, since: str | None) -> tuple[str | None, str]:
    if since:
        return since, f"{since} 이후"
    if days is None:
        return None, "전체 기간"
    start = (now() - timedelta(days=days)).isoformat(timespec="seconds")
    return start, f"최근 {days}일 ({start[:10]} 이후)"


def search_local_db(query: str, limit: int, since_iso: str | None) -> list[NewsItem]:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH}")

    terms = terms_from_query(query)
    where = []
    params: list[object] = []

    for term in terms:
        where.append("title LIKE ?")
        params.append(f"%{term}%")
    if since_iso:
        where.append("fetched_at >= ?")
        params.append(since_iso)

    sql = """
        SELECT title, url, source, fetched_at
        FROM seen_news
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY fetched_at DESC LIMIT ?"
    params.append(limit)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    return [
        NewsItem(
            title=row["title"] or "",
            url=row["url"] or "",
            source=row["source"] or "",
            date=row["fetched_at"] or "",
        )
        for row in rows
    ]


def parse_published_at(value: str) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def scenario_date_in_range(published_at: str, since_dt: datetime | None) -> bool:
    if since_dt is None:
        return True
    dt = parse_published_at(published_at)
    if dt is None:
        return True
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    if since_dt.tzinfo is None:
        since_dt = since_dt.replace(tzinfo=LOCAL_TZ)
    return dt >= since_dt.astimezone(dt.tzinfo)


def continuation_value(block: str, key: str) -> str:
    lines = block.splitlines()
    out: list[str] = []
    capture = False
    prefix = f"- {key}:"
    for line in lines:
        if line.startswith(prefix):
            capture = True
            out.append(line[len(prefix):].strip())
            continue
        if capture:
            if line.startswith("- ") or line.startswith("### "):
                break
            if line.startswith("  "):
                out.append(line.strip())
                continue
            if not line.strip():
                break
    return "\n".join(part for part in out if part)


def one_line_value(block: str, key: str) -> str:
    match = re.search(rf"^- {re.escape(key)}: (.*)$", block, flags=re.MULTILINE)
    return match.group(1).strip() if match else ""


def search_scenario_txt(query: str, limit: int, since_iso: str | None) -> list[ScenarioItem]:
    if not SCENARIO_DIR.exists():
        return []
    terms = [term.lower() for term in terms_from_query(query)]
    since_dt = datetime.fromisoformat(since_iso) if since_iso else None
    results: list[ScenarioItem] = []

    for path in sorted(SCENARIO_DIR.glob("*/*.txt")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        scenario = text.splitlines()[0].strip() if text else path.stem
        blocks = re.split(r"(?m)^### ", text)
        for block in blocks[1:]:
            title = block.splitlines()[0].strip()
            haystack = (title + "\n" + block).lower()
            if terms and not all(term in haystack for term in terms):
                continue
            published_at = one_line_value(block, "published_at")
            if not scenario_date_in_range(published_at, since_dt):
                continue
            item = ScenarioItem(
                title=title,
                scenario=scenario,
                file=str(path.relative_to(BASE_DIR)),
                score=one_line_value(block, "score"),
                source=one_line_value(block, "source"),
                published_at=published_at,
                url=one_line_value(block, "url"),
                matched_terms=continuation_value(block, "matched_terms"),
                significance=one_line_value(block, "significance"),
                summary=one_line_value(block, "summary"),
            )
            results.append(item)

    def sort_key(item: ScenarioItem) -> tuple[datetime, int]:
        dt = parse_published_at(item.published_at) or datetime.min.replace(tzinfo=timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        score = int(item.score) if item.score.isdigit() else 0
        return dt, score

    return sorted(results, key=sort_key, reverse=True)[:limit]


def search_scenario_db(query: str, limit: int, since_iso: str | None) -> list[ScenarioItem]:
    if not SCENARIO_DB_PATH.exists():
        return []

    terms = terms_from_query(query)
    where = []
    params: list[object] = []

    combined = """
        COALESCE(s.name, '') || ' ' ||
        COALESCE(s.description, '') || ' ' ||
        COALESCE(s.keywords, '') || ' ' ||
        COALESCE(n.title, '') || ' ' ||
        COALESCE(n.summary, '') || ' ' ||
        COALESCE(n.significance, '')
    """
    for term in terms:
        where.append(f"{combined} LIKE ?")
        params.append(f"%{term}%")
    sql = f"""
        SELECT
            s.id AS scenario_id,
            s.name AS scenario_name,
            s.description AS scenario_description,
            s.category AS category,
            s.keywords AS keywords,
            n.id AS node_id,
            n.title AS title,
            n.summary AS summary,
            n.significance AS significance,
            n.url AS url,
            n.source AS source,
            n.published_at AS published_at,
            n.node_order AS node_order,
            n.created_at AS created_at
        FROM nodes n
        JOIN scenarios s ON s.id = n.scenario_id
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += """
        ORDER BY
            COALESCE(n.published_at, n.created_at) DESC,
            n.id DESC
        LIMIT ?
    """
    params.append(max(limit * 10, limit))

    conn = sqlite3.connect(SCENARIO_DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    items: list[ScenarioItem] = []
    for row in rows:
        published_at = row["published_at"] or row["created_at"] or ""
        if not scenario_date_in_range(published_at, datetime.fromisoformat(since_iso) if since_iso else None):
            continue
        items.append(
            ScenarioItem(
                title=f"({row['node_order']}) {row['title']}",
                scenario=f"# [{row['scenario_id']}] {row['scenario_name']}",
                file=str(SCENARIO_DB_PATH.relative_to(BASE_DIR)),
                score="db",
                source=row["source"] or "",
                published_at=published_at,
                url=row["url"] or "",
                matched_terms="",
                significance=row["significance"] or "",
                summary=row["summary"] or "",
                category=row["category"] or "",
                keywords=row["keywords"] or "",
                description=row["scenario_description"] or "",
                node_id=str(row["node_id"]),
                scenario_id=str(row["scenario_id"]),
            )
        )
        if len(items) >= limit:
            break
    return items


def normalize_naver_link(link: str) -> str:
    if "news.naver.com/main/read.naver" in link:
        parsed = urlparse(link)
        qs = parse_qs(parsed.query)
        oid = (qs.get("oid") or [""])[0]
        aid = (qs.get("aid") or [""])[0]
        if oid and aid:
            return f"https://n.news.naver.com/mnews/article/{oid}/{aid}"
    return link


def search_naver_api(query: str, limit: int) -> list[NewsItem]:
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")
    if not client_id or not client_secret:
        return []

    display = max(1, min(limit, 100))
    resp = requests.get(
        "https://openapi.naver.com/v1/search/news.json",
        params={"query": query, "display": display, "sort": "date"},
        headers={
            "X-Naver-Client-Id": client_id,
            "X-Naver-Client-Secret": client_secret,
            **HEADERS,
        },
        timeout=12,
    )
    resp.raise_for_status()
    payload = resp.json()
    items = []
    for item in payload.get("items", []):
        items.append(
            NewsItem(
                title=clean_text(item.get("title", "")),
                url=normalize_naver_link(item.get("link") or item.get("originallink") or ""),
                source="naver_api",
                date=clean_text(item.get("pubDate", "")),
                summary=clean_text(item.get("description", "")),
            )
        )
    return items


def search_naver_page(query: str, limit: int) -> list[NewsItem]:
    url = (
        "https://search.naver.com/search.naver"
        f"?where=news&sm=tab_jum&query={quote_plus(query)}&sort=1"
    )
    resp = requests.get(url, headers=HEADERS, timeout=12)
    resp.raise_for_status()
    soup = make_soup(resp.text)

    items: list[NewsItem] = []
    for node in soup.select("a.news_tit"):
        title = clean_text(node.get("title") or node.get_text(" "))
        link = normalize_naver_link(node.get("href", ""))
        parent = node.find_parent("div", class_=re.compile("news_wrap|bx"))
        info_text = clean_text(parent.get_text(" ")) if parent else ""
        date_match = re.search(r"(\d{4}\.\d{2}\.\d{2}\.|\d+분 전|\d+시간 전|\d+일 전)", info_text)
        summary_node = parent.select_one(".news_dsc") if parent else None
        items.append(
            NewsItem(
                title=title,
                url=link,
                source="naver_page",
                date=date_match.group(1) if date_match else "",
                summary=clean_text(summary_node.get_text(" ")) if summary_node else "",
            )
        )
        if len(items) >= limit:
            break
    if items:
        return items

    # Naver changes news-search class names often. This fallback keeps the
    # parser useful by selecting likely news-title anchors from the rendered HTML.
    for node in soup.find_all("a", href=True):
        title = clean_text(node.get("title") or node.get_text(" "))
        link = normalize_naver_link(node.get("href", ""))
        if not title or len(title) < 12:
            continue
        if title in {"네이버뉴스"} or "javascript:" in link or link.startswith("#"):
            continue
        if "search.naver.com" in link or "news.naver.com/main/static" in link:
            continue
        if len(title) > 140:
            continue
        parent = node.find_parent("div", class_=re.compile("fds-news|news|bx|layout"))
        info_text = clean_text(parent.get_text(" ")) if parent else ""
        date_match = re.search(r"(\d{4}\.\d{2}\.\d{2}\.|\d+분 전|\d+시간 전|\d+일 전)", info_text)
        items.append(
            NewsItem(
                title=title,
                url=link,
                source="naver_page",
                date=date_match.group(1) if date_match else "",
                summary="",
            )
        )
        if len(items) >= limit:
            break
    return items


def fetch_article_text(url: str, max_chars: int = 1200) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        soup = make_soup(resp.text)
        for tag in soup(["script", "style", "noscript", "header", "footer"]):
            tag.decompose()

        selectors = [
            "#dic_area",
            "article",
            ".article_view",
            ".article-body",
            ".newsct_article",
            "#articeBody",
            "#articleBody",
        ]
        chunks: list[str] = []
        for selector in selectors:
            target = soup.select_one(selector)
            if target:
                chunks.append(clean_text(target.get_text(" ")))
                break
        if not chunks:
            paragraphs = [clean_text(p.get_text(" ")) for p in soup.find_all("p")]
            chunks = [p for p in paragraphs if len(p) > 30]
        text = clean_text(" ".join(chunks))
        return text[:max_chars]
    except Exception as exc:
        return f"[본문 수집 실패: {exc}]"


def make_soup(markup: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(markup, "lxml")
    except Exception:
        return BeautifulSoup(markup, "html.parser")


def dedupe(items: Iterable[NewsItem]) -> list[NewsItem]:
    seen = set()
    result = []
    for item in items:
        key = item.url or item.title
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def write_report(
    query: str,
    date_range: str,
    local_items: list[NewsItem],
    scenario_items: list[ScenarioItem],
    naver_items: list[NewsItem],
    fetch_articles: bool,
) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    slug = re.sub(r"[^0-9A-Za-z가-힣_-]+", "_", query).strip("_")[:48] or "search"
    path = OUTPUT_DIR / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{slug}.txt"

    lines = [
        "# LLM News Search Report",
        "",
        f"- query: {query}",
        f"- generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"- source_db: {DB_PATH}",
        f"- scenario_db: {SCENARIO_DB_PATH}",
        f"- date_filter: {date_range}",
        f"- local_results: {len(local_items)}",
        f"- scenario_results: {len(scenario_items)}",
        f"- naver_results: {len(naver_items)}",
        "",
    ]

    def append_items(label: str, items: list[NewsItem]):
        lines.extend([f"## {label}", ""])
        if not items:
            lines.extend(["(no results)", ""])
            return
        for idx, item in enumerate(items, 1):
            lines.extend(
                [
                    f"### {idx}. {item.title}",
                    f"- date: {item.date}",
                    f"- source: {item.source}",
                    f"- url: {item.url}",
                ]
            )
            if item.summary:
                lines.append(f"- summary: {item.summary}")
            if fetch_articles and item.url:
                article = fetch_article_text(item.url)
                lines.extend(["- article_excerpt:", textwrap.indent(article, "  ")])
            lines.append("")

    append_items("Local DB", local_items)

    lines.extend(["## Scenario DB / TXT", ""])
    if not scenario_items:
        lines.extend(["(no results)", ""])
    for idx, item in enumerate(scenario_items, 1):
        lines.extend(
            [
                f"### {idx}. {item.title}",
                f"- scenario: {item.scenario}",
                f"- file: {item.file}",
                f"- score: {item.score}",
                f"- category: {item.category}",
                f"- keywords: {item.keywords}",
                f"- node_id: {item.node_id}",
                f"- source: {item.source}",
                f"- published_at: {item.published_at}",
                f"- url: {item.url}",
            ]
        )
        if item.matched_terms:
            lines.append("- matched_terms:")
            lines.append(textwrap.indent(item.matched_terms, "  "))
        if item.significance:
            lines.append(f"- significance: {item.significance}")
        if item.summary:
            lines.append(f"- summary: {item.summary}")
        lines.append("")

    append_items("Naver News", naver_items)

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main(argv: list[str]) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Search local news DB and export TXT for LLM reading.")
    parser.add_argument("query", help="Search query. Space-separated terms are ANDed for local DB search.")
    parser.add_argument("--limit", type=int, default=30, help="Local DB result limit.")
    parser.add_argument("--days", type=int, default=None, help="Only local DB rows fetched within N days.")
    parser.add_argument("--since", default=None, help="ISO date/time lower bound for local DB fetched_at.")
    parser.add_argument("--naver", action="store_true", help="Also search Naver News.")
    parser.add_argument("--naver-limit", type=int, default=10, help="Naver result limit.")
    parser.add_argument("--scenario-limit", type=int, default=30, help="Scenario TXT result limit.")
    parser.add_argument(
        "--scenario-source",
        choices=["db", "txt", "both", "none"],
        default="db",
        help="Scenario source. Default reads live scenario.db, not exported TXT.",
    )
    parser.add_argument("--no-scenarios", action="store_true", help="Skip scenario search.")
    parser.add_argument("--fetch-articles", action="store_true", help="Fetch short article excerpts for result URLs.")
    args = parser.parse_args(argv)

    since_iso, date_range = format_date_range(args.days, args.since)
    local_items = search_local_db(args.query, args.limit, since_iso)
    scenario_items: list[ScenarioItem] = []
    if not args.no_scenarios and args.scenario_source != "none":
        if args.scenario_source in {"db", "both"}:
            scenario_items.extend(search_scenario_db(args.query, args.scenario_limit, since_iso))
        if args.scenario_source in {"txt", "both"}:
            remaining = max(args.scenario_limit - len(scenario_items), 0)
            if remaining:
                scenario_items.extend(search_scenario_txt(args.query, remaining, since_iso))

    naver_items: list[NewsItem] = []
    if args.naver:
        try:
            naver_items = search_naver_api(args.query, args.naver_limit)
            if not naver_items:
                naver_items = search_naver_page(args.query, args.naver_limit)
        except Exception as exc:
            naver_items = [NewsItem("Naver search failed", "", "error", "", str(exc))]

    local_items = dedupe(local_items)
    naver_items = dedupe(naver_items)
    out = write_report(args.query, date_range, local_items, scenario_items, naver_items, args.fetch_articles)
    print(out)
    print(f"local={len(local_items)} scenarios={len(scenario_items)} naver={len(naver_items)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
