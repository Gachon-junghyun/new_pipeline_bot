#!/usr/bin/env python3
"""
기사 본문 스크레이퍼
====================
news_alert.db 의 seen_news 테이블에서 URL을 읽고
실제 기사 페이지를 긁어 article_contents 에 저장합니다.

사용법:
  python article_scraper.py                        # 미수집 기사 50개
  python article_scraper.py --keyword 반도체       # 키워드 매칭 기사만
  python article_scraper.py --limit 100            # 최대 100개
  python article_scraper.py --workers 8            # 동시 작업 수
  python article_scraper.py --llm                  # llm_outputs/ 최신 파일의 URL 스크랩
  python article_scraper.py --url https://...      # 단일 URL 즉시 스크랩
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

import db_manager

log = logging.getLogger("scraper")

# ─────────────────────────────────────────────
# HTTP 설정
# ─────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}
TIMEOUT = 15
MIN_BODY_LEN = 100  # 이 길이 미만은 스크랩 실패로 간주

# ─────────────────────────────────────────────
# 소스별 CSS 선택자 (구체적 → 제네릭 순으로 시도)
# ─────────────────────────────────────────────
SOURCE_SELECTORS: dict[str, list[str]] = {
    "yonhap":       ["div.story-news.article", "article.story-news", "div[class*='article-txt']"],
    "hankyung":     ["div#articleBody", "div.article-body", "div[class*='article_body']"],
    "mk":           ["div#article_body", "div.news_cnt_detail_wrap", "div[itemprop='articleBody']"],
    "edaily":       ["div[itemprop='articleBody']", "div#articleBody", "div.article_body"],
    "mt":           ["div#textBody", "div.view_text", "div[class*='article']"],
    "heraldcorp":   ["div#articleText", "div.view-con", "article"],
    "chosun":       ["div.article-body", "section.article-body", "div[class*='article']"],
    "joongang":     ["div#article_body", "div.article_body_content", "div[class*='article']"],
    "donga":        ["div.article_txt", "div#content", "article"],
    "asiae":        ["div.article_con", "div[class*='article']", "article"],
    "sedaily":      ["div#article_view", "div.article_view", "div[class*='article']"],
    "bbc":          ["div[data-component='text-block']", "article", "div.story-body"],
    "cnbc":         ["div.ArticleBody-articleBody", "div[class*='article-body']", "article"],
    "marketwatch":  ["div.article__body", "div[class*='article-body']", "article"],
    "fxstreet":     ["div.fxs_article_content", "article", "div[class*='article']"],
    "nyt":          ["section[name='articleBody']", "div[class*='StoryBodyCompanion']", "article"],
    "google_kr":    [],  # 리디렉션 → 제네릭 파서로 처리
    "google_en":    [],
}


def _extract_with_selectors(soup: BeautifulSoup, selectors: list[str]) -> str:
    for sel in selectors:
        try:
            tag = soup.select_one(sel)
            if tag:
                text = tag.get_text(separator="\n", strip=True)
                if len(text) >= MIN_BODY_LEN:
                    return text
        except Exception:
            continue
    return ""


def _extract_generic(soup: BeautifulSoup) -> str:
    """선택자 실패 시 <p> 태그 누적으로 본문 추출."""
    for unwanted in soup.select("nav, header, footer, aside, script, style, .ad, [class*='ad-'], [id*='ad-']"):
        unwanted.decompose()

    # article > main > div 순으로 가장 긴 텍스트 블록 시도
    for tag_name in ("article", "main", "div"):
        candidates = soup.find_all(tag_name)
        best = max(
            (c.get_text(separator="\n", strip=True) for c in candidates),
            key=len,
            default="",
        )
        if len(best) >= MIN_BODY_LEN:
            return best

    # 최후 수단: 모든 <p> 합산
    paragraphs = [p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 30]
    return "\n".join(paragraphs)


def _clean_body(text: str) -> str:
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


# ─────────────────────────────────────────────
# 단일 기사 스크랩
# ─────────────────────────────────────────────
def scrape_article(url: str, source: str = "") -> tuple[str, str]:
    """
    Returns:
        (body_text, status)  status: 'ok' | 'error' | 'short'
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "lxml")

        # 소스 매핑 (URL 도메인 기반 보완)
        if not source:
            domain = urlparse(url).netloc
            for k in SOURCE_SELECTORS:
                if k in domain:
                    source = k
                    break

        selectors = SOURCE_SELECTORS.get(source, [])
        body = _extract_with_selectors(soup, selectors)
        if not body:
            body = _extract_generic(soup)

        body = _clean_body(body)
        if len(body) < MIN_BODY_LEN:
            return body, "short"
        return body, "ok"

    except requests.RequestException as e:
        log.warning(f"요청 실패 [{url[:60]}]: {e}")
        return "", "error"
    except Exception as e:
        log.warning(f"파싱 실패 [{url[:60]}]: {e}")
        return "", "error"


# ─────────────────────────────────────────────
# 배치 스크랩 (ThreadPoolExecutor)
# ─────────────────────────────────────────────
def scrape_batch(
    articles: list[dict],
    workers: int = 5,
    delay: float = 0.3,
) -> dict[str, int]:
    """
    articles: [{"url": ..., "source": ..., "title": ...}, ...]
    Returns: {"ok": N, "short": N, "error": N, "skip": N}
    """
    stats = {"ok": 0, "short": 0, "error": 0, "skip": 0}

    def _job(art: dict) -> tuple[str, str, str]:
        existing = db_manager.get_article_content(art["url"])
        if existing and existing["status"] == "ok":
            return art["url"], "", "skip"
        body, status = scrape_article(art["url"], art.get("source", ""))
        time.sleep(delay)
        return art["url"], body, status

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_job, art): art for art in articles}
        for i, future in enumerate(as_completed(futures), 1):
            url, body, status = future.result()
            stats[status] = stats.get(status, 0) + 1
            if status != "skip":
                db_manager.save_article_content(url, body, status)
            art = futures[future]
            log.info(f"[{i}/{len(articles)}] {status:5s} | {art.get('title', url)[:55]}")

    return stats


# ─────────────────────────────────────────────
# LLM 출력 파일에서 URL 추출
# ─────────────────────────────────────────────
def _load_urls_from_llm_output() -> list[dict]:
    output_dir = Path(__file__).resolve().parent / "llm_outputs"
    files = sorted(output_dir.glob("*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        log.error("llm_outputs/ 에 파일이 없습니다.")
        return []
    latest = files[0]
    log.info(f"LLM 출력 파일: {latest.name}")
    text = latest.read_text(encoding="utf-8")
    urls = re.findall(r"https?://[^\s\)\]\"']+", text)
    return [{"url": u, "source": "", "title": u} for u in dict.fromkeys(urls)]


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="기사 본문 스크레이퍼")
    p.add_argument("--keyword", "-k", default="", help="DB에서 제목 키워드 필터")
    p.add_argument("--limit",   "-n", type=int, default=50, help="최대 수집 수 (기본 50)")
    p.add_argument("--workers", "-w", type=int, default=5,  help="동시 작업 수 (기본 5)")
    p.add_argument("--delay",   "-d", type=float, default=0.3, help="요청 간 딜레이 초 (기본 0.3)")
    p.add_argument("--llm",     action="store_true", help="llm_outputs/ 최신 파일 URL 스크랩")
    p.add_argument("--url",     default="",  help="단일 URL 즉시 스크랩")
    return p.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    db_manager.init_db()
    args = _parse_args()

    # ── 단일 URL 모드 ───────────────────────────────
    if args.url:
        body, status = scrape_article(args.url)
        db_manager.save_article_content(args.url, body, status)
        print(f"\n[{status}] {args.url}\n{'─'*60}")
        print(body[:1000] if body else "(본문 없음)")
        return

    # ── LLM 출력 파일 모드 ─────────────────────────
    if args.llm:
        articles = _load_urls_from_llm_output()
        if not articles:
            sys.exit(1)
        articles = articles[:args.limit]
    else:
        # ── DB 미수집 모드 ─────────────────────────
        articles = db_manager.get_unscraped_articles(
            keyword=args.keyword or None,
            limit=args.limit,
        )

    if not articles:
        print("스크랩할 기사가 없습니다. (DB에 기사를 먼저 수집하세요: python news_fetcher.py)")
        return

    print(f"\n스크랩 시작: {len(articles)}개 | workers={args.workers} | delay={args.delay}s\n{'─'*60}")
    stats = scrape_batch(articles, workers=args.workers, delay=args.delay)
    print(f"\n{'─'*60}")
    print(f"완료 — ok:{stats['ok']}  short:{stats['short']}  error:{stats['error']}  skip:{stats['skip']}")


if __name__ == "__main__":
    main()
