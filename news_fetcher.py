"""
뉴스 수집기 — news_elert 전용
==============================
Reference: github.com/Gachon-junghyun/news_database/pipeline.py

RSS 피드만 사용 (BS 스크래핑 제외 — 스케줄 실행 속도 최적화)

실행 모드:
  python news_fetcher.py               # 기본 (피드당 20개)
  python news_fetcher.py --veryfast    # 피드당 10개, 빠른 수집
  python news_fetcher.py --full        # 피드당 40개, 전체 수집
"""

import sys
import time
import logging
from typing import List, Dict
from urllib.parse import quote

import feedparser
import requests

log = logging.getLogger("news_fetcher")

# ─────────────────────────────────────────────
# 공통 헤더 (pipeline.py 레퍼런스)
# ─────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# ─────────────────────────────────────────────
# RSS 피드 목록 (pipeline.py 레퍼런스 기반 선별)
# ─────────────────────────────────────────────
RSS_FEEDS: List[Dict] = [

    # ── 연합뉴스 ─────────────────────────────────────────────
    {"name": "연합 경제",       "url": "https://www.yna.co.kr/rss/economy.xml",          "source": "yonhap"},
    {"name": "연합 국제",       "url": "https://www.yna.co.kr/rss/international.xml",    "source": "yonhap"},
    {"name": "연합 산업",       "url": "https://www.yna.co.kr/rss/industry.xml",         "source": "yonhap"},
    {"name": "연합 전체",       "url": "https://www.yna.co.kr/rss/news.xml",             "source": "yonhap"},

    # ── 한국경제 ─────────────────────────────────────────────
    {"name": "한경 경제",       "url": "https://www.hankyung.com/feed/economy",          "source": "hankyung"},
    {"name": "한경 증권",       "url": "https://www.hankyung.com/feed/finance",          "source": "hankyung"},
    {"name": "한경 국제",       "url": "https://www.hankyung.com/feed/international",    "source": "hankyung"},
    {"name": "한경 산업",       "url": "https://www.hankyung.com/feed/industry",         "source": "hankyung"},

    # ── 매일경제 ─────────────────────────────────────────────
    {"name": "매경 경제",       "url": "https://www.mk.co.kr/rss/30100041/",             "source": "mk"},
    {"name": "매경 증권",       "url": "https://www.mk.co.kr/rss/30200030/",             "source": "mk"},
    {"name": "매경 국제",       "url": "https://www.mk.co.kr/rss/30300018/",             "source": "mk"},

    # ── 이데일리 ─────────────────────────────────────────────
    {"name": "이데일리 경제",   "url": "https://www.edaily.co.kr/rss/economy.xml",       "source": "edaily"},
    {"name": "이데일리 증권",   "url": "https://www.edaily.co.kr/rss/stock.xml",         "source": "edaily"},

    # ── 머니투데이 ───────────────────────────────────────────
    {"name": "머니투데이 전체", "url": "https://www.mt.co.kr/rss/mt_main.xml",           "source": "mt"},

    # ── 헤럴드경제 ───────────────────────────────────────────
    {"name": "헤럴드 금융",     "url": "https://biz.heraldcorp.com/rss/finance.xml",     "source": "heraldcorp"},
    {"name": "헤럴드 경제",     "url": "https://biz.heraldcorp.com/rss/economy.xml",     "source": "heraldcorp"},

    # ── 조선비즈 ─────────────────────────────────────────────
    {"name": "조선비즈",        "url": "https://biz.chosun.com/rssfeeds/economy/",       "source": "chosun"},
    {"name": "조선비즈 IT",     "url": "https://biz.chosun.com/rssfeeds/it-science/",    "source": "chosun"},

    # ── 중앙일보 ─────────────────────────────────────────────
    {"name": "중앙 경제",       "url": "https://rss.joins.com/joins_economy_list.xml",   "source": "joongang"},

    # ── 동아일보 ─────────────────────────────────────────────
    {"name": "동아 경제",       "url": "https://rss.donga.com/economy.xml",              "source": "donga"},

    # ── 아시아경제 ───────────────────────────────────────────
    {"name": "아시아경제",      "url": "https://www.asiae.co.kr/rss/all.htm",            "source": "asiae"},

    # ── 서울경제 ─────────────────────────────────────────────
    {"name": "서울경제 금융",   "url": "https://www.sedaily.com/RSS/Finance/",           "source": "sedaily"},
    {"name": "서울경제 경제",   "url": "https://www.sedaily.com/RSS/Economy/",           "source": "sedaily"},

    # ── BBC ───────────────────────────────────────────────────
    {"name": "BBC Business",    "url": "http://feeds.bbci.co.uk/news/business/rss.xml",  "source": "bbc"},
    {"name": "BBC World",       "url": "http://feeds.bbci.co.uk/news/world/rss.xml",     "source": "bbc"},

    # ── CNBC ─────────────────────────────────────────────────
    {"name": "CNBC Economy",    "url": "https://www.cnbc.com/id/20910258/device/rss/rss.html", "source": "cnbc"},
    {"name": "CNBC Finance",    "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html", "source": "cnbc"},

    # ── MarketWatch ──────────────────────────────────────────
    {"name": "MarketWatch Top", "url": "https://www.marketwatch.com/rss/topstories",     "source": "marketwatch"},

    # ── FXStreet ─────────────────────────────────────────────
    {"name": "FXStreet News",   "url": "https://www.fxstreet.com/rss/news",              "source": "fxstreet"},

    # ── NYT ──────────────────────────────────────────────────
    {"name": "NYT Business",    "url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "source": "nyt"},
    {"name": "NYT Economy",     "url": "https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml",  "source": "nyt"},

    # ── Google News RSS — 한국어 키워드 ─────────────────────
    {
        "name": "Google KR 경제",
        "url":  "https://news.google.com/rss/search?q=%EA%B2%BD%EC%A0%9C&hl=ko&gl=KR&ceid=KR:ko",
        "source": "google_kr",
    },
    {
        "name": "Google KR 주식증시",
        "url":  "https://news.google.com/rss/search?q=%EC%A3%BC%EC%8B%9D+%EC%A6%9D%EC%8B%9C&hl=ko&gl=KR&ceid=KR:ko",
        "source": "google_kr",
    },
    {
        "name": "Google KR 금리연준",
        "url":  "https://news.google.com/rss/search?q=%EA%B8%88%EB%A6%AC+%EC%97%B0%EC%A4%80&hl=ko&gl=KR&ceid=KR:ko",
        "source": "google_kr",
    },
    {
        "name": "Google KR 환율달러",
        "url":  "https://news.google.com/rss/search?q=%ED%99%98%EC%9C%A8+%EB%8B%AC%EB%9F%AC&hl=ko&gl=KR&ceid=KR:ko",
        "source": "google_kr",
    },
    {
        "name": "Google KR 반도체",
        "url":  "https://news.google.com/rss/search?q=%EB%B0%98%EB%8F%84%EC%B2%B4+%EC%82%BC%EC%84%B1&hl=ko&gl=KR&ceid=KR:ko",
        "source": "google_kr",
    },
    {
        "name": "Google KR 무역관세",
        "url":  "https://news.google.com/rss/search?q=%EB%AC%B4%EC%97%AD+%EA%B4%80%EC%84%B8&hl=ko&gl=KR&ceid=KR:ko",
        "source": "google_kr",
    },
    # ── Google News RSS — 영문 키워드 ───────────────────────
    {
        "name": "Google EN economy",
        "url":  "https://news.google.com/rss/search?q=economy+stocks&hl=en&gl=US&ceid=US:en",
        "source": "google_en",
    },
    {
        "name": "Google EN Fed inflation",
        "url":  "https://news.google.com/rss/search?q=Federal+Reserve+inflation&hl=en&gl=US&ceid=US:en",
        "source": "google_en",
    },
    {
        "name": "Google EN trade tariff",
        "url":  "https://news.google.com/rss/search?q=trade+tariff+US+China&hl=en&gl=US&ceid=US:en",
        "source": "google_en",
    },
]


# ─────────────────────────────────────────────
# 단일 피드 파싱
# ─────────────────────────────────────────────
def _fetch_feed(feed: Dict, max_items: int) -> List[Dict]:
    try:
        resp = requests.get(feed["url"], headers=HEADERS, timeout=12)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        articles = []
        for entry in parsed.entries[:max_items]:
            title = (entry.get("title") or "").strip()
            url   = (entry.get("link") or "").strip()
            pub   = entry.get("published", entry.get("updated", ""))
            summary = (entry.get("summary") or "").strip()
            if title and url:
                articles.append({
                    "title":     title,
                    "url":       url,
                    "source":    feed.get("source", ""),
                    "feed_name": feed.get("name", ""),
                    "published": pub,
                    "summary":   summary[:200] if summary else "",
                })
        return articles
    except Exception as e:
        log.warning(f"피드 실패 [{feed['name']}]: {e}")
        return []


# ─────────────────────────────────────────────
# 메인 수집 함수
# ─────────────────────────────────────────────
def fetch_news(max_per_feed: int = 20, delay: float = 0.1) -> List[Dict]:
    """
    모든 RSS 피드에서 뉴스 수집.

    Args:
        max_per_feed: 피드당 최대 수집 수
            - veryfast: 10
            - 기본:     20
            - full:     40
        delay: 피드 간 요청 딜레이(초)
    Returns:
        [{title, url, source, feed_name, published, summary}, ...]
    """
    all_articles: List[Dict] = []
    total_feeds = len(RSS_FEEDS)

    for i, feed in enumerate(RSS_FEEDS, 1):
        articles = _fetch_feed(feed, max_per_feed)
        all_articles.extend(articles)
        log.debug(f"[{i}/{total_feeds}] {feed['name']}: {len(articles)}개")
        if delay > 0:
            time.sleep(delay)

    log.info(
        f"수집 완료: 총 {len(all_articles)}개 | "
        f"피드 {total_feeds}개 | 피드당 최대 {max_per_feed}개"
    )
    return all_articles


# ─────────────────────────────────────────────
# 키워드 필터링
# ─────────────────────────────────────────────
def filter_by_keywords(articles: List[Dict], keywords: List[str]) -> List[Dict]:
    """
    articles 중 제목(title)에 keywords 중 하나라도 포함된 것만 반환.
    대소문자 구분 없음.
    """
    if not keywords:
        return []
    kw_lower = [k.lower() for k in keywords]
    matched = []
    for art in articles:
        title_lower = art.get("title", "").lower()
        for kw in kw_lower:
            if kw in title_lower:
                matched.append({**art, "matched_keyword": kw})
                break
    return matched


# ─────────────────────────────────────────────
# CLI 실행
# ─────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if "--veryfast" in sys.argv:
        mode = "VERYFAST"
        articles = fetch_news(max_per_feed=10, delay=0.05)
    elif "--full" in sys.argv:
        mode = "FULL"
        articles = fetch_news(max_per_feed=40, delay=0.15)
    else:
        mode = "DEFAULT"
        articles = fetch_news(max_per_feed=20, delay=0.1)

    print(f"\n[{mode}] 수집 결과 — 총 {len(articles)}개\n{'─'*60}")
    for art in articles[:15]:
        print(f"[{art['source']:15s}] {art['title'][:55]}")
    if len(articles) > 15:
        print(f"... 외 {len(articles) - 15}개")
