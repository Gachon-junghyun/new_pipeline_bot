"""
DB 관리 모듈 — news_elert
===========================
SQLite 기반으로 아래 4가지를 관리합니다.
  1. seen_news   — 중복 방지용 수집 이력 (URL 해시)
  2. users       — 봇 사용자 등록
  3. keywords    — 사용자별 알림 키워드
  4. sent_news   — 사용자별 전송 이력 (중복 전송 방지)
"""

import os
import sqlite3
import hashlib
from datetime import datetime
from typing import List, Dict, Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "news_alert.db")


# ─────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────
def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _url_hash(url: str) -> str:
    return hashlib.md5(url.strip().encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────
# DB 초기화
# ─────────────────────────────────────────────
def init_db():
    """테이블 생성 (없을 경우에만)"""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS seen_news (
            url_hash   TEXT PRIMARY KEY,
            url        TEXT NOT NULL,
            title      TEXT,
            source     TEXT,
            fetched_at TEXT
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id    INTEGER PRIMARY KEY,
            username   TEXT,
            chat_id    INTEGER,
            joined_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS keywords (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            keyword    TEXT NOT NULL,
            created_at TEXT,
            UNIQUE(user_id, keyword)
        );

        CREATE TABLE IF NOT EXISTS sent_news (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            url_hash   TEXT NOT NULL,
            sent_at    TEXT,
            UNIQUE(user_id, url_hash)
        );
    """)
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# 뉴스 중복 체크
# ─────────────────────────────────────────────
def filter_new_articles(articles: List[Dict]) -> List[Dict]:
    """
    articles 중 seen_news에 없는 것만 반환하고,
    동시에 seen_news에 등록 (한 번에 처리해서 레이스 컨디션 최소화).
    """
    if not articles:
        return []

    conn = _get_conn()
    new_articles = []
    for art in articles:
        url = art.get("url", "")
        if not url:
            continue
        h = _url_hash(url)
        row = conn.execute(
            "SELECT 1 FROM seen_news WHERE url_hash = ?", (h,)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT OR IGNORE INTO seen_news "
                "(url_hash, url, title, source, fetched_at) VALUES (?, ?, ?, ?, ?)",
                (h, url, art.get("title", ""), art.get("source", ""),
                 datetime.now().isoformat()),
            )
            new_articles.append(art)
    conn.commit()
    conn.close()
    return new_articles


# ─────────────────────────────────────────────
# 사용자 관리
# ─────────────────────────────────────────────
def register_user(user_id: int, username: str, chat_id: int):
    conn = _get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO users (user_id, username, chat_id, joined_at) "
        "VALUES (?, ?, ?, ?)",
        (user_id, username or "", chat_id, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


def get_all_users() -> List[Dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT user_id, username, chat_id FROM users"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# 키워드 관리
# ─────────────────────────────────────────────
def add_keyword(user_id: int, keyword: str) -> bool:
    """키워드 추가. 이미 있으면 False 반환."""
    kw = keyword.lower().strip()
    conn = _get_conn()
    before = conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE user_id = ?", (user_id,)
    ).fetchone()[0]
    conn.execute(
        "INSERT OR IGNORE INTO keywords (user_id, keyword, created_at) "
        "VALUES (?, ?, ?)",
        (user_id, kw, datetime.now().isoformat()),
    )
    conn.commit()
    after = conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE user_id = ?", (user_id,)
    ).fetchone()[0]
    conn.close()
    return after > before


def remove_keyword(user_id: int, keyword: str) -> bool:
    """키워드 삭제. 없으면 False 반환."""
    kw = keyword.lower().strip()
    conn = _get_conn()
    conn.execute(
        "DELETE FROM keywords WHERE user_id = ? AND keyword = ?", (user_id, kw)
    )
    changed = conn.total_changes
    conn.commit()
    conn.close()
    return changed > 0


def get_keywords(user_id: int) -> List[str]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT keyword FROM keywords WHERE user_id = ? ORDER BY created_at",
        (user_id,),
    ).fetchall()
    conn.close()
    return [r["keyword"] for r in rows]


def get_all_user_keywords() -> Dict[int, List[str]]:
    """
    {user_id: [keyword, ...]} 형태로 전 사용자 키워드 반환.
    user_id → chat_id 매핑도 포함.
    """
    conn = _get_conn()
    kw_rows = conn.execute("SELECT user_id, keyword FROM keywords").fetchall()
    user_rows = conn.execute("SELECT user_id, chat_id FROM users").fetchall()
    conn.close()

    chat_map = {r["user_id"]: r["chat_id"] for r in user_rows}
    result: Dict[int, Dict] = {}
    for r in kw_rows:
        uid = r["user_id"]
        if uid not in result:
            result[uid] = {
                "chat_id": chat_map.get(uid, uid),
                "keywords": [],
            }
        result[uid]["keywords"].append(r["keyword"])
    return result


# ─────────────────────────────────────────────
# 전송 이력 관리
# ─────────────────────────────────────────────
def was_sent(user_id: int, url: str) -> bool:
    h = _url_hash(url)
    conn = _get_conn()
    row = conn.execute(
        "SELECT 1 FROM sent_news WHERE user_id = ? AND url_hash = ?",
        (user_id, h),
    ).fetchone()
    conn.close()
    return row is not None


def mark_sent(user_id: int, url: str):
    h = _url_hash(url)
    conn = _get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO sent_news (user_id, url_hash, sent_at) "
        "VALUES (?, ?, ?)",
        (user_id, h, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


def batch_mark_sent(user_id: int, urls: List[str]):
    conn = _get_conn()
    now = datetime.now().isoformat()
    conn.executemany(
        "INSERT OR IGNORE INTO sent_news (user_id, url_hash, sent_at) VALUES (?, ?, ?)",
        [(user_id, _url_hash(u), now) for u in urls],
    )
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# 검색 기능 (DB 내 뉴스 검색)
# ─────────────────────────────────────────────
def search_news(keyword: str, limit: int = 20) -> List[Dict]:
    """seen_news 테이블에서 제목으로 검색."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT title, url, source, fetched_at "
        "FROM seen_news "
        "WHERE title LIKE ? "
        "ORDER BY fetched_at DESC "
        "LIMIT ?",
        (f"%{keyword}%", limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# 통계
# ─────────────────────────────────────────────
def get_stats() -> Dict:
    conn = _get_conn()
    total_news = conn.execute("SELECT COUNT(*) FROM seen_news").fetchone()[0]
    total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    total_kw = conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
    total_sent = conn.execute("SELECT COUNT(*) FROM sent_news").fetchone()[0]
    conn.close()
    return {
        "total_news": total_news,
        "total_users": total_users,
        "total_keywords": total_kw,
        "total_sent": total_sent,
    }
