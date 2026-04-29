"""
시나리오 DB 관리 — scenario.db
================================
  - scenarios : 시나리오 (이름, 설명, 카테고리, 키워드)
  - nodes     : 시나리오 내 이벤트 노드 (뉴스 기사 기반)

시나리오는 하나의 "이슈 흐름"이고,
노드는 그 흐름 위의 개별 뉴스 이벤트입니다.
"""

import os
import json
import sqlite3
import re
from datetime import datetime
from typing import List, Dict, Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scenario.db")


# ─────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────
def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _parse_keywords(value: str) -> List[str]:
    try:
        parsed = json.loads(value or "[]")
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        return []


def _scenario_row_to_dict(row: sqlite3.Row) -> Dict:
    d = dict(row)
    d["keywords"] = _parse_keywords(d.get("keywords"))
    if "relevance" in d and d["relevance"] is None:
        d["relevance"] = 0
    return d


def _extract_search_terms(text: str, max_terms: int = 8) -> List[str]:
    """
    LIKE 검색용 핵심 토큰 추출.
    긴 자연어 질문/기사 요약이 그대로 들어와도 주요 명사/영문 토큰 중심으로 후보를 찾는다.
    """
    text = (text or "").strip()
    if not text:
        return []

    terms: List[str] = []
    compact = re.sub(r"\s+", " ", text)
    if 2 <= len(compact) <= 80:
        terms.append(compact)

    stopwords = {
        "관련", "주요", "이슈", "시나리오", "분석", "전망", "뉴스", "기사",
        "그리고", "또는", "대한", "대해", "으로", "에서", "에게", "와", "과",
        "the", "and", "for", "with", "from", "this", "that",
    }
    for token in re.findall(r"[0-9A-Za-z가-힣]+", text.lower()):
        if len(token) < 2 or token in stopwords:
            continue
        if token not in terms:
            terms.append(token)
        if len(terms) >= max_terms:
            break
    return terms


def _search_scenario_rows(query: str, category: Optional[str], limit: int) -> List[sqlite3.Row]:
    terms = _extract_search_terms(query)
    if not terms:
        return []

    score_parts = []
    score_params = []
    where_parts = []
    where_params = []

    for term in terms:
        like = f"%{term}%"
        score_parts.append(
            "("
            "CASE WHEN s.name LIKE ? THEN 10 ELSE 0 END + "
            "CASE WHEN s.description LIKE ? THEN 5 ELSE 0 END + "
            "CASE WHEN s.keywords LIKE ? THEN 7 ELSE 0 END + "
            "CASE WHEN n.title LIKE ? THEN 8 ELSE 0 END + "
            "CASE WHEN n.summary LIKE ? THEN 4 ELSE 0 END + "
            "CASE WHEN n.significance LIKE ? THEN 6 ELSE 0 END + "
            "CASE WHEN n.source LIKE ? THEN 2 ELSE 0 END"
            ")"
        )
        score_params.extend([like] * 7)
        where_parts.append(
            "("
            "s.name LIKE ? OR s.description LIKE ? OR s.keywords LIKE ? OR "
            "n.title LIKE ? OR n.summary LIKE ? OR n.significance LIKE ? OR n.source LIKE ?"
            ")"
        )
        where_params.extend([like] * 7)

    where_sql = " OR ".join(where_parts)
    if category:
        where_sql = f"({where_sql}) AND s.category = ?"
        where_params.append(category)

    sql = f"""
        SELECT
            s.id, s.name, s.description, s.category, s.keywords,
            s.created_at, s.updated_at,
            MAX({" + ".join(score_parts)}) AS relevance,
            COUNT(DISTINCT n.id) AS matched_nodes
        FROM scenarios s
        LEFT JOIN nodes n ON n.scenario_id = s.id
        WHERE {where_sql}
        GROUP BY s.id
        ORDER BY relevance DESC, s.updated_at DESC
        LIMIT ?
    """

    conn = _get_conn()
    rows = conn.execute(sql, (*score_params, *where_params, limit)).fetchall()
    conn.close()
    return rows


# ─────────────────────────────────────────────
# DB 초기화
# ─────────────────────────────────────────────
def init_db():
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scenarios (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            description TEXT,
            category    TEXT DEFAULT 'other',
            keywords    TEXT DEFAULT '[]',
            created_at  TEXT,
            updated_at  TEXT
        );

        CREATE TABLE IF NOT EXISTS nodes (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            scenario_id  INTEGER NOT NULL
                             REFERENCES scenarios(id) ON DELETE CASCADE,
            title        TEXT NOT NULL,
            summary      TEXT,
            significance TEXT,
            url          TEXT,
            source       TEXT,
            published_at TEXT,
            node_order   INTEGER DEFAULT 0,
            created_at   TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_nodes_scenario
            ON nodes(scenario_id);
        CREATE INDEX IF NOT EXISTS idx_nodes_order
            ON nodes(scenario_id, node_order);
    """)
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# 시나리오 CRUD
# ─────────────────────────────────────────────
def create_scenario(name: str, description: str, category: str,
                    keywords: List[str]) -> int:
    conn = _get_conn()
    now = datetime.now().isoformat()
    cursor = conn.execute(
        "INSERT INTO scenarios (name, description, category, keywords, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (name, description, category,
         json.dumps(keywords, ensure_ascii=False), now, now),
    )
    scenario_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return scenario_id


def get_all_scenarios(limit: int = 200) -> List[Dict]:
    """updated_at 최신순으로 반환."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT id, name, description, category, keywords, created_at, updated_at "
        "FROM scenarios ORDER BY updated_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append(_scenario_row_to_dict(r))
    return result


def get_scenario_with_nodes(scenario_id: int) -> Optional[Dict]:
    conn = _get_conn()
    row = conn.execute(
        "SELECT id, name, description, category, keywords, created_at, updated_at "
        "FROM scenarios WHERE id = ?",
        (scenario_id,),
    ).fetchone()
    if not row:
        conn.close()
        return None
    scenario = dict(row)
    scenario["keywords"] = _parse_keywords(scenario["keywords"])

    nodes = conn.execute(
        "SELECT id, title, summary, significance, url, source, "
        "published_at, node_order, created_at "
        "FROM nodes WHERE scenario_id = ? "
        "ORDER BY node_order ASC, created_at ASC",
        (scenario_id,),
    ).fetchall()
    conn.close()
    scenario["nodes"] = [dict(n) for n in nodes]
    return scenario


def search_scenarios(query: str, category: Optional[str] = None,
                     limit: int = 10) -> List[Dict]:
    """
    시나리오 메타데이터와 노드 본문을 함께 검색한다.

    기업명/사건명이 시나리오 설명에는 없고 개별 노드에만 있어도 RAG 후보로 잡히도록
    scenarios와 nodes를 함께 훑는다.
    """
    rows = _search_scenario_rows(query, category=category, limit=limit)
    return [_scenario_row_to_dict(r) for r in rows]


def find_candidate_scenarios_for_article(article: Dict, limit: int = 50) -> List[Dict]:
    """
    새 기사와 관련 가능성이 높은 기존 시나리오 후보를 반환한다.

    최신순 상위 N개만 Gemini에 넘기던 방식 대신, 기사 제목/요약/출처를 기존 노드까지
    포함해 검색해서 오래된 시나리오도 후보에 다시 올라오게 한다.
    """
    query = " ".join(
        part for part in [
            article.get("title", ""),
            article.get("summary", ""),
            article.get("source", ""),
        ] if part
    )
    rows = _search_scenario_rows(query, category=None, limit=limit)
    candidates = [_scenario_row_to_dict(r) for r in rows]

    if len(candidates) >= limit:
        return candidates

    seen = {s["id"] for s in candidates}
    for scenario in get_all_scenarios(limit=limit):
        if scenario["id"] in seen:
            continue
        candidates.append(scenario)
        seen.add(scenario["id"])
        if len(candidates) >= limit:
            break
    return candidates


def get_scenarios_by_category(category: str, limit: int = 20) -> List[Dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT id, name, description, category, keywords, updated_at "
        "FROM scenarios WHERE category = ? "
        "ORDER BY updated_at DESC LIMIT ?",
        (category, limit),
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append(_scenario_row_to_dict(r))
    return result


# ─────────────────────────────────────────────
# 노드 CRUD
# ─────────────────────────────────────────────
def add_node_to_scenario(scenario_id: int, title: str, summary: str,
                          significance: str, url: str, source: str,
                          published_at: str) -> int:
    conn = _get_conn()
    max_order = conn.execute(
        "SELECT COALESCE(MAX(node_order), 0) FROM nodes WHERE scenario_id = ?",
        (scenario_id,),
    ).fetchone()[0]
    now = datetime.now().isoformat()
    cursor = conn.execute(
        "INSERT INTO nodes "
        "(scenario_id, title, summary, significance, url, source, "
        " published_at, node_order, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (scenario_id, title, summary, significance, url, source,
         published_at, max_order + 1, now),
    )
    node_id = cursor.lastrowid
    conn.execute(
        "UPDATE scenarios SET updated_at = ? WHERE id = ?", (now, scenario_id)
    )
    conn.commit()
    conn.close()
    return node_id


# ─────────────────────────────────────────────
# 통계
# ─────────────────────────────────────────────
def get_stats() -> Dict:
    conn = _get_conn()
    total_scenarios = conn.execute("SELECT COUNT(*) FROM scenarios").fetchone()[0]
    total_nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    cats = conn.execute(
        "SELECT category, COUNT(*) as cnt FROM scenarios "
        "GROUP BY category ORDER BY cnt DESC"
    ).fetchall()
    conn.close()
    return {
        "total_scenarios": total_scenarios,
        "total_nodes": total_nodes,
        "categories": {r["category"]: r["cnt"] for r in cats},
    }
