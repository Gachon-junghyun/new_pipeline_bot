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
        d = dict(r)
        d["keywords"] = json.loads(d["keywords"] or "[]")
        result.append(d)
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
    scenario["keywords"] = json.loads(scenario["keywords"] or "[]")

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
    conn = _get_conn()
    like = f"%{query}%"
    if category:
        rows = conn.execute(
            "SELECT id, name, description, category, keywords, updated_at "
            "FROM scenarios "
            "WHERE (name LIKE ? OR description LIKE ? OR keywords LIKE ?) "
            "  AND category = ? "
            "ORDER BY updated_at DESC LIMIT ?",
            (like, like, like, category, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, name, description, category, keywords, updated_at "
            "FROM scenarios "
            "WHERE name LIKE ? OR description LIKE ? OR keywords LIKE ? "
            "ORDER BY updated_at DESC LIMIT ?",
            (like, like, like, limit),
        ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d["keywords"] = json.loads(d["keywords"] or "[]")
        result.append(d)
    return result


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
        d = dict(r)
        d["keywords"] = json.loads(d["keywords"] or "[]")
        result.append(d)
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
