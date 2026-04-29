"""
Reusable scenario search/export layer.

Existing scenario tables are treated as read-only. This module scores scenarios
and their news nodes with saved keyword-group profiles, then exports readable
TXT files into searched_scenario/.

The original infrastructure search is kept as the default "infra" profile.

Usage:
  python infra_search.py
  python infra_search.py --profile infra --threshold 6 --limit 80
  python infra_search.py --query "AI 반도체 데이터센터" --no-export
"""

import argparse
import json
import os
import re
import sqlite3
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import scenario_db


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.join(BASE_DIR, "searched_scenario")
PROFILES_PATH = os.path.join(DEFAULT_OUTPUT_DIR, "search_profiles.json")
DEFAULT_PROFILE_ID = "infra"


INFRA_KEYWORDS: Dict[str, List[str]] = {
    "power": [
        "전력망", "전력 인프라", "송전", "배전", "변전소", "전력 설비",
        "grid", "power grid", "transmission", "substation",
    ],
    "energy_facility": [
        "발전소", "원전", "원자력", "LNG", "가스관", "파이프라인",
        "ESS", "에너지저장", "재생에너지", "태양광", "풍력",
    ],
    "digital": [
        "데이터센터", "데이터 센터", "해저케이블", "통신망", "5G",
        "6G", "클라우드 리전", "클라우드 인프라", "광케이블",
    ],
    "transport": [
        "철도", "도로", "고속도로", "항만", "공항", "물류 허브",
        "교량", "터널", "운송 인프라", "철도망", "항공 인프라",
    ],
    "water": [
        "수처리", "상하수도", "상수도", "하수도", "담수화",
        "폐기물 처리", "폐수", "수자원",
    ],
    "industrial_facility": [
        "산업단지", "산단", "반도체 클러스터", "배터리 공장",
        "반도체 공장", "제조 거점", "생산기지", "공장 증설",
    ],
    "urban": [
        "스마트시티", "도시 인프라", "주택 공급", "공공주택",
        "재개발", "재건축", "SOC", "사회간접자본",
    ],
}


MACRO_KEYWORDS: Dict[str, List[str]] = {
    "rates": [
        "금리", "기준금리", "연준", "Fed", "FOMC", "인하", "인상",
        "bond yield", "Treasury yield", "terminal rate",
    ],
    "inflation": [
        "물가", "인플레이션", "CPI", "PCE", "임금", "유가", "식료품",
        "inflation", "price pressure",
    ],
    "growth": [
        "GDP", "성장률", "경기", "침체", "둔화", "회복", "소비", "투자",
        "recession", "soft landing",
    ],
    "currency": [
        "환율", "달러", "원화", "엔화", "위안", "강달러", "외환",
        "FX", "dollar index",
    ],
    "fiscal_policy": [
        "재정", "예산", "추경", "국채", "부채한도", "감세", "보조금",
        "fiscal", "deficit",
    ],
}


TECH_AI_KEYWORDS: Dict[str, List[str]] = {
    "ai": [
        "AI", "인공지능", "생성형 AI", "LLM", "딥러닝", "추론", "에이전트",
        "artificial intelligence", "generative AI",
    ],
    "semiconductor": [
        "반도체", "HBM", "DRAM", "낸드", "파운드리", "TSMC", "삼성전자",
        "SK하이닉스", "엔비디아", "Nvidia", "GPU", "ASIC",
    ],
    "data_center": [
        "데이터센터", "데이터 센터", "서버", "클라우드", "전력 수요",
        "AI 인프라", "cloud", "hyperscaler",
    ],
    "regulation": [
        "AI 규제", "수출통제", "칩스법", "CHIPS Act", "보조금",
        "export control", "sanction",
    ],
    "platform": [
        "빅테크", "플랫폼", "오픈AI", "구글", "마이크로소프트", "애플",
        "Meta", "Amazon",
    ],
}


ENERGY_KEYWORDS: Dict[str, List[str]] = {
    "oil_gas": [
        "원유", "유가", "석유", "가스", "LNG", "OPEC", "셰일", "천연가스",
        "crude", "oil", "gas",
    ],
    "nuclear": [
        "원전", "원자력", "SMR", "핵연료", "두산에너빌리티", "체코 원전",
        "nuclear",
    ],
    "renewables": [
        "재생에너지", "태양광", "풍력", "해상풍력", "수소", "ESS",
        "renewable", "solar", "wind", "hydrogen",
    ],
    "grid": [
        "전력망", "송전", "배전", "전력 수요", "전력난", "전기요금",
        "grid", "transmission",
    ],
    "climate": [
        "탄소중립", "기후", "배출권", "탄소세", "탈탄소", "전기화",
        "net zero", "carbon",
    ],
}


GEOPOLITICS_KEYWORDS: Dict[str, List[str]] = {
    "conflict": [
        "전쟁", "분쟁", "확전", "휴전", "공습", "미사일", "군사",
        "war", "conflict", "ceasefire",
    ],
    "us_china": [
        "미중", "미-중", "중국", "미국", "대만", "남중국해", "기술패권",
        "US China", "Taiwan",
    ],
    "sanctions": [
        "제재", "수출통제", "관세", "블랙리스트", "압박", "보복",
        "sanction", "tariff", "export control",
    ],
    "middle_east": [
        "중동", "이란", "이스라엘", "가자", "홍해", "후티", "호르무즈",
        "Middle East", "Red Sea",
    ],
    "alliance": [
        "동맹", "안보", "나토", "NATO", "방산", "국방", "정상회담",
        "alliance", "defense",
    ],
}


REAL_ESTATE_KEYWORDS: Dict[str, List[str]] = {
    "housing": [
        "주택", "아파트", "전세", "월세", "매매", "분양", "청약",
        "housing", "home price",
    ],
    "policy": [
        "부동산 정책", "대출 규제", "LTV", "DSR", "세제", "공급대책",
        "재건축 규제",
    ],
    "construction": [
        "건설", "PF", "프로젝트파이낸싱", "미분양", "착공", "준공",
        "construction",
    ],
    "urban": [
        "재개발", "재건축", "도시정비", "신도시", "공공주택", "임대주택",
    ],
    "global_property": [
        "상업용 부동산", "오피스", "리츠", "CRE", "mortgage", "rent",
    ],
}


TRADE_SUPPLY_CHAIN_KEYWORDS: Dict[str, List[str]] = {
    "trade": [
        "무역", "수출", "수입", "관세", "통상", "FTA", "보호무역",
        "trade", "tariff",
    ],
    "supply_chain": [
        "공급망", "리쇼어링", "니어쇼어링", "탈중국", "조달", "부품",
        "supply chain", "reshoring",
    ],
    "shipping": [
        "해운", "물류", "운임", "항만", "컨테이너", "홍해", "파나마 운하",
        "shipping", "freight",
    ],
    "industrial_policy": [
        "산업정책", "보조금", "IRA", "칩스법", "배터리", "핵심광물",
        "critical minerals",
    ],
    "export_controls": [
        "수출통제", "제재", "라이선스", "희토류", "갈륨", "흑연",
        "export control",
    ],
}


FINANCE_MARKET_KEYWORDS: Dict[str, List[str]] = {
    "equity": [
        "증시", "주식", "코스피", "코스닥", "나스닥", "S&P", "상장", "IPO",
        "equity", "stock market",
    ],
    "credit": [
        "채권", "스프레드", "회사채", "국채", "부도", "신용등급",
        "credit", "bond",
    ],
    "banking": [
        "은행", "대출", "예금", "연체율", "금융위기", "자본비율",
        "bank", "liquidity",
    ],
    "crypto": [
        "비트코인", "암호화폐", "가상자산", "스테이블코인", "ETF",
        "Bitcoin", "crypto",
    ],
    "earnings": [
        "실적", "영업이익", "매출", "가이던스", "어닝", "컨센서스",
        "earnings",
    ],
}


POLICY_REGULATION_KEYWORDS: Dict[str, List[str]] = {
    "government": [
        "정부", "정책", "법안", "규제", "시행령", "국회", "대통령",
        "government", "policy",
    ],
    "tax": [
        "세금", "세제", "법인세", "소득세", "상속세", "감세", "증세",
        "tax",
    ],
    "industry_regulation": [
        "공정위", "독점", "플랫폼 규제", "개인정보", "망사용료", "환경규제",
        "antitrust", "privacy",
    ],
    "labor": [
        "고용", "노동", "임금", "최저임금", "노조", "파업", "인력난",
        "labor", "employment",
    ],
    "public_investment": [
        "공공투자", "SOC", "인프라 예산", "국책사업", "민자사업", "PPP",
    ],
}


DEFAULT_PROFILES = {
    DEFAULT_PROFILE_ID: {
        "id": DEFAULT_PROFILE_ID,
        "name": "Infrastructure",
        "description": "Power, energy, digital, transport, water, industrial, and urban infrastructure.",
        "threshold": 12,
        "limit": 100,
        "label": "all",
        "keyword_groups": INFRA_KEYWORDS,
    },
    "macro": {
        "id": "macro",
        "name": "Macro Economy",
        "description": "Rates, inflation, growth, FX, and fiscal policy.",
        "threshold": 12,
        "limit": 100,
        "label": "all",
        "keyword_groups": MACRO_KEYWORDS,
    },
    "tech_ai": {
        "id": "tech_ai",
        "name": "AI & Semiconductors",
        "description": "AI, chips, data centers, cloud platforms, and technology regulation.",
        "threshold": 12,
        "limit": 100,
        "label": "all",
        "keyword_groups": TECH_AI_KEYWORDS,
    },
    "energy": {
        "id": "energy",
        "name": "Energy & Climate",
        "description": "Oil, gas, nuclear, renewables, power grid, and climate transition.",
        "threshold": 12,
        "limit": 100,
        "label": "all",
        "keyword_groups": ENERGY_KEYWORDS,
    },
    "geopolitics": {
        "id": "geopolitics",
        "name": "Geopolitics",
        "description": "Conflicts, sanctions, alliances, US-China rivalry, and Middle East risks.",
        "threshold": 12,
        "limit": 100,
        "label": "all",
        "keyword_groups": GEOPOLITICS_KEYWORDS,
    },
    "real_estate": {
        "id": "real_estate",
        "name": "Real Estate",
        "description": "Housing, construction, PF risks, urban redevelopment, and property policy.",
        "threshold": 12,
        "limit": 100,
        "label": "all",
        "keyword_groups": REAL_ESTATE_KEYWORDS,
    },
    "trade_supply_chain": {
        "id": "trade_supply_chain",
        "name": "Trade & Supply Chain",
        "description": "Trade, tariffs, supply chains, shipping, industrial policy, and export controls.",
        "threshold": 12,
        "limit": 100,
        "label": "all",
        "keyword_groups": TRADE_SUPPLY_CHAIN_KEYWORDS,
    },
    "finance_market": {
        "id": "finance_market",
        "name": "Finance & Markets",
        "description": "Equities, credit, banks, crypto, earnings, and market stress.",
        "threshold": 12,
        "limit": 100,
        "label": "all",
        "keyword_groups": FINANCE_MARKET_KEYWORDS,
    },
    "policy_regulation": {
        "id": "policy_regulation",
        "name": "Policy & Regulation",
        "description": "Government policy, tax, regulation, labor, and public investment.",
        "threshold": 12,
        "limit": 100,
        "label": "all",
        "keyword_groups": POLICY_REGULATION_KEYWORDS,
    },
}


FIELD_WEIGHTS = {
    "scenario_name": 10,
    "scenario_keywords": 8,
    "scenario_description": 5,
    "node_title": 7,
    "node_significance": 6,
    "node_summary": 3,
    "node_source": 1,
}


LABELS = [
    ("primary", 25),
    ("secondary", 12),
    ("weak", 6),
]


def _connect(db_path: str = scenario_db.DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_keywords(value: str) -> List[str]:
    try:
        parsed = json.loads(value or "[]")
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        return []


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()


def _term_count(text: str, term: str) -> int:
    text = _normalize(text)
    term = _normalize(term)
    if not text or not term:
        return 0
    return len(re.findall(re.escape(term), text))


def _score_field(text: str, field: str, keyword_groups: Dict[str, List[str]]) -> Tuple[int, List[Dict]]:
    weight = FIELD_WEIGHTS[field]
    score = 0
    matches: List[Dict] = []

    for tag, terms in keyword_groups.items():
        for term in terms:
            count = _term_count(text, term)
            if count <= 0:
                continue
            points = count * weight
            score += points
            matches.append({
                "tag": tag,
                "term": term,
                "field": field,
                "count": count,
                "points": points,
            })

    return score, matches


def _score_text_fields(
    fields: Iterable[Tuple[str, str]],
    keyword_groups: Dict[str, List[str]],
) -> Tuple[int, List[Dict]]:
    total = 0
    matches: List[Dict] = []
    for field, text in fields:
        score, field_matches = _score_field(text, field, keyword_groups)
        total += score
        matches.extend(field_matches)
    return total, matches


def _label_for_score(score: int) -> str:
    for label, threshold in LABELS:
        if score >= threshold:
            return label
    return "exclude"


def _tag_scores(matches: List[Dict]) -> Dict[str, int]:
    scores: Dict[str, int] = {}
    for match in matches:
        scores[match["tag"]] = scores.get(match["tag"], 0) + match["points"]
    return dict(sorted(scores.items(), key=lambda item: item[1], reverse=True))


def _compact_matches(matches: List[Dict]) -> List[str]:
    seen = {}
    for match in matches:
        key = (match["tag"], match["term"])
        if key not in seen:
            seen[key] = 0
        seen[key] += match["count"]
    return [
        f"{tag}:{term} x{count}"
        for (tag, term), count in sorted(seen.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
    ]


def chronological_nodes(result_or_nodes) -> List[Dict]:
    """Return matched nodes in scenario timeline order."""
    nodes = result_or_nodes.get("matched_nodes", []) if isinstance(result_or_nodes, dict) else result_or_nodes

    def sort_key(node: Dict):
        order = node.get("node_order")
        order_value = order if isinstance(order, int) else 10**9
        return (
            order_value,
            node.get("published_at") or "",
            node.get("created_at") or "",
            node.get("id") or 0,
        )

    return sorted(nodes or [], key=sort_key)


def _clean_keyword_groups(keyword_groups: Dict[str, List[str]]) -> Dict[str, List[str]]:
    cleaned: Dict[str, List[str]] = {}
    for raw_tag, raw_terms in (keyword_groups or {}).items():
        tag = _safe_key(raw_tag)
        if not tag:
            continue
        terms = []
        for term in raw_terms or []:
            normalized = re.sub(r"\s+", " ", str(term).strip())
            if normalized and normalized not in terms:
                terms.append(normalized)
        if terms:
            cleaned[tag] = terms
    return cleaned


def _safe_key(value: str) -> str:
    value = re.sub(r"[^\w가-힣.-]+", "_", str(value or "").strip())
    value = re.sub(r"_+", "_", value).strip("_")
    return value[:60]


def _terms_from_query(query: str) -> List[str]:
    compact = re.sub(r"\s+", " ", (query or "").strip())
    if not compact:
        return []
    terms = [compact]
    for token in re.findall(r"[0-9A-Za-z가-힣]+", compact.lower()):
        if len(token) >= 2 and token not in terms:
            terms.append(token)
    return terms[:12]


def build_keyword_groups(
    keyword_groups: Optional[Dict[str, List[str]]] = None,
    query: str = "",
) -> Dict[str, List[str]]:
    groups = _clean_keyword_groups(keyword_groups or INFRA_KEYWORDS)
    query_terms = _terms_from_query(query)
    if query_terms:
        groups = {**groups, "query": query_terms}
    return groups


def parse_keyword_groups_text(text: str) -> Dict[str, List[str]]:
    """
    Parse editable keyword text.

    Supported formats:
      power: 전력망, 송전, power grid
      AI 반도체 데이터센터

    Lines without ":" are placed under the "custom" group.
    """
    groups: Dict[str, List[str]] = {}
    custom_terms: List[str] = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            raw_tag, raw_terms = line.split(":", 1)
            tag = _safe_key(raw_tag)
            terms = [
                term.strip()
                for term in re.split(r"[,|]", raw_terms)
                if term.strip()
            ]
            if tag and terms:
                groups.setdefault(tag, [])
                for term in terms:
                    if term not in groups[tag]:
                        groups[tag].append(term)
        else:
            custom_terms.extend(term for term in re.split(r"[,|]", line) if term.strip())
    if custom_terms:
        groups["custom"] = custom_terms
    return _clean_keyword_groups(groups)


def format_keyword_groups_text(keyword_groups: Dict[str, List[str]]) -> str:
    groups = _clean_keyword_groups(keyword_groups)
    return "\n".join(
        f"{tag}: {', '.join(terms)}"
        for tag, terms in groups.items()
    )


def _default_profile(profile_id: str = DEFAULT_PROFILE_ID) -> Dict:
    profile = DEFAULT_PROFILES.get(profile_id) or DEFAULT_PROFILES[DEFAULT_PROFILE_ID]
    return json.loads(json.dumps(profile, ensure_ascii=False))


def _normalize_profile(profile: Dict) -> Dict:
    base = _default_profile()
    merged = {**base, **(profile or {})}
    profile_id = _safe_key(merged.get("id") or merged.get("name") or DEFAULT_PROFILE_ID)
    merged["id"] = profile_id or DEFAULT_PROFILE_ID
    merged["name"] = str(merged.get("name") or merged["id"])
    merged["description"] = str(merged.get("description") or "")
    merged["threshold"] = int(merged.get("threshold") or 12)
    merged["limit"] = int(merged.get("limit") or 100)
    merged["label"] = merged.get("label") if merged.get("label") in {"all", "primary", "secondary", "weak"} else "all"
    merged["keyword_groups"] = _clean_keyword_groups(merged.get("keyword_groups") or INFRA_KEYWORDS)
    return merged


def load_profiles(path: str = PROFILES_PATH) -> Dict[str, Dict]:
    profiles = {key: _normalize_profile(value) for key, value in DEFAULT_PROFILES.items()}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        saved = data.get("profiles", data) if isinstance(data, dict) else {}
        for key, value in saved.items():
            profile = _normalize_profile({**value, "id": value.get("id") or key})
            profiles[profile["id"]] = profile
    return profiles


def save_profile(profile: Dict, path: str = PROFILES_PATH) -> Dict:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    profiles = load_profiles(path)
    normalized = _normalize_profile(profile)
    profiles[normalized["id"]] = normalized
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"profiles": profiles}, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return normalized


def get_profile(profile_id: str = DEFAULT_PROFILE_ID, path: str = PROFILES_PATH) -> Dict:
    profiles = load_profiles(path)
    return profiles.get(profile_id) or profiles[DEFAULT_PROFILE_ID]


def load_scenarios(db_path: str = scenario_db.DB_PATH) -> List[Dict]:
    conn = _connect(db_path)
    scenario_rows = conn.execute(
        """
        SELECT id, name, description, category, keywords, created_at, updated_at
        FROM scenarios
        ORDER BY updated_at DESC
        """
    ).fetchall()
    node_rows = conn.execute(
        """
        SELECT id, scenario_id, title, summary, significance, url, source,
               published_at, node_order, created_at
        FROM nodes
        ORDER BY scenario_id, node_order ASC, created_at ASC
        """
    ).fetchall()
    conn.close()

    nodes_by_scenario: Dict[int, List[Dict]] = {}
    for row in node_rows:
        node = dict(row)
        nodes_by_scenario.setdefault(node["scenario_id"], []).append(node)

    scenarios = []
    for row in scenario_rows:
        scenario = dict(row)
        scenario["keywords"] = _parse_keywords(scenario["keywords"])
        scenario["nodes"] = nodes_by_scenario.get(scenario["id"], [])
        scenarios.append(scenario)
    return scenarios


def score_scenario(
    scenario: Dict,
    keyword_groups: Optional[Dict[str, List[str]]] = None,
) -> Dict:
    groups = build_keyword_groups(keyword_groups)
    meta_score, meta_matches = _score_text_fields([
        ("scenario_name", scenario.get("name", "")),
        ("scenario_description", scenario.get("description", "")),
        ("scenario_keywords", " ".join(scenario.get("keywords", []))),
    ], groups)

    scored_nodes = []
    for node in scenario.get("nodes", []):
        node_score, node_matches = _score_text_fields([
            ("node_title", node.get("title", "")),
            ("node_significance", node.get("significance", "")),
            ("node_summary", node.get("summary", "")),
            ("node_source", node.get("source", "")),
        ], groups)
        if node_score <= 0:
            continue
        scored_node = {
            **node,
            "infra_score": node_score,
            "search_score": node_score,
            "matches": node_matches,
            "matched_terms": _compact_matches(node_matches),
            "tag_scores": _tag_scores(node_matches),
        }
        scored_nodes.append(scored_node)

    scored_nodes.sort(key=lambda node: (node["infra_score"], node.get("node_order") or 0), reverse=True)
    top_node_score = sum(node["infra_score"] for node in scored_nodes[:5])
    repeat_bonus = min(len(scored_nodes) * 2, 10)
    total_score = meta_score + top_node_score + repeat_bonus
    all_matches = meta_matches + [
        match for node in scored_nodes for match in node["matches"]
    ]

    return {
        "id": scenario["id"],
        "name": scenario.get("name", ""),
        "description": scenario.get("description", ""),
        "category": scenario.get("category", ""),
        "keywords": scenario.get("keywords", []),
        "created_at": scenario.get("created_at", ""),
        "updated_at": scenario.get("updated_at", ""),
        "node_count": len(scenario.get("nodes", [])),
        "matched_node_count": len(scored_nodes),
        "infra_score": total_score,
        "search_score": total_score,
        "metadata_score": meta_score,
        "top_node_score": top_node_score,
        "repeat_bonus": repeat_bonus,
        "label": _label_for_score(total_score),
        "tag_scores": _tag_scores(all_matches),
        "matched_terms": _compact_matches(all_matches),
        "metadata_matches": meta_matches,
        "matched_nodes": scored_nodes,
    }


def search_scenarios(
    threshold: int = 12,
    limit: int = 200,
    db_path: str = scenario_db.DB_PATH,
    keyword_groups: Optional[Dict[str, List[str]]] = None,
    query: str = "",
    category: str = "",
) -> List[Dict]:
    groups = build_keyword_groups(keyword_groups, query=query)
    results = [
        score_scenario(scenario, keyword_groups=groups)
        for scenario in load_scenarios(db_path=db_path)
        if not category or scenario.get("category") == category
    ]
    results = [
        result for result in results
        if result["infra_score"] >= threshold
    ]
    results.sort(key=lambda item: (item["infra_score"], item["matched_node_count"], item["updated_at"]), reverse=True)
    return results[:limit]


def search_profile_scenarios(
    profile_id: str = DEFAULT_PROFILE_ID,
    threshold: Optional[int] = None,
    limit: Optional[int] = None,
    label: str = "all",
    query: str = "",
    category: str = "",
    db_path: str = scenario_db.DB_PATH,
) -> List[Dict]:
    profile = get_profile(profile_id)
    results = search_scenarios(
        threshold=threshold if threshold is not None else profile["threshold"],
        limit=limit if limit is not None else profile["limit"],
        db_path=db_path,
        keyword_groups=profile["keyword_groups"],
        query=query,
        category=category,
    )
    selected_label = label or profile.get("label") or "all"
    if selected_label != "all":
        results = [result for result in results if result["label"] == selected_label]
    return results


def search_infra_scenarios(
    threshold: int = 12,
    limit: int = 200,
    db_path: str = scenario_db.DB_PATH,
) -> List[Dict]:
    return search_scenarios(
        threshold=threshold,
        limit=limit,
        db_path=db_path,
        keyword_groups=INFRA_KEYWORDS,
    )


def _safe_filename(value: str) -> str:
    value = re.sub(r"[^\w가-힣.-]+", "_", value.strip())
    value = re.sub(r"_+", "_", value).strip("_")
    return value[:80] or "scenario"


def _format_matches(matches: List[str], limit: int = 20) -> str:
    if not matches:
        return "-"
    visible = matches[:limit]
    suffix = "" if len(matches) <= limit else f"\n  ... +{len(matches) - limit} more"
    return "\n  ".join(visible) + suffix


def format_scenario_txt(result: Dict, profile_name: str = "") -> str:
    lines = [
        f"# [{result['id']}] {result['name']}",
        "",
        f"profile: {profile_name or '-'}",
        f"label: {result['label']}",
        f"score: {result['infra_score']} (metadata={result['metadata_score']}, nodes={result['top_node_score']}, repeat={result['repeat_bonus']})",
        f"category: {result['category']}",
        f"updated_at: {result['updated_at']}",
        f"nodes: {result['node_count']} total / {result['matched_node_count']} matched",
        f"keywords: {', '.join(result['keywords'])}",
        "",
        "## Description",
        result["description"] or "",
        "",
        "## Matched Tags",
    ]

    if result["tag_scores"]:
        for tag, score in result["tag_scores"].items():
            lines.append(f"- {tag}: {score}")
    else:
        lines.append("- none")

    lines.extend([
        "",
        "## Matched Terms",
        f"  {_format_matches(result['matched_terms'])}",
        "",
        "## Matched News Nodes",
    ])

    for node in chronological_nodes(result):
        lines.extend([
            "",
            f"### ({node.get('node_order')}) {node.get('title', '')}",
            f"- score: {node['infra_score']}",
            f"- source: {node.get('source', '')}",
            f"- published_at: {node.get('published_at', '')}",
            f"- url: {node.get('url', '')}",
            f"- matched_terms: {_format_matches(node['matched_terms'], limit=12)}",
            f"- significance: {node.get('significance', '')}",
            f"- summary: {node.get('summary', '')}",
        ])

    return "\n".join(lines).strip() + "\n"


def export_txt_files(
    results: List[Dict],
    output_dir: str = DEFAULT_OUTPUT_DIR,
    profile_name: str = "",
    clear_existing: bool = False,
) -> List[str]:
    os.makedirs(output_dir, exist_ok=True)
    if clear_existing:
        for filename in os.listdir(output_dir):
            if filename.endswith(".txt"):
                os.remove(os.path.join(output_dir, filename))
    written = []
    index_lines = [
        "# Scenario Search Results",
        "",
        f"generated_at: {datetime.now().isoformat()}",
        f"profile: {profile_name or '-'}",
        f"total: {len(results)}",
        "",
    ]

    for rank, result in enumerate(results, 1):
        filename = f"{rank:03d}_{result['id']}_{_safe_filename(result['name'])}.txt"
        path = os.path.join(output_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(format_scenario_txt(result, profile_name=profile_name))
        written.append(path)
        index_lines.append(
            f"{rank:03d}. [{result['label']}] score={result['infra_score']} "
            f"matched_nodes={result['matched_node_count']} id={result['id']} {result['name']} -> {filename}"
        )

    index_path = os.path.join(output_dir, "index.txt")
    with open(index_path, "w", encoding="utf-8") as f:
        f.write("\n".join(index_lines).strip() + "\n")
    written.insert(0, index_path)
    return written


def main():
    parser = argparse.ArgumentParser(description="Search scenarios with saved keyword-group profiles.")
    parser.add_argument("--profile", default=DEFAULT_PROFILE_ID, help="Saved profile id to use.")
    parser.add_argument("--query", default="", help="Extra search text to add as a query keyword group.")
    parser.add_argument("--category", default="", help="Optional scenario category filter.")
    parser.add_argument("--threshold", type=int, default=12, help="Minimum infra score to include.")
    parser.add_argument("--limit", type=int, default=200, help="Maximum scenarios to return/export.")
    parser.add_argument("--label", default="all", choices=["all", "primary", "secondary", "weak"])
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="TXT export directory.")
    parser.add_argument("--no-export", action="store_true", help="Only print summary, do not write files.")
    args = parser.parse_args()

    profile = get_profile(args.profile)
    results = search_profile_scenarios(
        profile_id=args.profile,
        threshold=args.threshold,
        limit=args.limit,
        label=args.label,
        query=args.query,
        category=args.category,
    )
    for rank, result in enumerate(results[:30], 1):
        tags = ", ".join(list(result["tag_scores"].keys())[:4])
        print(
            f"{rank:02d}. [{result['label']}] score={result['infra_score']} "
            f"nodes={result['matched_node_count']} id={result['id']} "
            f"{result['name']} ({tags})"
        )

    print(f"\n총 {len(results)}개 시나리오가 [{profile['name']}] threshold {args.threshold} 이상입니다.")
    if not args.no_export:
        written = export_txt_files(results, output_dir=args.output_dir, profile_name=profile["name"])
        print(f"TXT 저장 완료: {args.output_dir} ({len(written)} files incl. index)")


if __name__ == "__main__":
    main()
