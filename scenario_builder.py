"""
Gemini 2.5 Flash 기반 시나리오 분류/생성기
============================================
뉴스 기사를 받아:
  1. 기존 시나리오 중 유사한 것이 있으면 → 노드 추가
  2. 없으면 → 새 시나리오 생성 + 첫 노드 추가
  3. RAG 쿼리: 회사·이슈 → 관련 시나리오 컨텍스트 → AI 분석 응답
"""

import os
import re
import json
import logging
import html
from typing import Optional, Dict, List

from google import genai
from google.genai import types

import scenario_db

log = logging.getLogger("scenario_builder")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
SIMILARITY_THRESHOLD = int(os.getenv("SIMILARITY_THRESHOLD", "65"))
MODEL_NAME = "gemini-2.5-flash"

_client: Optional[genai.Client] = None

# ─────────────────────────────────────────────
# 세션 토큰 누적 카운터
# ─────────────────────────────────────────────
_token_stats = {
    "calls": 0,
    "input_total": 0,
    "output_total": 0,
}


def get_token_stats() -> dict:
    return {**_token_stats, "total": _token_stats["input_total"] + _token_stats["output_total"]}


def reset_token_stats():
    _token_stats["calls"] = 0
    _token_stats["input_total"] = 0
    _token_stats["output_total"] = 0


# ─────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────
def _get_client() -> genai.Client:
    global _client
    if _client is None:
        _client = genai.Client(api_key=GEMINI_API_KEY)
        print(f"[모델] {MODEL_NAME} | thinking OFF (budget=0)")
    return _client


def _call_gemini(prompt: str, label: str = "") -> str:
    client = _get_client()
    _token_stats["calls"] += 1
    call_num = _token_stats["calls"]

    print(f"\n[Gemini #{call_num}] {label} | 요청 시작...")

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=0),
        ),
    )

    usage = response.usage_metadata
    in_tok  = getattr(usage, "prompt_token_count", 0) or 0
    out_tok = getattr(usage, "candidates_token_count", 0) or 0
    total_tok = in_tok + out_tok

    _token_stats["input_total"]  += in_tok
    _token_stats["output_total"] += out_tok

    print(
        f"[Gemini #{call_num}] {label}\n"
        f"  input:  {in_tok:,} tokens\n"
        f"  output: {out_tok:,} tokens\n"
        f"  합계:   {total_tok:,} tokens\n"
        f"  ── 세션 누계: 호출 {call_num}회 | "
        f"in {_token_stats['input_total']:,} / out {_token_stats['output_total']:,} / "
        f"total {_token_stats['input_total'] + _token_stats['output_total']:,}"
    )

    return response.text.strip()


def _strip_html(text: str) -> str:
    """HTML 태그·엔티티 제거 후 순수 텍스트 반환."""
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)      # 태그 제거
    text = re.sub(r"\s+", " ", text).strip()   # 공백 정리
    return text


def _parse_json(text: str) -> dict:
    """Gemini 응답에서 JSON 추출. 코드블록·전후 텍스트 모두 처리."""
    # 1) ```json ... ``` 블록 우선 추출
    block = re.search(r"```(?:json)?\s*([\s\S]+?)```", text)
    if block:
        text = block.group(1)
    else:
        # 2) 중괄호로 감싸진 첫 번째 JSON 객체 추출
        obj = re.search(r"\{[\s\S]+\}", text)
        if obj:
            text = obj.group(0)
    return json.loads(text.strip())


# ─────────────────────────────────────────────
# 1. 시나리오 매칭
# ─────────────────────────────────────────────
def find_matching_scenario(article: dict,
                            scenarios: List[Dict]) -> Optional[Dict]:
    """
    기존 시나리오 중 article과 가장 관련 있는 것을 찾는다.
    Returns: {"scenario_id", "scenario_name", "similarity", "significance"}
             또는 None (유사 시나리오 없음)
    """
    if not scenarios:
        return None

    # 최대 50개만 검토 (프롬프트 길이 제한)
    candidates = scenarios[:50]
    scenario_list = "\n".join(
        f"ID={s['id']} | 이름: {s['name']} | 설명: {(s.get('description') or '')[:80]} "
        f"| 카테고리: {s['category']} | 키워드: {', '.join(s['keywords'][:5])}"
        for s in candidates
    )

    prompt = f"""당신은 거시경제·산업·지정학 트렌드를 추적하는 시나리오 분석가입니다.

[뉴스 기사]
제목: {article.get('title', '')}
요약: {article.get('summary', '')}
출처: {article.get('source', '')}

━━━ STEP 1: 중요도 판단 ━━━
아래에 해당하면 "skip": true로 처리하세요 (시나리오 가치 없음):
- 인사발령, 부고, 인사이동
- 지역 행사·축제·생활정보
- 단순 사건사고 (화재, 교통사고 등 1회성)
- 스포츠 경기 결과
- 연예·문화 소식
- 루틴한 기업 공시 (정기 인사, 단순 계약 체결)

아래에 해당하면 중요 뉴스로 판단하세요:
- 거시경제 변수 (금리, 환율, 무역, 재정정책)
- 산업 구조 변화 (반도체, 에너지, AI, 제조업 재편)
- 지정학 이슈 (무역전쟁, 제재, 분쟁, 동맹 변화)
- 기업 전략 변화 (대형 M&A, IPO, 신사업 진출, 구조조정)
- 기술 패러다임 전환 (AI, 우주, 바이오, 에너지전환)
- 규제·정책 변화 (법안, 세금, 환경규제)
- 글로벌 공급망 이슈

[기존 시나리오 목록]
{scenario_list}

━━━ STEP 2: 매칭 판단 (중요 뉴스인 경우만) ━━━
위 시나리오 목록 중 이 뉴스와 같은 테마·이슈에 속하는 것이 있으면 매칭하세요.

JSON 형식으로만 응답 (다른 텍스트 없이):
중요하지 않은 뉴스: {{"skip": true, "reason": "스킵 이유"}}
중요하고 매칭 있음: {{"skip": false, "match": true, "scenario_id": <ID>, "scenario_name": "<이름>", "similarity": <0-100>, "significance": "<이 뉴스가 해당 시나리오 흐름에서 갖는 의미 한 문장>"}}
중요하지만 매칭 없음: {{"skip": false, "match": false, "similarity": 0}}"""

    title_short = article.get("title", "")[:30]
    print(f"\n[MATCH?] \"{title_short}\" vs 시나리오 {len(candidates)}개 검색 중...")
    try:
        response = _call_gemini(prompt, label=f"triage+매칭 | \"{title_short}\"")
        result = _parse_json(response)
        if result.get("skip"):
            print(f"[SKIP] {result.get('reason', '')}")
            return {"skip": True}
        if result.get("match") and result.get("similarity", 0) >= SIMILARITY_THRESHOLD:
            print(f"[MATCH ✓] → [{result['scenario_name']}] (유사도 {result['similarity']})")
            return result
        print(f"[MATCH ✗] 유사 시나리오 없음 → 새 시나리오 생성 대기")
        return None
    except Exception as e:
        log.error(f"시나리오 매칭 오류: {e}")
        return None


# ─────────────────────────────────────────────
# 2. 새 시나리오 생성
# ─────────────────────────────────────────────
def create_scenario_from_article(article: dict) -> Dict:
    """
    뉴스 기사를 바탕으로 새 시나리오 메타데이터를 생성한다.
    """
    prompt = f"""당신은 거시경제·산업·지정학 트렌드를 추적하는 시나리오 분석가입니다.
이 뉴스가 속하는 "거시 테마 시나리오"를 설계하세요.

시나리오란 여러 뉴스 이벤트가 누적되며 전개되는 하나의 큰 이슈 흐름입니다.
개별 사건이 아닌, 그 사건이 속한 더 큰 그림을 표현해야 합니다.

[뉴스 기사]
제목: {article.get('title', '')}
요약: {article.get('summary', '')}
출처: {article.get('source', '')}

━━━ 좋은 시나리오 이름 예시 ━━━
"미-중 AI 패권경쟁"         (개별 AI 규제 뉴스가 들어올 때)
"글로벌 에너지 전환 가속"   (재생에너지·탈탄소 관련 뉴스)
"연준 금리 정책 피벗"       (금리 인하 기대 관련 뉴스)
"한국 반도체 공급망 재편"   (삼성·SK 관련 전략 뉴스)
"SpaceX 민간우주 상업화"    (발사·IPO·계약 뉴스)
"트럼프 관세 2.0"           (미국 보호무역 뉴스)
"중동 지정학 리스크"        (이란·이스라엘·호르무즈 뉴스)

━━━ 나쁜 이름 예시 (이렇게 하지 마세요) ━━━
"삼성전자 2분기 실적 발표"  → 너무 구체적, 1회성
"연합뉴스 기사"             → 의미 없음
"경제 뉴스"                 → 너무 광범위

카테고리 선택 기준:
- energy: 에너지, 석유, 가스, 재생에너지, 전력
- finance: 금리, 환율, 주식, 채권, 은행, 암호화폐
- geopolitics: 외교, 전쟁, 제재, 동맹, 안보
- tech: AI, 반도체, 우주, 바이오, 플랫폼
- trade: 무역, 관세, 공급망, 수출입
- macro: 거시경제, 고용, 물가, GDP, 재정정책
- corporate: 기업 M&A, IPO, 구조조정, 전략 변화
- other: 위에 해당 없는 경우 (가능한 피할 것)

JSON 형식으로만 응답 (다른 텍스트 없이):
{{
  "name": "테마 시나리오 이름 (15자 이내, 거시 이슈 중심)",
  "description": "이 시나리오가 무엇이며 왜 중요한지, 어떤 방향으로 전개될 수 있는지 2~3문장",
  "category": "위 카테고리 중 하나",
  "keywords": ["핵심키워드1", "핵심키워드2", "핵심키워드3", "핵심키워드4", "핵심키워드5"],
  "first_node_significance": "이 뉴스가 시나리오 흐름의 시작점으로 갖는 의미 한 문장"
}}"""

    title_short = article.get("title", "")[:30]
    print(f"\n[CREATE] \"{title_short}\" 로 새 시나리오 생성 중...")
    try:
        response = _call_gemini(prompt, label=f"시나리오 생성 | \"{title_short}\"")
        result = _parse_json(response)
        # 필수 필드 검증
        if not result.get("name") or not result.get("category"):
            print(f"[CREATE FAIL] 필수 필드 누락 — 스킵: {result}")
            return None
        if result.get("category") not in {
            "energy", "finance", "geopolitics", "tech", "trade", "macro", "corporate", "other"
        }:
            result["category"] = "other"
        return result
    except Exception as e:
        log.error(f"시나리오 생성 오류 (JSON 파싱 실패) — 스킵: {e}")
        return None  # fallback으로 쓰레기 데이터 만들지 않음


# ─────────────────────────────────────────────
# 3. 메인 파이프라인
# ─────────────────────────────────────────────
def process_article(article: dict) -> Dict:
    """
    뉴스 기사를 처리하여 시나리오에 연결하거나 새 시나리오를 생성한다.

    Returns:
        {
            "action":        "added" | "created",
            "scenario_id":   int,
            "scenario_name": str,
            "node_id":       int,
            "similarity":    int (added 일 때만)
        }
    """
    # ── HTML 클린 (Google News RSS 등이 summary에 태그를 삽입함)
    article = {
        **article,
        "title":   _strip_html(article.get("title", "")),
        "summary": _strip_html(article.get("summary", "")),
    }

    scenarios = scenario_db.find_candidate_scenarios_for_article(article)
    print(
        f"\n{'='*55}\n"
        f"[PIPELINE] {article.get('title', '')[:50]}\n"
        f"  source: {article.get('source','')} | 매칭 후보 {len(scenarios)}개\n"
        f"{'='*55}"
    )

    # ── triage + 기존 시나리오 매칭
    match = find_matching_scenario(article, scenarios)

    # 중요하지 않은 뉴스 → 스킵
    if match and match.get("skip"):
        return {"action": "skipped", "scenario_id": None, "scenario_name": None, "node_id": None}

    if match and not match.get("skip"):
        node_id = scenario_db.add_node_to_scenario(
            scenario_id=match["scenario_id"],
            title=article.get("title", ""),
            summary=article.get("summary", ""),
            significance=match.get("significance", ""),
            url=article.get("url", ""),
            source=article.get("source", ""),
            published_at=article.get("published", ""),
        )
        log.info(
            f"[ADDED] [{match['scenario_name']}] ← {article.get('title', '')[:40]} "
            f"(유사도 {match['similarity']})"
        )
        return {
            "action": "added",
            "scenario_id": match["scenario_id"],
            "scenario_name": match["scenario_name"],
            "node_id": node_id,
            "similarity": match["similarity"],
        }

    # ── 새 시나리오 생성
    meta = create_scenario_from_article(article)
    if not meta:
        print(f"[SKIP] Gemini 시나리오 생성 실패 — 저장 안 함")
        return {"action": "skipped", "scenario_id": None, "scenario_name": None, "node_id": None}

    scenario_id = scenario_db.create_scenario(
        name=meta["name"],
        description=meta["description"],
        category=meta["category"],
        keywords=meta.get("keywords", []),
    )
    node_id = scenario_db.add_node_to_scenario(
        scenario_id=scenario_id,
        title=article.get("title", ""),
        summary=article.get("summary", ""),
        significance=meta.get("first_node_significance", "시나리오 시작 이벤트"),
        url=article.get("url", ""),
        source=article.get("source", ""),
        published_at=article.get("published", ""),
    )
    log.info(
        f"[CREATED] [{meta['name']}] ({meta['category']}) ← {article.get('title', '')[:40]}"
    )
    return {
        "action": "created",
        "scenario_id": scenario_id,
        "scenario_name": meta["name"],
        "node_id": node_id,
    }


# ─────────────────────────────────────────────
# 4. RAG 쿼리
# ─────────────────────────────────────────────
def rag_query(query: str, company: Optional[str] = None,
              category: Optional[str] = None) -> Dict:
    """
    관련 시나리오를 검색하고 Gemini로 분석 응답을 생성한다.

    - company가 있으면 회사명으로 시나리오 검색 후, 해당 카테고리 시나리오도 보강
    - category가 있으면 카테고리 시나리오 우선 포함
    """
    # ── 관련 시나리오 수집
    found: Dict[int, Dict] = {}

    if company:
        for s in scenario_db.search_scenarios(company, category=category, limit=5):
            found[s["id"]] = s

    search_term = company or query
    for s in scenario_db.search_scenarios(search_term, limit=5):
        found[s["id"]] = s

    if category and len(found) < 5:
        for s in scenario_db.get_scenarios_by_category(category, limit=5):
            found[s["id"]] = s

    if not found:
        return {"answer": "관련 시나리오가 없습니다.", "scenarios": []}

    # ── 컨텍스트 구성 (각 시나리오의 최근 노드 10개)
    context_parts = []
    scenario_meta = []
    for sid, s in list(found.items())[:6]:
        full = scenario_db.get_scenario_with_nodes(sid)
        if not full:
            continue
        nodes_text = "\n".join(
            f"  [{n['node_order']}] {n['title']} — {n['significance'] or n['summary'][:60]}"
            for n in full["nodes"][-10:]
        )
        context_parts.append(
            f"[시나리오: {full['name']} | 카테고리: {full['category']}]\n"
            f"설명: {full['description']}\n"
            f"진행 흐름 (최근 {len(full['nodes'][-10:])}개 이벤트):\n{nodes_text}"
        )
        scenario_meta.append({
            "id": full["id"],
            "name": full["name"],
            "category": full["category"],
            "node_count": len(full["nodes"]),
        })

    context = "\n\n".join(context_parts)
    company_line = f"\n분석 대상 기업/주체: {company}" if company else ""

    prompt = f"""다음 시나리오 정보를 바탕으로 질문에 답하세요.

[관련 시나리오 정보]
{context}

[질문]{company_line}
{query}

위 시나리오들을 참고하여 구체적인 분석을 제공하세요.
이 이슈가 어떻게 전개되어 왔고, 현재 상태는 어떤지, 앞으로 주목할 포인트는 무엇인지 중심으로 답변하세요."""

    print(f"\n[RAG] 쿼리: \"{query[:50]}\" | 참조 시나리오 {len(scenario_meta)}개")
    try:
        answer = _call_gemini(prompt, label=f"RAG | \"{query[:40]}\"")
        return {"answer": answer, "scenarios": scenario_meta}
    except Exception as e:
        log.error(f"RAG 쿼리 오류: {e}")
        return {"answer": f"오류: {e}", "scenarios": scenario_meta}
