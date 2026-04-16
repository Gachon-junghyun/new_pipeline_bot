"""
FastAPI — 시나리오 RAG 엔드포인트
====================================
실행:
  uvicorn api:app --reload --port 8000

엔드포인트:
  GET  /scenarios                    — 시나리오 목록
  GET  /scenarios/stats              — 통계
  GET  /scenarios/{id}               — 시나리오 + 노드 상세
  POST /scenarios/search             — 텍스트 검색
  GET  /scenarios/category/{cat}     — 카테고리별 조회
  POST /rag/query                    — RAG 분석 쿼리
  GET  /rag/company/{company_name}   — 기업별 시나리오 분석
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional

import scenario_db
import scenario_builder

# DB 초기화 (없으면 생성)
scenario_db.init_db()

app = FastAPI(
    title="Pipeline Bot — Scenario RAG API",
    version="1.0.0",
    description="뉴스 기반 시나리오 그래프 + Gemini RAG 분석 API",
)


# ─────────────────────────────────────────────
# 요청 모델
# ─────────────────────────────────────────────
class RagQueryRequest(BaseModel):
    query: str = Field(..., description="분석 질문")
    company: Optional[str] = Field(None, description="분석할 기업/주체 이름")
    category: Optional[str] = Field(
        None,
        description="카테고리 필터: energy | finance | geopolitics | tech | trade | macro | corporate | other",
    )


class SearchRequest(BaseModel):
    query: str = Field(..., description="검색 키워드")
    category: Optional[str] = Field(None, description="카테고리 필터")
    limit: int = Field(10, ge=1, le=50, description="최대 결과 수")


# ─────────────────────────────────────────────
# 시나리오 엔드포인트
# ─────────────────────────────────────────────
@app.get("/scenarios", summary="시나리오 목록 조회")
def list_scenarios(
    category: Optional[str] = Query(None, description="카테고리 필터"),
    limit: int = Query(20, ge=1, le=100),
):
    if category:
        scenarios = scenario_db.get_scenarios_by_category(category, limit=limit)
    else:
        scenarios = scenario_db.get_all_scenarios(limit=limit)
    return {"scenarios": scenarios, "total": len(scenarios)}


@app.get("/scenarios/stats", summary="시나리오 통계")
def get_stats():
    return scenario_db.get_stats()


@app.post("/scenarios/search", summary="시나리오 텍스트 검색")
def search_scenarios(req: SearchRequest):
    results = scenario_db.search_scenarios(
        req.query, category=req.category, limit=req.limit
    )
    return {"results": results, "total": len(results)}


@app.get("/scenarios/category/{category}", summary="카테고리별 시나리오")
def scenarios_by_category(
    category: str,
    limit: int = Query(20, ge=1, le=100),
):
    scenarios = scenario_db.get_scenarios_by_category(category, limit=limit)
    return {"category": category, "scenarios": scenarios, "total": len(scenarios)}


@app.get("/scenarios/{scenario_id}", summary="시나리오 상세 (노드 포함)")
def get_scenario(scenario_id: int):
    scenario = scenario_db.get_scenario_with_nodes(scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return scenario


# ─────────────────────────────────────────────
# RAG 엔드포인트
# ─────────────────────────────────────────────
@app.post("/rag/query", summary="RAG 분석 쿼리")
def rag_query(req: RagQueryRequest):
    """
    관련 시나리오를 검색하고 Gemini로 분석 응답을 생성합니다.

    - `company`가 있으면 회사명으로 관련 시나리오를 먼저 검색
    - `category`가 있으면 해당 카테고리 시나리오를 우선 포함 (예: energy 기업이면 "energy")
    """
    result = scenario_builder.rag_query(
        query=req.query,
        company=req.company,
        category=req.category,
    )
    return result


@app.get("/rag/company/{company_name}", summary="기업별 시나리오 분석")
def rag_company(
    company_name: str,
    category: Optional[str] = Query(
        None,
        description="카테고리 힌트 (예: energy, finance). 에너지 기업이면 'energy' 전달 권장",
    ),
    question: str = Query(
        "",
        description="추가 질문 (없으면 기본 분석 질문 사용)",
    ),
):
    """
    기업명으로 관련 시나리오를 조회하고 AI 분석을 반환합니다.
    에너지 기업이면 category=energy 를 함께 전달하면 더 정확한 시나리오가 연결됩니다.
    """
    query = question if question else f"{company_name}와 관련된 주요 이슈·시나리오 분석 및 향후 전망"
    result = scenario_builder.rag_query(
        query=query,
        company=company_name,
        category=category,
    )
    return result
