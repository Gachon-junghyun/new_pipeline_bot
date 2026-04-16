"""
네이버 금융 테마/업종별 시세 크롤러 + Gemini 브리핑
=====================================================
등락률 +3% 이상인 테마·업종을 크롤링한 뒤
Gemini 2.5 Flash 로 한국어 투자 브리핑 메시지를 생성한다.

주요 함수:
  crawl_hot_themes(threshold=3.0)   → 3% 이상 테마 DataFrame
  crawl_hot_upjong(threshold=3.0)   → 3% 이상 업종 DataFrame
  build_briefing_message(label)     → 텔레그램용 브리핑 문자열
"""

import os
import re
import time
import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd
from google import genai
from google.genai import types

logger = logging.getLogger("market_briefing")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
MODEL_NAME = "gemini-2.5-flash"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

THEME_URL  = "https://finance.naver.com/sise/theme.naver"
UPJONG_URL = "https://finance.naver.com/sise/sise_group.naver"

_client: Optional[genai.Client] = None


def _get_client() -> genai.Client:
    global _client
    if _client is None:
        _client = genai.Client(api_key=GEMINI_API_KEY)
    return _client


# ─────────────────────────────────────────
# 크롤러 내부 헬퍼
# ─────────────────────────────────────────

def _fetch_soup(base_url: str, params: dict, referer: str) -> BeautifulSoup:
    headers = {**HEADERS, "Referer": referer}
    session = requests.Session()
    session.get("https://finance.naver.com", headers=headers, timeout=10)
    resp = session.get(base_url, params=params, headers=headers, timeout=10)
    resp.encoding = "euc-kr"
    return BeautifulSoup(resp.text, "lxml")


def _parse_change_rate(text: str) -> float:
    """'+3.45%' 또는 '-1.20%' 문자열을 float 으로 변환. 파싱 실패 시 0.0"""
    text = text.strip().replace(",", "").replace("%", "").replace("+", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def _parse_rows(soup: BeautifulSoup, name_col: str) -> list[dict]:
    table = soup.select_one("table.type_1")
    if table is None:
        logger.warning("테이블을 찾지 못했습니다 (차단 가능성).")
        return []

    rows = []
    for tr in table.select("tr"):
        tds = tr.select("td")
        if len(tds) < 6:
            continue
        name_tag = tds[0].select_one("a")
        if name_tag is None:
            continue

        name = name_tag.get_text(strip=True)
        link = "https://finance.naver.com" + name_tag["href"]

        rate_span = tds[1].select_one("span")
        change_rate_str = rate_span.get_text(strip=True) if rate_span else tds[1].get_text(strip=True)
        change_rate_val = _parse_change_rate(change_rate_str)

        stock_total = tds[2].get_text(strip=True)
        up_count    = tds[3].get_text(strip=True)
        flat_count  = tds[4].get_text(strip=True)
        down_count  = tds[5].get_text(strip=True)

        rows.append({
            name_col:       name,
            "등락률":       change_rate_str,
            "등락률_float": change_rate_val,
            "종목수":       stock_total,
            "상승":         up_count,
            "보합":         flat_count,
            "하락":         down_count,
            "링크":         link,
        })
    return rows


# ─────────────────────────────────────────
# 공개 크롤러
# ─────────────────────────────────────────

def crawl_themes(field: str = "change_rate", ordering: str = "desc") -> pd.DataFrame:
    """테마별 전체 시세 크롤링 (최대 3페이지)"""
    all_rows: list[dict] = []
    first_names: set[str] = set()

    for page in range(1, 4):
        soup = _fetch_soup(
            THEME_URL,
            {"field": field, "ordering": ordering, "page": page},
            referer=THEME_URL,
        )
        rows = _parse_rows(soup, "테마명")
        if not rows:
            break

        page_names = {r["테마명"] for r in rows}
        if page == 1:
            first_names = page_names
        elif page_names == first_names:
            break

        all_rows.extend(rows)
        time.sleep(0.5)

    return pd.DataFrame(all_rows)


def crawl_upjong(field: str = "change_rate", ordering: str = "desc") -> pd.DataFrame:
    """업종별 전체 시세 크롤링 (페이지네이션 없음)"""
    soup = _fetch_soup(
        UPJONG_URL,
        {"type": "upjong", "field": field, "ordering": ordering},
        referer=f"{UPJONG_URL}?type=upjong",
    )
    rows = _parse_rows(soup, "업종명")
    return pd.DataFrame(rows)


def crawl_hot_themes(threshold: float = 3.0) -> pd.DataFrame:
    """등락률 threshold% 이상 테마 반환"""
    df = crawl_themes()
    if df.empty:
        return df
    return df[df["등락률_float"] >= threshold].sort_values("등락률_float", ascending=False)


def crawl_hot_upjong(threshold: float = 3.0) -> pd.DataFrame:
    """등락률 threshold% 이상 업종 반환"""
    df = crawl_upjong()
    if df.empty:
        return df
    return df[df["등락률_float"] >= threshold].sort_values("등락률_float", ascending=False)


# ─────────────────────────────────────────
# Gemini 브리핑 생성
# ─────────────────────────────────────────

def _df_to_text(df: pd.DataFrame, name_col: str) -> str:
    if df.empty:
        return "  (해당 없음)"
    lines = []
    for _, r in df.iterrows():
        up   = r.get("상승", "-")
        down = r.get("하락", "-")
        lines.append(f"  • {r[name_col]} {r['등락률']}  (상승 {up} / 하락 {down})")
    return "\n".join(lines)


def _gemini_briefing(theme_text: str, upjong_text: str, label: str) -> str:
    """Gemini 를 호출해 투자 브리핑 텍스트 반환"""
    prompt = f"""당신은 한국 주식 시장 전문 애널리스트입니다.
아래는 오늘 {label} 기준, 네이버 금융에서 수집한 +3% 이상 급등 테마·업종 데이터입니다.

[급등 테마]
{theme_text}

[급등 업종]
{upjong_text}

위 데이터를 바탕으로 다음 형식으로 간결한 한국어 투자 브리핑을 작성하세요:
1. 오늘의 핵심 흐름 (2~3줄 요약)
2. 주목 테마 TOP 3 와 각각의 상승 배경 추정 (1~2줄씩)
3. 주목 업종 TOP 3 와 각각의 상승 배경 추정 (1~2줄씩)
4. 투자자 유의사항 (1줄)

텔레그램 메시지용이므로 너무 길지 않게(전체 500자 이내), 이모지 적절히 사용하세요."""

    try:
        client = _get_client()
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.4,
                max_output_tokens=600,
            ),
        )
        return response.text.strip()
    except Exception as e:
        logger.error(f"Gemini 브리핑 생성 오류: {e}")
        return "⚠️ AI 브리핑 생성에 실패했습니다."


# ─────────────────────────────────────────
# 외부 공개 메인 함수
# ─────────────────────────────────────────

def build_briefing_message(label: str = "장중", threshold: float = 3.0) -> str:
    """
    +3% 이상 테마·업종을 크롤링하고 Gemini 브리핑을 생성해
    텔레그램 전송 가능한 문자열 반환.

    Args:
        label: '장중' 또는 '장마감 후' 등 메시지에 표시될 시점 라벨
        threshold: 급등 기준 등락률 (기본 3.0%)
    """
    logger.info(f"[브리핑] 크롤링 시작 (label={label}, threshold={threshold}%)")

    try:
        hot_themes = crawl_hot_themes(threshold).head(10)
        hot_upjong = crawl_hot_upjong(threshold).head(10)
    except Exception as e:
        logger.error(f"[브리핑] 크롤링 오류: {e}")
        return f"⚠️ 시세 크롤링 실패: {e}"

    theme_count  = len(hot_themes)
    upjong_count = len(hot_upjong)
    logger.info(f"[브리핑] 급등 테마 {theme_count}개 / 업종 {upjong_count}개")

    theme_text  = _df_to_text(hot_themes, "테마명")
    upjong_text = _df_to_text(hot_upjong, "업종명")

    # 급등 항목이 아예 없으면 Gemini 호출 없이 바로 반환
    if theme_count == 0 and upjong_count == 0:
        return (
            f"📊 *{label} 테마/업종 브리핑*\n\n"
            f"현재 +{threshold}% 이상 급등한 테마·업종이 없습니다."
        )

    ai_text = _gemini_briefing(theme_text, upjong_text, label)

    # 헤더 + 원시 데이터 + AI 분석 조합
    header = (
        f"📊 *{label} 급등 테마/업종 브리핑* (+{threshold:.0f}% 이상)\n"
        f"{'─'*30}\n"
    )

    raw_section = ""
    if theme_count > 0:
        raw_section += f"🔥 *급등 테마* ({theme_count}개)\n{theme_text}\n\n"
    if upjong_count > 0:
        raw_section += f"📈 *급등 업종* ({upjong_count}개)\n{upjong_text}\n\n"

    ai_section = f"{'─'*30}\n🤖 *AI 브리핑*\n{ai_text}"

    full_msg = header + raw_section + ai_section

    # 텔레그램 메시지 한도(4096자) 처리
    if len(full_msg) > 4000:
        full_msg = full_msg[:4000] + "\n...(생략)"

    return full_msg


# ─────────────────────────────────────────
# 단독 실행 테스트
# ─────────────────────────────────────────
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    logging.basicConfig(level=logging.INFO)

    msg = build_briefing_message(label="장중 테스트")
    print(msg)
