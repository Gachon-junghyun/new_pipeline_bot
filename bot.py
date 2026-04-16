"""
파이프라인 봇 — new_pipeline_bot
====================================
기존 news_elert 기능 + 시나리오 그래프 기능

명령어:
  /start, /help          — 시작 / 도움말
  /add <키워드>           — 알림 키워드 추가
  /remove <키워드>        — 알림 키워드 삭제
  /keywords              — 등록된 키워드 목록
  /search <키워드>        — DB에서 뉴스 검색
  /status                — 봇 통계
  /fetch                 — 즉시 뉴스 수집 (관리자)
  /scenarios [카테고리]   — 현재 시나리오 목록
  /scenario <id>         — 시나리오 상세 (노드 타임라인)
  /rag <질문>             — AI 시나리오 분석

스케줄:
  매시간   → veryfast 수집 → 키워드 알림 + 시나리오 파이프라인
  매일 22시 → full 수집 → 키워드 알림 + 시나리오 파이프라인

환경 변수 (.env):
  BOT_TOKEN            — 텔레그램 봇 토큰
  ADMIN_USER_ID        — 관리자 Telegram User ID
  GEMINI_API_KEY       — Google Gemini API 키
  SIMILARITY_THRESHOLD — 시나리오 매칭 임계값 (기본 65)
  MAX_PROCESS_PER_RUN  — 한 사이클에 처리할 최대 기사 수 (기본 30)
"""

import asyncio
import logging
import os
import sys
from datetime import time as dtime, timezone
from functools import partial

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import Conflict
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

import db_manager as db
import news_fetcher as fetcher
import scenario_db
import scenario_builder
import naver_market_briefing as briefing

# ─────────────────────────────────────────────
# 0. 환경 설정 & 로깅
# ─────────────────────────────────────────────
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
MAX_PROCESS_PER_RUN = int(os.getenv("MAX_PROCESS_PER_RUN", "30"))

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("pipeline_bot")


# ─────────────────────────────────────────────
# 1. 뉴스 발송 공통 함수
# ─────────────────────────────────────────────
async def _send_articles(context, articles: list, user_id: int, chat_id: int) -> int:
    sent_count = 0
    for art in articles:
        url = art.get("url", "")
        if not url or db.was_sent(user_id, url):
            continue
        title = art.get("title", "(제목 없음)")
        source = art.get("source", "")
        kw = art.get("matched_keyword", "")
        pub = art.get("published", "")[:16]
        msg = (
            f"🔔 *[{source.upper()}]* 키워드: `{kw}`\n"
            f"*{title}*\n"
            f"{url}\n"
            f"_{pub}_"
        )
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="Markdown",
                disable_web_page_preview=False,
            )
            db.mark_sent(user_id, url)
            sent_count += 1
        except Exception as e:
            logger.warning(f"전송 실패 (user={user_id}): {e}")
    return sent_count


# ─────────────────────────────────────────────
# 2. 시나리오 파이프라인 (비동기 래퍼)
# ─────────────────────────────────────────────
async def _run_scenario_pipeline(new_articles: list) -> dict:
    """
    새 기사들을 시나리오 파이프라인에 투입한다.
    scenario_builder.process_article 은 동기 함수이므로 스레드풀에서 실행.

    Returns:
        {
            "created": int, "added": int, "skipped": int, "errors": int,
            "created_scenarios": [{"id": int, "name": str}, ...],
            "updated_scenarios": {scenario_id: scenario_name, ...},
        }
    """
    to_process = new_articles[:MAX_PROCESS_PER_RUN]
    stats = {
        "created": 0, "added": 0, "skipped": 0, "errors": 0,
        "created_scenarios": [],
        "updated_scenarios": {},
    }
    loop = asyncio.get_event_loop()

    for art in to_process:
        try:
            result = await loop.run_in_executor(
                None, partial(scenario_builder.process_article, art)
            )
            if result["action"] == "created":
                stats["created"] += 1
                stats["created_scenarios"].append({
                    "id": result["scenario_id"],
                    "name": result["scenario_name"],
                })
            elif result["action"] == "added":
                stats["added"] += 1
                sid = result["scenario_id"]
                if sid and sid not in stats["updated_scenarios"]:
                    stats["updated_scenarios"][sid] = result["scenario_name"]
            else:
                stats["skipped"] += 1
        except Exception as e:
            logger.error(f"시나리오 파이프라인 오류 [{art.get('title', '')[:30]}]: {e}")
            stats["errors"] += 1

    return stats


async def _send_scenario_updates(
    context,
    created_scenarios: list,
    updated_scenarios: dict,
    mode: str = "hourly",
):
    """
    파이프라인 실행 결과를 모든 사용자에게 브로드캐스트.

    - 새 시나리오(created): 이름·카테고리·설명·키워드 포함
    - 업데이트 시나리오(updated): evening 모드일 때만 요약 포함
    """
    if not created_scenarios and not updated_scenarios:
        return

    lines = []

    if created_scenarios:
        lines.append("🆕 *새 시나리오가 생성되었습니다*")
        for sc in created_scenarios:
            data = scenario_db.get_scenario_with_nodes(sc["id"])
            if not data:
                continue
            desc = data["description"][:80] + ("..." if len(data["description"]) > 80 else "")
            kws = ", ".join(data["keywords"][:4])
            lines.append(
                f"\n• *{data['name']}* `{data['category']}`\n"
                f"  {desc}\n"
                f"  _{kws}_\n"
                f"  `/scenario {data['id']}`"
            )

    if updated_scenarios:
        lines.append("\n\n📌 *업데이트된 시나리오*")
        for sid, name in list(updated_scenarios.items())[:8]:
            lines.append(f"• [{sid}] {name}  →  `/scenario {sid}`")

    if not lines:
        return

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n_(생략)_"

    await _broadcast(context, text)


# ─────────────────────────────────────────────
# 3. 스케줄 작업
# ─────────────────────────────────────────────
async def job_hourly_fetch(context: ContextTypes.DEFAULT_TYPE):
    logger.info("[스케줄] 매시간 수집 시작 (veryfast, 피드당 10개)")
    try:
        raw = fetcher.fetch_news(max_per_feed=10, delay=0.05)
        new_articles = db.filter_new_articles(raw)
        logger.info(f"[스케줄] 신규 기사 {len(new_articles)}개 / 전체 {len(raw)}개")

        if not new_articles:
            return

        # ── 키워드 알림
        user_kw_map = db.get_all_user_keywords()
        total_sent = 0
        for user_id, info in user_kw_map.items():
            keywords = info.get("keywords", [])
            chat_id = info.get("chat_id", user_id)
            if not keywords:
                continue
            matched = fetcher.filter_by_keywords(new_articles, keywords)
            if matched:
                cnt = await _send_articles(context, matched, user_id, chat_id)
                total_sent += cnt
        logger.info(f"[스케줄] 키워드 알림 {total_sent}건 전송")

        # ── 시나리오 파이프라인
        sc_stats = await _run_scenario_pipeline(new_articles)
        logger.info(
            f"[스케줄] 시나리오 파이프라인 완료 — "
            f"생성 {sc_stats['created']}개 / 추가 {sc_stats['added']}개 / "
            f"스킵 {sc_stats['skipped']}개 / 오류 {sc_stats['errors']}건"
        )

        # ── 시나리오 알림 (새 시나리오만)
        await _send_scenario_updates(
            context,
            sc_stats["created_scenarios"],
            sc_stats["updated_scenarios"],
            mode="hourly",
        )

    except Exception as e:
        logger.error(f"[스케줄] 매시간 수집 오류: {e}", exc_info=True)


# ─────────────────────────────────────────────
# 장 개장/마감 알람 + 브리핑 스케줄
# ─────────────────────────────────────────────
async def _broadcast(context, text: str, parse_mode: str = "Markdown"):
    """등록된 모든 사용자에게 메시지 전송"""
    user_kw_map = db.get_all_user_keywords()
    for user_id, info in user_kw_map.items():
        chat_id = info.get("chat_id", user_id)
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.warning(f"브로드캐스트 실패 (user={user_id}): {e}")


async def job_market_open(context: ContextTypes.DEFAULT_TYPE):
    """09:00 KST — 장 개장 알람"""
    logger.info("[스케줄] 장 개장 알람")
    await _broadcast(
        context,
        "🔔 *장 개장 알림*\n\n"
        "📅 한국 증시가 개장했습니다. (09:00 KST)\n"
        "오늘도 성공적인 투자 되세요! 💪\n\n"
        "_10:00에 장초반 급등 테마/업종 브리핑이 전송됩니다._",
    )


async def job_morning_briefing(context: ContextTypes.DEFAULT_TYPE):
    """10:00 KST — 장초반 테마/업종 +3% 브리핑"""
    logger.info("[스케줄] 장초반 테마/업종 브리핑 시작")
    loop = asyncio.get_event_loop()
    try:
        msg = await loop.run_in_executor(
            None,
            lambda: briefing.build_briefing_message(label="장초반 (10:00)", threshold=3.0),
        )
        await _broadcast(context, msg)
        logger.info("[스케줄] 장초반 브리핑 전송 완료")
    except Exception as e:
        logger.error(f"[스케줄] 장초반 브리핑 오류: {e}", exc_info=True)


async def job_market_close(context: ContextTypes.DEFAULT_TYPE):
    """15:30 KST — 장 마감 알람"""
    logger.info("[스케줄] 장 마감 알람")
    await _broadcast(
        context,
        "🔔 *장 마감 알림*\n\n"
        "📅 한국 증시가 마감되었습니다. (15:30 KST)\n"
        "_16:00에 장마감 후 테마/업종 브리핑이 전송됩니다._",
    )


async def job_close_briefing(context: ContextTypes.DEFAULT_TYPE):
    """16:00 KST — 장마감 후 테마/업종 +3% 브리핑"""
    logger.info("[스케줄] 장마감 후 테마/업종 브리핑 시작")
    loop = asyncio.get_event_loop()
    try:
        msg = await loop.run_in_executor(
            None,
            lambda: briefing.build_briefing_message(label="장마감 후 (16:00)", threshold=3.0),
        )
        await _broadcast(context, msg)
        logger.info("[스케줄] 장마감 후 브리핑 전송 완료")
    except Exception as e:
        logger.error(f"[스케줄] 장마감 후 브리핑 오류: {e}", exc_info=True)


async def job_evening_fetch(context: ContextTypes.DEFAULT_TYPE):
    logger.info("[스케줄] 저녁 22:00 대량 수집 시작 (full, 피드당 40개)")
    try:
        raw = fetcher.fetch_news(max_per_feed=40, delay=0.15)
        new_articles = db.filter_new_articles(raw)
        logger.info(f"[스케줄] 저녁: 신규 기사 {len(new_articles)}개")

        if not new_articles:
            return

        # ── 키워드 알림
        user_kw_map = db.get_all_user_keywords()
        total_sent = 0
        for user_id, info in user_kw_map.items():
            keywords = info.get("keywords", [])
            chat_id = info.get("chat_id", user_id)
            if not keywords:
                continue
            matched = fetcher.filter_by_keywords(new_articles, keywords)
            if matched:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🌙 *저녁 뉴스 알림* — 오늘 {len(matched)}건 매칭",
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass
                cnt = await _send_articles(context, matched, user_id, chat_id)
                total_sent += cnt
        logger.info(f"[스케줄] 저녁 알림 {total_sent}건 전송")

        # ── 시나리오 파이프라인
        sc_stats = await _run_scenario_pipeline(new_articles)
        logger.info(
            f"[스케줄] 저녁 시나리오 파이프라인 — "
            f"생성 {sc_stats['created']}개 / 추가 {sc_stats['added']}개"
        )

        # ── 시나리오 알림 (새 시나리오 + 업데이트 요약)
        await _send_scenario_updates(
            context,
            sc_stats["created_scenarios"],
            sc_stats["updated_scenarios"],
            mode="evening",
        )

    except Exception as e:
        logger.error(f"[스케줄] 저녁 수집 오류: {e}", exc_info=True)


# ─────────────────────────────────────────────
# 4. 명령어 핸들러 — 기존 기능
# ─────────────────────────────────────────────
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    db.register_user(user.id, user.username or user.first_name, chat_id)

    keyboard = [
        [
            InlineKeyboardButton("📋 도움말", callback_data="help"),
            InlineKeyboardButton("🔑 내 키워드", callback_data="keywords"),
        ],
        [
            InlineKeyboardButton("📊 봇 통계", callback_data="status"),
            InlineKeyboardButton("🗺 시나리오", callback_data="scenarios"),
        ],
    ]
    await update.message.reply_text(
        f"안녕하세요, {user.first_name}님! 🗞\n\n"
        "뉴스 키워드 알림 + 시나리오 분석 봇입니다.\n\n"
        "📌 키워드 등록: `/add 반도체`\n"
        "🗺 시나리오 보기: `/scenarios`\n"
        "🤖 AI 분석: `/rag 미-중 무역전쟁 현황`",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


async def help_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "━━━━━ 📖 파이프라인 봇 명령어 ━━━━━\n"
        "/start                     — 시작 & 메뉴\n"
        "/add <키워드>               — 알림 키워드 추가\n"
        "/remove <키워드>            — 키워드 삭제\n"
        "/keywords                  — 등록된 키워드 목록\n"
        "/search <키워드>            — DB에서 뉴스 검색\n"
        "/status                    — 봇 통계\n"
        "/fetch [veryfast|full]     — 즉시 수집 🔐관리자\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📊 *시장 브리핑:*\n"
        "/briefing                  — 테마/업종 급등 AI 브리핑\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🗺 *시나리오 명령어:*\n"
        "/scenarios [카테고리]       — 시나리오 목록\n"
        "  카테고리: energy, finance, geopolitics,\n"
        "            tech, trade, macro, corporate\n"
        "/scenario <id>             — 시나리오 상세\n"
        "/rag <질문>                 — AI 분석 쿼리\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⏰ *자동 수집:* 매시간 + 매일 22:00\n"
        "📊 *장 알람:* 09:00 개장 / 10:00 급등브리핑 / 15:30 마감 / 16:00 장후브리핑"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def add_keyword(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "사용법: `/add <키워드>`\n예) `/add 반도체`", parse_mode="Markdown"
        )
        return
    keyword = " ".join(ctx.args)
    user = update.effective_user
    db.register_user(user.id, user.username or "", update.effective_chat.id)
    added = db.add_keyword(user.id, keyword)
    if added:
        await update.message.reply_text(f"✅ 키워드 추가: `{keyword}`", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"⚠️ 이미 등록된 키워드: `{keyword}`", parse_mode="Markdown")


async def remove_keyword(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "사용법: `/remove <키워드>`", parse_mode="Markdown"
        )
        return
    keyword = " ".join(ctx.args)
    removed = db.remove_keyword(update.effective_user.id, keyword)
    if removed:
        await update.message.reply_text(f"🗑 키워드 삭제: `{keyword}`", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"❌ 등록되지 않은 키워드: `{keyword}`", parse_mode="Markdown")


async def list_keywords(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    keywords = db.get_keywords(update.effective_user.id)
    if not keywords:
        await update.message.reply_text(
            "등록된 키워드가 없습니다.\n`/add <키워드>` 로 추가하세요.",
            parse_mode="Markdown",
        )
        return
    kw_list = "\n".join(f"  • `{kw}`" for kw in keywords)
    await update.message.reply_text(
        f"🔑 *내 알림 키워드* ({len(keywords)}개)\n\n{kw_list}",
        parse_mode="Markdown",
    )


async def search_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text(
            "사용법: `/search <키워드>`\n예) `/search 금리`", parse_mode="Markdown"
        )
        return
    keyword = " ".join(ctx.args)
    results = db.search_news(keyword, limit=10)
    if not results:
        await update.message.reply_text(
            f"🔍 `{keyword}` 검색 결과 없음", parse_mode="Markdown"
        )
        return
    header = f"🔍 *'{keyword}' 검색 결과* ({len(results)}건)\n{'─'*30}\n"
    lines = [
        f"[{r['source']}] [{r['title'][:45]}]({r['url']}) _{r.get('fetched_at','')[:10]}_"
        for r in results
    ]
    await update.message.reply_text(
        header + "\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )


async def status_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    news_stats = db.get_stats()
    sc_stats = scenario_db.get_stats()
    await update.message.reply_text(
        "📊 *봇 현황*\n\n"
        "*[뉴스 DB]*\n"
        f"  수집된 뉴스:  {news_stats['total_news']:,}개\n"
        f"  등록 사용자:  {news_stats['total_users']}명\n"
        f"  전체 키워드:  {news_stats['total_keywords']}개\n"
        f"  총 전송 건수: {news_stats['total_sent']:,}건\n\n"
        "*[시나리오 DB]*\n"
        f"  시나리오:  {sc_stats['total_scenarios']}개\n"
        f"  노드(이벤트): {sc_stats['total_nodes']}개\n"
        f"  카테고리: {', '.join(f'{k}({v})' for k,v in sc_stats['categories'].items())}\n\n"
        "⏰ *스케줄*\n"
        "  뉴스: 매시간 수집 + 22:00 대량 수집\n"
        "  장 알람: 09:00 개장 / 10:00 급등브리핑\n"
        "           15:30 마감 / 16:00 장후브리핑",
        parse_mode="Markdown",
    )


async def fetch_now(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ 관리자 전용 명령어입니다.")
        return

    mode = ctx.args[0] if ctx.args else "veryfast"
    await update.message.reply_text(f"🔄 수집 시작 (모드: {mode}) ...")

    try:
        if mode == "full":
            raw = fetcher.fetch_news(max_per_feed=40, delay=0.1)
        else:
            raw = fetcher.fetch_news(max_per_feed=10, delay=0.05)

        new_articles = db.filter_new_articles(raw)
        await update.message.reply_text(
            f"✅ 수집 완료 — 전체: {len(raw)}개 / 신규: {len(new_articles)}개\n"
            f"⏳ 시나리오 파이프라인 처리 중 (최대 {MAX_PROCESS_PER_RUN}개)..."
        )

        # 키워드 알림
        user_kw_map = db.get_all_user_keywords()
        total_sent = 0
        for user_id, info in user_kw_map.items():
            keywords = info.get("keywords", [])
            chat_id = info.get("chat_id", user_id)
            if not keywords:
                continue
            matched = fetcher.filter_by_keywords(new_articles, keywords)
            if matched:
                cnt = await _send_articles(ctx, matched, user_id, chat_id)
                total_sent += cnt

        # 시나리오 파이프라인
        sc_stats = await _run_scenario_pipeline(new_articles)

        await update.message.reply_text(
            f"📤 알림 {total_sent}건 전송\n"
            f"🗺 시나리오 — 생성 {sc_stats['created']}개 / 추가 {sc_stats['added']}개 / 오류 {sc_stats['errors']}건"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ 오류: {e}")


# ─────────────────────────────────────────────
# 5. 명령어 핸들러 — 시나리오 기능
# ─────────────────────────────────────────────
async def scenarios_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/scenarios [카테고리] — 시나리오 목록"""
    category = ctx.args[0].lower() if ctx.args else None
    if category:
        scenarios = scenario_db.get_scenarios_by_category(category, limit=10)
        header = f"🗺 *[{category}] 시나리오 목록* ({len(scenarios)}개)\n{'─'*30}\n"
    else:
        scenarios = scenario_db.get_all_scenarios(limit=10)
        header = f"🗺 *시나리오 목록* (최신 {len(scenarios)}개)\n{'─'*30}\n"

    if not scenarios:
        await update.message.reply_text("등록된 시나리오가 없습니다.")
        return

    lines = []
    for i, s in enumerate(scenarios, 1):
        kws = ", ".join(s["keywords"][:3])
        updated = (s.get("updated_at") or "")[:16].replace("T", " ")
        lines.append(
            f"*{i}.* *[{s['id']}]* {s['name']} `{s['category']}`\n"
            f"  {s['description'][:60]}...\n"
            f"  키워드: _{kws}_  |  🕐 _{updated}_\n"
            f"  `/scenario {s['id']}` 로 상세 보기"
        )

    await update.message.reply_text(
        header + "\n\n".join(lines),
        parse_mode="Markdown",
    )


async def scenario_detail_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/scenario <id> — 시나리오 상세 + 노드 타임라인"""
    if not ctx.args or not ctx.args[0].isdigit():
        await update.message.reply_text(
            "사용법: `/scenario <id>`\n예) `/scenario 1`", parse_mode="Markdown"
        )
        return

    scenario_id = int(ctx.args[0])
    data = scenario_db.get_scenario_with_nodes(scenario_id)
    if not data:
        await update.message.reply_text(f"❌ 시나리오 ID {scenario_id} 없음")
        return

    nodes = data["nodes"]
    node_lines = []
    for n in nodes[-15:]:  # 최근 15개 노드
        pub = (n.get("published_at") or "")[:10]
        significance = n.get("significance") or ""
        url = n.get("url", "")
        node_lines.append(
            f"  *{n['node_order']}.* [{n['title'][:40]}]({url})\n"
            f"    └ _{significance[:60]}_  `{pub}`"
        )

    node_text = "\n".join(node_lines) if node_lines else "  (노드 없음)"
    total = len(nodes)
    shown = min(total, 15)

    text = (
        f"🗺 *시나리오 #{data['id']}: {data['name']}*\n"
        f"카테고리: `{data['category']}`\n"
        f"키워드: _{', '.join(data['keywords'])}_\n\n"
        f"📋 *설명*\n{data['description']}\n\n"
        f"⏳ *이벤트 흐름* (총 {total}개 중 최근 {shown}개)\n"
        f"{'─'*30}\n"
        f"{node_text}"
    )
    await update.message.reply_text(
        text, parse_mode="Markdown", disable_web_page_preview=True
    )


async def briefing_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/briefing — 현재 시점 테마/업종 급등 브리핑 즉시 요청"""
    await update.message.reply_text("📊 테마/업종 데이터 수집 중... (15~30초 소요)", parse_mode="Markdown")
    loop = asyncio.get_event_loop()
    try:
        msg = await loop.run_in_executor(
            None,
            lambda: briefing.build_briefing_message(label="현재", threshold=3.0),
        )
        await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        await update.message.reply_text(f"❌ 브리핑 오류: {e}")


async def rag_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/rag <질문> — Gemini RAG 시나리오 분석"""
    if not ctx.args:
        await update.message.reply_text(
            "사용법: `/rag <질문>`\n"
            "예) `/rag 미-중 무역전쟁 현황`\n"
            "예) `/rag 삼성전자 반도체 관련 시나리오`",
            parse_mode="Markdown",
        )
        return

    query = " ".join(ctx.args)
    await update.message.reply_text(f"🤖 분석 중... (`{query[:40]}`)", parse_mode="Markdown")

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, partial(scenario_builder.rag_query, query)
        )
        answer = result.get("answer", "응답 없음")
        sc_list = result.get("scenarios", [])
        sc_names = ", ".join(f"[{s['id']}]{s['name']}" for s in sc_list)

        # 텔레그램 메시지 길이 제한(4096) 처리
        if len(answer) > 3800:
            answer = answer[:3800] + "...(생략)"

        ref_line = f"\n\n📌 *참조 시나리오:* _{sc_names}_" if sc_names else ""
        await update.message.reply_text(
            f"🤖 *AI 분석*\n\n{answer}{ref_line}",
            parse_mode="Markdown",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ RAG 오류: {e}")


# ─────────────────────────────────────────────
# 6. Inline 콜백
# ─────────────────────────────────────────────
async def button_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "help":
        await query.edit_message_text(
            "━━━ 명령어 목록 ━━━\n"
            "/add <키워드>   — 키워드 추가\n"
            "/remove <키>   — 키워드 삭제\n"
            "/keywords      — 키워드 목록\n"
            "/search <키>   — 뉴스 검색\n"
            "/scenarios     — 시나리오 목록\n"
            "/scenario <id> — 시나리오 상세\n"
            "/rag <질문>     — AI 분석\n"
            "/status        — 봇 통계"
        )
    elif data == "keywords":
        keywords = db.get_keywords(update.effective_user.id)
        if keywords:
            kw_list = "\n".join(f"  • {kw}" for kw in keywords)
            text = f"🔑 내 키워드 ({len(keywords)}개)\n\n{kw_list}"
        else:
            text = "등록된 키워드 없음\n/add <키워드> 로 추가하세요"
        await query.edit_message_text(text)
    elif data == "status":
        ns = db.get_stats()
        ss = scenario_db.get_stats()
        await query.edit_message_text(
            f"📊 봇 현황\n"
            f"뉴스: {ns['total_news']:,}개 | 전송: {ns['total_sent']:,}건\n"
            f"시나리오: {ss['total_scenarios']}개 | 노드: {ss['total_nodes']}개"
        )
    elif data == "scenarios":
        scenarios = scenario_db.get_all_scenarios(limit=10)
        if scenarios:
            lines = [
                f"{i}. [{s['id']}] {s['name']} ({s['category']}) "
                f"🕐{(s.get('updated_at') or '')[:10]}"
                for i, s in enumerate(scenarios, 1)
            ]
            text = "🗺 최근 업데이트 시나리오 TOP 10\n\n" + "\n".join(lines) + "\n\n/scenarios 로 상세 보기"
        else:
            text = "아직 시나리오가 없습니다."
        await query.edit_message_text(text)


# ─────────────────────────────────────────────
# 7. 에러 핸들러
# ─────────────────────────────────────────────
async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    error = ctx.error
    if isinstance(error, Conflict):
        logger.critical(
            "❌ CONFLICT: 같은 봇 토큰으로 다른 인스턴스가 이미 실행 중입니다."
        )
        sys.exit(1)
    logger.error(f"Update {update} caused error: {error}", exc_info=error)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text("⚠️ 오류가 발생했습니다.")


# ─────────────────────────────────────────────
# 8. 메인
# ─────────────────────────────────────────────
def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN이 .env에 설정되지 않았습니다!")

    db.init_db()
    scenario_db.init_db()
    logger.info("DB 초기화 완료 (news_alert.db + scenario.db)")

    app = Application.builder().token(BOT_TOKEN).build()

    # ── 기존 명령어
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("add", add_keyword))
    app.add_handler(CommandHandler("remove", remove_keyword))
    app.add_handler(CommandHandler("keywords", list_keywords))
    app.add_handler(CommandHandler("search", search_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("fetch", fetch_now))

    # ── 시나리오 명령어
    app.add_handler(CommandHandler("briefing", briefing_command))
    app.add_handler(CommandHandler("scenarios", scenarios_command))
    app.add_handler(CommandHandler("scenario", scenario_detail_command))
    app.add_handler(CommandHandler("rag", rag_command))

    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    jq = app.job_queue
    jq.run_repeating(job_hourly_fetch, interval=3600, first=60, name="hourly_fetch")
    logger.info("스케줄 등록: 매시간 수집 (veryfast) + 시나리오 파이프라인")

    jq.run_daily(
        job_evening_fetch,
        time=dtime(hour=13, minute=0, tzinfo=timezone.utc),
        name="evening_fetch",
    )
    logger.info("스케줄 등록: 매일 22:00 대량 수집 (full) + 시나리오 파이프라인")

    # ── 장 개장/마감 알람 + 브리핑 (KST = UTC+9) ──────────────────────
    # 09:00 KST = 00:00 UTC
    jq.run_daily(
        job_market_open,
        time=dtime(hour=0, minute=0, tzinfo=timezone.utc),
        name="market_open",
    )
    logger.info("스케줄 등록: 09:00 KST 장 개장 알람")

    # 10:00 KST = 01:00 UTC
    jq.run_daily(
        job_morning_briefing,
        time=dtime(hour=1, minute=0, tzinfo=timezone.utc),
        name="morning_briefing",
    )
    logger.info("스케줄 등록: 10:00 KST 장초반 테마/업종 브리핑")

    # 15:30 KST = 06:30 UTC
    jq.run_daily(
        job_market_close,
        time=dtime(hour=6, minute=30, tzinfo=timezone.utc),
        name="market_close",
    )
    logger.info("스케줄 등록: 15:30 KST 장 마감 알람")

    # 16:00 KST = 07:00 UTC
    jq.run_daily(
        job_close_briefing,
        time=dtime(hour=7, minute=0, tzinfo=timezone.utc),
        name="close_briefing",
    )
    logger.info("스케줄 등록: 16:00 KST 장마감 후 테마/업종 브리핑")

    logger.info("🤖 파이프라인 봇 시작 (polling 모드)")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
