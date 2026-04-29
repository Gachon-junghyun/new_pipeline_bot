# new_pipeline_bot 프로젝트 리뷰

작성일: 2026-04-29

## 한줄 요약

이 프로젝트는 Telegram 뉴스 알림 봇에 RSS 수집, Gemini 기반 시나리오 분류/RAG, FastAPI 조회 API, 인프라 시나리오 검색 GUI를 결합한 개인/소규모 운영용 파이프라인입니다. 핵심 기능의 방향은 명확하지만, 현재 상태로 장기 운영하려면 비밀키 관리, 메시지 포맷 안정성, 비동기 작업 분리, API 보호, 테스트/운영 문서 보강이 우선입니다.

## 프로젝트 구성

| 파일/디렉터리 | 역할 |
| --- | --- |
| `bot.py` | Telegram 봇 진입점. 키워드 알림, 뉴스 수집 스케줄, 시장 브리핑, 시나리오 명령어, RAG 명령어 처리 |
| `news_fetcher.py` | 국내외 RSS 피드 수집 및 키워드 매칭 |
| `db_manager.py` | `news_alert.db` 관리. 사용자, 키워드, 수집 이력, 발송 이력 저장 |
| `scenario_builder.py` | Gemini로 기사 중요도 판단, 기존 시나리오 매칭, 신규 시나리오 생성, RAG 답변 생성 |
| `scenario_db.py` | `scenario.db` 관리. 시나리오/노드 CRUD, 검색, 통계 |
| `api.py` | FastAPI 기반 시나리오 조회 및 RAG API |
| `naver_market_briefing.py` | 네이버 금융 테마/업종 크롤링 및 Gemini 브리핑 생성 |
| `infra_search.py` | 인프라 관련 시나리오 점수화 및 TXT export |
| `infra_gui.py` | 인프라 시나리오 검색용 로컬 FastAPI GUI |
| `searched_scenario/` | 인프라 검색 결과 TXT 산출물 |

## 좋은 점

- 기능 경계가 비교적 잘 나뉘어 있습니다. 수집, 뉴스 DB, 시나리오 DB, Gemini 분석, Telegram UI가 별도 모듈로 분리되어 있어 개선하기 쉽습니다.
- SQLite 기반이라 로컬 운영과 백업이 단순합니다.
- `scenario_db.find_candidate_scenarios_for_article()`가 최신 시나리오만 보는 대신 기사 제목/요약/출처로 기존 노드까지 검색하도록 개선되어, 오래된 시나리오 재매칭 가능성이 높아졌습니다.
- Telegram 스케줄, 즉시 수집, 시나리오 상세, RAG, 시장 브리핑까지 사용 흐름이 실제 운영 중심으로 구성되어 있습니다.
- `infra_search.py`와 `infra_gui.py`는 기존 시나리오 데이터를 읽기 전용으로 활용하므로 메인 DB를 망가뜨릴 위험이 낮습니다.

## 주요 발견 사항

### P0. `.env.template`에 실제 비밀키가 들어 있습니다

- 위치: `.env.template:2`, `.env.template:6`
- 영향: Telegram 봇 토큰과 Gemini API 키가 템플릿 파일에 실제 값으로 저장되어 있고, `git ls-files` 기준 추적 대상입니다. 이 파일이 외부로 공유되면 봇 탈취, API 비용 발생, 데이터 노출로 이어질 수 있습니다.
- 권장 조치:
  - 즉시 Telegram bot token과 Gemini API key를 폐기/재발급합니다.
  - `.env.template`에는 placeholder만 남깁니다.
  - 이미 원격 저장소에 올라갔다면 git history에서도 제거합니다.

예시:

```env
BOT_TOKEN=your_telegram_bot_token
ADMIN_USER_ID=your_telegram_user_id
GEMINI_API_KEY=your_gemini_api_key
SIMILARITY_THRESHOLD=65
MAX_PROCESS_PER_RUN=30
```

### P1. Telegram Markdown 메시지가 사용자/뉴스 데이터에 취약합니다

- 위치: `bot.py:86-97`, `bot.py:584-593`, `bot.py:617-636`, `bot.py:666-684`
- 영향: 뉴스 제목, 키워드, 시나리오명, 설명, Gemini 응답에 `_`, `*`, `[`, `]`, `(`, `)` 등이 포함되면 Telegram Markdown 파싱 오류가 발생해 메시지 전송이 실패할 수 있습니다. 현재 실패는 로그로만 남거나 사용자에게 단순 오류로 보입니다.
- 권장 조치:
  - `telegram.helpers.escape_markdown()`을 사용하거나 `parse_mode=None`/HTML 모드로 통일합니다.
  - 외부 입력이 들어가는 모든 메시지 조립부에 escape 헬퍼를 적용합니다.

### P1. 비동기 핸들러 안에서 긴 동기 작업이 직접 실행됩니다

- 위치: `bot.py:527-531`, `bot.py:199-200`, `bot.py:324-325`
- 영향: `/fetch`와 스케줄 뉴스 수집에서 `fetcher.fetch_news()`가 async 함수 안에서 직접 실행됩니다. RSS 피드가 많고 timeout도 있어 이벤트 루프가 수 초에서 수십 초 동안 막힐 수 있습니다. 이 동안 봇 응답, 콜백, 다른 스케줄 실행이 지연됩니다.
- 권장 조치:
  - `fetch_news()`도 `run_in_executor()` 또는 `asyncio.to_thread()`로 분리합니다.
  - 이미 브리핑/Gemini 일부는 executor를 사용하므로 같은 패턴으로 맞추면 됩니다.

### P1. FastAPI RAG API에 인증/비용 보호가 없습니다

- 위치: `api.py:83-118`
- 영향: API 서버가 외부에 노출되면 누구나 `/rag/query` 또는 `/rag/company/{company_name}`로 Gemini 호출을 유발할 수 있습니다. 비용 증가와 rate limit, 내부 시나리오 데이터 노출 위험이 있습니다.
- 권장 조치:
  - 로컬 전용이면 README에 `127.0.0.1` 바인딩을 명시합니다.
  - 외부 접근 가능성이 있으면 API key, Basic Auth, IP allowlist, rate limit 중 최소 하나를 적용합니다.

### P1. 브로드캐스트 대상이 "키워드가 있는 사용자"로 제한됩니다

- 위치: `bot.py:251-255`
- 영향: `_broadcast()`가 `db.get_all_user_keywords()`를 사용하기 때문에 `/start`만 한 사용자, 또는 키워드를 모두 삭제한 사용자는 장 개장/마감 알림과 시나리오 업데이트를 받지 못합니다. 함수 이름과 주석은 "등록된 모든 사용자"인데 실제 동작은 다릅니다.
- 권장 조치:
  - 브로드캐스트는 `db.get_all_users()`를 사용합니다.
  - 키워드 알림만 `get_all_user_keywords()`를 사용하도록 분리합니다.

### P2. 시나리오 노드 중복 방지 장치가 약합니다

- 위치: `scenario_db.py:292-315`
- 영향: `nodes` 테이블에는 URL 또는 `(scenario_id, url)` 기준 유니크 제약이 없습니다. 같은 기사가 다른 RSS URL 또는 Google News URL로 들어오거나 재처리되면 동일 시나리오에 중복 노드가 쌓일 수 있습니다.
- 권장 조치:
  - URL 정규화 후 `UNIQUE(scenario_id, url)` 또는 URL hash 컬럼을 둡니다.
  - `add_node_to_scenario()`에서 이미 존재하는 URL이면 기존 node id를 반환하도록 합니다.

### P2. 시나리오 검색은 LIKE 기반이라 데이터가 커질수록 느려질 수 있습니다

- 위치: `scenario_db.py:76-125`
- 영향: `scenarios`와 `nodes`를 `LIKE '%term%'`로 검색합니다. 현재 DB 규모에서는 괜찮지만 노드가 수만 개로 늘면 RAG 후보 검색, 기사 매칭 후보 검색, API 검색이 느려질 가능성이 큽니다.
- 권장 조치:
  - SQLite FTS5 가상 테이블을 도입합니다.
  - 최소한 `nodes(title, summary, significance)` 검색용 별도 인덱싱/캐시 테이블을 고려합니다.

### P2. Gemini 호출 실패/형식 오류에 대한 재시도와 관측성이 부족합니다

- 위치: `scenario_builder.py:61-94`, `scenario_builder.py:177-189`, `scenario_builder.py:246-260`
- 영향: JSON 파싱 실패나 일시적 API 오류가 발생하면 기사는 스킵되거나 신규 시나리오 생성으로 흘러갈 수 있습니다. 운영 중에는 왜 스킵되었는지, 비용이 얼마나 발생했는지 추적하기 어렵습니다.
- 권장 조치:
  - Gemini 호출에 timeout, 1-2회 제한 재시도, 구조화 로그를 추가합니다.
  - `print()` 대신 logger를 사용하고 기사 URL, action, latency, token 사용량을 남깁니다.
  - JSON schema 또는 response mime type을 사용할 수 있으면 구조화 응답을 강제합니다.

### P2. 로그 파일이 커질 수 있고 회전 정책이 없습니다

- 위치: `bot.py:62-68`
- 현재 확인: `bot.log` 약 9.5MB
- 영향: 장기 실행 시 로그가 계속 커집니다. 작은 서버/로컬 머신에서는 디스크 사용량 관리가 어려워질 수 있습니다.
- 권장 조치:
  - `RotatingFileHandler` 또는 `TimedRotatingFileHandler`를 사용합니다.
  - Gemini 프롬프트/응답 전문을 로그에 남기지 않는 정책도 함께 둡니다.

### P3. 테스트와 실행 문서가 부족합니다

- 현재 상태: `requirements.txt`는 있으나 README, 테스트 디렉터리, CI 설정은 보이지 않습니다.
- 영향: 새 환경에서 봇/API/GUI를 어떻게 띄우는지, 스케줄 시간이 어떤 기준인지, DB 백업/초기화는 어떻게 하는지 재현하기 어렵습니다.
- 권장 조치:
  - `README.md`에 설치, `.env` 설정, 봇 실행, API 실행, infra GUI 실행, DB 백업/초기화 절차를 추가합니다.
  - DB 유틸, 키워드 필터, Markdown escaping, 시나리오 검색에 대한 작은 단위 테스트부터 추가합니다.

## 추천 개선 순서

1. 비밀키 회수/재발급 및 `.env.template` 정리
2. Telegram 메시지 escape 공통 함수 추가
3. `_broadcast()` 대상 버그 수정
4. `/fetch`와 스케줄 수집을 executor/thread로 이동
5. FastAPI RAG 엔드포인트 인증 또는 로컬 전용 운영 명시
6. `nodes` 중복 방지 제약 추가 및 마이그레이션
7. 로그 로테이션과 구조화 로그 도입
8. README와 최소 테스트 추가

## 검증 결과

다음 정적 검증은 통과했습니다.

```bash
python -m compileall -q .
```

현재 저장소 상태 기준으로 이미 수정/추가된 파일이 있습니다.

```text
M scenario_builder.py
M scenario_db.py
?? infra_gui.py
?? infra_search.py
?? searched_scenario/
```

이 리뷰 문서는 코드 동작을 바꾸지 않고 추가 문서로만 작성했습니다.
