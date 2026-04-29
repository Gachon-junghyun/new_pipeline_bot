# LLM Search Behavior

This repo keeps a lightweight local news database in `news_alert.db`.
When the user asks for market or company research from gathered news, use this flow:

1. Run `python3 LLM_search.py "<query>"` from the repo root.
2. Prefer local DB results first. Treat web/Naver results as supplemental.
3. Read the generated TXT under `llm_outputs/`.
4. Preserve dates in the answer. Do not summarize a news item without its date.
5. If the user asks for a recent window, use `--days N` and state the exact date range.
6. Do not infer investment conclusions from headline counts alone. Separate:
   - confirmed local news signals
   - repeated/duplicate coverage
   - missing data or items that need follow-up

Useful commands:

```bash
python3 LLM_search.py "네이버 AI" --days 14 --naver --limit 30
python3 LLM_search.py "SK하이닉스 HBM" --days 14 --limit 40
python3 LLM_search.py "삼성SDS AI 클라우드" --days 30 --naver --fetch-articles
```
