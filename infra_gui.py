"""
Local GUI for reusable scenario search.

Run:
  uvicorn infra_gui:app --reload --port 8010

Open:
  http://127.0.0.1:8010
"""

import html
import os
from typing import Dict, List
from urllib.parse import urlencode

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

import infra_search


app = FastAPI(
    title="Scenario Search Browser",
    version="1.1.0",
)


def _esc(value) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _label_class(label: str) -> str:
    return {
        "primary": "label-primary",
        "secondary": "label-secondary",
        "weak": "label-weak",
    }.get(label, "label-muted")


def _selected(current: str, value: str) -> str:
    return "selected" if current == value else ""


def _style() -> str:
    return """
    <style>
      :root {
        color-scheme: light;
        --bg: #f7f8fa;
        --panel: #ffffff;
        --line: #d9dee7;
        --text: #1f2933;
        --muted: #667085;
        --blue: #1f6feb;
        --green: #138a52;
        --amber: #a16207;
        --red: #b42318;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: var(--bg);
        color: var(--text);
        line-height: 1.45;
      }
      header {
        position: sticky;
        top: 0;
        z-index: 10;
        background: rgba(255,255,255,0.97);
        border-bottom: 1px solid var(--line);
        padding: 14px 22px;
      }
      main { max-width: 1240px; margin: 0 auto; padding: 18px 22px 46px; }
      h1 { font-size: 20px; margin: 0 0 10px; letter-spacing: 0; }
      h2 { font-size: 18px; margin: 26px 0 10px; letter-spacing: 0; }
      h3 { font-size: 15px; margin: 0 0 8px; letter-spacing: 0; }
      a { color: var(--blue); text-decoration: none; }
      a:hover { text-decoration: underline; }
      form { margin: 0; }
      .controls {
        display: grid;
        grid-template-columns: 1.2fr 1fr 110px 110px 120px 130px;
        gap: 10px;
        align-items: end;
      }
      .editor {
        display: grid;
        grid-template-columns: minmax(260px, 1fr) minmax(340px, 1.8fr);
        gap: 12px;
        margin-top: 12px;
      }
      .panel {
        background: var(--panel);
        border: 1px solid var(--line);
        padding: 12px;
      }
      label { display: grid; gap: 4px; color: var(--muted); font-size: 12px; }
      input, select, button, textarea {
        border: 1px solid var(--line);
        border-radius: 6px;
        padding: 0 10px;
        font: inherit;
        background: #fff;
      }
      input, select, button { height: 36px; }
      textarea {
        min-height: 154px;
        padding: 10px;
        resize: vertical;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        font-size: 12px;
        line-height: 1.5;
      }
      button {
        color: white;
        background: var(--text);
        cursor: pointer;
        white-space: nowrap;
      }
      .secondary-button { background: #475467; }
      .light-button { color: var(--text); background: #fff; }
      .actions { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
      .toolbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        margin: 14px 0;
        color: var(--muted);
        font-size: 13px;
      }
      .table {
        width: 100%;
        border-collapse: collapse;
        background: var(--panel);
        border: 1px solid var(--line);
      }
      th, td {
        border-bottom: 1px solid var(--line);
        padding: 10px 12px;
        text-align: left;
        vertical-align: top;
      }
      th {
        background: #eef2f7;
        color: #344054;
        font-size: 12px;
        white-space: nowrap;
      }
      td { font-size: 13px; }
      .score { font-weight: 700; font-variant-numeric: tabular-nums; }
      .muted { color: var(--muted); }
      .pill {
        display: inline-flex;
        align-items: center;
        height: 22px;
        padding: 0 8px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 650;
        border: 1px solid var(--line);
        white-space: nowrap;
      }
      .label-primary { color: var(--red); background: #fff1f0; border-color: #ffccc7; }
      .label-secondary { color: var(--green); background: #edfdf4; border-color: #c7eed8; }
      .label-weak { color: var(--amber); background: #fff8e6; border-color: #f3df9d; }
      .label-muted { color: var(--muted); background: #f2f4f7; }
      .tags { display: flex; flex-wrap: wrap; gap: 6px; }
      .tag { background: #f2f4f7; border: 1px solid var(--line); border-radius: 5px; padding: 2px 6px; }
      .scenario-head {
        background: var(--panel);
        border: 1px solid var(--line);
        padding: 16px;
        margin-bottom: 14px;
      }
      .description { white-space: pre-wrap; }
      .node {
        background: var(--panel);
        border: 1px solid var(--line);
        padding: 14px;
        margin: 10px 0;
      }
      .node-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin: 7px 0;
        color: var(--muted);
        font-size: 12px;
      }
      .summary, .significance { white-space: pre-wrap; margin: 8px 0 0; }
      @media (max-width: 980px) {
        .controls { grid-template-columns: 1fr 1fr 100px 100px; }
        .editor { grid-template-columns: 1fr; }
      }
      @media (max-width: 760px) {
        .controls { grid-template-columns: 1fr 1fr; }
        main { padding: 14px; }
        th:nth-child(5), td:nth-child(5), th:nth-child(7), td:nth-child(7) { display: none; }
      }
    </style>
    """


def _page(title: str, body: str) -> HTMLResponse:
    return HTMLResponse(
        "<!doctype html><html><head>"
        '<meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{_esc(title)}</title>"
        f"{_style()}</head><body>{body}</body></html>"
    )


def _profile_from_params(profile_id: str, threshold, limit, label, keywords_text: str) -> Dict:
    profile = infra_search.get_profile(profile_id)
    profile = {**profile}
    profile["threshold"] = int(threshold if threshold is not None else profile["threshold"])
    profile["limit"] = int(limit if limit is not None else profile["limit"])
    profile["label"] = label or profile.get("label") or "all"
    if keywords_text.strip():
        profile["keyword_groups"] = infra_search.parse_keyword_groups_text(keywords_text)
    return profile


def _get_results(profile: Dict, label: str, query: str, category: str) -> List[Dict]:
    results = infra_search.search_scenarios(
        threshold=profile["threshold"],
        limit=profile["limit"],
        keyword_groups=profile["keyword_groups"],
        query=query,
        category=category,
    )
    selected_label = label or profile.get("label") or "all"
    if selected_label != "all":
        results = [result for result in results if result["label"] == selected_label]
    return results


def _state_query(profile_id: str, threshold: int, limit: int, label: str, query: str, category: str) -> str:
    return urlencode({
        "profile": profile_id,
        "threshold": threshold,
        "limit": limit,
        "label": label,
        "query": query,
        "category": category,
    })


@app.get("/", response_class=HTMLResponse)
def index(
    profile: str = Query(infra_search.DEFAULT_PROFILE_ID),
    threshold: int | None = Query(None, ge=1, le=500),
    limit: int | None = Query(None, ge=1, le=1000),
    label: str = Query(""),
    query: str = Query(""),
    category: str = Query(""),
    message: str = Query(""),
):
    profiles = infra_search.load_profiles()
    active = _profile_from_params(profile, threshold, limit, label, "")
    active_label = label or active.get("label") or "all"
    keywords_text = infra_search.format_keyword_groups_text(active["keyword_groups"])
    results = _get_results(active, active_label, query, category)

    rows = []
    for rank, result in enumerate(results, 1):
        top_tags = list(result["tag_scores"].items())[:4]
        tags = "".join(
            f'<span class="tag">{_esc(tag)} {_esc(score)}</span>'
            for tag, score in top_tags
        ) or '<span class="muted">-</span>'
        qs = _state_query(active["id"], active["threshold"], active["limit"], active_label, query, category)
        rows.append(
            "<tr>"
            f"<td>{rank}</td>"
            f'<td><span class="pill {_label_class(result["label"])}">{_esc(result["label"])}</span></td>'
            f'<td class="score">{result["search_score"]}</td>'
            f'<td><a href="/scenario/{result["id"]}?{qs}">'
            f"{_esc(result['name'])}</a><div class=\"muted\">id {result['id']} · {_esc(result['category'])}</div></td>"
            f"<td>{result['matched_node_count']} / {result['node_count']}</td>"
            f'<td><div class="tags">{tags}</div></td>'
            f"<td>{_esc(result['updated_at'][:16].replace('T', ' '))}</td>"
            "</tr>"
        )

    profile_options = "".join(
        f'<option value="{_esc(pid)}" {_selected(active["id"], pid)}>{_esc(item["name"])} ({_esc(pid)})</option>'
        for pid, item in sorted(profiles.items())
    )
    label_options = "".join(
        f'<option value="{value}" {_selected(active_label, value)}>{value}</option>'
        for value in ["all", "primary", "secondary", "weak"]
    )
    notice = f'<div class="panel">{_esc(message)}</div>' if message else ""
    export_qs = _state_query(active["id"], active["threshold"], active["limit"], active_label, query, category)

    body = f"""
    <header>
      <h1>Scenario Search Browser</h1>
      <form method="get" action="/">
        <div class="controls">
          <label>profile
            <select name="profile" onchange="this.form.submit()">{profile_options}</select>
          </label>
          <label>query
            <input type="search" name="query" value="{_esc(query)}" placeholder="extra terms">
          </label>
          <label>threshold
            <input type="number" name="threshold" value="{active['threshold']}" min="1" max="500">
          </label>
          <label>limit
            <input type="number" name="limit" value="{active['limit']}" min="1" max="1000">
          </label>
          <label>label
            <select name="label">{label_options}</select>
          </label>
          <label>category
            <input name="category" value="{_esc(category)}" placeholder="optional">
          </label>
        </div>
        <div class="actions" style="margin-top:10px">
          <button type="submit">Search</button>
          <a class="pill label-muted" href="/export?{export_qs}">Export TXT</a>
          <a class="pill label-muted" href="/txt-index">View TXT index</a>
          <span class="muted">Profiles are stored in searched_scenario/search_profiles.json</span>
        </div>
      </form>
    </header>
    <main>
      {notice}
      <form method="post" action="/profiles/save">
        <input type="hidden" name="source_profile" value="{_esc(active['id'])}">
        <div class="editor">
          <section class="panel">
            <h3>Save Profile</h3>
            <label>profile id
              <input name="profile_id" value="{_esc(active['id'])}" placeholder="my_search">
            </label>
            <label>name
              <input name="name" value="{_esc(active['name'])}">
            </label>
            <label>description
              <input name="description" value="{_esc(active.get('description', ''))}">
            </label>
            <input type="hidden" name="threshold" value="{active['threshold']}">
            <input type="hidden" name="limit" value="{active['limit']}">
            <input type="hidden" name="label" value="{_esc(active_label)}">
            <div class="actions" style="margin-top:10px">
              <button type="submit">Save Profile</button>
              <a class="pill label-muted" href="/profiles.json">Raw profiles</a>
            </div>
          </section>
          <section class="panel">
            <h3>Keyword Groups</h3>
            <textarea name="keywords_text">{_esc(keywords_text)}</textarea>
          </section>
        </div>
      </form>

      <div class="toolbar">
        <span>{len(results)} scenarios matched · profile {_esc(active['name'])}</span>
        <span class="muted">score = metadata + top matched nodes + repeat bonus</span>
      </div>
      <table class="table">
        <thead>
          <tr>
            <th>#</th><th>label</th><th>score</th><th>scenario</th>
            <th>matched nodes</th><th>tags</th><th>updated</th>
          </tr>
        </thead>
        <tbody>{''.join(rows) if rows else '<tr><td colspan="7">No results.</td></tr>'}</tbody>
      </table>
    </main>
    """
    return _page("Scenario Search Browser", body)


@app.post("/profiles/save")
async def save_profile(request: Request):
    form = await request.form()
    profile_id = str(form.get("profile_id") or form.get("source_profile") or infra_search.DEFAULT_PROFILE_ID)
    profile = {
        "id": profile_id,
        "name": str(form.get("name") or profile_id),
        "description": str(form.get("description") or ""),
        "threshold": int(form.get("threshold") or 12),
        "limit": int(form.get("limit") or 100),
        "label": str(form.get("label") or "all"),
        "keyword_groups": infra_search.parse_keyword_groups_text(str(form.get("keywords_text") or "")),
    }
    saved = infra_search.save_profile(profile)
    qs = urlencode({
        "profile": saved["id"],
        "threshold": saved["threshold"],
        "limit": saved["limit"],
        "label": saved["label"],
        "message": f"Saved profile: {saved['name']}",
    })
    return RedirectResponse(f"/?{qs}", status_code=303)


@app.get("/scenario/{scenario_id}", response_class=HTMLResponse)
def scenario_detail(
    scenario_id: int,
    profile: str = Query(infra_search.DEFAULT_PROFILE_ID),
    threshold: int | None = Query(None, ge=1, le=500),
    limit: int | None = Query(None, ge=1, le=1000),
    label: str = Query(""),
    query: str = Query(""),
    category: str = Query(""),
):
    active = _profile_from_params(profile, threshold, limit, label, "")
    active_label = label or active.get("label") or "all"
    results = _get_results(active, active_label, query, category)
    result = next((item for item in results if item["id"] == scenario_id), None)
    back_qs = _state_query(active["id"], active["threshold"], active["limit"], active_label, query, category)
    if not result:
        return _page(
            "Scenario not found",
            f'<main><p>Scenario not found in current filter.</p><p><a href="/?{back_qs}">Back</a></p></main>',
        )

    tags = "".join(
        f'<span class="tag">{_esc(tag)} {_esc(score)}</span>'
        for tag, score in result["tag_scores"].items()
    )
    terms = "".join(
        f'<span class="tag">{_esc(term)}</span>'
        for term in result["matched_terms"][:40]
    )

    nodes = []
    for node in infra_search.chronological_nodes(result):
        node_terms = "".join(
            f'<span class="tag">{_esc(term)}</span>'
            for term in node["matched_terms"][:20]
        )
        url = node.get("url") or ""
        url_html = f'<a href="{_esc(url)}" target="_blank" rel="noreferrer">Open article</a>' if url else ""
        nodes.append(
            '<section class="node">'
            f"<h3>{_esc(node.get('node_order'))}. {_esc(node.get('title'))}</h3>"
            '<div class="node-meta">'
            f'<span class="score">score {node["search_score"]}</span>'
            f"<span>{_esc(node.get('source'))}</span>"
            f"<span>{_esc(node.get('published_at'))}</span>"
            f"<span>{url_html}</span>"
            "</div>"
            f'<div class="tags">{node_terms}</div>'
            f'<p class="significance"><strong>Significance</strong><br>{_esc(node.get("significance"))}</p>'
            f'<p class="summary"><strong>Summary</strong><br>{_esc(node.get("summary"))}</p>'
            "</section>"
        )

    body = f"""
    <header>
      <h1><a href="/?{back_qs}">Scenario Search Browser</a></h1>
    </header>
    <main>
      <section class="scenario-head">
        <h2>[{result['id']}] {_esc(result['name'])}</h2>
        <p>
          <span class="pill {_label_class(result['label'])}">{_esc(result['label'])}</span>
          <span class="score"> score {result['search_score']}</span>
          <span class="muted"> · {_esc(result['category'])} · matched nodes {result['matched_node_count']} / {result['node_count']}</span>
        </p>
        <p class="description">{_esc(result['description'])}</p>
        <h3>Tags</h3>
        <div class="tags">{tags}</div>
        <h3>Matched Terms</h3>
        <div class="tags">{terms}</div>
      </section>
      <h2>Matched News Nodes <span class="muted">(timeline order)</span></h2>
      {''.join(nodes) if nodes else '<p>No matched nodes.</p>'}
    </main>
    """
    return _page(result["name"], body)


@app.get("/export", response_class=HTMLResponse)
def export(
    profile: str = Query(infra_search.DEFAULT_PROFILE_ID),
    threshold: int | None = Query(None, ge=1, le=500),
    limit: int | None = Query(None, ge=1, le=1000),
    label: str = Query(""),
    query: str = Query(""),
    category: str = Query(""),
):
    active = _profile_from_params(profile, threshold, limit, label, "")
    active_label = label or active.get("label") or "all"
    results = _get_results(active, active_label, query, category)
    export_dir = os.path.join(infra_search.DEFAULT_OUTPUT_DIR, active["id"])
    written = infra_search.export_txt_files(
        results,
        output_dir=export_dir,
        profile_name=active["name"],
        clear_existing=True,
    )
    items = "".join(f"<li>{_esc(path)}</li>" for path in written[:30])
    more = "" if len(written) <= 30 else f"<p>... and {len(written) - 30} more files</p>"
    back_qs = _state_query(active["id"], active["threshold"], active["limit"], active_label, query, category)
    body = f"""
    <main>
      <h1>TXT Export Complete</h1>
      <p>{len(results)} scenarios exported to <code>{_esc(export_dir)}</code>.</p>
      <ul>{items}</ul>
      {more}
      <p><a href="/?{back_qs}">Back to results</a></p>
    </main>
    """
    return _page("TXT Export Complete", body)


@app.get("/txt-index", response_class=PlainTextResponse)
def txt_index(profile: str = Query(infra_search.DEFAULT_PROFILE_ID)):
    path = os.path.join(infra_search.DEFAULT_OUTPUT_DIR, profile, "index.txt")
    fallback = os.path.join(infra_search.DEFAULT_OUTPUT_DIR, "index.txt")
    for candidate in [path, fallback]:
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            continue
    return "No TXT export yet. Use Export TXT first.\n"


@app.get("/profiles.json", response_class=PlainTextResponse)
def raw_profiles():
    profiles = infra_search.load_profiles()
    import json

    return json.dumps({"profiles": profiles}, ensure_ascii=False, indent=2) + "\n"
