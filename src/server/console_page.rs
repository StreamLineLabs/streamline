//! Embedded Web Console — lightweight dashboard served at /console
//!
//! A single-page web console embedded directly in the Streamline binary,
//! accessible at `http://localhost:9094/console`. Uses the dashboard
//! and inspector REST APIs for data.
//!
//! No external dependencies — pure HTML/CSS/JS served via include_str.

use axum::{
    response::{Html, IntoResponse},
    routing::get,
    Router,
};

/// Create the embedded console router
pub fn create_console_page_router() -> Router {
    Router::new()
        .route("/console", get(console_page))
        .route("/console/", get(console_page))
}

async fn console_page() -> impl IntoResponse {
    Html(CONSOLE_HTML)
}

const CONSOLE_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Streamline Console</title>
<style>
  :root { --bg: #0d1117; --surface: #161b22; --border: #30363d; --text: #c9d1d9; --accent: #58a6ff; --green: #3fb950; --yellow: #d29922; --red: #f85149; }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', monospace; background: var(--bg); color: var(--text); }
  .header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 12px 24px; display: flex; align-items: center; gap: 16px; }
  .header h1 { font-size: 18px; color: var(--accent); }
  .header .version { font-size: 12px; color: #8b949e; }
  .nav { display: flex; gap: 8px; margin-left: auto; }
  .nav button { background: none; border: 1px solid var(--border); color: var(--text); padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; }
  .nav button.active, .nav button:hover { background: var(--accent); color: #fff; border-color: var(--accent); }
  .content { max-width: 1200px; margin: 0 auto; padding: 24px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
  .card h3 { font-size: 13px; color: #8b949e; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
  .card .value { font-size: 28px; font-weight: bold; }
  .status-ok { color: var(--green); }
  .status-err { color: var(--red); }
  table { width: 100%; border-collapse: collapse; background: var(--surface); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
  th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid var(--border); font-size: 13px; }
  th { color: #8b949e; font-weight: 600; text-transform: uppercase; font-size: 11px; }
  .section { margin-bottom: 32px; }
  .section h2 { font-size: 16px; margin-bottom: 12px; }
  #messages-panel, #sql-panel { display: none; }
  .msg-value { font-family: monospace; font-size: 12px; max-width: 500px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .search-bar { display: flex; gap: 8px; margin-bottom: 16px; }
  .search-bar input, .search-bar select { background: var(--surface); border: 1px solid var(--border); color: var(--text); padding: 8px 12px; border-radius: 6px; font-size: 14px; }
  .search-bar input { flex: 1; }
  .btn { background: var(--accent); color: #fff; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; }
  .btn:hover { opacity: 0.9; }
  .loading { color: #8b949e; text-align: center; padding: 40px; }
  .sql-editor { width: 100%; background: var(--surface); border: 1px solid var(--border); color: var(--text); padding: 12px; border-radius: 6px; font-family: monospace; font-size: 14px; min-height: 80px; resize: vertical; }
  pre { background: var(--surface); border: 1px solid var(--border); padding: 12px; border-radius: 6px; overflow-x: auto; font-size: 12px; max-height: 400px; overflow-y: auto; }
</style>
</head>
<body>
<div class="header">
  <h1>&#9889; Streamline Console</h1>
  <span class="version">v0.2.0</span>
  <div class="nav">
    <button class="active" onclick="showPanel('dashboard', this)">Dashboard</button>
    <button onclick="showPanel('messages', this)">Messages</button>
    <button onclick="showPanel('sql', this)">SQL</button>
  </div>
</div>
<div class="content">
  <div id="dashboard-panel">
    <div class="grid">
      <div class="card"><h3>Status</h3><div class="value status-ok" id="health">Loading...</div></div>
      <div class="card"><h3>Uptime</h3><div class="value" id="uptime">-</div></div>
      <div class="card"><h3>Topics</h3><div class="value" id="topics">-</div></div>
      <div class="card"><h3>Groups</h3><div class="value" id="groups">-</div></div>
    </div>
    <div class="section"><h2>Topics</h2>
      <table><thead><tr><th>Name</th><th>Partitions</th><th>Messages</th></tr></thead>
      <tbody id="t-topics"><tr><td colspan="3" class="loading">Loading...</td></tr></tbody></table>
    </div>
    <div class="section"><h2>Consumer Groups</h2>
      <table><thead><tr><th>Group</th><th>State</th><th>Members</th><th>Lag</th></tr></thead>
      <tbody id="t-groups"><tr><td colspan="4" class="loading">Loading...</td></tr></tbody></table>
    </div>
  </div>
  <div id="messages-panel">
    <div class="search-bar">
      <input id="m-topic" placeholder="Topic name" />
      <input id="m-search" placeholder="Search (optional)" />
      <button class="btn" onclick="browse()">Browse</button>
      <button class="btn" onclick="search()">Search</button>
    </div>
    <table><thead><tr><th>Offset</th><th>Partition</th><th>Key</th><th>Value</th><th>Time</th></tr></thead>
    <tbody id="t-msgs"><tr><td colspan="5" class="loading">Enter a topic name</td></tr></tbody></table>
  </div>
  <div id="sql-panel">
    <h2>SQL Query Editor</h2>
    <textarea class="sql-editor" id="sql-in">SELECT * FROM streamline_topic('events') LIMIT 10</textarea>
    <div style="margin-top:8px"><button class="btn" onclick="runSQL()">Run Query</button></div>
    <div id="sql-out" style="margin-top:16px"></div>
  </div>
</div>
<script>
function showPanel(n, btn) {
  ['dashboard','messages','sql'].forEach(p => document.getElementById(p+'-panel').style.display = p===n ? 'block' : 'none');
  document.querySelectorAll('.nav button').forEach(b => b.classList.remove('active'));
  if(btn) btn.classList.add('active');
}
function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function fmtUp(s) { if(s<60) return Math.floor(s)+'s'; if(s<3600) return Math.floor(s/60)+'m'; return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m'; }

async function load() {
  try {
    const r = await fetch('/api/v1/dashboard'); const d = await r.json();
    document.getElementById('health').textContent = d.health_status?.status || 'Healthy';
    document.getElementById('uptime').textContent = fmtUp(d.uptime_seconds || 0);
    document.getElementById('topics').textContent = d.stats?.topic_count || 0;
  } catch(e) { document.getElementById('health').textContent = 'Offline'; document.getElementById('health').className = 'value status-err'; }
  try {
    const r = await fetch('/api/v1/dashboard/topics/summary'); const d = await r.json();
    const ts = d.topics || d || [];
    document.getElementById('t-topics').innerHTML = ts.length ? ts.map(t =>
      '<tr><td>'+esc(t.name||t.topic)+'</td><td>'+(t.partitions||t.partition_count||'-')+'</td><td>'+(t.messages||t.total_messages||'-')+'</td></tr>'
    ).join('') : '<tr><td colspan="3">No topics</td></tr>';
  } catch(e) {}
  try {
    const r = await fetch('/api/v1/dashboard/groups'); const gs = await r.json();
    const arr = Array.isArray(gs) ? gs : [];
    document.getElementById('groups').textContent = arr.length;
    document.getElementById('t-groups').innerHTML = arr.length ? arr.map(g =>
      '<tr><td>'+esc(g.group_id)+'</td><td>'+esc(g.state)+'</td><td>'+g.members+'</td><td>'+g.total_lag+'</td></tr>'
    ).join('') : '<tr><td colspan="4">No groups</td></tr>';
  } catch(e) {}
}

async function browse() {
  const t = document.getElementById('m-topic').value.trim(); if(!t) return;
  try {
    const r = await fetch('/api/v1/inspect/'+encodeURIComponent(t)+'/latest?count=50');
    renderMsgs(await r.json());
  } catch(e) { document.getElementById('t-msgs').innerHTML = '<tr><td colspan="5">Error: '+esc(e.message)+'</td></tr>'; }
}
async function search() {
  const t = document.getElementById('m-topic').value.trim();
  const q = document.getElementById('m-search').value.trim(); if(!t||!q) return;
  try {
    const r = await fetch('/api/v1/inspect/'+encodeURIComponent(t)+'/search?q='+encodeURIComponent(q));
    renderMsgs(await r.json());
  } catch(e) { document.getElementById('t-msgs').innerHTML = '<tr><td colspan="5">Error</td></tr>'; }
}
function renderMsgs(data) {
  const msgs = data.messages || [];
  document.getElementById('t-msgs').innerHTML = msgs.length ? msgs.map(m => {
    const v = typeof m.value === 'object' ? JSON.stringify(m.value) : String(m.value);
    const ts = m.timestamp ? new Date(m.timestamp).toLocaleTimeString() : '-';
    return '<tr><td>'+m.offset+'</td><td>'+m.partition+'</td><td>'+esc(m.key||'-')+'</td><td class="msg-value" title="'+esc(v)+'">'+esc(v)+'</td><td>'+ts+'</td></tr>';
  }).join('') : '<tr><td colspan="5">No messages</td></tr>';
}

async function runSQL() {
  const sql = document.getElementById('sql-in').value.trim(); if(!sql) return;
  const out = document.getElementById('sql-out');
  out.innerHTML = '<div class="loading">Running...</div>';
  try {
    const r = await fetch('/api/v1/query', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({sql:sql, max_rows:100}) });
    const d = await r.json();
    out.innerHTML = '<pre>'+esc(JSON.stringify(d, null, 2))+'</pre>';
  } catch(e) { out.innerHTML = '<pre style="color:var(--red)">Error: '+esc(e.message)+'</pre>'; }
}

load(); setInterval(load, 10000);
</script>
</body>
</html>"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_console_html_content() {
        assert!(CONSOLE_HTML.len() > 1000, "Console HTML must be substantive");
        assert!(CONSOLE_HTML.contains("Streamline Console"), "Must contain title");
        assert!(CONSOLE_HTML.contains("/api/v1/dashboard"), "Must reference dashboard API");
        assert!(CONSOLE_HTML.contains("/api/v1/inspect"), "Must reference inspector API");
        assert!(CONSOLE_HTML.contains("/api/v1/query"), "Must reference query API");
    }
}
