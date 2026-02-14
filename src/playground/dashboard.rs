//! Web dashboard HTML template for playground mode.
//!
//! Single-page HTML application with embedded JS/CSS — no external dependencies.
//! Provides topic browsing, message producing/consuming, and live metrics.

/// Returns the complete playground dashboard as a static HTML string.
pub const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Streamline Playground</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0a0e17;--surface:#131a2b;--surface2:#1a2235;--border:#253049;--primary:#38bdf8;--green:#4ade80;--yellow:#facc15;--red:#f87171;--orange:#fb923c;--text:#e2e8f0;--muted:#7b8ba5;--font:'SF Mono',SFMono-Regular,Menlo,Consolas,monospace;--font-ui:system-ui,-apple-system,sans-serif}
body{background:var(--bg);color:var(--text);font-family:var(--font-ui);line-height:1.6;min-height:100vh}
a{color:var(--primary);text-decoration:none}
a:hover{text-decoration:underline}
.container{max-width:1280px;margin:0 auto;padding:0 1.25rem}
header{background:var(--surface);border-bottom:1px solid var(--border);padding:.75rem 0}
header .container{display:flex;align-items:center;justify-content:space-between}
header h1{font-size:1.1rem;display:flex;align-items:center;gap:.5rem;font-family:var(--font-ui)}
header h1 .logo{color:var(--primary);font-weight:800}
.badge{font-size:.65rem;padding:2px 8px;border-radius:12px;background:var(--green);color:#000;font-weight:700;letter-spacing:.03em}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.75rem;margin:1.25rem 0}
.stat-card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:.75rem 1rem}
.stat-card h3{font-size:.65rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.2rem}
.stat-card .value{font-size:1.5rem;font-weight:700;color:var(--primary);font-family:var(--font)}
.panels{display:grid;grid-template-columns:1fr 1fr;gap:.75rem;margin-top:.75rem}
@media(max-width:900px){.panels{grid-template-columns:1fr}}
.panel{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.panel-header{padding:.6rem 1rem;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;background:var(--surface2)}
.panel-header h2{font-size:.85rem;font-weight:600}
.panel-body{padding:.75rem 1rem;max-height:400px;overflow-y:auto}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:.35rem .5rem;border-bottom:1px solid var(--border);font-size:.8rem}
th{color:var(--muted);font-weight:600;font-size:.7rem;text-transform:uppercase;letter-spacing:.05em}
td.msgs{font-family:var(--font);color:var(--green);font-size:.75rem}
.form-group{margin-bottom:.6rem}
.form-group label{display:block;font-size:.75rem;color:var(--muted);margin-bottom:.15rem}
input,textarea,select{width:100%;padding:.4rem .5rem;background:var(--bg);border:1px solid var(--border);border-radius:4px;color:var(--text);font-family:var(--font-ui);font-size:.82rem}
input:focus,textarea:focus,select:focus{outline:none;border-color:var(--primary)}
textarea{resize:vertical;min-height:60px;font-family:var(--font);font-size:.78rem}
button{padding:.4rem .8rem;border:none;border-radius:4px;cursor:pointer;font-size:.8rem;font-weight:600;transition:all .15s}
button:hover{opacity:.85;transform:translateY(-1px)}
.btn-primary{background:var(--primary);color:#000}
.btn-success{background:var(--green);color:#000}
.btn-sm{padding:.25rem .5rem;font-size:.72rem}
.msg-log{list-style:none}
.msg-log li{padding:.3rem 0;border-bottom:1px solid var(--border);font-family:var(--font);font-size:.75rem;word-break:break-all;line-height:1.4}
.msg-log .offset{color:var(--muted);margin-right:.4rem;font-size:.7rem}
.msg-log .key{color:var(--orange);margin-right:.4rem}
.msg-log .ts{color:var(--muted);font-size:.65rem;margin-left:.5rem}
.empty{color:var(--muted);font-style:italic;text-align:center;padding:1.5rem}
.actions{display:flex;gap:.4rem;flex-wrap:wrap;align-items:center}
#status-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);margin-right:.3rem;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.full-width{grid-column:1/-1}
.code{font-family:var(--font);font-size:.75rem;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:.5rem .75rem;margin:.3rem 0;overflow-x:auto;white-space:pre;line-height:1.5}
.code .prompt{color:var(--green)}
.code .comment{color:var(--muted)}
.connect-grid{display:grid;grid-template-columns:1fr 1fr;gap:.5rem}
@media(max-width:768px){.connect-grid{grid-template-columns:1fr}}
.connect-item h4{font-size:.75rem;color:var(--primary);margin-bottom:.25rem}
footer{text-align:center;color:var(--muted);font-size:.7rem;padding:1.5rem 0 1rem}
.auto-on{background:var(--yellow) !important;color:#000 !important}
</style>
</head>
<body>
<header>
<div class="container">
  <h1>⚡ <span class="logo">Streamline</span> Playground</h1>
  <div><span id="status-dot"></span><span class="badge">RUNNING</span></div>
</div>
</header>
<div class="container">
  <div class="stats">
    <div class="stat-card"><h3>Version</h3><div class="value" id="version">—</div></div>
    <div class="stat-card"><h3>Topics</h3><div class="value" id="topic-count">—</div></div>
    <div class="stat-card"><h3>Total Messages</h3><div class="value" id="msg-count">—</div></div>
    <div class="stat-card"><h3>Uptime</h3><div class="value" id="uptime">—</div></div>
  </div>

  <div class="panels">
    <!-- Topics Panel -->
    <div class="panel">
      <div class="panel-header">
        <h2>📋 Topics</h2>
        <div class="actions">
          <input id="new-topic" placeholder="topic-name" style="width:110px">
          <button class="btn-primary btn-sm" onclick="createTopic()">+ Create</button>
        </div>
      </div>
      <div class="panel-body">
        <table><thead><tr><th>Name</th><th>Partitions</th><th>Messages</th><th></th></tr></thead>
        <tbody id="topics-table"><tr><td colspan="4" class="empty">Loading…</td></tr></tbody></table>
      </div>
    </div>

    <!-- Produce Panel -->
    <div class="panel">
      <div class="panel-header"><h2>📤 Produce Message</h2></div>
      <div class="panel-body">
        <div class="form-group"><label>Topic</label><select id="produce-topic"></select></div>
        <div class="form-group"><label>Key (optional)</label><input id="produce-key" placeholder="message-key"></div>
        <div class="form-group"><label>Value</label><textarea id="produce-value" placeholder='{"event":"click","user":"u42"}'></textarea></div>
        <div class="actions">
          <button class="btn-primary" onclick="produceMessage()">Send Message</button>
          <button class="btn-success btn-sm" onclick="produceSample()">⚡ Sample</button>
        </div>
        <div id="produce-result" style="margin-top:.4rem;font-size:.78rem"></div>
      </div>
    </div>

    <!-- Consume Panel -->
    <div class="panel full-width">
      <div class="panel-header">
        <h2>📥 Messages</h2>
        <div class="actions">
          <select id="consume-topic" style="width:140px"></select>
          <input id="consume-offset" type="number" value="0" min="0" placeholder="offset" style="width:70px">
          <button class="btn-primary btn-sm" onclick="consumeMessages()">Fetch</button>
          <button class="btn-sm" id="auto-btn" onclick="toggleAutoRefresh()">⟳ Auto</button>
        </div>
      </div>
      <div class="panel-body" style="max-height:350px">
        <ul class="msg-log" id="messages-list"><li class="empty">Select a topic and click Fetch</li></ul>
      </div>
    </div>

    <!-- Connection Info Panel -->
    <div class="panel full-width">
      <div class="panel-header"><h2>🔗 Connection Info</h2></div>
      <div class="panel-body">
        <div class="connect-grid">
          <div class="connect-item">
            <h4>Kafka CLI (kcat)</h4>
            <div class="code"><span class="comment"># Produce</span>
<span class="prompt">$</span> echo '{"hello":"world"}' | kcat -b <span id="c-kafka">localhost:9092</span> -t demo-events -P
<span class="comment"># Consume</span>
<span class="prompt">$</span> kcat -b <span id="c-kafka2">localhost:9092</span> -t demo-events -C -e</div>
          </div>
          <div class="connect-item">
            <h4>HTTP REST API</h4>
            <div class="code"><span class="comment"># List topics</span>
<span class="prompt">$</span> curl <span id="c-http">http://localhost:9094</span>/api/v1/topics
<span class="comment"># Produce</span>
<span class="prompt">$</span> curl -X POST <span id="c-http2">http://localhost:9094</span>/api/v1/topics/demo-events/messages \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"value":"hello"}]}'</div>
          </div>
          <div class="connect-item">
            <h4>Python (kafka-python)</h4>
            <div class="code">from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers='<span id="c-py">localhost:9092</span>')
producer.send('demo-events',
    b'{"hello":"world"}')</div>
          </div>
          <div class="connect-item">
            <h4>Node.js (kafkajs)</h4>
            <div class="code">const { Kafka } = require('kafkajs');
const kafka = new Kafka({
  brokers: ['<span id="c-node">localhost:9092</span>'] });
const producer = kafka.producer();
await producer.send({ topic: 'demo-events',
  messages: [{ value: '{"hello":"world"}' }]
});</div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <footer>⚡ Streamline — The Redis of Streaming · In-memory playground mode · Data resets on restart</footer>
</div>

<script>
const BASE = window.location.origin;
const kafkaHost = window.location.hostname + ':9092';
const httpBase = window.location.origin;
let autoRefresh = null;
let startTime = Date.now();

async function api(path, opts) {
  try {
    const r = await fetch(BASE + path, opts);
    if (!r.ok) return null;
    return await r.json();
  } catch(e) { console.error('API error:', e); return null; }
}

function fmtNum(n) {
  if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return String(n);
}

async function refreshStatus() {
  const d = await api('/api/v1/playground/status');
  if (!d) return;
  document.getElementById('version').textContent = d.version || '—';
  document.getElementById('topic-count').textContent = d.available_topics ?? '—';
  const secs = Math.floor((Date.now() - startTime) / 1000);
  if (secs < 60) document.getElementById('uptime').textContent = secs + 's';
  else if (secs < 3600) document.getElementById('uptime').textContent = Math.floor(secs/60) + 'm ' + (secs%60) + 's';
  else document.getElementById('uptime').textContent = Math.floor(secs/3600) + 'h ' + Math.floor((secs%3600)/60) + 'm';
}

async function refreshTopics() {
  const data = await api('/api/v1/topics');
  const tb = document.getElementById('topics-table');
  const pt = document.getElementById('produce-topic');
  const ct = document.getElementById('consume-topic');
  if (!data || !Array.isArray(data) || data.length === 0) {
    tb.innerHTML = '<tr><td colspan="4" class="empty">No topics yet — create one above</td></tr>';
    document.getElementById('msg-count').textContent = '0';
    return;
  }
  let totalMsgs = 0;
  tb.innerHTML = '';
  const prevPT = pt.value, prevCT = ct.value;
  pt.innerHTML = ''; ct.innerHTML = '';
  data.forEach(function(t) {
    var msgs = t.total_messages || 0;
    totalMsgs += msgs;
    var tr = document.createElement('tr');
    tr.innerHTML = '<td>' + escHtml(t.name) + '</td><td>' + t.partition_count + '</td><td class="msgs">' + fmtNum(msgs) + '</td><td><button class="btn-sm btn-primary" onclick="selectTopic(\'' + escAttr(t.name) + '\')">View</button></td>';
    tb.appendChild(tr);
    [pt, ct].forEach(function(sel) { var o = document.createElement('option'); o.value = t.name; o.text = t.name; sel.add(o); });
  });
  if (prevPT) pt.value = prevPT;
  if (prevCT) ct.value = prevCT;
  document.getElementById('msg-count').textContent = fmtNum(totalMsgs);
}

async function createTopic() {
  var name = document.getElementById('new-topic').value.trim();
  if (!name) return;
  await api('/api/v1/topics', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({name: name, partitions: 1}) });
  document.getElementById('new-topic').value = '';
  refreshTopics();
}

async function produceMessage() {
  var topic = document.getElementById('produce-topic').value;
  var key = document.getElementById('produce-key').value || null;
  var value = document.getElementById('produce-value').value;
  if (!topic || !value) return;
  var body;
  try { body = {records:[{key: key, value: JSON.parse(value), partition: 0}]}; }
  catch(e) { body = {records:[{key: key, value: value, partition: 0}]}; }
  var d = await api('/api/v1/topics/' + encodeURIComponent(topic) + '/messages', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify(body)
  });
  var el = document.getElementById('produce-result');
  if (d && d.offsets && d.offsets.length > 0) {
    el.innerHTML = '<span style="color:var(--green)">✓ Written to partition ' + d.offsets[0].partition + ' at offset ' + d.offsets[0].offset + '</span>';
    refreshTopics();
  } else {
    el.innerHTML = '<span style="color:var(--red)">✗ Failed to produce</span>';
  }
}

function produceSample() {
  var samples = [
    '{"type":"page_view","user_id":"u' + Math.floor(Math.random()*100) + '","page":"/products","ts":' + Date.now() + '}',
    '{"metric":"cpu_usage","value":' + (20 + Math.random()*60).toFixed(1) + ',"host":"web-01","ts":' + Date.now() + '}',
    '{"level":"INFO","service":"api-gw","msg":"Request processed in ' + Math.floor(Math.random()*200) + 'ms","ts":' + Date.now() + '}',
    '{"event":"order_placed","order_id":"ord-' + Math.floor(Math.random()*9999) + '","amount":' + (Math.random()*500).toFixed(2) + ',"ts":' + Date.now() + '}',
  ];
  document.getElementById('produce-value').value = samples[Math.floor(Math.random() * samples.length)];
}

async function consumeMessages() {
  var topic = document.getElementById('consume-topic').value;
  if (!topic) return;
  var offset = parseInt(document.getElementById('consume-offset').value) || 0;
  var d = await api('/api/v1/topics/' + encodeURIComponent(topic) + '/partitions/0/messages?offset=' + offset + '&limit=50');
  var ul = document.getElementById('messages-list');
  if (!d || !d.records || d.records.length === 0) {
    ul.innerHTML = '<li class="empty">No messages in ' + escHtml(topic) + ' at offset ' + offset + '</li>';
    return;
  }
  ul.innerHTML = '';
  d.records.forEach(function(m) {
    var li = document.createElement('li');
    var val = typeof m.value === 'string' ? m.value : JSON.stringify(m.value);
    var cls = '';
    if (val.indexOf('"ERROR"') !== -1) cls = 'level-ERROR';
    else if (val.indexOf('"WARN"') !== -1) cls = 'level-WARN';
    var keyHtml = m.key ? '<span class="key">' + escHtml(m.key) + '</span>' : '';
    li.innerHTML = '<span class="offset">[' + m.offset + ']</span>' + keyHtml + escHtml(val);
    if (cls) li.className = cls;
    ul.appendChild(li);
  });
}

function selectTopic(name) {
  document.getElementById('consume-topic').value = name;
  document.getElementById('consume-offset').value = '0';
  consumeMessages();
}

function toggleAutoRefresh() {
  var btn = document.getElementById('auto-btn');
  if (autoRefresh) { clearInterval(autoRefresh); autoRefresh = null; btn.className = 'btn-sm'; btn.textContent = '⟳ Auto'; return; }
  autoRefresh = setInterval(function() { consumeMessages(); refreshStatus(); refreshTopics(); }, 3000);
  btn.className = 'btn-sm auto-on'; btn.textContent = '⟳ On';
}

function escHtml(s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function escAttr(s) { return s.replace(/'/g, "\\'").replace(/"/g, '&quot;'); }

// Set connection info based on current host
(function() {
  var h = window.location.hostname || 'localhost';
  ['c-kafka','c-kafka2'].forEach(function(id) { document.getElementById(id).textContent = h + ':9092'; });
  ['c-http','c-http2'].forEach(function(id) { document.getElementById(id).textContent = window.location.origin; });
  ['c-py','c-node'].forEach(function(id) { document.getElementById(id).textContent = h + ':9092'; });
})();

refreshStatus();
refreshTopics();
setInterval(refreshStatus, 5000);
setInterval(refreshTopics, 10000);
</script>
</body>
</html>"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dashboard_html_is_valid() {
        assert!(DASHBOARD_HTML.contains("<!DOCTYPE html>"));
        assert!(DASHBOARD_HTML.contains("</html>"));
        assert!(DASHBOARD_HTML.contains("Streamline Playground"));
    }

    #[test]
    fn test_dashboard_has_key_elements() {
        assert!(DASHBOARD_HTML.contains("topics-table"));
        assert!(DASHBOARD_HTML.contains("produce-topic"));
        assert!(DASHBOARD_HTML.contains("consume-topic"));
        assert!(DASHBOARD_HTML.contains("messages-list"));
    }
}
