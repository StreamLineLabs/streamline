//! HTML templates for the Schema Registry Web UI
//!
//! This module contains inline HTML templates rendered as Rust string constants.
//! The UI uses Tailwind CSS (CDN) for styling and highlight.js (CDN) for
//! schema syntax highlighting. No build tools are required.

/// Base HTML layout wrapping page content.
///
/// Placeholders:
/// - `{title}` - Page title
/// - `{content}` - Inner HTML body content
pub(crate) const BASE_LAYOUT: &str = r#"<!DOCTYPE html>
<html lang="en" class="h-full bg-gray-50">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title} - Streamline Schema Registry</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css">
  <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/json.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/protobuf.min.js"></script>
  <style>
    .schema-code { max-height: 500px; overflow: auto; }
    .diff-added { background-color: #d4edda; }
    .diff-removed { background-color: #f8d7da; }
    .fade-in { animation: fadeIn 0.2s ease-in; }
    @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
  </style>
</head>
<body class="h-full">
  <div class="min-h-full">
    <!-- Navigation -->
    <nav class="bg-gray-900 shadow-lg">
      <div class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
        <div class="flex h-16 items-center justify-between">
          <div class="flex items-center space-x-4">
            <a href="/ui/schemas" class="text-white font-bold text-xl tracking-tight">
              Streamline <span class="text-indigo-400">Schema Registry</span>
            </a>
          </div>
          <div class="flex items-center space-x-4">
            <a href="/ui/schemas" class="text-gray-300 hover:text-white px-3 py-2 text-sm font-medium rounded-md hover:bg-gray-700 transition">
              Schemas
            </a>
            <a href="/ui/schemas/compare" class="text-gray-300 hover:text-white px-3 py-2 text-sm font-medium rounded-md hover:bg-gray-700 transition">
              Compare
            </a>
            <a href="/ui/schemas/register" class="bg-indigo-600 hover:bg-indigo-500 text-white px-3 py-2 text-sm font-medium rounded-md transition">
              Register Schema
            </a>
          </div>
        </div>
      </div>
    </nav>

    <!-- Main Content -->
    <main class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-8 fade-in">
      {content}
    </main>

    <!-- Footer -->
    <footer class="bg-gray-100 border-t border-gray-200 mt-12">
      <div class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-4 text-center text-sm text-gray-500">
        Streamline Schema Registry &middot; Powered by
        <a href="https://github.com/streamlinelabs/streamline" class="text-indigo-600 hover:underline">Streamline</a>
      </div>
    </footer>
  </div>

  <script>
    document.addEventListener('DOMContentLoaded', function() {
      document.querySelectorAll('pre code').forEach(function(el) {
        hljs.highlightElement(el);
      });
    });
  </script>
</body>
</html>"#;

/// Schema list page template.
///
/// Placeholders:
/// - `{subject_rows}` - Table rows for each subject
/// - `{total_subjects}` - Total subject count
/// - `{total_versions}` - Total version count
/// - `{global_compatibility}` - Global compatibility level
pub(crate) const SCHEMA_LIST_PAGE: &str = r#"
<!-- Dashboard Stats -->
<div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
  <div class="bg-white rounded-lg shadow p-6">
    <div class="text-sm font-medium text-gray-500">Total Subjects</div>
    <div class="mt-1 text-3xl font-bold text-gray-900">{total_subjects}</div>
  </div>
  <div class="bg-white rounded-lg shadow p-6">
    <div class="text-sm font-medium text-gray-500">Total Versions</div>
    <div class="mt-1 text-3xl font-bold text-gray-900">{total_versions}</div>
  </div>
  <div class="bg-white rounded-lg shadow p-6">
    <div class="text-sm font-medium text-gray-500">Global Compatibility</div>
    <div class="mt-1 text-3xl font-bold text-indigo-600">{global_compatibility}</div>
  </div>
</div>

<!-- Subject List -->
<div class="bg-white rounded-lg shadow">
  <div class="px-6 py-4 border-b border-gray-200">
    <h2 class="text-lg font-semibold text-gray-900">Registered Subjects</h2>
  </div>
  {subject_rows}
</div>
"#;

/// Single subject row for the list page.
///
/// Placeholders:
/// - `{subject}` - Subject name
/// - `{schema_type}` - Schema type (AVRO, PROTOBUF, JSON)
/// - `{version_count}` - Number of versions
/// - `{latest_version}` - Latest version number
/// - `{compatibility}` - Compatibility level
/// - `{type_color}` - Tailwind color class for schema type badge
pub(crate) const SUBJECT_ROW: &str = r#"
  <a href="/ui/schemas/{subject}" class="block px-6 py-4 hover:bg-gray-50 border-b border-gray-100 transition">
    <div class="flex items-center justify-between">
      <div class="flex items-center space-x-3">
        <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium {type_color}">
          {schema_type}
        </span>
        <span class="text-sm font-medium text-gray-900">{subject}</span>
      </div>
      <div class="flex items-center space-x-6 text-sm text-gray-500">
        <span>{version_count} version(s)</span>
        <span>Latest: v{latest_version}</span>
        <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-700">
          {compatibility}
        </span>
      </div>
    </div>
  </a>
"#;

/// Empty state when no subjects are registered.
pub(crate) const EMPTY_SUBJECTS: &str = r#"
  <div class="px-6 py-12 text-center">
    <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
        d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/>
    </svg>
    <h3 class="mt-2 text-sm font-semibold text-gray-900">No schemas registered</h3>
    <p class="mt-1 text-sm text-gray-500">Get started by registering your first schema.</p>
    <div class="mt-6">
      <a href="/ui/schemas/register" class="inline-flex items-center rounded-md bg-indigo-600 px-3 py-2 text-sm font-semibold text-white hover:bg-indigo-500">
        Register Schema
      </a>
    </div>
  </div>
"#;

/// Schema detail page template.
///
/// Placeholders:
/// - `{subject}` - Subject name
/// - `{schema_type}` - Schema type
/// - `{compatibility}` - Subject compatibility level
/// - `{version_count}` - Total versions
/// - `{version_tabs}` - Version tab elements
/// - `{version_panels}` - Version panel elements (hidden/shown by JS)
/// - `{latest_schema}` - Latest schema content (pretty-printed)
/// - `{latest_version}` - Latest version number
/// - `{latest_id}` - Latest schema ID
/// - `{type_color}` - Badge color
pub(crate) const SCHEMA_DETAIL_PAGE: &str = r#"
<!-- Breadcrumb -->
<nav class="mb-6 text-sm text-gray-500">
  <a href="/ui/schemas" class="hover:text-indigo-600">Schemas</a>
  <span class="mx-2">/</span>
  <span class="text-gray-900 font-medium">{subject}</span>
</nav>

<!-- Subject Header -->
<div class="bg-white rounded-lg shadow mb-6">
  <div class="px-6 py-5">
    <div class="flex items-center justify-between">
      <div>
        <h1 class="text-2xl font-bold text-gray-900">{subject}</h1>
        <div class="mt-2 flex items-center space-x-3">
          <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium {type_color}">
            {schema_type}
          </span>
          <span class="text-sm text-gray-500">{version_count} version(s)</span>
          <span class="text-sm text-gray-500">Compatibility: <strong>{compatibility}</strong></span>
        </div>
      </div>
      <div class="flex space-x-2">
        <a href="/ui/schemas/compare?left={subject}&leftVersion={latest_version}"
           class="inline-flex items-center rounded-md bg-white px-3 py-2 text-sm font-semibold text-gray-700 ring-1 ring-gray-300 hover:bg-gray-50">
          Compare
        </a>
      </div>
    </div>
  </div>
</div>

<!-- Version Selector & Schema Viewer -->
<div class="bg-white rounded-lg shadow">
  <div class="border-b border-gray-200">
    <nav class="flex -mb-px px-6" id="version-tabs">
      {version_tabs}
    </nav>
  </div>
  <div id="version-panels">
    {version_panels}
  </div>
</div>

<script>
function showVersion(version) {
  // Hide all panels
  document.querySelectorAll('.version-panel').forEach(function(panel) {
    panel.classList.add('hidden');
  });
  // Deactivate all tabs
  document.querySelectorAll('.version-tab').forEach(function(tab) {
    tab.classList.remove('border-indigo-500', 'text-indigo-600');
    tab.classList.add('border-transparent', 'text-gray-500');
  });
  // Show selected panel
  var panel = document.getElementById('panel-v' + version);
  if (panel) {
    panel.classList.remove('hidden');
    panel.querySelectorAll('pre code').forEach(function(el) {
      hljs.highlightElement(el);
    });
  }
  // Activate selected tab
  var tab = document.getElementById('tab-v' + version);
  if (tab) {
    tab.classList.add('border-indigo-500', 'text-indigo-600');
    tab.classList.remove('border-transparent', 'text-gray-500');
  }
}
// Show latest version on load
document.addEventListener('DOMContentLoaded', function() {
  showVersion({latest_version});
});
</script>
"#;

/// Version tab element for the detail page.
///
/// Placeholders:
/// - `{version}` - Version number
/// - `{active_class}` - Active/inactive CSS classes
pub(crate) const VERSION_TAB: &str = r#"
      <button id="tab-v{version}" onclick="showVersion({version})"
        class="version-tab whitespace-nowrap py-4 px-4 border-b-2 font-medium text-sm cursor-pointer {active_class}">
        v{version}
      </button>
"#;

/// Version panel element for the detail page.
///
/// Placeholders:
/// - `{version}` - Version number
/// - `{id}` - Schema ID
/// - `{schema_type}` - Schema type
/// - `{schema_content}` - The schema definition (HTML-escaped)
/// - `{hidden_class}` - "hidden" or "" depending on active state
pub(crate) const VERSION_PANEL: &str = r#"
    <div id="panel-v{version}" class="version-panel p-6 {hidden_class}">
      <div class="flex items-center justify-between mb-4">
        <div class="text-sm text-gray-500">
          <span class="font-medium">Version {version}</span> &middot; Schema ID: {id} &middot; Type: {schema_type}
        </div>
        <a href="/ui/schemas/{subject}/versions/{version}"
           class="text-sm text-indigo-600 hover:text-indigo-500">Permalink</a>
      </div>
      <div class="schema-code rounded-lg overflow-hidden">
        <pre><code class="language-json">{schema_content}</code></pre>
      </div>
    </div>
"#;

/// Schema version detail page (permalink for a specific version).
///
/// Placeholders:
/// - `{subject}` - Subject name
/// - `{version}` - Version number
/// - `{id}` - Schema ID
/// - `{schema_type}` - Schema type
/// - `{schema_content}` - The schema definition (HTML-escaped)
/// - `{type_color}` - Badge color
pub(crate) const SCHEMA_VERSION_PAGE: &str = r#"
<!-- Breadcrumb -->
<nav class="mb-6 text-sm text-gray-500">
  <a href="/ui/schemas" class="hover:text-indigo-600">Schemas</a>
  <span class="mx-2">/</span>
  <a href="/ui/schemas/{subject}" class="hover:text-indigo-600">{subject}</a>
  <span class="mx-2">/</span>
  <span class="text-gray-900 font-medium">v{version}</span>
</nav>

<div class="bg-white rounded-lg shadow">
  <div class="px-6 py-5 border-b border-gray-200">
    <div class="flex items-center justify-between">
      <div>
        <h1 class="text-xl font-bold text-gray-900">{subject} &mdash; Version {version}</h1>
        <div class="mt-1 flex items-center space-x-3 text-sm text-gray-500">
          <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium {type_color}">
            {schema_type}
          </span>
          <span>Schema ID: {id}</span>
        </div>
      </div>
    </div>
  </div>
  <div class="p-6">
    <div class="schema-code rounded-lg overflow-hidden">
      <pre><code class="language-json">{schema_content}</code></pre>
    </div>
  </div>
</div>
"#;

/// Schema comparison page template.
///
/// Placeholders:
/// - `{left_subject}` - Left subject (default or from query param)
/// - `{left_version}` - Left version (default or from query param)
/// - `{right_subject}` - Right subject
/// - `{right_version}` - Right version
/// - `{subject_options}` - <option> elements for subject selects
pub(crate) const SCHEMA_COMPARE_PAGE: &str = r#"
<h1 class="text-2xl font-bold text-gray-900 mb-6">Schema Comparison</h1>

<div class="bg-white rounded-lg shadow mb-6">
  <div class="px-6 py-4 border-b border-gray-200">
    <form id="compare-form" class="flex flex-wrap items-end gap-4">
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Left Subject</label>
        <select name="left" id="left-subject" class="rounded-md border-gray-300 shadow-sm text-sm py-2 px-3 border focus:ring-indigo-500 focus:border-indigo-500">
          {subject_options}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Left Version</label>
        <input type="number" name="leftVersion" id="left-version" value="{left_version}" min="1"
          class="rounded-md border-gray-300 shadow-sm text-sm py-2 px-3 border w-24 focus:ring-indigo-500 focus:border-indigo-500">
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Right Subject</label>
        <select name="right" id="right-subject" class="rounded-md border-gray-300 shadow-sm text-sm py-2 px-3 border focus:ring-indigo-500 focus:border-indigo-500">
          {subject_options}
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Right Version</label>
        <input type="number" name="rightVersion" id="right-version" value="{right_version}" min="1"
          class="rounded-md border-gray-300 shadow-sm text-sm py-2 px-3 border w-24 focus:ring-indigo-500 focus:border-indigo-500">
      </div>
      <div>
        <button type="submit"
          class="inline-flex items-center rounded-md bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-500 transition">
          Compare
        </button>
      </div>
    </form>
  </div>
</div>

<!-- Diff Output -->
<div id="diff-output" class="grid grid-cols-1 md:grid-cols-2 gap-6">
  <div class="bg-white rounded-lg shadow">
    <div class="px-6 py-3 border-b border-gray-200 bg-gray-50 rounded-t-lg">
      <h3 class="text-sm font-medium text-gray-700" id="left-label">Left</h3>
    </div>
    <div class="p-4 schema-code">
      <pre><code class="language-json" id="left-schema"></code></pre>
    </div>
  </div>
  <div class="bg-white rounded-lg shadow">
    <div class="px-6 py-3 border-b border-gray-200 bg-gray-50 rounded-t-lg">
      <h3 class="text-sm font-medium text-gray-700" id="right-label">Right</h3>
    </div>
    <div class="p-4 schema-code">
      <pre><code class="language-json" id="right-schema"></code></pre>
    </div>
  </div>
</div>

<!-- Line-by-line diff -->
<div id="line-diff" class="mt-6 bg-white rounded-lg shadow hidden">
  <div class="px-6 py-3 border-b border-gray-200 bg-gray-50 rounded-t-lg">
    <h3 class="text-sm font-medium text-gray-700">Line-by-Line Diff</h3>
  </div>
  <div class="p-4 font-mono text-sm overflow-x-auto" id="line-diff-content"></div>
</div>

<script>
document.getElementById('compare-form').addEventListener('submit', function(e) {
  e.preventDefault();
  var leftSubject = document.getElementById('left-subject').value;
  var leftVersion = document.getElementById('left-version').value;
  var rightSubject = document.getElementById('right-subject').value;
  var rightVersion = document.getElementById('right-version').value;

  if (!leftSubject || !rightSubject) { alert('Please select both subjects.'); return; }

  Promise.all([
    fetch('/subjects/' + encodeURIComponent(leftSubject) + '/versions/' + leftVersion).then(r => r.json()),
    fetch('/subjects/' + encodeURIComponent(rightSubject) + '/versions/' + rightVersion).then(r => r.json())
  ]).then(function(results) {
    var left = results[0];
    var right = results[1];

    var leftFormatted = formatSchema(left.schema);
    var rightFormatted = formatSchema(right.schema);

    document.getElementById('left-label').textContent = leftSubject + ' v' + leftVersion;
    document.getElementById('right-label').textContent = rightSubject + ' v' + rightVersion;

    var leftEl = document.getElementById('left-schema');
    leftEl.textContent = leftFormatted;
    hljs.highlightElement(leftEl);

    var rightEl = document.getElementById('right-schema');
    rightEl.textContent = rightFormatted;
    hljs.highlightElement(rightEl);

    // Generate line diff
    generateLineDiff(leftFormatted, rightFormatted);
  }).catch(function(err) {
    alert('Error fetching schemas: ' + err.message);
  });
});

function formatSchema(schema) {
  try { return JSON.stringify(JSON.parse(schema), null, 2); } catch(e) { return schema; }
}

function generateLineDiff(left, right) {
  var leftLines = left.split('\n');
  var rightLines = right.split('\n');
  var maxLines = Math.max(leftLines.length, rightLines.length);
  var html = '';

  for (var i = 0; i < maxLines; i++) {
    var l = leftLines[i] || '';
    var r = rightLines[i] || '';
    if (l === r) {
      html += '<div class="flex"><span class="w-10 text-gray-400 text-right pr-2 select-none">' + (i+1) + '</span><span class="flex-1 whitespace-pre">' + escapeHtml(l) + '</span></div>';
    } else {
      if (l) html += '<div class="flex diff-removed"><span class="w-10 text-gray-400 text-right pr-2 select-none">' + (i+1) + '</span><span class="flex-1 whitespace-pre text-red-700">- ' + escapeHtml(l) + '</span></div>';
      if (r) html += '<div class="flex diff-added"><span class="w-10 text-gray-400 text-right pr-2 select-none">' + (i+1) + '</span><span class="flex-1 whitespace-pre text-green-700">+ ' + escapeHtml(r) + '</span></div>';
    }
  }

  var diffContainer = document.getElementById('line-diff');
  document.getElementById('line-diff-content').innerHTML = html;
  diffContainer.classList.remove('hidden');
}

function escapeHtml(text) {
  var div = document.createElement('div');
  div.appendChild(document.createTextNode(text));
  return div.innerHTML;
}

// Pre-select subjects from query params
(function() {
  var params = new URLSearchParams(window.location.search);
  if (params.get('left')) document.getElementById('left-subject').value = params.get('left');
  if (params.get('leftVersion')) document.getElementById('left-version').value = params.get('leftVersion');
  if (params.get('right')) document.getElementById('right-subject').value = params.get('right');
  if (params.get('rightVersion')) document.getElementById('right-version').value = params.get('rightVersion');
})();
</script>
"#;

/// Schema registration form page.
pub(crate) const SCHEMA_REGISTER_PAGE: &str = r#"
<h1 class="text-2xl font-bold text-gray-900 mb-6">Register New Schema</h1>

<div class="bg-white rounded-lg shadow">
  <form id="register-form" class="p-6 space-y-6">
    <div>
      <label for="subject" class="block text-sm font-medium text-gray-700 mb-1">Subject Name</label>
      <input type="text" id="subject" name="subject" required
        placeholder="e.g., user-events-value"
        class="w-full rounded-md border-gray-300 shadow-sm text-sm py-2 px-3 border focus:ring-indigo-500 focus:border-indigo-500">
      <p class="mt-1 text-xs text-gray-500">Convention: &lt;topic&gt;-key or &lt;topic&gt;-value</p>
    </div>

    <div>
      <label for="schema-type" class="block text-sm font-medium text-gray-700 mb-1">Schema Type</label>
      <select id="schema-type" name="schemaType"
        class="rounded-md border-gray-300 shadow-sm text-sm py-2 px-3 border focus:ring-indigo-500 focus:border-indigo-500">
        <option value="AVRO">Avro</option>
        <option value="JSON">JSON Schema</option>
        <option value="PROTOBUF">Protobuf</option>
      </select>
    </div>

    <div>
      <label for="schema-content" class="block text-sm font-medium text-gray-700 mb-1">Schema Definition</label>
      <textarea id="schema-content" name="schema" rows="16" required
        placeholder='{"type": "record", "name": "User", "fields": [...]}'
        class="w-full rounded-md border-gray-300 shadow-sm text-sm py-2 px-3 border font-mono focus:ring-indigo-500 focus:border-indigo-500"></textarea>
    </div>

    <div class="flex items-center justify-between pt-4 border-t border-gray-200">
      <div id="register-result" class="text-sm"></div>
      <div class="flex space-x-3">
        <button type="button" id="validate-btn"
          class="inline-flex items-center rounded-md bg-white px-4 py-2 text-sm font-semibold text-gray-700 ring-1 ring-gray-300 hover:bg-gray-50 transition">
          Validate
        </button>
        <button type="submit"
          class="inline-flex items-center rounded-md bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-500 transition">
          Register
        </button>
      </div>
    </div>
  </form>
</div>

<script>
document.getElementById('register-form').addEventListener('submit', function(e) {
  e.preventDefault();
  var subject = document.getElementById('subject').value;
  var schemaType = document.getElementById('schema-type').value;
  var schema = document.getElementById('schema-content').value;

  var resultEl = document.getElementById('register-result');
  resultEl.innerHTML = '<span class="text-gray-500">Registering...</span>';

  fetch('/api/schemas/' + encodeURIComponent(subject), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ schema: schema, schemaType: schemaType, references: [] })
  }).then(function(resp) {
    return resp.json().then(function(data) { return { ok: resp.ok, data: data }; });
  }).then(function(result) {
    if (result.ok) {
      resultEl.innerHTML = '<span class="text-green-600 font-medium">Schema registered with ID: ' + result.data.id + '</span>';
    } else {
      resultEl.innerHTML = '<span class="text-red-600 font-medium">Error: ' + (result.data.message || JSON.stringify(result.data)) + '</span>';
    }
  }).catch(function(err) {
    resultEl.innerHTML = '<span class="text-red-600 font-medium">Network error: ' + err.message + '</span>';
  });
});

document.getElementById('validate-btn').addEventListener('click', function() {
  var schema = document.getElementById('schema-content').value;
  var schemaType = document.getElementById('schema-type').value;
  var resultEl = document.getElementById('register-result');

  try {
    if (schemaType === 'AVRO' || schemaType === 'JSON') {
      JSON.parse(schema);
      resultEl.innerHTML = '<span class="text-green-600 font-medium">Valid JSON syntax</span>';
    } else {
      resultEl.innerHTML = '<span class="text-green-600 font-medium">Syntax appears valid (server-side validation on register)</span>';
    }
  } catch(e) {
    resultEl.innerHTML = '<span class="text-red-600 font-medium">Invalid JSON: ' + e.message + '</span>';
  }
});
</script>
"#;
