/* Letterbot web: Home dashboard refresh.
 *
 * Reads its configuration from data-* attributes of the element carrying
 * data-dashboard-url (no inline script, CSP-friendly) and re-renders the
 * Home cards from the JSON dashboard API. Every value is inserted as text
 * or escaped before it reaches innerHTML.
 */
(function () {
  'use strict';

  var shell = document.querySelector('[data-dashboard-url]');
  if (!shell || !window.LetterbotLive) return;
  var dashboardUrl = shell.getAttribute('data-dashboard-url');
  var refreshMs = parseInt(shell.getAttribute('data-dashboard-refresh-ms'), 10) || 15000;

  const byId = (id) => document.getElementById(id);
  const body = byId('processed-table-body');
  const escapeHtml = (value) => String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  const displayValue = (value, fallback = 'unknown') => value === null || value === undefined || value === '' ? fallback : String(value);
  const setText = (id, value) => { const node = byId(id); if (node) node.textContent = String(value); };
  const setBadge = (id, label, klass) => { const node = byId(id); if (node) { node.className = `badge ${klass || 'muted'}`; node.textContent = String(label || 'UNKNOWN'); } };
  const emptyText = (id, fallback) => { const node = byId(id); return (node && node.getAttribute('data-empty-text')) || fallback; };
  const metaSummary = (meta) => {
    const sections = meta && meta.sections ? meta.sections : {};
    const issues = Object.entries(sections).filter(([, entry]) => entry && entry.status && entry.status !== 'ok').map(([name, entry]) => `${name}: ${entry.detail || entry.status}`);
    return issues.length ? issues.slice(0, 4).join(' · ') : 'Canonical runtime storage and events are in sync for visible sections.';
  };
  const maskEmail = (value) => {
    const text = String(value || '').trim();
    if (!text || !text.includes('@')) return text || 'Sender hidden';
    const parts = text.split('@', 2);
    return `${parts[0].slice(0, 1) || ''}…@${parts[1] || ''}`;
  };
  const renderList = (id, items, fallbackText, mapper) => {
    const node = byId(id);
    if (!node) return;
    const rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      node.innerHTML = `<li class="hint">${escapeHtml(emptyText(id, fallbackText))}</li>`;
      return;
    }
    node.innerHTML = rows.map(mapper).join('');
  };
  const renderProcessed = (processed) => {
    setBadge('processed-table-status', processed && processed.status_label, processed && processed.status_class);
    setText('processed-table-detail', displayValue(processed && processed.detail, 'No processed emails yet for this scope.'));
    setText('processed-count', displayValue(processed && processed.available_count, '0'));
    if (!body) return;
    const rows = processed && Array.isArray(processed.rows) ? processed.rows : [];
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="5" class="table-empty">No processed emails yet for this scope.</td></tr>';
      return;
    }
    body.innerHTML = rows.map((row) => `<tr><td class="mono">${escapeHtml(displayValue(row.date, 'UNAVAILABLE'))}</td><td class="mono">${escapeHtml(displayValue(row.time, 'UNAVAILABLE'))}</td><td>${escapeHtml(displayValue(row.sender_display, 'Sender hidden'))}</td><td><span class="badge ${escapeHtml(String(row.priority_class || 'muted'))}">${escapeHtml(displayValue(row.priority_label, 'UNKNOWN'))}</span></td><td><div class="info-primary">${escapeHtml(displayValue(row.info_primary, 'UNAVAILABLE'))}</div>${row.info_secondary ? `<div class="info-secondary">${escapeHtml(String(row.info_secondary))}</div>` : ''}</td></tr>`).join('');
  };
  const renderDashboard = (data) => {
    setText('preview-emails-today', displayValue(data.emails_today));
    setText('preview-emails-hour', displayValue(data.emails_last_hour));
    setText('preview-llm-calls', displayValue(data.llm_calls_today));
    setText('preview-llm-fallback', displayValue(data.llm_fallback_today));
    setText('preview-priority-red', displayValue(data.priority && data.priority.red));
    setText('preview-dashboard-status', displayValue((data.health && data.health.status_label) || (data.meta && data.meta.status), 'UNKNOWN'));
    setText('preview-dashboard-updated', `Updated: ${displayValue(data.meta && data.meta.generated_at, 'unknown')}`);
    setText('preview-dashboard-detail', metaSummary(data.meta || {}));
    setText('dashboard-meta-detail', metaSummary(data.meta || {}));
    setText('preview-events-count', Array.isArray(data.recent_events) ? data.recent_events.length : '0');
    setBadge('dashboard-meta-status', (data.health && data.health.status_label) || 'UNKNOWN', (data.health && data.health.status_class) || 'muted');
    setBadge('health-imap-status', (data.runtime && data.runtime.imap_status_label) || 'UNKNOWN', (data.runtime && data.runtime.imap_status_class) || 'muted');
    setBadge('health-pipeline-status', (data.runtime && data.runtime.status_label) || 'UNKNOWN', (data.runtime && data.runtime.status_class) || 'muted');
    setText('home-health-detail', displayValue(data.health && data.health.detail, 'No health evidence yet for this scope.'));
    setText('health-imap-meta', displayValue(data.runtime && data.runtime.last_imap_success_display, 'UNAVAILABLE'));
    setText('health-pipeline-meta', displayValue(data.runtime && data.runtime.last_processed_display, 'UNAVAILABLE'));
    setText('runtime-last-evidence', displayValue(data.runtime && data.runtime.last_runtime_evidence_display, 'UNAVAILABLE'));
    setText('runtime-last-evidence-relative', displayValue(data.runtime && data.runtime.last_runtime_evidence_relative, 'No runtime evidence yet.'));
    setText('runtime-started-at', displayValue(data.runtime && data.runtime.started_at_display, 'UNAVAILABLE'));
    setText('runtime-uptime', displayValue(data.runtime && data.runtime.uptime_display, 'UNAVAILABLE'));
    setText('runtime-last-evidence-side', displayValue(data.runtime && data.runtime.last_runtime_evidence_display, 'UNAVAILABLE'));
    setText('runtime-last-processed', displayValue(data.runtime && data.runtime.last_processed_display, 'UNAVAILABLE'));
    setBadge('home-latency-status', data.latency && data.latency.status_label, data.latency && data.latency.status_class);
    setText('home-latency-detail', displayValue(data.latency && data.latency.detail, 'No latency data yet for this scope.'));
    setText('home-latency-p50', displayValue(data.latency && data.latency.pipeline_p50, '—'));
    setText('home-latency-p95', displayValue(data.latency && data.latency.pipeline_p95, '—'));
    setText('metric-latency-p95', displayValue(data.latency && data.latency.pipeline_p95, 'NO DATA'));
    setText('home-latency-llm-p90', displayValue(data.latency && data.latency.llm_p90, '—'));
    setText('home-latency-samples', displayValue(data.latency && data.latency.sample_count_display, '0 spans'));
    setText('home-latency-error-rate', displayValue(data.latency && data.latency.error_rate, '—'));
    setText('home-latency-fallback-rate', displayValue(data.latency && data.latency.fallback_rate, '—'));
    setText('home-latency-empty', data.latency && data.latency.available ? '' : displayValue(data.latency && data.latency.status_label, 'NO LATENCY DATA'));
    setBadge('home-ai-status', data.ai && data.ai.status_label, data.ai && data.ai.status_class);
    setText('home-ai-detail', displayValue(data.ai && data.ai.detail, 'No recent LLM calls or decision traces for this scope.'));
    setText('home-ai-calls', displayValue(data.ai && data.ai.llm_calls_today, 'unknown'));
    setText('home-ai-fallback', displayValue(data.ai && data.ai.llm_fallback_today, 'unknown'));
    setText('home-ai-trace-coverage', displayValue(data.ai && data.ai.trace_coverage, '0/0'));
    setText('metric-ai-trace-coverage', displayValue(data.ai && data.ai.trace_coverage, '0/0'));
    renderList('preview-recent-events', data.recent_events, 'No recent events yet.', (item) => `<li>${escapeHtml(String(item.text || item.type || 'event'))}</li>`);
    renderList('home-health-components', data.health && data.health.components, 'No health evidence yet for this scope.', (item) => `<li><span class="badge ${escapeHtml(String(item.status_class || 'muted'))}">${escapeHtml(String(item.name || 'Component'))}: ${escapeHtml(String(item.status || 'UNKNOWN'))}</span>${item.detail ? ` ${escapeHtml(String(item.detail))}` : ''}</li>`);
    const aiItems = [];
    (Array.isArray(data.ai && data.ai.recent_providers) ? data.ai.recent_providers : []).forEach((item) => aiItems.push({ html: `<li>Provider: ${escapeHtml(String(item.label || 'unknown'))}</li>` }));
    (Array.isArray(data.ai && data.ai.recent_traces) ? data.ai.recent_traces : []).forEach((item) => aiItems.push({ html: `<li>${escapeHtml(String(item.decision_kind || 'decision'))}: ${escapeHtml(String(item.codes_text || 'no explain codes'))}</li>` }));
    renderList('home-ai-items', aiItems.map((item) => item.html), 'No recent AI decision traces recorded yet.', (item) => item);
    renderList('top-contacts-list', data.top_contacts, 'No contact summary for this scope yet.', (item) => `<li>${escapeHtml(maskEmail(item.sender_email))} - ${escapeHtml(displayValue(item.emails_count, '0'))} emails</li>`);
    renderList('top-issuers-list', data.top_issuers, 'No issuer summary for this scope yet.', (item) => `<li>${escapeHtml(String(item.issuer_label || item.issuer_key || 'issuer'))} - waiting ${escapeHtml(displayValue(item.documents_waiting_attention_count, '0'))}</li>`);
    renderProcessed(data.processed_table || {});
  };

  const refreshDashboard = () => fetch(dashboardUrl + window.location.search, { credentials: 'same-origin' })
    .then((resp) => { if (!resp.ok) throw new Error('dashboard refresh failed'); return resp.json(); })
    .then((data) => { renderDashboard(data); window.LetterbotLive.report('dashboard', true); })
    .catch(() => { window.LetterbotLive.report('dashboard', false); });

  window.LetterbotLive.register('dashboard', refreshDashboard, refreshMs);
  // The contact and issuer lists are only filled by the API: load them now.
  refreshDashboard();
})();
