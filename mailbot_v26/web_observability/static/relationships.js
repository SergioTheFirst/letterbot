/* Letterbot web: relationship graph and contact detail tables.
 *
 * The server puts the read-only data in data-graph / data-contact JSON
 * attributes; every value is escaped before it is inserted.
 */
(function () {
  'use strict';

  var esc = function (value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  };

  var parseJson = function (raw) {
    try {
      return JSON.parse(raw || '{}');
    } catch (error) {
      return null;
    }
  };

  var countOf = function (item) {
    if (item.email_count !== undefined && item.email_count !== null) return item.email_count;
    if (item.emails_total !== undefined && item.emails_total !== null) return item.emails_total;
    return 0;
  };

  var renderGraph = function () {
    var el = document.getElementById('relationships-data');
    var content = document.getElementById('graph-content');
    if (!el || !content) return;
    var graph = parseJson(el.getAttribute('data-graph'));
    if (graph === null) {
      content.innerHTML = '<div class="hint">Relationship data could not be read.</div>';
      return;
    }
    var nodes = Array.isArray(graph.nodes)
      ? graph.nodes.filter(function (n) { return n && n.id !== 'user:me'; })
      : [];
    if (!nodes.length) {
      content.innerHTML = '<div class="hint">No relationship data yet. Adjust scope or window.</div>';
      return;
    }
    var rows = nodes.map(function (n) {
      var name = n.display_name || n.label || n.entity_id || n.id || 'unknown';
      var trust = typeof n.trust_score === 'number' ? n.trust_score.toFixed(2) : '-';
      return '<tr><td>' + esc(name) + '</td><td>' + esc(countOf(n)) + '</td><td>' + esc(trust) + '</td></tr>';
    }).join('');
    content.innerHTML = '<div class="table-scroll"><table class="table compact">' +
      '<thead><tr><th>Contact</th><th>Emails</th><th>Trust</th></tr></thead>' +
      '<tbody>' + rows + '</tbody></table></div>';
  };

  var renderContact = function () {
    var el = document.getElementById('contact-data');
    var content = document.getElementById('contact-content');
    if (!el || !content) return;
    var payload = parseJson(el.getAttribute('data-contact'));
    var contact = payload && payload.contact ? payload.contact : null;
    if (!contact || !contact.id) {
      content.innerHTML = '<div class="hint">No data for this contact.</div>';
      return;
    }
    var trust = typeof contact.trust_score === 'number' ? contact.trust_score.toFixed(2) : '-';
    content.innerHTML =
      '<div class="detail-grid">' +
      '<div><div class="detail-label">Entity</div><div class="detail-value">' + esc(contact.id) + '</div></div>' +
      '<div><div class="detail-label">Label</div><div class="detail-value">' + esc(contact.label || '-') + '</div></div>' +
      '<div><div class="detail-label">Trust score</div><div class="detail-value">' + esc(trust) + '</div></div>' +
      '<div><div class="detail-label">Emails</div><div class="detail-value">' + esc(countOf(contact)) + '</div></div>' +
      '</div>';
  };

  renderGraph();
  renderContact();
})();
