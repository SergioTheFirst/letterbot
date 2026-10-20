/* Letterbot web: shared live refresh.
 *
 * Elements with data-live-partial="<url>" reload their HTML from that URL
 * every data-live-interval-ms milliseconds. Page scripts add their own
 * refresh tasks through window.LetterbotLive.register(). Polling pauses
 * while the tab is hidden and runs once when it becomes visible again.
 * A failed refresh keeps the last good data on screen and says so in the
 * page's [data-refresh-status] live region ("Couldn't refresh; data from
 * 14:05"); the text comes from its data-error-text attribute.
 */
(function () {
  'use strict';

  var loadedAt = new Date();
  var statusNode = document.querySelector('[data-refresh-status]');
  var errorText = statusNode ? statusNode.getAttribute('data-error-text') || '' : '';
  var tasks = {};
  var runners = [];

  function clock(date) {
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }

  function render() {
    if (!statusNode) return;
    var oldest = null;
    Object.keys(tasks).forEach(function (name) {
      var task = tasks[name];
      if (!task.ok && (oldest === null || task.lastOk < oldest)) oldest = task.lastOk;
    });
    if (oldest === null) {
      statusNode.textContent = '';
      statusNode.classList.remove('is-error');
      return;
    }
    statusNode.textContent = errorText.replace('{time}', clock(oldest));
    statusNode.classList.add('is-error');
  }

  function report(name, ok) {
    var task = tasks[name] || (tasks[name] = { ok: true, lastOk: loadedAt });
    task.ok = Boolean(ok);
    if (task.ok) task.lastOk = new Date();
    render();
  }

  function register(name, run, intervalMs) {
    if (!tasks[name]) tasks[name] = { ok: true, lastOk: loadedAt };
    runners.push(run);
    window.setInterval(function () { if (!document.hidden) run(); }, intervalMs);
  }

  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'visible') {
      runners.forEach(function (run) { run(); });
    }
  });

  Array.prototype.forEach.call(document.querySelectorAll('[data-live-partial]'), function (node) {
    var url = node.getAttribute('data-live-partial');
    var name = 'partial:' + url;
    var intervalMs = parseInt(node.getAttribute('data-live-interval-ms'), 10) || 30000;
    register(name, function () {
      return fetch(url + window.location.search, { credentials: 'same-origin' })
        .then(function (resp) {
          if (!resp.ok) throw new Error('refresh failed');
          return resp.text();
        })
        .then(function (html) {
          // Server-rendered, escaped partial from this origin.
          node.innerHTML = html;
          report(name, true);
        })
        .catch(function () { report(name, false); });
    }, intervalMs);
  });

  window.LetterbotLive = { register: register, report: report };
})();
