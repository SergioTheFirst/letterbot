/* Letterbot web: archive rows open their detail.
 *
 * One delegated listener: a click anywhere on a row with data-row-link
 * follows it, unless the click hit a real control (links keep working for
 * the keyboard and for "open in new tab"). Only same-origin paths are
 * followed.
 */
(function () {
  'use strict';

  document.addEventListener('click', function (event) {
    var target = event.target;
    if (!target || !target.closest) return;
    if (target.closest('a, button, input, select, textarea, label')) return;
    var row = target.closest('[data-row-link]');
    if (!row) return;
    var href = row.getAttribute('data-row-link') || '';
    if (href.charAt(0) !== '/' || href.charAt(1) === '/') return;
    window.location.href = href;
  });
})();
