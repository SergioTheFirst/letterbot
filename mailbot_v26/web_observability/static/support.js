/* Letterbot web: copy buttons on the support page.
 *
 * One delegated listener for every button carrying data-copy-target; the
 * button text says whether the copy worked.
 */
(function () {
  'use strict';

  var flash = function (button, text) {
    var original = button.getAttribute('data-label') || button.textContent;
    button.setAttribute('data-label', original);
    button.textContent = text;
    window.setTimeout(function () { button.textContent = original; }, 1200);
  };

  var fallbackCopy = function (value) {
    var input = document.createElement('textarea');
    input.value = value;
    document.body.appendChild(input);
    input.select();
    var copied = false;
    try {
      copied = document.execCommand('copy');
    } catch (error) {
      copied = false;
    }
    document.body.removeChild(input);
    return copied;
  };

  var copyValue = function (elementId, button) {
    var node = document.getElementById(elementId);
    var value = node ? (node.textContent || '').trim() : '';
    if (!value) return;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(value).then(
        function () { flash(button, 'Copied'); },
        function () { flash(button, fallbackCopy(value) ? 'Copied' : 'Copy failed'); }
      );
      return;
    }
    flash(button, fallbackCopy(value) ? 'Copied' : 'Copy failed');
  };

  document.addEventListener('click', function (event) {
    var target = event.target;
    var button = target && target.closest ? target.closest('[data-copy-target]') : null;
    if (!button) return;
    copyValue(button.getAttribute('data-copy-target'), button);
  });
})();
