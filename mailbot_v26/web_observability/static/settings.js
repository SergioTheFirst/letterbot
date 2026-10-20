/* Settings pages (/accounts): "Copy" buttons for the accounts.ini examples.
 * Loaded as a file (the CSP allows no inline scripts or handlers); one
 * delegated listener, labels come from data-* attributes (EN/RU). Plain HTTP
 * over the network has no navigator.clipboard, so the old copy command is the
 * fallback, and the text is selected when both fail. */
(function () {
  "use strict";

  function announce(message) {
    var region = document.querySelector("[data-copy-status]");
    if (region) {
      region.textContent = message;
    }
  }

  function selectText(node) {
    var selection = window.getSelection();
    if (!selection) {
      return;
    }
    var range = document.createRange();
    range.selectNodeContents(node);
    selection.removeAllRanges();
    selection.addRange(range);
  }

  function legacyCopy(node) {
    selectText(node);
    try {
      return document.execCommand("copy");
    } catch (error) {
      return false;
    }
  }

  function flash(button, label) {
    if (!button.hasAttribute("data-label")) {
      button.setAttribute("data-label", button.textContent);
    }
    button.textContent = label;
    window.setTimeout(function () {
      button.textContent = button.getAttribute("data-label");
    }, 2000);
  }

  document.addEventListener("click", function (event) {
    var origin = event.target;
    var button = origin && origin.closest ? origin.closest("[data-copy-target]") : null;
    if (!button) {
      return;
    }
    var target = document.getElementById(button.getAttribute("data-copy-target"));
    if (!target) {
      return;
    }
    var copied = button.getAttribute("data-copied-text") || "Copied";
    var failed = button.getAttribute("data-failed-text") || "Copy failed";
    var succeed = function () {
      flash(button, copied);
      announce(copied);
    };
    var fail = function () {
      selectText(target);
      announce(failed);
    };
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(target.textContent).then(succeed, function () {
        if (legacyCopy(target)) {
          succeed();
        } else {
          fail();
        }
      });
    } else if (legacyCopy(target)) {
      succeed();
    } else {
      fail();
    }
  });
})();
