/* Commands page (/commands). Loaded as a file: the CSP allows no inline
 * scripts or handlers. It only copies command lines: navigator.clipboard on
 * a secure origin (this computer), else the old copy command, and when both
 * fail (plain HTTP over the network) the text is selected for Ctrl+C. Labels
 * come from the page (data-* attributes). */
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
