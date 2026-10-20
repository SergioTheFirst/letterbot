/* Settings pages (/accounts). Loaded as a file: the CSP allows no inline
 * scripts or handlers, so every listener is attached here and all labels come
 * from the page (data-* attributes and the settings-editor-data JSON block).
 *
 * 1. "Copy" buttons for the accounts.ini examples. Plain HTTP over the
 *    network has no navigator.clipboard, so the old copy command is the
 *    fallback, and the text is selected when both fail.
 * 2. The editor (S2): reads GET /api/settings/accounts and posts JSON with the
 *    X-CSRF-Token header. Secret fields are write-only: they start empty and
 *    the server never sends a secret back. Values are only ever written to
 *    the page with textContent / value, never as HTML. */
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

  /* ------------------------------------------------------------ editor */

  var root = document.querySelector("[data-settings-editor]");
  if (!root || !window.fetch) {
    return;
  }
  var webCard = document.querySelector("[data-web-card]");
  var dataNode = document.getElementById("settings-editor-data");
  var csrfNode = document.querySelector('meta[name="csrf-token"]');
  var config = {};
  try {
    config = JSON.parse(dataNode ? dataNode.getAttribute("data-json") : "{}") || {};
  } catch (error) {
    config = {};
  }
  var TEXTS = config.texts || {};
  var PRESETS = config.presets || {};
  var CSRF = csrfNode ? csrfNode.getAttribute("content") : "";
  var FLASH_KEY = "letterbot-settings-flash";
  var state = null;
  var editing = null;
  var touched = {};
  var submitted = false;
  var validateTimer = null;

  var accountForm = root.querySelector("[data-account-form]");
  var telegramForm = root.querySelector("[data-telegram-form]");
  var historyPanel = root.querySelector("[data-history]");
  var editorMessage = root.querySelector("[data-editor-message]");

  function t(key, params) {
    var value = Object.prototype.hasOwnProperty.call(TEXTS, key) ? TEXTS[key] : key;
    Object.keys(params || {}).forEach(function (name) {
      value = value.split("{" + name + "}").join(String(params[name]));
    });
    return value;
  }

  function show(node, visible) {
    if (node) {
      node.hidden = !visible;
    }
  }

  function clear(node) {
    while (node && node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  function element(tag, text, className) {
    var node = document.createElement(tag);
    if (text) {
      node.textContent = text;
    }
    if (className) {
      node.className = className;
    }
    return node;
  }

  function say(node, message, kind) {
    if (node) {
      node.textContent = message || "";
      node.setAttribute("data-kind", kind || "");
    }
  }

  function api(method, url, body) {
    var options = {
      method: method,
      credentials: "same-origin",
      headers: { Accept: "application/json" }
    };
    if (body !== undefined) {
      options.headers["Content-Type"] = "application/json";
      options.headers["X-CSRF-Token"] = CSRF;
      options.body = JSON.stringify(body);
    }
    return fetch(url, options).then(function (response) {
      return response.json().then(
        function (data) {
          return { status: response.status, data: data || {} };
        },
        function () {
          return { status: response.status, data: {} };
        }
      );
    });
  }

  function busy(button, label) {
    var previous = button.textContent;
    button.disabled = true;
    button.textContent = label;
    return function () {
      button.disabled = false;
      button.textContent = previous;
    };
  }

  function rememberFlash(message) {
    try {
      window.sessionStorage.setItem(FLASH_KEY, message);
    } catch (error) {
      /* Private mode: the message is simply not shown after the reload. */
    }
  }

  function takeFlash() {
    try {
      var message = window.sessionStorage.getItem(FLASH_KEY);
      window.sessionStorage.removeItem(FLASH_KEY);
      return message || "";
    } catch (error) {
      return "";
    }
  }

  function savedText(data) {
    return data.backup
      ? t("editor.saved", { time: data.saved_at, name: data.backup })
      : t("editor.saved_first", { time: data.saved_at });
  }

  function hidePanels() {
    show(accountForm, false);
    show(telegramForm, false);
    show(historyPanel, false);
  }

  /* ---------- secrets: "Saved - Replace" reveals an empty field */

  function setSecret(scope, name, saved) {
    var box = scope.querySelector('[data-secret="' + name + '"]');
    var input = box.querySelector("input");
    input.value = "";
    show(box.querySelector("[data-secret-saved]"), saved);
    show(box.querySelector("[data-secret-replace]"), saved);
    show(input, !saved);
  }

  function secretValue(scope, name) {
    var input = scope.querySelector('[data-secret="' + name + '"] input');
    return input && !input.hidden ? input.value : "";
  }

  function revealSecret(box) {
    show(box.querySelector("[data-secret-saved]"), false);
    show(box.querySelector("[data-secret-replace]"), false);
    var input = box.querySelector("input");
    show(input, true);
    return input;
  }

  root.addEventListener("click", function (event) {
    var replace = event.target.closest ? event.target.closest("[data-secret-replace]") : null;
    if (!replace) {
      return;
    }
    revealSecret(replace.closest("[data-secret]")).focus();
  });

  /* ---------- issues */

  function showIssues(form, issues) {
    var list = form.querySelector("[data-form-issues]");
    clear(list);
    Array.prototype.forEach.call(form.querySelectorAll("[aria-invalid]"), function (node) {
      node.removeAttribute("aria-invalid");
    });
    (issues || []).forEach(function (issue) {
      if (!submitted && issue.field && !touched[issue.field]) {
        return;
      }
      var item = element("li", issue.text, issue.severity === "error" ? "settings-issue-error" : "settings-issue-warning");
      list.appendChild(item);
      var input = issue.field && form.elements[issue.field];
      if (input && input.setAttribute && issue.severity === "error") {
        input.setAttribute("aria-invalid", "true");
      }
    });
  }

  function handleSave(result, form, messageNode) {
    var data = result.data || {};
    if (result.status === 200 && data.ok && data.unchanged) {
      say(messageNode, t("editor.no_changes"), "ok");
      return;
    }
    if (result.status === 200 && data.ok) {
      rememberFlash(savedText(data));
      window.location.reload();
      return;
    }
    if (result.status === 409) {
      say(messageNode, data.code === "CONFLICT" ? t("editor.conflict") : t("editor.unreadable"), "error");
      return;
    }
    if (result.status === 422) {
      submitted = true;
      if (form) {
        showIssues(form, data.issues);
      }
      say(messageNode, t("editor.rejected"), "error");
      return;
    }
    if (result.status === 429) {
      say(messageNode, t("editor.save_wait"), "error");
      return;
    }
    say(messageNode, t(result.status === 500 ? "editor.failed" : "editor.request_failed"), "error");
  }

  function failed(messageNode) {
    return function () {
      say(messageNode, t("editor.request_failed"), "error");
    };
  }

  /* ---------- list, toolbar, history */

  function renderList() {
    var list = root.querySelector("[data-account-list]");
    clear(list);
    var accounts = state.accounts || [];
    if (!accounts.length) {
      list.appendChild(element("li", t("editor.list_empty"), "hint"));
      return;
    }
    accounts.forEach(function (account) {
      var row = element("li", "", "settings-editor-row");
      if (!account.editable) {
        row.appendChild(element("span", t("editor.invalid_row"), "hint"));
        list.appendChild(row);
        return;
      }
      row.appendChild(element("span", "[" + account.id + "]", "mono"));
      row.appendChild(element("span", account.template ? t("editor.template_row") : account.host || "", "hint"));
      var edit = element("button", t("editor.edit"), "ghost");
      edit.type = "button";
      edit.setAttribute("data-edit", account.id);
      var remove = element("button", t("editor.remove"), "ghost");
      remove.type = "button";
      remove.setAttribute("data-remove", account.id);
      row.appendChild(edit);
      row.appendChild(remove);
      list.appendChild(row);
    });
  }

  function renderHistory() {
    var list = historyPanel.querySelector("[data-backup-list]");
    clear(list);
    var backups = state.backups || [];
    show(historyPanel.querySelector("[data-history-empty]"), !backups.length);
    backups.forEach(function (name) {
      var row = element("li", "", "settings-editor-row");
      row.appendChild(element("span", name, "mono"));
      var restore = element("button", t("editor.restore"), "ghost");
      restore.type = "button";
      restore.setAttribute("data-restore", name);
      row.appendChild(restore);
      list.appendChild(row);
    });
  }

  function findAccount(id) {
    return (state.accounts || []).filter(function (account) {
      return account.id === id;
    })[0];
  }

  /* ---------- account form */

  function syncOutlook() {
    var unsupported = !!(PRESETS[accountForm.elements.provider.value] || {}).unsupported;
    show(accountForm.querySelector("[data-outlook-note]"), unsupported);
    accountForm.querySelector('[data-action="save-account"]').disabled = unsupported;
    accountForm.querySelector('[data-action="test-imap"]').disabled = unsupported;
  }

  function syncChat() {
    var custom = accountForm.querySelector('input[name="chat_mode"][value="custom"]').checked;
    accountForm.elements.telegram_chat_id.disabled = !custom;
  }

  function applyPreset(key) {
    var preset = PRESETS[key] || {};
    if (preset.host) {
      accountForm.elements.host.value = preset.host;
      accountForm.elements.port.value = preset.port;
      accountForm.elements.use_ssl.checked = !!preset.use_ssl;
    }
    syncOutlook();
  }

  function setChatMode(mode) {
    accountForm.querySelector('input[name="chat_mode"][value="' + mode + '"]').checked = true;
    syncChat();
  }

  /* The saved password is used only with the server it was saved for: once
     host, port or SSL differ, the server asks for it again, so open the field. */
  function serverMoved() {
    var account = editing && editing.op === "update" ? findAccount(editing.id) : null;
    if (!account || !account.password_set) {
      return false;
    }
    var fields = accountForm.elements;
    return (
      fields.host.value.trim().toLowerCase() !== String(account.host || "").toLowerCase() ||
      fields.port.value.trim() !== String(account.port) ||
      fields.use_ssl.checked !== (account.use_ssl !== false)
    );
  }

  function syncServerPassword() {
    var box = accountForm.querySelector('[data-secret="password"]');
    if (serverMoved() && box.querySelector("input").hidden) {
      revealSecret(box);
      touched.password = true;
    }
  }

  function openAccount(account) {
    hidePanels();
    editing = account ? { op: "update", id: account.id } : { op: "add", id: "" };
    touched = {};
    submitted = false;
    var fields = accountForm.elements;
    accountForm.querySelector("[data-form-title]").textContent = account
      ? t("editor.form_edit", { id: account.id })
      : t("editor.form_add");
    show(accountForm.querySelector("[data-name-field]"), !account);
    fields.id.value = "";
    fields.login.value = account ? account.login : "";
    show(accountForm.querySelector("[data-login-hidden]"), !!(account && account.login_hidden));
    fields.host.value = account ? account.host : "";
    fields.port.value = account && account.port ? account.port : 993;
    fields.use_ssl.checked = account ? account.use_ssl !== false : true;
    fields.provider.value = account && PRESETS[account.provider] ? account.provider : account ? "other" : "gmail";
    if (!account) {
      applyPreset("gmail");
    }
    setSecret(accountForm, "password", !!(account && account.password_set));
    var custom = !!(account && account.chat_source === "mailbox");
    fields.telegram_chat_id.value = custom ? account.chat_id : "";
    setChatMode(custom ? "custom" : "default");
    syncOutlook();
    showIssues(accountForm, []);
    say(accountForm.querySelector("[data-form-message]"), "");
    show(accountForm, true);
    (account ? fields.login : fields.id).focus();
  }

  function accountChange() {
    var fields = accountForm.elements;
    var values = {
      login: fields.login.value.trim(),
      host: fields.host.value.trim(),
      port: fields.port.value.trim(),
      use_ssl: fields.use_ssl.checked,
      telegram_chat_id: fields.chat_mode.value === "custom" ? fields.telegram_chat_id.value.trim() : ""
    };
    if (editing.op === "update" && !values.login) {
      delete values.login;
    }
    var change = {
      op: editing.op,
      id: editing.op === "add" ? fields.id.value.trim() : editing.id,
      fields: values
    };
    var password = secretValue(accountForm, "password");
    if (password) {
      change.password = password;
    }
    return change;
  }

  function validateSoon() {
    window.clearTimeout(validateTimer);
    validateTimer = window.setTimeout(function () {
      if (accountForm.hidden || !editing) {
        return;
      }
      api("POST", "/api/settings/accounts/validate", { changes: [accountChange()] }).then(function (result) {
        if (result.status === 200) {
          showIssues(accountForm, result.data.issues);
        }
      }, function () {
        /* Validation on blur is a hint only; saving reports any failure. */
      });
    }, 300);
  }

  accountForm.addEventListener("focusout", function (event) {
    if (event.target && event.target.name) {
      touched[event.target.name === "chat_mode" ? "telegram_chat_id" : event.target.name] = true;
      validateSoon();
    }
  });

  accountForm.addEventListener("change", function (event) {
    var name = event.target && event.target.name;
    if (name === "provider") {
      applyPreset(event.target.value);
    } else if (name === "chat_mode") {
      syncChat();
    }
    if (name === "provider" || name === "host" || name === "port" || name === "use_ssl") {
      syncServerPassword();
    }
  });

  accountForm.addEventListener("submit", function (event) {
    event.preventDefault();
    submitted = true;
    var message = accountForm.querySelector("[data-form-message]");
    var done = busy(accountForm.querySelector('[data-action="save-account"]'), t("editor.saving"));
    api("POST", "/api/settings/accounts", { etag: state.etag, changes: [accountChange()] }).then(
      function (result) {
        done();
        handleSave(result, accountForm, message);
      },
      function () {
        done();
        failed(message)();
      }
    );
  });

  function testImap(button) {
    var change = accountChange();
    var message = accountForm.querySelector("[data-form-message]");
    var done = busy(button, t("editor.testing"));
    api("POST", "/api/settings/accounts/test-imap", {
      id: editing.op === "update" ? editing.id : "",
      host: change.fields.host,
      port: change.fields.port,
      use_ssl: change.fields.use_ssl,
      login: change.fields.login || "",
      password: change.password || ""
    }).then(
      function (result) {
        done();
        var code = (result.data && result.data.code) || "OTHER";
        if (result.status === 200 && result.data.ok) {
          say(message, t("editor.result.OK"), "ok");
          return;
        }
        var text = t("editor.result." + code);
        say(message, result.status === 200 ? text + " " + t("editor.test_warning") : text, "warn");
      },
      function () {
        done();
        failed(message)();
      }
    );
  }

  /* ---------- Telegram form */

  function openTelegram() {
    hidePanels();
    setSecret(telegramForm, "bot_token", !!state.telegram.token_set);
    telegramForm.elements.chat_id.value = state.telegram.chat_id || "";
    showIssues(telegramForm, []);
    say(telegramForm.querySelector("[data-form-message]"), "");
    show(telegramForm, true);
    telegramForm.elements.chat_id.focus();
  }

  function testTelegram(button, action) {
    var message = telegramForm.querySelector("[data-form-message]");
    var done = busy(button, t("editor.testing"));
    api("POST", "/api/settings/telegram/test", {
      action: action,
      bot_token: secretValue(telegramForm, "bot_token"),
      chat_id: action === "send" ? telegramForm.elements.chat_id.value.trim() : ""
    }).then(
      function (result) {
        done();
        var data = result.data || {};
        if (result.status === 200 && data.ok) {
          if (action === "send") {
            say(message, t("editor.tg.OK_send"), "ok");
          } else {
            say(message, data.bot_username ? t("editor.tg.OK_getme", { name: data.bot_username }) : t("editor.tg.OK_getme_anonymous"), "ok");
          }
          return;
        }
        var code = data.code || "OTHER";
        var key = TEXTS["editor.tg." + code] ? "editor.tg." + code : "editor.result." + code;
        say(message, t(key), "warn");
      },
      function () {
        done();
        failed(message)();
      }
    );
  }

  telegramForm.addEventListener("submit", function (event) {
    event.preventDefault();
    submitted = true;
    var message = telegramForm.querySelector("[data-form-message]");
    var telegram = { chat_id: telegramForm.elements.chat_id.value.trim() };
    var token = secretValue(telegramForm, "bot_token");
    if (token) {
      telegram.bot_token = token;
    }
    api("POST", "/api/settings/accounts", { etag: state.etag, changes: [], telegram: telegram }).then(
      function (result) {
        handleSave(result, telegramForm, message);
      },
      failed(message)
    );
  });

  /* ---------- toolbar and row actions */

  root.addEventListener("click", function (event) {
    var target = event.target.closest ? event.target.closest("button") : null;
    if (!target || !root.contains(target) || !state) {
      return;
    }
    var action = target.getAttribute("data-action");
    if (action === "add") {
      openAccount(null);
    } else if (action === "telegram") {
      openTelegram();
    } else if (action === "history") {
      hidePanels();
      renderHistory();
      show(historyPanel, true);
    } else if (action === "cancel") {
      hidePanels();
    } else if (action === "test-imap") {
      testImap(target);
    } else if (action === "tg-getme" || action === "tg-send") {
      testTelegram(target, action === "tg-send" ? "send" : "getme");
    } else if (target.hasAttribute("data-edit")) {
      openAccount(findAccount(target.getAttribute("data-edit")));
    } else if (target.hasAttribute("data-remove")) {
      var id = target.getAttribute("data-remove");
      if (window.confirm(t("editor.remove_confirm", { id: id }))) {
        api("POST", "/api/settings/accounts", { etag: state.etag, changes: [{ op: "remove", id: id }] }).then(
          function (result) {
            handleSave(result, null, editorMessage);
          },
          failed(editorMessage)
        );
      }
    } else if (target.hasAttribute("data-restore")) {
      var name = target.getAttribute("data-restore");
      if (window.confirm(t("editor.restore_confirm", { name: name }))) {
        api("POST", "/api/settings/accounts/restore", { etag: state.etag, name: name }).then(
          function (result) {
            if (result.status === 200 && result.data.ok) {
              rememberFlash(t("editor.restored", { name: name, time: result.data.saved_at }));
              window.location.reload();
            } else {
              handleSave(result, null, editorMessage);
            }
          },
          failed(editorMessage)
        );
      }
    }
  });

  /* ---------- dashboard password and network access */

  function renderWeb() {
    if (!webCard) {
      return;
    }
    var web = state.web || {};
    webCard.querySelector("[data-web-password-state]").textContent = t(
      web.password_set ? "editor.web_password_set" : "editor.web_password_missing"
    );
    show(webCard.querySelector("[data-current-password-field]"), !web.local && web.password_set);
    var lanText = t(web.lan_saved ? "editor.web_lan_on" : "editor.web_lan_off");
    if (web.restart_required) {
      lanText += " " + t("editor.web_restart");
    }
    webCard.querySelector("[data-lan-state]").textContent = lanText;
    var toggle = webCard.querySelector('[data-action="lan-toggle"]');
    toggle.textContent = t(web.lan_saved ? "editor.web_disable_lan" : "editor.web_enable_lan");
    show(toggle, !!(web.local && web.lan_toggle));
    var note = "";
    if (!web.local) {
      note = t("editor.web.LOOPBACK_ONLY");
    } else if (!web.lan_toggle) {
      note = t("editor.web.LAUNCHER_UNSUPPORTED");
    }
    say(webCard.querySelector("[data-lan-message]"), note);
    show(webCard, true);
  }

  if (webCard) {
    var passwordForm = webCard.querySelector("[data-web-password-form]");
    passwordForm.addEventListener("submit", function (event) {
      event.preventDefault();
      var message = passwordForm.querySelector("[data-form-message]");
      api("POST", "/api/settings/web-password", {
        new_password: passwordForm.elements.new_password.value,
        current_password: passwordForm.elements.current_password.value
      }).then(function (result) {
        var data = result.data || {};
        passwordForm.elements.current_password.value = "";
        if (result.status === 200 && data.ok) {
          passwordForm.elements.new_password.value = "";
          say(message, t("editor.web_password_saved", { time: data.saved_at }), "ok");
          state.web.password_set = true;
          renderWeb();
          return;
        }
        say(message, t("editor.web." + (data.code || "INVALID")), "error");
      }, failed(passwordForm.querySelector("[data-form-message]")));
    });

    webCard.querySelector('[data-action="lan-toggle"]').addEventListener("click", function () {
      var message = webCard.querySelector("[data-lan-message]");
      var lan = !state.web.lan_saved;
      api("POST", "/api/settings/web-access", { lan: lan }).then(function (result) {
        var data = result.data || {};
        if (result.status === 200 && data.ok) {
          state.web.lan_saved = lan;
          state.web.restart_required = !!data.restart_required;
          renderWeb();
          return;
        }
        say(message, t("editor.web." + (data.code || "INVALID")), "error");
      }, failed(message));
    });
  }

  /* ---------- configuration files (init-config / migrate-config) */

  var setupCard = document.querySelector("[data-setup-card]");

  function renderSetup() {
    if (!setupCard) {
      return;
    }
    var setup = state.setup || {};
    var missing = setup.missing || [];
    var legacy = setup.legacy || [];
    var line = t("editor.setup_present");
    if (setup.can_migrate) {
      line = t("editor.setup_legacy", { files: legacy.join(", ") });
    } else if (missing.length) {
      line = t("editor.setup_missing", { files: missing.join(", ") });
    }
    setupCard.querySelector("[data-setup-state]").textContent = line;
    show(setupCard.querySelector('[data-action="init-config"]'), missing.length > 0 && !setup.can_migrate);
    show(setupCard.querySelector('[data-action="migrate-config"]'), !!setup.can_migrate);
  }

  if (setupCard) {
    setupCard.addEventListener("click", function (event) {
      var button = event.target.closest ? event.target.closest("button[data-action]") : null;
      var action = button ? button.getAttribute("data-action") : "";
      if (!state || (action !== "init-config" && action !== "migrate-config")) {
        return;
      }
      var message = setupCard.querySelector("[data-setup-message]");
      var done = busy(button, t("editor.setup_working"));
      api("POST", "/api/settings/" + action, {}).then(
        function (result) {
          done();
          var data = result.data || {};
          if (result.status === 200 && data.ok) {
            var created = data.created || [];
            rememberFlash(
              created.length ? t("editor.setup_created", { files: created.join(", ") }) : t("editor.setup_nothing")
            );
            window.location.reload();
            return;
          }
          var key = "editor.setup." + (data.code || "FAILED");
          say(message, Object.prototype.hasOwnProperty.call(TEXTS, key) ? t(key) : t("editor.setup.FAILED"), "error");
        },
        function () {
          done();
          failed(message)();
        }
      );
    });
  }

  /* ---------- load */

  function render() {
    show(root.querySelector("[data-editor-loading]"), false);
    var toolbar = root.querySelector("[data-editor-toolbar]");
    var readable = state.file === "ok" || state.file === "missing";
    show(toolbar, true);
    toolbar.querySelector('[data-action="add"]').disabled = !readable;
    toolbar.querySelector('[data-action="telegram"]').disabled = !readable;
    if (!readable) {
      say(editorMessage, t("editor.file_problem"), "error");
    }
    renderList();
    renderWeb();
    renderSetup();
  }

  var flashMessage = takeFlash();
  api("GET", "/api/settings/accounts").then(
    function (result) {
      if (result.status !== 200 || !result.data.ok) {
        say(root.querySelector("[data-editor-loading]"), t("editor.load_failed"), "error");
        return;
      }
      state = result.data;
      render();
      if (flashMessage) {
        say(editorMessage, flashMessage, "ok");
      }
    },
    function () {
      say(root.querySelector("[data-editor-loading]"), t("editor.load_failed"), "error");
    }
  );
})();
