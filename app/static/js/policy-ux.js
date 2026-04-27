/**
 * Think-tank / policy UX: brief mode, copy link, citations, onboarding, lightweight analytics.
 */
(function () {
  'use strict';

  var BRIEF_KEY = 'gt_brief_mode';
  var ROLE_KEY = 'gt_workflow_role';
  var ONBOARD_KEY = 'gt_onboarding_done_v1';
  var PAGE_SENT_PREFIX = 'gt_ux_sent_';

  function getBody() {
    return document.getElementById('body-theme') || document.body;
  }

  function copyText(text, okMsg) {
    if (!text) return;
    function done() {
      if (okMsg) {
        try {
          var t = okMsg;
          /* optional toast — keep minimal */
          var prev = document.querySelector('.gt-toast');
          if (prev) prev.remove();
          var el = document.createElement('p');
          el.className = 'gt-toast';
          el.setAttribute('role', 'status');
          el.textContent = t;
          document.body.appendChild(el);
          setTimeout(function () {
            try {
              el.remove();
            } catch (e) {}
          }, 2200);
        } catch (e) {}
      }
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done).catch(function () {
        fallbackCopy(text);
        done();
      });
    } else {
      fallbackCopy(text);
      done();
    }
  }

  function fallbackCopy(text) {
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.setAttribute('readonly', '');
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand('copy');
    } catch (e) {}
    document.body.removeChild(ta);
  }

  function postUxEvent(eventName, meta) {
    try {
      var body = document.getElementById('body-theme');
      var path = (body && body.getAttribute('data-terminal-path')) || window.location.pathname;
      var role = '';
      try {
        role = localStorage.getItem(ROLE_KEY) || '';
      } catch (e) {}
      var payload = { event: eventName, path: path, role: role };
      if (meta && typeof meta === 'object') payload.meta = meta;
      fetch('/api/ux/event', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        credentials: 'same-origin',
      }).catch(function () {});
    } catch (e) {}
  }

  function maybeSendPageView() {
    try {
      var key = PAGE_SENT_PREFIX + window.location.pathname + window.location.search;
      if (sessionStorage.getItem(key)) return;
      sessionStorage.setItem(key, '1');
      postUxEvent('page_view');
    } catch (e) {
      postUxEvent('page_view');
    }
  }

  function applyBriefMode(on) {
    var b = getBody();
    if (!b) return;
    b.classList.toggle('terminal-brief-mode', !!on);
    try {
      localStorage.setItem(BRIEF_KEY, on ? '1' : '0');
    } catch (e) {}
    var btn = document.getElementById('gt-toggle-brief-mode');
    if (btn) {
      btn.setAttribute('aria-pressed', on ? 'true' : 'false');
      btn.textContent = on ? 'Exit brief mode' : 'Brief mode';
    }
    if (on) postUxEvent('brief_mode_on');
  }

  function initBriefMode() {
    var on = false;
    try {
      on = localStorage.getItem(BRIEF_KEY) === '1';
    } catch (e) {}
    applyBriefMode(on);
    var btn = document.getElementById('gt-toggle-brief-mode');
    if (btn) {
      btn.addEventListener('click', function () {
        var b = getBody();
        var next = !(b && b.classList.contains('terminal-brief-mode'));
        applyBriefMode(next);
      });
    }
  }

  function initCopyPageUrl() {
    var btn = document.getElementById('gt-copy-page-url');
    if (!btn) return;
    btn.addEventListener('click', function () {
      var url = window.location.href;
      copyText(url, 'Page link copied');
      postUxEvent('copy_page_url');
    });
  }

  function initCopyCitationDelegation() {
    document.addEventListener('click', function (e) {
      var t = e.target;
      if (!t || !t.closest) return;
      var btn = t.closest('.gt-copy-citation');
      if (!btn) return;
      e.preventDefault();
      var host = btn.closest('[data-cite-ref]');
      if (!host) host = btn;
      var ref = host.getAttribute('data-cite-ref') || '';
      var title = host.getAttribute('data-cite-title') || document.title || '';
      var url = host.getAttribute('data-cite-url') || window.location.href;
      var org = host.getAttribute('data-cite-org') || 'Geopolitical Terminal';
      var line =
        title +
        '. ' +
        org +
        ', ' +
        ref +
        '. ' +
        url +
        ' (accessed ' +
        new Date().toISOString().slice(0, 10) +
        ').';
      copyText(line, 'Citation copied');
      postUxEvent('copy_citation', { ref: ref });
    });
  }

  function applyWorkflowRole(role) {
    var b = getBody();
    if (!b) return;
    var roles = ['monitor', 'analyst', 'comms', 'lead'];
    b.classList.remove('gt-role-monitor', 'gt-role-analyst', 'gt-role-comms', 'gt-role-lead');
    if (role && roles.indexOf(role) !== -1) {
      b.classList.add('gt-role-' + role);
      try {
        localStorage.setItem(ROLE_KEY, role);
      } catch (e) {}
    }
  }

  function buildOnboarding() {
    if (document.getElementById('gt-onboarding-overlay')) return;
    var overlay = document.createElement('div');
    overlay.id = 'gt-onboarding-overlay';
    overlay.className = 'gt-onboarding-overlay';
    overlay.setAttribute('role', 'dialog');
    overlay.setAttribute('aria-modal', 'true');
    overlay.setAttribute('aria-labelledby', 'gt-onboarding-title');
    overlay.innerHTML =
      '<div class="gt-onboarding-dialog">' +
      '<h2 id="gt-onboarding-title">How do you use the Terminal?</h2>' +
      '<p class="gt-onboarding-intro">Optional — we use this only in your browser to tune density. You can change it anytime in Help → Think tanks & workflows.</p>' +
      '<div class="gt-onboarding-roles">' +
      '<button type="button" class="btn btn-secondary gt-onb-role" data-role="monitor">Monitor &amp; triage</button>' +
      '<button type="button" class="btn btn-secondary gt-onb-role" data-role="analyst">Analyst &amp; researcher</button>' +
      '<button type="button" class="btn btn-secondary gt-onb-role" data-role="comms">Comms &amp; briefings</button>' +
      '<button type="button" class="btn btn-secondary gt-onb-role" data-role="lead">Lead / coordinator</button>' +
      '</div>' +
      '<div class="gt-onboarding-footer">' +
      '<button type="button" class="btn-link" id="gt-onboarding-skip">Skip</button>' +
      '</div>' +
      '</div>';
    document.body.appendChild(overlay);

    function close() {
      overlay.remove();
      try {
        localStorage.setItem(ONBOARD_KEY, '1');
      } catch (e) {}
    }

    overlay.addEventListener('click', function (e) {
      if (e.target === overlay) close();
    });
    overlay.querySelectorAll('.gt-onb-role').forEach(function (bt) {
      bt.addEventListener('click', function () {
        var role = this.getAttribute('data-role');
        applyWorkflowRole(role);
        postUxEvent('onboarding_role', { role: role });
        close();
      });
    });
    var skip = document.getElementById('gt-onboarding-skip');
    if (skip)
      skip.addEventListener('click', function () {
        postUxEvent('onboarding_skip');
        close();
      });
  }

  function maybeOnboarding() {
    try {
      if (localStorage.getItem(ONBOARD_KEY)) return;
    } catch (e) {
      return;
    }
    /* Defer so first paint is fast */
    setTimeout(buildOnboarding, 600);
  }

  function restoreRoleClass() {
    try {
      var r = localStorage.getItem(ROLE_KEY);
      if (r) applyWorkflowRole(r);
    } catch (e) {}
  }

  function setPrintFooterUrl() {
    var ft = document.querySelector('.terminal-footer');
    if (ft) ft.setAttribute('data-print-url', window.location.href);
  }

  function init() {
    restoreRoleClass();
    setPrintFooterUrl();
    initBriefMode();
    initCopyPageUrl();
    initCopyCitationDelegation();
    maybeSendPageView();
    maybeOnboarding();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
