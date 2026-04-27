/**
 * Messaging pop-up panel and drag-and-drop to share.
 * Requires: #chat-panel, #chat-panel-backdrop, #chat-panel-toggle when authenticated.
 * Draggable sources: elements with [data-chat-drag] and data-chat-type, data-chat-id, data-chat-extra, data-chat-label.
 */
(function () {
  'use strict';

  var DRAG_TYPE = 'application/x-geo-chat-attachment';
  var FORWARD_URL = '/messaging/forward';

  var panel, backdrop, toggle;
  var currentChannelId = null;
  var currentChannelSlug = null;
  var dropPayload = null;
  var attachment = { type: '', id: '', extra: '', label: '' };

  function byId(id) { return document.getElementById(id); }

  function escapeHtml(s) {
    if (s == null || s === '') return '';
    var div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
  }

  function openPanel() {
    if (!panel || !backdrop) return;
    panel.classList.add('is-open');
    backdrop.classList.add('is-open');
    backdrop.setAttribute('aria-hidden', 'false');
    loadChannels();
    showChannelList();
  }

  function closePanel() {
    if (!panel || !backdrop) return;
    panel.classList.remove('is-open');
    backdrop.classList.remove('is-open');
    backdrop.setAttribute('aria-hidden', 'true');
    hideForwardForm();
  }

  function showChannelList() {
    currentChannelId = null;
    currentChannelSlug = null;
    var hint = byId('chat-panel-drop-hint');
    var listWrap = byId('chat-panel-channel-list-wrap');
    var viewWrap = byId('chat-panel-channel-view');
    var fwdWrap = byId('chat-panel-forward-wrap');
    if (hint) hint.style.display = 'block';
    if (listWrap) listWrap.style.display = 'block';
    if (viewWrap) viewWrap.style.display = 'none';
    if (fwdWrap) fwdWrap.style.display = 'none';
    var title = byId('chat-panel-title');
    if (title) title.textContent = 'Messaging';
  }

  function showChannelView(channelId, channelSlug) {
    currentChannelId = channelId;
    currentChannelSlug = channelSlug || 'Channel';
    var hint = byId('chat-panel-drop-hint');
    var listWrap = byId('chat-panel-channel-list-wrap');
    var viewWrap = byId('chat-panel-channel-view');
    var fwdWrap = byId('chat-panel-forward-wrap');
    var composer = byId('chat-panel-composer');
    var title = byId('chat-panel-title');
    if (hint) hint.style.display = 'none';
    if (listWrap) listWrap.style.display = 'none';
    if (viewWrap) viewWrap.style.display = 'flex';
    if (fwdWrap) fwdWrap.style.display = 'none';
    if (composer) composer.style.display = 'block';
    if (title) title.textContent = currentChannelSlug;
    loadMessages(channelId);
    clearAttachment();
  }

  function showForwardForm(payload) {
    dropPayload = payload;
    var hint = byId('chat-panel-drop-hint');
    var listWrap = byId('chat-panel-channel-list-wrap');
    var viewWrap = byId('chat-panel-channel-view');
    var fwdWrap = byId('chat-panel-forward-wrap');
    var labelEl = byId('chat-panel-forward-label');
    if (hint) hint.style.display = 'none';
    if (listWrap) listWrap.style.display = 'none';
    if (viewWrap) viewWrap.style.display = 'none';
    if (fwdWrap) fwdWrap.style.display = 'block';
    if (labelEl) labelEl.textContent = 'Share in channel:';
    loadChannelsIntoSelect();
  }

  function hideForwardForm() {
    dropPayload = null;
    var fwdWrap = byId('chat-panel-forward-wrap');
    if (fwdWrap) fwdWrap.style.display = 'none';
  }

  function loadChannels() {
    var list = byId('chat-channel-list');
    if (!list) return;
    list.innerHTML = '';
    fetch('/api/messaging/channels', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var channels = data.channels || [];
        channels.forEach(function (c) {
          var li = document.createElement('li');
          var a = document.createElement('a');
          a.href = '#';
          a.innerHTML = '<span class="channel-name">' + escapeHtml(c.name || c.slug) + '</span><span class="channel-meta">' + escapeHtml(c.channel_type || '') + '</span>';
          a.addEventListener('click', function (e) {
            e.preventDefault();
            showChannelView(parseInt(c.id, 10), c.slug || c.name);
          });
          li.appendChild(a);
          list.appendChild(li);
        });
      })
      .catch(function () {});
  }

  function loadChannelsIntoSelect() {
    var select = byId('chat-panel-forward-select');
    if (!select) return;
    select.innerHTML = '<option value="">— Select channel —</option>';
    fetch('/api/messaging/channels', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        (data.channels || []).forEach(function (c) {
          var opt = document.createElement('option');
          opt.value = c.id;
          opt.textContent = c.name || c.slug;
          select.appendChild(opt);
        });
      })
      .catch(function () {});
  }

  function loadMessages(channelId) {
    var ul = byId('chat-message-list');
    if (!ul) return;
    ul.innerHTML = '<li class="chat-message-item">Loading…</li>';
    fetch('/api/messaging/channels/' + channelId + '/messages?limit=50', { credentials: 'same-origin' })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        ul.innerHTML = '';
        var messages = data.messages || [];
        messages.forEach(function (m) {
          var li = document.createElement('li');
          li.className = 'chat-message-item';
          var body = escapeHtml((m.content || '').slice(0, 500));
          var att = '';
          if (m.attachment_type) {
            if (m.attachment_type === 'article') att = ' <a href="/article/' + (m.attachment_id || '') + '">Article #' + (m.attachment_id || '') + '</a>';
            else if (m.attachment_type === 'country' && m.attachment_extra) att = ' <a href="/country/' + escapeHtml(m.attachment_extra) + '">Country: ' + escapeHtml(m.attachment_extra) + '</a>';
            else if (m.attachment_type === 'risk') att = ' <a href="/risk">Risk dashboard</a>';
            else att = ' ' + escapeHtml(m.attachment_type) + (m.attachment_id ? ' #' + m.attachment_id : '');
          }
          li.innerHTML = '<span class="msg-author">' + escapeHtml(m.author_display || m.username || '') + '</span><span class="msg-time">' + (m.created_at ? m.created_at.slice(0, 19).replace('T', ' ') : '') + '</span><div class="msg-body">' + body + '</div>' + (att ? '<div class="msg-attachment">' + att + '</div>' : '');
          ul.appendChild(li);
        });
        ul.scrollTop = ul.scrollHeight;
      })
      .catch(function () {
        ul.innerHTML = '<li class="chat-message-item">Failed to load messages.</li>';
      });
  }

  function setAttachment(type, id, extra, label) {
    attachment.type = type || '';
    attachment.id = id || '';
    attachment.extra = extra || '';
    attachment.label = label || '';
    var chip = byId('chat-panel-attach-chip');
    var labelEl = byId('chat-panel-attach-label');
    if (chip && labelEl) {
      if (attachment.type) {
        labelEl.textContent = attachment.label || (attachment.type + (attachment.id ? ' #' + attachment.id : '') + (attachment.extra ? ' ' + attachment.extra : ''));
        chip.style.display = 'inline-flex';
      } else {
        chip.style.display = 'none';
      }
    }
  }

  function clearAttachment() {
    setAttachment('', '', '', '');
  }

  function sendMessage() {
    if (!currentChannelId) return;
    var input = byId('chat-panel-message-input');
    var content = (input && input.value || '').trim();
    if (!content) return;
    var payload = { content: content };
    if (attachment.type) {
      payload.attachment_type = attachment.type;
      if (attachment.id) payload.attachment_id = attachment.id;
      if (attachment.extra) payload.attachment_country_code = attachment.extra;
    }
    var btn = byId('chat-panel-send');
    if (btn) btn.disabled = true;
    fetch('/api/messaging/channels/' + currentChannelId + '/messages', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })
      .then(function (r) {
        if (!r.ok) throw new Error('Send failed');
        return r.json();
      })
      .then(function () {
        if (input) input.value = '';
        clearAttachment();
        loadMessages(currentChannelId);
      })
      .catch(function () { alert('Failed to send.'); })
      .then(function () { if (btn) btn.disabled = false; });
  }

  function sendForward() {
    if (!dropPayload) return;
    var channelId = (byId('chat-panel-forward-select') && byId('chat-panel-forward-select').value) || '';
    var content = (byId('chat-panel-forward-content') && byId('chat-panel-forward-content').value) || '';
    content = content.trim() || '—';
    if (!channelId) {
      alert('Select a channel.');
      return;
    }
    var form = new FormData();
    form.append('channel_id', channelId);
    form.append('attachment_type', dropPayload.type);
    if (dropPayload.id) form.append('attachment_id', dropPayload.id);
    if (dropPayload.extra) form.append('attachment_country_code', dropPayload.extra);
    form.append('content', content);
    var btn = byId('chat-panel-forward-send');
    if (btn) btn.disabled = true;
    fetch(FORWARD_URL, { method: 'POST', credentials: 'same-origin', body: form })
      .then(function (r) {
        if (!r.ok) throw new Error('Forward failed');
        return r.text();
      })
      .then(function () {
        hideForwardForm();
        showChannelList();
      })
      .catch(function () { alert('Failed to send.'); })
      .then(function () { if (btn) btn.disabled = false; });
  }

  function isAppDrag(e) {
    var types = e.dataTransfer && e.dataTransfer.types;
    if (!types) return false;
    for (var i = 0; i < types.length; i++) {
      if (types[i] === DRAG_TYPE || types[i] === 'text/plain') return true;
    }
    return false;
  }

  function getDropData(e) {
    try {
      var raw = (e.dataTransfer.getData(DRAG_TYPE) || e.dataTransfer.getData('text/plain') || '').trim();
      var p = JSON.parse(raw);
      return (p && p.type) ? p : null;
    } catch (err) {
      return null;
    }
  }

  function onDragEnter(e) {
    if (isAppDrag(e)) {
      e.preventDefault();
      var overlay = byId('chat-panel-drop-overlay');
      if (overlay) overlay.classList.add('is-drag-over');
    }
  }

  function onDragOver(e) {
    if (isAppDrag(e)) {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'copy';
    }
  }

  function onDragLeave(e) {
    var body = byId('chat-panel-body');
    if (body && !body.contains(e.relatedTarget)) {
      var overlay = byId('chat-panel-drop-overlay');
      if (overlay) overlay.classList.remove('is-drag-over');
    }
  }

  function onDrop(e) {
    e.preventDefault();
    var overlay = byId('chat-panel-drop-overlay');
    if (overlay) overlay.classList.remove('is-drag-over');
    var payload = getDropData(e);
    if (!payload || !payload.type) return;
    if (currentChannelId) {
      setAttachment(payload.type, payload.id || '', payload.extra || '', payload.label || '');
      var input = byId('chat-panel-message-input');
      if (input) input.focus();
    } else {
      showForwardForm(payload);
    }
  }

  function initDraggables() {
    var nodes = document.querySelectorAll('[data-chat-drag]');
    for (var i = 0; i < nodes.length; i++) {
      var el = nodes[i];
      el.setAttribute('draggable', 'true');
      el.addEventListener('dragstart', function (ev) {
        var target = ev.currentTarget;
        var type = (target.getAttribute('data-chat-type') || '').trim();
        if (!type) return;
        ev.stopPropagation();
        var payload = {
          type: type,
          id: (target.getAttribute('data-chat-id') || '').trim(),
          extra: (target.getAttribute('data-chat-extra') || '').trim(),
          label: (target.getAttribute('data-chat-label') || '').trim()
        };
        var json = JSON.stringify(payload);
        ev.dataTransfer.setData(DRAG_TYPE, json);
        ev.dataTransfer.setData('text/plain', json);
        ev.dataTransfer.effectAllowed = 'copy';
      });
    }
  }

  function bindPanel() {
    if (toggle) toggle.addEventListener('click', openPanel);
    if (backdrop) backdrop.addEventListener('click', closePanel);
    var closeBtn = byId('chat-panel-close');
    if (closeBtn) closeBtn.addEventListener('click', closePanel);
    var backBtn = byId('chat-panel-back');
    if (backBtn) backBtn.addEventListener('click', showChannelList);
    var sendBtn = byId('chat-panel-send');
    if (sendBtn) sendBtn.addEventListener('click', sendMessage);
    var fwdSend = byId('chat-panel-forward-send');
    if (fwdSend) fwdSend.addEventListener('click', sendForward);
    var fwdCancel = byId('chat-panel-forward-cancel');
    if (fwdCancel) fwdCancel.addEventListener('click', function () {
      hideForwardForm();
      showChannelList();
    });
    var attachClear = byId('chat-panel-attach-clear');
    if (attachClear) attachClear.addEventListener('click', clearAttachment);
    var bodyEl = byId('chat-panel-body');
    if (bodyEl) {
      bodyEl.addEventListener('dragenter', onDragEnter);
      bodyEl.addEventListener('dragover', onDragOver);
      bodyEl.addEventListener('dragleave', onDragLeave);
      bodyEl.addEventListener('drop', onDrop);
    }
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && panel && panel.classList.contains('is-open')) {
        closePanel();
      }
    });
  }

  function init() {
    panel = document.getElementById('chat-panel');
    backdrop = document.getElementById('chat-panel-backdrop');
    toggle = document.getElementById('chat-panel-toggle');
    if (panel && backdrop) {
      bindPanel();
    }
    initDraggables();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
