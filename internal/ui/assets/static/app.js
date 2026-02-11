(() => {
  const banner = document.getElementById('error-banner');
  const toast = document.getElementById('toast');
  const modal = document.getElementById('modal');
  const modalTitle = document.getElementById('modal-title');
  const modalBody = document.getElementById('modal-body');
  const modalCancel = document.getElementById('modal-cancel');
  const modalConfirm = document.getElementById('modal-confirm');

  const api = async (method, url, body) => {
    const opts = { method, headers: { 'Content-Type': 'application/json' } };
    if (body !== undefined && body !== null) {
      opts.body = JSON.stringify(body);
    }
    const res = await fetch(url, opts);
    let payload = null;
    try { payload = await res.json(); } catch (e) { /* ignore */ }
    if (!res.ok || !payload || payload.ok === false) {
      const err = payload && payload.error ? payload.error : `Request failed (${res.status})`;
      const details = payload && payload.details ? payload.details : '';
      const e = new Error(err);
      e.details = details;
      throw e;
    }
    return payload.data;
  };

  const showBanner = (msg, details) => {
    if (!banner) return;
    banner.textContent = details ? `${msg} — ${details}` : msg;
    banner.classList.remove('hidden');
  };

  const clearBanner = () => {
    if (!banner) return;
    banner.textContent = '';
    banner.classList.add('hidden');
  };

  const showToast = (msg) => {
    if (!toast) return;
    toast.textContent = msg;
    toast.classList.remove('hidden');
    setTimeout(() => toast.classList.add('hidden'), 3000);
  };

  const bindNavButtons = () => {
    document.addEventListener('click', (e) => {
      const btn = e.target.closest('[data-nav]');
      if (!btn) return;
      const target = btn.dataset.nav || '';
      if (!target) return;
      window.location.href = target;
    });
  };

  const bindZpoolImportWatcher = () => {
    if (!modal) return;
    const dismissed = new Map();
    const cooldownMs = 10 * 60 * 1000;
    let inFlight = false;

    const poolKey = (pool) => (pool && pool.id ? pool.id : (pool ? pool.name : ''));

    const now = () => Date.now();
    const isDismissed = (key) => {
      const until = dismissed.get(key);
      return typeof until === 'number' && until > now();
    };
    const dismiss = (keys) => {
      const until = now() + cooldownMs;
      keys.forEach((key) => {
        if (key) dismissed.set(key, until);
      });
    };

    const promptImport = async (pools) => {
      if (!modal.classList.contains('hidden')) return;
      const names = pools.map((pool) => pool.name).filter((name) => name);
      if (!names.length) return;
      const body = names.length === 1
        ? `Import pool "${names[0]}" now?`
        : `Import ${names.length} pools now? (${names.join(', ')})`;
      const ok = await confirmModal('Import ZFS pools', body);
      if (!ok) {
        dismiss(pools.map((pool) => poolKey(pool)));
        return;
      }
      try {
        for (const pool of pools) {
          const payload = pool && pool.id
            ? { pool_id: pool.id, pool: pool.name, confirm: true }
            : { pool: pool.name, confirm: true };
          await api('POST', '/api/zfs/import', payload);
        }
        dismiss(pools.map((pool) => poolKey(pool)));
        showToast(names.length === 1 ? `Imported ${names[0]}` : `Imported ${names.length} pools`);
      } catch (err) {
        showBanner(err.message, err.details);
      }
    };

    const poll = async () => {
      if (inFlight) return;
      inFlight = true;
      try {
        const pools = await api('GET', '/api/zfs/importable');
        const candidates = (pools || []).filter((pool) => pool && pool.name && !isDismissed(poolKey(pool)));
        if (candidates.length) {
          await promptImport(candidates);
        }
      } catch (err) {
        // Ignore background polling failures to avoid banner spam.
      } finally {
        inFlight = false;
      }
    };

    poll();
    setInterval(poll, 20000);
  };

  const setBusy = (btn, busy) => {
    if (!btn) return;
    btn.disabled = busy;
    btn.classList.toggle('busy', busy);
  };

  const withBusy = async (btn, fn) => {
    setBusy(btn, true);
    try {
      return await fn();
    } finally {
      setBusy(btn, false);
    }
  };

  const confirmModal = (title, body) => {
    return new Promise((resolve) => {
      modalTitle.textContent = title;
      modalBody.textContent = body;
      modal.classList.remove('hidden');
      const cleanup = () => {
        modal.classList.add('hidden');
        modalCancel.removeEventListener('click', onCancel);
        modalConfirm.removeEventListener('click', onConfirm);
        document.removeEventListener('keydown', onKey);
      };
      const onCancel = () => { cleanup(); resolve(false); };
      const onConfirm = () => { cleanup(); resolve(true); };
      const onKey = (e) => {
        if (e.key === 'Escape') { cleanup(); resolve(false); }
        if (e.key === 'Enter') { cleanup(); resolve(true); }
      };
      modalCancel.addEventListener('click', onCancel);
      modalConfirm.addEventListener('click', onConfirm);
      document.addEventListener('keydown', onKey);
    });
  };

  const setStatus = (msg) => {
    const el = document.getElementById('global-status');
    if (el) el.textContent = msg;
  };

  const buildDatasetTree = (datasets, filterText) => {
    const root = { name: '', full: '', children: new Map(), data: null };
    const query = (filterText || '').trim().toLowerCase();
    const list = query
      ? datasets.filter((ds) => ds.name.toLowerCase().includes(query))
      : datasets;
    list.forEach((ds) => {
      const parts = ds.name.split('/');
      let node = root;
      parts.forEach((part, idx) => {
        if (!node.children.has(part)) {
          node.children.set(part, {
            name: part,
            full: parts.slice(0, idx + 1).join('/'),
            children: new Map(),
            data: null,
          });
        }
        node = node.children.get(part);
        if (idx === parts.length - 1) {
          node.data = ds;
        }
      });
    });
    return root;
  };

  const initDatasetTree = (opts) => {
    const container = opts.container;
    const filterInput = opts.filterInput;
    const emptyEl = opts.emptyEl;
    const state = {
      datasets: [],
      index: {},
      selected: '',
      expanded: new Set(),
      filter: '',
    };

    const indexDatasets = (datasets) => {
      const out = {};
      datasets.forEach((ds) => {
        out[ds.name] = ds;
      });
      return out;
    };

    const ensureExpandedFor = (name) => {
      if (!name) return;
      const parts = name.split('/');
      for (let i = 0; i < parts.length; i += 1) {
        state.expanded.add(parts.slice(0, i + 1).join('/'));
      }
    };

    const renderNode = (node, wrap, depth, forceOpen) => {
      const children = Array.from(node.children.values()).sort((a, b) => a.name.localeCompare(b.name));
      children.forEach((child) => {
        const row = document.createElement('div');
        row.className = `dataset-row${state.selected === child.full ? ' selected' : ''}`;
        row.style.paddingLeft = `${depth * 12}px`;
        const toggle = document.createElement('button');
        toggle.type = 'button';
        toggle.className = 'dataset-toggle';
        if (!child.children.size) {
          toggle.classList.add('spacer');
          toggle.disabled = true;
          toggle.textContent = '';
        } else {
          toggle.dataset.action = 'dataset-toggle';
          toggle.dataset.name = child.full;
          const isOpen = forceOpen || state.expanded.has(child.full);
          toggle.textContent = isOpen ? '-' : '+';
        }
        const nameBtn = document.createElement('button');
        nameBtn.type = 'button';
        nameBtn.className = 'dataset-name';
        nameBtn.dataset.action = 'dataset-select';
        nameBtn.dataset.name = child.full;
        nameBtn.textContent = child.name;
        row.appendChild(toggle);
        row.appendChild(nameBtn);

        if (opts.showMeta && child.data) {
          const meta = document.createElement('div');
          meta.className = 'dataset-meta';
          const label = document.createElement('span');
          label.className = 'dataset-meta-label';
          const type = child.data.type || 'dataset';
          const usedBytes = parseSize(child.data.used);
          const availBytes = parseSize(child.data.available);
          const maxBytes = usedBytes !== null && availBytes !== null ? usedBytes + availBytes : null;
          const maxText = maxBytes !== null ? formatSize(maxBytes) : (child.data.available || '?');
          label.textContent = `${type} ${child.data.used} used / ${maxText} max`;
          meta.appendChild(label);
          if (maxBytes !== null && usedBytes !== null && maxBytes > 0) {
            const bar = document.createElement('div');
            bar.className = 'dataset-bar';
            const fill = document.createElement('div');
            const pct = Math.min(100, Math.max(0, Math.round((usedBytes / maxBytes) * 100)));
            fill.className = 'dataset-bar-fill';
            fill.style.width = `${pct}%`;
            fill.title = `${pct}% used`;
            bar.appendChild(fill);
            meta.appendChild(bar);
          }
          row.appendChild(meta);
        }

        if (opts.onEdit || opts.onDestroy) {
          const actions = document.createElement('div');
          actions.className = 'dataset-actions';
          if (opts.onEdit) {
            const editBtn = document.createElement('button');
            editBtn.type = 'button';
            editBtn.className = 'btn';
            editBtn.dataset.action = 'dataset-edit';
            editBtn.dataset.name = child.full;
            editBtn.textContent = 'Edit';
            actions.appendChild(editBtn);
          }
          if (opts.onDestroy) {
            const delBtn = document.createElement('button');
            delBtn.type = 'button';
            delBtn.className = 'btn';
            delBtn.dataset.action = 'dataset-destroy';
            delBtn.dataset.name = child.full;
            delBtn.textContent = 'Destroy';
            actions.appendChild(delBtn);
          }
          row.appendChild(actions);
        }

        wrap.appendChild(row);

        if (child.children.size) {
          const childWrap = document.createElement('div');
          const open = forceOpen || state.expanded.has(child.full);
          childWrap.className = 'dataset-children';
          childWrap.style.display = open ? '' : 'none';
          wrap.appendChild(childWrap);
          renderNode(child, childWrap, depth + 1, forceOpen);
        }
      });
    };

    const render = () => {
      if (!container) return;
      container.innerHTML = '';
      const tree = buildDatasetTree(state.datasets, state.filter);
      const forceOpen = state.filter !== '';
      renderNode(tree, container, 0, forceOpen);
      if (emptyEl) {
        emptyEl.classList.toggle('hidden', state.datasets.length !== 0);
      }
    };

    const setDatasets = (datasets) => {
      state.datasets = datasets || [];
      state.index = indexDatasets(state.datasets);
      state.expanded = new Set();
      state.datasets.forEach((ds) => {
        const top = ds.name.split('/')[0];
        if (top) state.expanded.add(top);
      });
      ensureExpandedFor(state.selected);
      render();
    };

    const setSelected = (name) => {
      state.selected = name || '';
      ensureExpandedFor(state.selected);
      render();
      if (opts.onSelect) {
        opts.onSelect(state.selected, state.index[state.selected] || null);
      }
    };

    if (filterInput) {
      filterInput.addEventListener('input', (e) => {
        state.filter = e.target.value || '';
        render();
      });
    }

    if (container) {
      container.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-action]');
        if (!btn) return;
        const name = btn.dataset.name || '';
        if (btn.dataset.action === 'dataset-toggle') {
          if (state.expanded.has(name)) {
            state.expanded.delete(name);
          } else {
            state.expanded.add(name);
          }
          render();
          e.preventDefault();
          return;
        }
        if (btn.dataset.action === 'dataset-select') {
          setSelected(name);
          e.preventDefault();
          return;
        }
        if (btn.dataset.action === 'dataset-edit' && opts.onEdit) {
          opts.onEdit(name, state.index[name] || null);
          e.preventDefault();
          return;
        }
        if (btn.dataset.action === 'dataset-destroy' && opts.onDestroy) {
          opts.onDestroy(name, state.index[name] || null);
          e.preventDefault();
        }
      });
    }

    return { setDatasets, setSelected, getSelected: () => state.selected };
  };

  const parseSize = (value) => {
    if (!value) return null;
    const raw = value.trim();
    if (raw === '-' || raw.toLowerCase() === 'none') return null;
    if (raw === '0') return 0;
    const match = raw.match(/^([0-9]*\.?[0-9]+)\s*([kKmMgGtTpPeE]?)[bB]?$/);
    if (!match) return null;
    const num = parseFloat(match[1]);
    const unit = match[2].toUpperCase();
    const scale = {
      '': 1,
      K: 1024,
      M: 1024 ** 2,
      G: 1024 ** 3,
      T: 1024 ** 4,
      P: 1024 ** 5,
      E: 1024 ** 6,
    }[unit];
    if (!scale || Number.isNaN(num)) return null;
    return Math.round(num * scale);
  };

  const formatSize = (bytes) => {
    if (bytes === null || bytes === undefined) return '-';
    if (bytes === 0) return '0';
    const units = ['B', 'K', 'M', 'G', 'T', 'P', 'E'];
    let idx = 0;
    let val = bytes;
    while (val >= 1024 && idx < units.length - 1) {
      val /= 1024;
      idx += 1;
    }
    const fixed = val >= 10 || idx === 0 ? val.toFixed(0) : val.toFixed(1);
    return `${fixed}${units[idx]}`;
  };

  const parseLines = (value) => {
    if (!value) return [];
    return value.split(/\r?\n/).map((item) => item.trim()).filter((item) => item);
  };

  const parseArgs = (value) => {
    const trimmed = (value || '').trim();
    if (!trimmed) return [];
    return trimmed.split(/\s+/).map((item) => item.trim()).filter((item) => item);
  };

  const parseAliases = (value) => {
    const out = {};
    parseLines(value).forEach((line) => {
      let key = '';
      let val = '';
      if (line.includes('=')) {
        const parts = line.split('=');
        key = (parts.shift() || '').trim();
        val = parts.join('=').trim();
      } else {
        const parts = line.split(/\s+/);
        key = (parts.shift() || '').trim();
        val = parts.join(' ').trim();
      }
      if (!key || !val) return;
      out[key] = val;
    });
    return out;
  };

  const formatAliases = (aliases) => {
    if (!aliases) return '';
    return Object.keys(aliases)
      .sort()
      .map((key) => `${key}=${aliases[key]}`)
      .join('\n');
  };

  const bindDashboard = () => {
    const grid = document.getElementById('dashboard-grid');
    if (!grid) return;

    const refreshBtn = document.getElementById('dashboard-refresh');
    const editBtn = document.getElementById('dashboard-edit');
    const saveBtn = document.getElementById('dashboard-save');
    const cancelBtn = document.getElementById('dashboard-cancel');
    const updatedEl = document.getElementById('dashboard-updated');
    const emptyEl = document.getElementById('dashboard-empty');
    const configWindow = document.getElementById('dashboard-config-window');
    const configEl = document.getElementById('dashboard-config');

    const widgetDefs = {
      pools: { title: 'ZFS Pools', link: '/zfs/pools', hint: 'Health + allocation' },
      cache: { title: 'L2ARC Cache', link: '/zfs/mounts', hint: 'Used vs total cache' },
      datasets: { title: 'Datasets', link: '/zfs/datasets', hint: 'Used vs available' },
      snapshots: { title: 'Snapshots', link: '/zfs/snapshots', hint: 'Total snapshots' },
      schedules: { title: 'Schedules', link: '/zfs/schedules', hint: 'Enabled vs disabled' },
      samba: { title: 'Samba', link: '/samba/users', hint: 'Users and shares' },
      settings: { title: 'System Settings', link: '/settings', hint: 'Autostart status' },
    };

    let layout = [];
    let draftLayout = [];
    let summary = {};
    let errors = {};
    let editMode = false;
    const storageKey = 'rrd.dashboard.layout';

    const cloneLayout = (items) => (items || []).map((item) => ({
      id: item.id,
      enabled: item.enabled,
    }));

    const normalizeLayout = (items) => {
      const out = [];
      const seen = new Set();
      (items || []).forEach((item) => {
        if (!item || !item.id || !widgetDefs[item.id]) return;
        if (seen.has(item.id)) return;
        out.push({ id: item.id, enabled: !!item.enabled });
        seen.add(item.id);
      });
      Object.keys(widgetDefs).forEach((id) => {
        if (seen.has(id)) return;
        out.push({ id, enabled: true });
        seen.add(id);
      });
      return out;
    };

    const loadStoredLayout = () => {
      try {
        const raw = localStorage.getItem(storageKey);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) return null;
        return parsed;
      } catch (e) {
        return null;
      }
    };

    const storeLayout = (items) => {
      try {
        localStorage.setItem(storageKey, JSON.stringify(items || []));
      } catch (e) {
        /* ignore */
      }
    };

    const clearStoredLayout = () => {
      try {
        localStorage.removeItem(storageKey);
      } catch (e) {
        /* ignore */
      }
    };

    const formatWhen = (raw) => {
      if (!raw) return '';
      const date = new Date(raw);
      if (Number.isNaN(date.getTime())) return raw;
      return date.toLocaleString();
    };

    const buildDonut = (percent, label, subLabel) => {
      const pct = Math.max(0, Math.min(100, Math.round(percent || 0)));
      const wrap = document.createElement('div');
      wrap.className = 'dashboard-donut';
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('viewBox', '0 0 36 36');
      const bg = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      bg.setAttribute('cx', '18');
      bg.setAttribute('cy', '18');
      bg.setAttribute('r', '15.915');
      bg.classList.add('donut-bg');
      const ring = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      ring.setAttribute('cx', '18');
      ring.setAttribute('cy', '18');
      ring.setAttribute('r', '15.915');
      ring.setAttribute('stroke-dasharray', `${pct} ${100 - pct}`);
      ring.setAttribute('stroke-dashoffset', '25');
      ring.classList.add('donut-ring');
      svg.appendChild(bg);
      svg.appendChild(ring);
      const center = document.createElement('div');
      center.className = 'donut-center';
      const labelEl = document.createElement('div');
      labelEl.className = 'donut-label';
      labelEl.textContent = label;
      const subEl = document.createElement('div');
      subEl.className = 'donut-sub';
      subEl.textContent = subLabel || '';
      center.appendChild(labelEl);
      center.appendChild(subEl);
      wrap.appendChild(svg);
      wrap.appendChild(center);
      return wrap;
    };

    const buildHalfDonut = (percent, label, subLabel) => {
      const pct = Math.max(0, Math.min(100, Math.round(percent || 0)));
      const wrap = document.createElement('div');
      wrap.className = 'dashboard-half';
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('viewBox', '0 0 36 18');
      const bg = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      bg.setAttribute('d', 'M2 16 A16 16 0 0 1 34 16');
      bg.setAttribute('pathLength', '100');
      bg.classList.add('donut-half-bg');
      const ring = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      ring.setAttribute('d', 'M2 16 A16 16 0 0 1 34 16');
      ring.setAttribute('pathLength', '100');
      ring.setAttribute('stroke-dasharray', `${pct} ${100 - pct}`);
      ring.classList.add('donut-half-ring');
      svg.appendChild(bg);
      svg.appendChild(ring);
      const center = document.createElement('div');
      center.className = 'donut-center';
      const labelEl = document.createElement('div');
      labelEl.className = 'donut-label';
      labelEl.textContent = label;
      const subEl = document.createElement('div');
      subEl.className = 'donut-sub';
      subEl.textContent = subLabel || '';
      center.appendChild(labelEl);
      center.appendChild(subEl);
      wrap.appendChild(svg);
      wrap.appendChild(center);
      return wrap;
    };

    const buildSplitBar = (leftPct, leftLabel, rightLabel) => {
      const pct = Math.max(0, Math.min(100, Math.round(leftPct || 0)));
      const wrap = document.createElement('div');
      wrap.className = 'dashboard-split';
      const bar = document.createElement('div');
      bar.className = 'split-bar';
      const left = document.createElement('div');
      left.className = 'split-bar-left';
      left.style.width = `${pct}%`;
      const right = document.createElement('div');
      right.className = 'split-bar-right';
      right.style.width = `${100 - pct}%`;
      bar.appendChild(left);
      bar.appendChild(right);
      const legend = document.createElement('div');
      legend.className = 'split-legend';
      const leftText = document.createElement('div');
      leftText.textContent = leftLabel || '';
      const rightText = document.createElement('div');
      rightText.textContent = rightLabel || '';
      legend.appendChild(leftText);
      legend.appendChild(rightText);
      wrap.appendChild(bar);
      wrap.appendChild(legend);
      return wrap;
    };

    const buildBlocks = (count) => {
      const wrap = document.createElement('div');
      wrap.className = 'dashboard-blocks';
      const totalBlocks = 10;
      const filled = Math.min(totalBlocks, Math.ceil((count || 0) / 10));
      for (let i = 0; i < totalBlocks; i += 1) {
        const block = document.createElement('div');
        block.className = 'block';
        if (i < filled) block.classList.add('filled');
        wrap.appendChild(block);
      }
      return wrap;
    };

    const buildStat = (label, subLabel) => {
      const wrap = document.createElement('div');
      wrap.className = 'dashboard-stat';
      const big = document.createElement('div');
      big.className = 'stat-value';
      big.textContent = label || '';
      const sub = document.createElement('div');
      sub.className = 'stat-sub';
      sub.textContent = subLabel || '';
      wrap.appendChild(big);
      wrap.appendChild(sub);
      return wrap;
    };

    const buildPill = (label, active) => {
      const wrap = document.createElement('div');
      wrap.className = 'dashboard-pill';
      if (active) wrap.classList.add('active');
      wrap.textContent = label || '';
      return wrap;
    };

    const wrapLink = (def, content) => {
      const link = document.createElement('a');
      link.href = def.link;
      link.className = 'dashboard-link';
      link.setAttribute('aria-label', `${def.title} details`);
      link.title = `Open ${def.title}`;
      link.appendChild(content);
      return link;
    };

    const buildLines = (lines) => {
      const list = document.createElement('div');
      list.className = 'dashboard-lines';
      (lines || []).forEach((line) => {
        const row = document.createElement('div');
        row.className = 'dashboard-line';
        row.textContent = line;
        list.appendChild(row);
      });
      return list;
    };

    const buildWidgetInfo = (id) => {
      const err = errors[id];
      if (err) {
        return {
          type: 'stat',
          percent: 0,
          label: 'ERR',
          sub: 'Check',
          lines: [`${err}`],
          error: true,
        };
      }
      const data = summary || {};
      if (id === 'pools') {
        const pools = data.pools || {};
        const alloc = pools.alloc_bytes || 0;
        const size = pools.size_bytes || 0;
        const pct = size > 0 ? (alloc / size) * 100 : 0;
        return {
          type: 'half',
          percent: pct,
          label: `${Math.round(pct)}%`,
          sub: 'Allocated',
          lines: [
            `Pools: ${pools.count || 0}`,
            `Healthy: ${pools.healthy || 0}`,
            `Degraded: ${pools.degraded || 0}`,
            `Alloc: ${formatSize(alloc)}`,
            `Size: ${formatSize(size)}`,
          ],
        };
      }
      if (id === 'datasets') {
        const ds = data.datasets || {};
        const used = ds.used_bytes || 0;
        const avail = ds.available_bytes || 0;
        const total = used + avail;
        const pct = total > 0 ? (used / total) * 100 : 0;
        return {
          type: 'donut',
          percent: pct,
          label: `${Math.round(pct)}%`,
          sub: 'Used',
          lines: [
            `Datasets: ${ds.count || 0}`,
            `Used: ${formatSize(used)}`,
            `Available: ${formatSize(avail)}`,
          ],
        };
      }
      if (id === 'cache') {
        const cache = data.cache || {};
        const used = cache.used_bytes || 0;
        const total = cache.total_bytes || 0;
        const devices = cache.devices || [];
        const pct = total > 0 ? (used / total) * 100 : 0;
        if (total > 0) {
          return {
            type: 'donut',
            percent: pct,
            label: `${Math.round(pct)}%`,
            sub: 'Used',
            lines: [
              `Used: ${formatSize(used)}`,
              `Total: ${formatSize(total)}`,
              `Devices: ${devices.length}`,
            ],
          };
        }
        return {
          type: 'stat',
          label: formatSize(used),
          sub: cache.present ? 'Used' : 'No cache',
          lines: [
            cache.present ? `Devices: ${devices.length}` : 'No cache devices detected',
          ],
        };
      }
      if (id === 'snapshots') {
        const snaps = data.snapshots || {};
        const total = snaps.count || 0;
        return {
          type: 'blocks',
          label: `${total}`,
          sub: 'Snapshots',
          count: total,
          lines: [`Total: ${total}`, '1 block = 10 snapshots'],
        };
      }
      if (id === 'schedules') {
        const schedules = data.schedules || {};
        const total = schedules.count || 0;
        const enabled = schedules.enabled || 0;
        const disabled = schedules.disabled || 0;
        const pct = total > 0 ? (enabled / total) * 100 : 0;
        return {
          type: 'split',
          percent: pct,
          label: `${enabled}/${total}`,
          sub: 'Enabled',
          enabled,
          disabled,
          lines: [`Total: ${total}`, `Disabled: ${disabled}`],
        };
      }
      if (id === 'samba') {
        const samba = data.samba || {};
        const users = samba.users || 0;
        const shares = samba.shares || 0;
        return {
          type: 'stat',
          label: `${users}`,
          sub: 'Users',
          lines: [`Shares: ${shares}`],
        };
      }
      if (id === 'settings') {
        const settings = data.settings || {};
        const enabled = !!settings.autostart_enabled;
        return {
          type: 'pill',
          label: enabled ? 'Autostart ON' : 'Autostart OFF',
          sub: 'Autostart',
          active: enabled,
          lines: [enabled ? 'Autostart enabled' : 'Autostart disabled'],
        };
      }
      return {
        type: 'stat',
        percent: 0,
        label: '--',
        sub: '',
        lines: [],
      };
    };

    const buildCard = (item, editing) => {
      const def = widgetDefs[item.id];
      if (!def) return null;
      const info = buildWidgetInfo(item.id);
      const card = document.createElement('div');
      card.className = 'dashboard-card';
      card.dataset.widgetId = item.id;
      if (editing) {
        card.classList.add('dashboard-card-editing');
        card.draggable = true;
      }
      if (!item.enabled) {
        card.classList.add('dashboard-card-disabled');
      }

      const header = document.createElement('div');
      header.className = 'dashboard-card-header';
      const title = document.createElement('div');
      title.className = 'dashboard-card-title';
      title.textContent = def.title;
      header.appendChild(title);
      const body = document.createElement('div');
      body.className = 'dashboard-card-body';
      let graph = null;
      if (info.type === 'half') {
        graph = buildHalfDonut(info.percent, info.label, info.sub);
      } else if (info.type === 'donut') {
        graph = buildDonut(info.percent, info.label, info.sub);
      } else if (info.type === 'split') {
        graph = buildSplitBar(info.percent, `${info.enabled || 0} enabled`, `${info.disabled || 0} disabled`);
      } else if (info.type === 'blocks') {
        const stat = buildStat(info.label, info.sub);
        const blocks = buildBlocks(info.count || 0);
        const wrap = document.createElement('div');
        wrap.className = 'dashboard-block-wrap';
        wrap.appendChild(stat);
        wrap.appendChild(blocks);
        graph = wrap;
      } else if (info.type === 'pill') {
        graph = buildPill(info.label, !!info.active);
      } else {
        graph = buildStat(info.label, info.sub);
      }
      body.appendChild(wrapLink(def, graph));
      body.appendChild(buildLines(info.lines));

      if (info.error) {
        const err = document.createElement('div');
        err.className = 'dashboard-error';
        err.textContent = 'Data error';
        body.appendChild(err);
      }

      const footer = document.createElement('div');
      footer.className = 'dashboard-card-footer';
      const hint = document.createElement('div');
      hint.className = 'muted tiny';
      hint.textContent = def.hint || '';
      footer.appendChild(hint);

      card.appendChild(header);
      card.appendChild(body);
      card.appendChild(footer);
      return card;
    };

    const renderConfig = () => {
      if (!configEl) return;
      configEl.innerHTML = '';
      draftLayout.forEach((item) => {
        const def = widgetDefs[item.id];
        if (!def) return;
        const row = document.createElement('label');
        row.className = 'dashboard-config-row';
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = !!item.enabled;
        checkbox.addEventListener('change', () => {
          item.enabled = checkbox.checked;
          renderGrid();
        });
        const text = document.createElement('span');
        text.textContent = def.title;
        row.appendChild(checkbox);
        row.appendChild(text);
        configEl.appendChild(row);
      });
    };

    const renderGrid = () => {
      grid.innerHTML = '';
      let enabledCount = 0;
      draftLayout.forEach((item) => {
        if (item.enabled) enabledCount += 1;
        if (!item.enabled && !editMode) return;
        const card = buildCard(item, editMode);
        if (card) grid.appendChild(card);
      });
      if (emptyEl) {
        const shouldShow = enabledCount === 0 && !editMode;
        emptyEl.classList.toggle('hidden', !shouldShow);
      }
    };

    const syncOrderFromDOM = () => {
      const order = Array.from(grid.querySelectorAll('.dashboard-card')).map((el) => el.dataset.widgetId);
      const byId = {};
      draftLayout.forEach((item) => { byId[item.id] = item; });
      draftLayout = order.map((id) => byId[id]).filter((item) => item);
    };

    let dragCard = null;
    grid.addEventListener('dragstart', (e) => {
      if (!editMode) return;
      const card = e.target.closest('.dashboard-card');
      if (!card) return;
      dragCard = card;
      card.classList.add('dashboard-card-dragging');
      e.dataTransfer.effectAllowed = 'move';
    });
    grid.addEventListener('dragover', (e) => {
      if (!editMode || !dragCard) return;
      e.preventDefault();
      const target = e.target.closest('.dashboard-card');
      if (!target || target === dragCard) return;
      const rect = target.getBoundingClientRect();
      const before = (e.clientY - rect.top) < rect.height / 2;
      grid.insertBefore(dragCard, before ? target : target.nextSibling);
    });
    grid.addEventListener('dragend', () => {
      if (!editMode || !dragCard) return;
      dragCard.classList.remove('dashboard-card-dragging');
      dragCard = null;
      syncOrderFromDOM();
    });

    const setEditMode = (flag) => {
      editMode = flag;
      if (editMode) {
        draftLayout = cloneLayout(layout);
      }
      if (configWindow) configWindow.classList.toggle('hidden', !editMode);
      if (editBtn) editBtn.classList.toggle('hidden', editMode);
      if (saveBtn) saveBtn.classList.toggle('hidden', !editMode);
      if (cancelBtn) cancelBtn.classList.toggle('hidden', !editMode);
      renderConfig();
      renderGrid();
    };

    const loadDashboard = async () => {
      const data = await api('GET', '/api/dashboard');
      let nextLayout = normalizeLayout(data.layout || []);
      const stored = loadStoredLayout();
      const usingLocal = !editMode && stored && stored.length > 0;
      if (usingLocal) {
        nextLayout = normalizeLayout(stored);
      }
      if (!editMode) {
        layout = nextLayout;
        draftLayout = cloneLayout(layout);
      }
      summary = data.summary || {};
      errors = data.errors || {};
      if (updatedEl && summary.updated) {
        const suffix = usingLocal ? ' (layout: local)' : '';
        updatedEl.textContent = `Updated ${formatWhen(summary.updated)}${suffix}`;
      }
      renderGrid();
      if (editMode) renderConfig();
    };

    if (refreshBtn) {
      refreshBtn.addEventListener('click', () => {
        clearBanner();
        loadDashboard().catch((err) => showBanner(err.message, err.details));
      });
    }
    if (editBtn) {
      editBtn.addEventListener('click', () => {
        setEditMode(true);
      });
    }
    if (cancelBtn) {
      cancelBtn.addEventListener('click', () => {
        draftLayout = cloneLayout(layout);
        setEditMode(false);
      });
    }
    if (saveBtn) {
      saveBtn.addEventListener('click', async () => {
        clearBanner();
        try {
          const res = await withBusy(saveBtn, () => api('PUT', '/api/dashboard', { widgets: draftLayout }));
          layout = normalizeLayout((res && res.layout) || draftLayout);
          draftLayout = cloneLayout(layout);
          clearStoredLayout();
          setEditMode(false);
          showToast('Dashboard saved');
        } catch (err) {
          const message = (err.message || '').toLowerCase();
          const details = (err.details || '').toLowerCase();
          if (message.includes('dashboard update failed') && details.includes('config path not set')) {
            storeLayout(draftLayout);
            layout = normalizeLayout(draftLayout);
            draftLayout = cloneLayout(layout);
            setEditMode(false);
            showToast('Dashboard saved locally');
            return;
          }
          showBanner(err.message, err.details);
        }
      });
    }

    loadDashboard().catch((err) => showBanner(err.message, err.details));
  };

  const bindTerminal = () => {
    const form = document.getElementById('cmd-form');
    if (!form) return;
    const input = document.getElementById('cmd-input');
    const output = document.getElementById('cmd-output');
    const autoScroll = document.getElementById('auto-scroll');
    const status = document.getElementById('cmd-status');
    const historyWrap = document.getElementById('terminal-history');
    const historyEmpty = document.getElementById('terminal-history-empty');
    const favoritesWrap = document.getElementById('terminal-favorites');
    const favoritesEmpty = document.getElementById('terminal-favorites-empty');
    const suggestions = document.getElementById('cmd-suggestions');
    const preview = document.getElementById('cmd-preview');

    const state = {
      aliases: {},
      favorites: [],
      history: [],
      historyLimit: 20,
      suggestions: [],
      suggestionIndex: -1,
    };

    const appendOutput = (text) => {
      output.textContent += text;
      if (autoScroll.checked) {
        output.scrollTop = output.scrollHeight;
      }
    };

    const setPreview = (text, warn) => {
      if (!preview) return;
      preview.textContent = text;
      preview.classList.toggle('warn', !!warn);
    };

    const resolvePreview = (cmd) => {
      const trimmed = cmd.trim();
      if (!trimmed) return { text: 'Resolved: -', warn: false };
      const parts = trimmed.split(/\s+/);
      if (parts[0].startsWith('/')) {
        return { text: `Resolved: ${trimmed}`, warn: false };
      }
      const alias = state.aliases[parts[0]];
      if (!alias) {
        return { text: `Resolved: unknown (${parts[0]})`, warn: true };
      }
      const resolved = [alias].concat(parts.slice(1)).join(' ');
      return { text: `Resolved: ${resolved}`, warn: false };
    };

    const updatePreview = () => {
      const resolved = resolvePreview(input.value);
      setPreview(resolved.text, resolved.warn);
    };

    const uniquePool = (items) => {
      const seen = new Set();
      const out = [];
      items.forEach((item) => {
        if (!item || seen.has(item)) return;
        seen.add(item);
        out.push(item);
      });
      return out;
    };

    const buildSuggestions = (value) => {
      const trimmed = value.trim();
      if (!trimmed) return [];
      const lower = trimmed.toLowerCase();
      const pool = uniquePool([
        ...Object.keys(state.aliases || {}),
        ...state.favorites,
        ...state.history,
      ]);
      return pool.filter((item) => item.toLowerCase().startsWith(lower)).slice(0, 12);
    };

    const renderSuggestions = () => {
      if (!suggestions) return;
      suggestions.innerHTML = '';
      if (!state.suggestions.length) {
        suggestions.classList.add('hidden');
        return;
      }
      suggestions.classList.remove('hidden');
      state.suggestions.forEach((item, idx) => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = `suggestion-item${idx === state.suggestionIndex ? ' active' : ''}`;
        btn.dataset.action = 'terminal-suggest';
        btn.dataset.cmd = item;
        btn.textContent = item;
        suggestions.appendChild(btn);
      });
    };

    const updateSuggestions = () => {
      state.suggestions = buildSuggestions(input.value);
      state.suggestionIndex = -1;
      renderSuggestions();
    };

    const favoriteSet = () => new Set(state.favorites);

    const buildRow = (cmd, isFavorite) => {
      const row = document.createElement('div');
      row.className = 'terminal-item';
      const cmdBtn = document.createElement('button');
      cmdBtn.type = 'button';
      cmdBtn.className = 'terminal-cmd';
      cmdBtn.dataset.action = 'terminal-fill';
      cmdBtn.dataset.cmd = cmd;
      cmdBtn.textContent = cmd;
      const actions = document.createElement('div');
      actions.className = 'terminal-actions';
      const runBtn = document.createElement('button');
      runBtn.type = 'button';
      runBtn.className = 'btn ghost';
      runBtn.dataset.action = 'terminal-run';
      runBtn.dataset.cmd = cmd;
      runBtn.textContent = 'Run';
      const starBtn = document.createElement('button');
      starBtn.type = 'button';
      starBtn.className = `star-btn${isFavorite ? ' is-favorite' : ''}`;
      starBtn.dataset.action = 'terminal-favorite';
      starBtn.dataset.cmd = cmd;
      starBtn.dataset.favorite = isFavorite ? 'false' : 'true';
      starBtn.title = isFavorite ? 'Remove favorite' : 'Add favorite';
      starBtn.setAttribute('aria-label', starBtn.title);
      starBtn.textContent = '*';
      actions.appendChild(runBtn);
      actions.appendChild(starBtn);
      row.appendChild(cmdBtn);
      row.appendChild(actions);
      return row;
    };

    const renderList = (wrap, empty, items, favorites) => {
      if (!wrap) return;
      wrap.innerHTML = '';
      if (!items.length) {
        empty?.classList.remove('hidden');
        return;
      }
      empty?.classList.add('hidden');
      items.forEach((cmd) => {
        wrap.appendChild(buildRow(cmd, favorites.has(cmd)));
      });
    };

    const renderHistory = () => {
      renderList(historyWrap, historyEmpty, state.history, favoriteSet());
    };

    const renderFavorites = () => {
      renderList(favoritesWrap, favoritesEmpty, state.favorites, favoriteSet());
    };

    const addHistory = (cmd) => {
      const trimmed = cmd.trim();
      if (!trimmed) return;
      state.history = [trimmed, ...state.history.filter((item) => item !== trimmed)];
      state.history = state.history.slice(0, state.historyLimit);
      renderHistory();
    };

    const setInput = (cmd) => {
      input.value = cmd;
      updatePreview();
      updateSuggestions();
      input.focus();
    };

    const runCommand = async (cmdOverride) => {
      clearBanner();
      const cmd = (cmdOverride || input.value).trim();
      if (!cmd) return;
      input.value = cmd;
      const submitBtn = form.querySelector('button[type="submit"]');
      status.textContent = 'Running...';
      setStatus('Command running');
      output.textContent = '';
      try {
        const data = await withBusy(submitBtn, () => api('POST', '/api/cmd/run', { cmd }));
        addHistory(cmd);
        const id = data.job_id;
        const evt = new EventSource(`/api/jobs/${id}/stream`);
        evt.onmessage = (ev) => appendOutput(ev.data + "\n");
        evt.onerror = () => { evt.close(); };
        const poll = async () => {
          try {
            const job = await api('GET', `/api/jobs/${id}`);
            status.textContent = job.done ? `Exit ${job.exit_code} • ${job.duration}` : 'Running...';
            if (job.done) {
              setStatus('Idle');
              evt.close();
              return;
            }
            setTimeout(poll, 1000);
          } catch (err) {
            showBanner(err.message, err.details);
            evt.close();
          }
        };
        poll();
      } catch (err) {
        showBanner(err.message, err.details);
        status.textContent = 'Failed';
        setStatus('Idle');
      }
    };

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      runCommand();
    });

    input.addEventListener('input', () => {
      updatePreview();
      updateSuggestions();
    });

    input.addEventListener('keydown', (e) => {
      if (!state.suggestions.length) return;
      if (e.key === 'Tab') {
        e.preventDefault();
        state.suggestionIndex = (state.suggestionIndex + 1) % state.suggestions.length;
        input.value = state.suggestions[state.suggestionIndex];
        updatePreview();
        renderSuggestions();
      }
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        state.suggestionIndex = Math.min(state.suggestionIndex + 1, state.suggestions.length - 1);
        renderSuggestions();
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        state.suggestionIndex = Math.max(state.suggestionIndex - 1, 0);
        renderSuggestions();
      }
      if (e.key === 'Escape') {
        suggestions.classList.add('hidden');
      }
    });

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action]');
      if (!btn) {
        if (!e.target.closest('.cmd-input-wrap')) {
          suggestions.classList.add('hidden');
        }
        return;
      }
      if (btn.dataset.action === 'clear-output') {
        output.textContent = '';
        return;
      }
      if (btn.dataset.action === 'copy-output') {
        navigator.clipboard.writeText(output.textContent);
        showToast('Output copied');
        return;
      }
      if (btn.dataset.action === 'terminal-fill') {
        setInput(btn.dataset.cmd || '');
        return;
      }
      if (btn.dataset.action === 'terminal-run') {
        await runCommand(btn.dataset.cmd || '');
        return;
      }
      if (btn.dataset.action === 'terminal-suggest') {
        setInput(btn.dataset.cmd || '');
        suggestions.classList.add('hidden');
        return;
      }
      if (btn.dataset.action === 'terminal-favorite') {
        const cmd = btn.dataset.cmd || '';
        const favorite = btn.dataset.favorite === 'true';
        if (!cmd) return;
        try {
          const res = await api('POST', '/api/terminal/favorites', { cmd, favorite });
          state.favorites = res.favorites || [];
          renderFavorites();
          renderHistory();
        } catch (err) {
          showBanner(err.message, err.details);
        }
      }
    });

    const loadMeta = async () => {
      const data = await api('GET', '/api/terminal/meta');
      state.aliases = data.aliases || {};
      state.favorites = data.favorites || [];
      state.history = data.history || [];
      state.historyLimit = data.history_limit || 20;
      renderFavorites();
      renderHistory();
      updatePreview();
      updateSuggestions();
    };

    loadMeta().catch((err) => showBanner(err.message, err.details));
  };

  const renderTable = (tableId, rows, emptyId, rowBuilder) => {
    const table = document.querySelector(`${tableId} tbody`);
    const empty = document.querySelector(emptyId);
    if (!table) return;
    table.innerHTML = '';
    if (!rows.length) {
      empty?.classList.remove('hidden');
      return;
    }
    empty?.classList.add('hidden');
    rows.forEach((row) => table.appendChild(rowBuilder(row)));
  };

  const bindSambaUsers = () => {
    const table = document.getElementById('samba-users-table');
    if (!table) return;
    const form = document.getElementById('samba-user-form');

    const loadUsers = async () => {
      const users = await api('GET', '/api/samba/users');
      renderTable('#samba-users-table', users, '#samba-users-empty', (user) => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${user.name}</td>
          <td>
            <button class="btn" data-action="samba-passwd" data-user="${user.name}">Password</button>
            <button class="btn" data-action="samba-disable" data-user="${user.name}">Disable</button>
            <button class="btn" data-action="samba-enable" data-user="${user.name}">Enable</button>
            <button class="btn" data-action="samba-delete" data-user="${user.name}">Delete</button>
          </td>`;
        return tr;
      });
    };

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      clearBanner();
      const username = document.getElementById('samba-username').value.trim();
      const password = document.getElementById('samba-password').value;
      const passwordConfirm = document.getElementById('samba-password2').value;
      if (password !== passwordConfirm) {
        showBanner('Passwords do not match');
        return;
      }
      try {
        const btn = form.querySelector('button[type="submit"]');
        await withBusy(btn, () => api('POST', '/api/samba/users', { username, password, password_confirm: passwordConfirm }));
        showToast('User added');
        form.reset();
        loadUsers();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action^="samba-"]');
      if (!btn) return;
      const user = btn.dataset.user;
      if (!user) return;
      try {
        if (btn.dataset.action === 'samba-passwd') {
          const pw = prompt(`New password for ${user}`);
          if (!pw) return;
          const ok = await confirmModal('Confirm password reset', `Reset Samba password for ${user}?`);
          if (!ok) return;
          await withBusy(btn, () => api('POST', `/api/samba/users/${encodeURIComponent(user)}/passwd`, { password: pw, confirm: true }));
          showToast('Password updated');
        }
        if (btn.dataset.action === 'samba-disable') {
          const ok = await confirmModal('Disable user', `Disable ${user}?`);
          if (!ok) return;
          await withBusy(btn, () => api('POST', `/api/samba/users/${encodeURIComponent(user)}/disable`, { confirm: true }));
          showToast('User disabled');
        }
        if (btn.dataset.action === 'samba-enable') {
          await withBusy(btn, () => api('POST', `/api/samba/users/${encodeURIComponent(user)}/enable`, {}));
          showToast('User enabled');
        }
        if (btn.dataset.action === 'samba-delete') {
          const ok = await confirmModal('Delete user', `Delete ${user}?`);
          if (!ok) return;
          await withBusy(btn, () => api('POST', `/api/samba/users/${encodeURIComponent(user)}/delete`, { confirm: true }));
          showToast('User deleted');
        }
        loadUsers();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    loadUsers();
  };

  const bindSambaShares = () => {
    const table = document.getElementById('samba-shares-table');
    if (!table) return;
    const form = document.getElementById('share-form');

    const loadShares = async () => {
      const shares = await api('GET', '/api/samba/shares');
      renderTable('#samba-shares-table', shares, '#samba-shares-empty', (share) => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${share.name}</td><td>${share.path}</td><td>${share.read_only}</td><td>${share.browseable}</td><td>${share.guest_ok}</td>
          <td>
            <button class="btn" data-action="share-edit" data-name="${share.name}">Edit</button>
            <button class="btn" data-action="share-delete" data-name="${share.name}">Delete</button>
          </td>`;
        return tr;
      });
    };

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      clearBanner();
      const body = {
        name: document.getElementById('share-name').value.trim(),
        path: document.getElementById('share-path').value.trim(),
        read_only: document.getElementById('share-readonly').value,
        browseable: document.getElementById('share-browseable').value,
        guest_ok: document.getElementById('share-guest').value,
        comment: document.getElementById('share-comment').value.trim(),
      };
      try {
        const btn = form.querySelector('button[type="submit"]');
        await withBusy(btn, () => api('POST', '/api/samba/shares', body));
        showToast('Share saved');
        form.reset();
        loadShares();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      try {
        if (btn.dataset.action === 'test-samba') {
          const res = await withBusy(btn, () => api('POST', '/api/samba/testparm', {}));
          showToast('Config tested');
          alert(res.output || 'testparm completed');
        }
        if (btn.dataset.action === 'reload-samba') {
          const res = await withBusy(btn, () => api('POST', '/api/samba/reload', {}));
          showToast('Samba reloaded');
          if (res.output) alert(res.output);
        }
        if (btn.dataset.action === 'share-delete') {
          const name = btn.dataset.name;
          const ok = await confirmModal('Delete share', `Delete share ${name}?`);
          if (!ok) return;
          await withBusy(btn, () => api('DELETE', `/api/samba/shares/${encodeURIComponent(name)}`, { confirm: true }));
          showToast('Share deleted');
          loadShares();
        }
        if (btn.dataset.action === 'share-edit') {
          const name = btn.dataset.name;
          const path = prompt('New path for share', '');
          if (!path) return;
          const ok = await confirmModal('Edit share', `Update share ${name}?`);
          if (!ok) return;
          await withBusy(btn, () => api('PUT', `/api/samba/shares/${encodeURIComponent(name)}`, { path, confirm: true }));
          showToast('Share updated');
          loadShares();
        }
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    loadShares();
  };

  const bindZFSPools = () => {
    const table = document.getElementById('zfs-pools-table');
    if (!table) return;
    const drawer = document.getElementById('pool-status');
    const drawerOut = document.getElementById('pool-status-output');
    const createForm = document.getElementById('pool-create-form');
    const poolName = document.getElementById('pool-name');
    const deviceList = document.getElementById('pool-device-list');
    const deviceEmpty = document.getElementById('pool-device-empty');
    const cacheEnabled = document.getElementById('pool-cache-enabled');
    const cacheSelect = document.getElementById('pool-cache-device');
    const refreshDevicesBtn = document.querySelector('[data-action="pool-devices-refresh"]');
    let availableDevices = [];

    const loadPools = async () => {
      const pools = await api('GET', '/api/zfs/pools');
      renderTable('#zfs-pools-table', pools, '#zfs-pools-empty', (pool) => {
        const tr = document.createElement('tr');
        const health = pool.health || '';
        const healthClass = health.toUpperCase() === 'ONLINE' ? 'ok' : 'warn';
        const sizeBytes = parseSize(pool.size);
        const allocBytes = parseSize(pool.alloc);
        let pct = null;
        if (sizeBytes !== null && allocBytes !== null && sizeBytes > 0) {
          pct = Math.min(100, Math.max(0, Math.round((allocBytes / sizeBytes) * 100)));
        }
        const healthCell = document.createElement('td');
        const badge = document.createElement('span');
        badge.className = `badge ${healthClass}`;
        badge.textContent = health;
        healthCell.appendChild(badge);
        if (pct !== null) {
          const bar = document.createElement('div');
          bar.className = 'health-bar';
          const fill = document.createElement('div');
          fill.className = 'health-bar-fill';
          fill.style.width = `${pct}%`;
          fill.title = `${pct}% allocated`;
          bar.appendChild(fill);
          healthCell.appendChild(bar);
        }
        tr.innerHTML = `<td>${pool.name}</td><td>${pool.size}</td><td>${pool.alloc}</td><td>${pool.free}</td>`;
        tr.appendChild(healthCell);
        const cacheCell = document.createElement('td');
        const cacheDevices = pool.cache_devices || [];
        if (pool.cached || cacheDevices.length) {
          const badge = document.createElement('span');
          badge.className = 'badge ok';
          badge.textContent = 'Cached';
          cacheCell.appendChild(badge);
          if (cacheDevices.length) {
            const list = document.createElement('div');
            list.className = 'muted tiny';
            list.textContent = cacheDevices.join(', ');
            cacheCell.appendChild(list);
          }
        } else {
          cacheCell.textContent = 'No';
        }
        tr.appendChild(cacheCell);
        const actionCell = document.createElement('td');
        const btn = document.createElement('button');
        btn.className = 'btn';
        btn.dataset.action = 'pool-status';
        btn.dataset.name = pool.name;
        btn.textContent = 'View status';
        actionCell.appendChild(btn);
        const editBtn = document.createElement('button');
        editBtn.className = 'btn';
        editBtn.dataset.action = 'pool-edit';
        editBtn.dataset.name = pool.name;
        editBtn.textContent = 'Edit';
        actionCell.appendChild(editBtn);
        tr.appendChild(actionCell);
        return tr;
      });
    };

    const deviceValue = (name) => {
      if (!name) return '';
      if (name.includes('/')) return name;
      return `/dev/${name}`;
    };

    const renderDeviceList = () => {
      if (!deviceList) return;
      deviceList.innerHTML = '';
      availableDevices.forEach((drive) => {
        const label = document.createElement('label');
        label.className = 'device-row';
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.value = deviceValue(drive.name);
        input.addEventListener('change', updateCacheOptions);
        const text = document.createElement('span');
        const meta = drive.mediasize || '';
        const descr = drive.description || '';
        text.textContent = `${drive.name}${meta ? ` (${meta})` : ''}${descr ? ` — ${descr}` : ''}`;
        label.appendChild(input);
        label.appendChild(text);
        deviceList.appendChild(label);
      });
      if (deviceEmpty) {
        deviceEmpty.classList.toggle('hidden', availableDevices.length > 0);
      }
    };

    const selectedDataDevices = () => {
      if (!deviceList) return [];
      return Array.from(deviceList.querySelectorAll('input[type="checkbox"]:checked')).map((el) => el.value);
    };

    const updateCacheOptions = () => {
      if (!cacheSelect) return;
      const selected = new Set(selectedDataDevices());
      cacheSelect.innerHTML = '<option value="">Select cache device</option>';
      availableDevices.forEach((drive) => {
        const val = deviceValue(drive.name);
        if (selected.has(val)) return;
        const option = document.createElement('option');
        option.value = val;
        option.textContent = `${drive.name}${drive.mediasize ? ` (${drive.mediasize})` : ''}`;
        cacheSelect.appendChild(option);
      });
      cacheSelect.disabled = !cacheEnabled || !cacheEnabled.checked;
      if (cacheSelect.disabled) {
        cacheSelect.value = '';
      }
    };

    const loadDevices = async () => {
      if (!deviceList) return;
      const res = await api('GET', '/api/zfs/drives');
      const drives = Array.isArray(res) ? res : (res.drives || []);
      const filtered = drives.filter((drive) => !drive.pool);
      const seen = new Set();
      availableDevices = filtered.filter((drive) => {
        if (!drive.name) return false;
        if (seen.has(drive.name)) return false;
        seen.add(drive.name);
        return true;
      });
      renderDeviceList();
      updateCacheOptions();
    };

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action="pool-status"]');
      if (!btn) return;
      try {
        const res = await withBusy(btn, () => api('GET', `/api/zfs/pools/status?pool=${encodeURIComponent(btn.dataset.name)}`));
        drawerOut.textContent = res.output || '';
        drawer.classList.remove('hidden');
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action="pool-edit"]');
      if (!btn) return;
      const pool = btn.dataset.name;
      if (!pool) return;
      const prop = prompt(`Pool property to set for ${pool}`, 'autoexpand');
      if (!prop) return;
      const value = prompt(`Value for ${prop}`, 'on');
      if (!value) return;
      try {
        await withBusy(btn, () => api('PUT', `/api/zfs/pools/${encodeURIComponent(pool)}`, { property: prop, value }));
        showToast('Pool updated');
        loadPools();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    if (refreshDevicesBtn) {
      refreshDevicesBtn.addEventListener('click', async () => {
        clearBanner();
        try {
          await withBusy(refreshDevicesBtn, () => loadDevices());
        } catch (err) {
          showBanner(err.message, err.details);
        }
      });
    }

    if (createForm) {
      createForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        clearBanner();
        const name = (poolName.value || '').trim();
        const vdevs = selectedDataDevices();
        const cache = cacheEnabled && cacheEnabled.checked && cacheSelect && cacheSelect.value ? [cacheSelect.value] : [];
        if (!name) {
          showBanner('pool name required');
          return;
        }
        if (vdevs.length === 0) {
          showBanner('add at least one device');
          return;
        }
        if (cacheEnabled && cacheEnabled.checked && cache.length === 0) {
          showBanner('select a cache device');
          return;
        }
        const ok = await confirmModal('Create pool', `Create pool ${name} with ${vdevs.join(', ')}? This will wipe the listed devices.`);
        if (!ok) return;
        const btn = createForm.querySelector('button[type="submit"]');
        try {
          await withBusy(btn, () => api('POST', '/api/zfs/pools', { name, vdevs, cache, confirm: true }));
          showToast('Pool created');
          createForm.reset();
          if (cacheSelect) cacheSelect.disabled = true;
          loadPools();
          loadDevices();
        } catch (err) {
          showBanner(err.message, err.details);
        }
      });
    }

    if (cacheEnabled) {
      cacheEnabled.addEventListener('change', updateCacheOptions);
    }

    loadPools();
    loadDevices();
  };

  const bindZFSMounts = () => {
    const drivesTable = document.getElementById('zfs-drives-table');
    const mountsTable = document.getElementById('zfs-mounts-table');
    if (!drivesTable && !mountsTable) return;
    const cacheSummary = document.getElementById('cache-summary');
    const cacheFill = document.getElementById('cache-bar-fill');
    const cacheDevices = document.getElementById('cache-devices');
    const mountForm = document.getElementById('mount-create-form');
    const mountDataset = document.getElementById('mount-dataset');
    const mountPoint = document.getElementById('mount-point');
    const mountCanmount = document.getElementById('mount-canmount');
    const labelForm = document.getElementById('gpt-label-form');
    const labelProvider = document.getElementById('gpt-provider');
    const labelName = document.getElementById('gpt-label');
    const labelsTable = document.getElementById('gpt-labels-table');
    const labelsEmpty = document.getElementById('gpt-labels-empty');

    const loadDrives = async () => {
      if (!drivesTable) return;
      const res = await api('GET', '/api/zfs/drives');
      const drives = Array.isArray(res) ? res : (res.drives || []);
      const cache = res && res.cache ? res.cache : {};
      const errors = res && res.errors ? res.errors : {};
      if (errors.pool_devices) {
        showBanner('Pool device info unavailable', errors.pool_devices);
      }
      if (cacheSummary) {
        const usedBytes = cache.used_bytes || 0;
        const totalBytes = cache.total_bytes || 0;
        const cacheList = cache.devices || [];
        if (errors.cache) {
          cacheSummary.textContent = `Cache info unavailable: ${errors.cache}`;
        } else if (totalBytes > 0) {
          cacheSummary.textContent = `L2ARC used ${formatSize(usedBytes)} of ${formatSize(totalBytes)}`;
        } else if (cacheList.length) {
          cacheSummary.textContent = `L2ARC used ${formatSize(usedBytes)} (total unknown)`;
        } else {
          cacheSummary.textContent = 'No cache devices detected.';
        }
        if (cacheFill) {
          const pct = totalBytes > 0 ? Math.min(100, Math.max(0, Math.round((usedBytes / totalBytes) * 100))) : 0;
          cacheFill.style.width = `${pct}%`;
          cacheFill.title = totalBytes > 0 ? `${pct}% used` : '';
        }
        if (cacheDevices) {
          const list = cacheList.map((dev) => `${dev.name}${dev.size ? ` (${dev.size})` : ''}`);
          cacheDevices.textContent = list.length ? `Cache devices: ${list.join(', ')}` : '';
        }
      }
      renderTable('#zfs-drives-table', drives, '#zfs-drives-empty', (drive) => {
        const tr = document.createElement('tr');
        const avail = drive.free || '';
        const total = drive.size || drive.mediasize || '';
        const availText = avail && total ? `${avail} / ${total}` : (total ? `- / ${total}` : '-');
        tr.innerHTML = `<td>${drive.name}</td><td>${drive.pool || ''}</td><td>${drive.role || ''}</td><td>${availText}</td><td>${drive.description || ''}</td><td>${drive.ident || ''}</td>`;
        return tr;
      });
    };

    const loadMounts = async () => {
      if (!mountsTable) return;
      const mounts = await api('GET', '/api/zfs/mounts');
      renderTable('#zfs-mounts-table', mounts, '#zfs-mounts-empty', (mnt) => {
        const tr = document.createElement('tr');
        const badgeClass = mnt.mounted ? 'ok' : '';
        const badgeText = mnt.mounted ? 'Yes' : 'No';
        const action = mnt.mounted ? 'zfs-unmount' : 'zfs-mount';
        const actionLabel = mnt.mounted ? 'Unmount' : 'Mount';
        tr.innerHTML = `<td>${mnt.name}</td><td>${mnt.mountpoint}</td><td>${mnt.canmount}</td><td><span class="badge ${badgeClass}">${badgeText}</span></td>\n          <td><button class="btn" data-action="${action}" data-dataset="${mnt.name}">${actionLabel}</button></td>`;
        return tr;
      });
    };

    const loadLabels = async () => {
      if (!labelsTable) return;
      const labels = await api('GET', '/api/zfs/labels');
      renderTable('#gpt-labels-table', labels, '#gpt-labels-empty', (item) => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${item.label}</td><td>${item.provider}</td>`;
        return tr;
      });
    };

    const refreshAll = () => Promise.all([loadDrives(), loadMounts(), loadLabels()]);

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      try {
        if (btn.dataset.action === 'drives-refresh') {
          await withBusy(btn, () => loadDrives());
        }
        if (btn.dataset.action === 'mounts-refresh') {
          await withBusy(btn, () => loadMounts());
        }
        if (btn.dataset.action === 'labels-refresh') {
          await withBusy(btn, () => loadLabels());
        }
        if (btn.dataset.action === 'zfs-mount') {
          const dataset = btn.dataset.dataset;
          if (!dataset) return;
          await withBusy(btn, () => api('POST', '/api/zfs/mounts', { dataset, action: 'mount' }));
          showToast('Dataset mounted');
          loadMounts();
        }
        if (btn.dataset.action === 'zfs-unmount') {
          const dataset = btn.dataset.dataset;
          if (!dataset) return;
          const ok = await confirmModal('Unmount dataset', `Unmount ${dataset}?`);
          if (!ok) return;
          await withBusy(btn, () => api('POST', '/api/zfs/mounts', { dataset, action: 'unmount', confirm: true }));
          showToast('Dataset unmounted');
          loadMounts();
        }
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    if (mountForm) {
      mountForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        clearBanner();
        const name = (mountDataset.value || '').trim();
        const point = (mountPoint.value || '').trim();
        const canmount = (mountCanmount.value || '').trim();
        if (!name || !point) {
          showBanner('dataset and mountpoint required');
          return;
        }
        const btn = mountForm.querySelector('button[type="submit"]');
        try {
          await withBusy(btn, () => api('POST', '/api/zfs/datasets', {
            name,
            kind: 'filesystem',
            size: '',
            properties: { mountpoint: point, canmount },
          }));
          showToast('Mount created');
          mountForm.reset();
          loadMounts();
        } catch (err) {
          showBanner(err.message, err.details);
        }
      });
    }

    if (labelForm) {
      labelForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        clearBanner();
        const provider = (labelProvider.value || '').trim();
        const label = (labelName.value || '').trim();
        if (!provider || !label) {
          showBanner('provider and label required');
          return;
        }
        const ok = await confirmModal('Create GPT label', `Create gpt/${label} on ${provider}?`);
        if (!ok) return;
        const btn = labelForm.querySelector('button[type="submit"]');
        try {
          await withBusy(btn, () => api('POST', '/api/zfs/labels', { label, provider, confirm: true }));
          showToast('Label created');
          labelForm.reset();
          loadLabels();
        } catch (err) {
          showBanner(err.message, err.details);
        }
      });
    }

    refreshAll().catch((err) => showBanner(err.message, err.details));
  };

  const bindZFSSnapshots = () => {
    const treeEl = document.getElementById('snapshot-dataset-tree');
    if (!treeEl) return;
    const form = document.getElementById('snapshot-form');
    const preview = document.getElementById('snapshot-preview');
    const selectedLabel = document.getElementById('snapshot-selected');
    const filterInput = document.getElementById('snapshot-dataset-filter');
    const emptyEl = document.getElementById('snapshot-dataset-empty');

    const picker = initDatasetTree({
      container: treeEl,
      filterInput,
      emptyEl,
      showMeta: true,
      onSelect: (name) => {
        if (selectedLabel) {
          selectedLabel.textContent = name ? `Selected: ${name}` : 'Selected: -';
        }
        loadSnapshots();
        updatePreview();
      },
    });

    const loadDatasets = async () => {
      const datasets = await api('GET', '/api/zfs/datasets');
      picker.setDatasets(datasets);
      if (datasets.length) {
        if (!picker.getSelected()) {
          picker.setSelected(datasets[0].name);
        }
      } else {
        picker.setSelected('');
      }
    };

    const loadSnapshots = async () => {
      const dataset = picker.getSelected();
      if (!dataset) return;
      const snaps = await api('GET', `/api/zfs/snapshots?dataset=${encodeURIComponent(dataset)}`);
      renderTable('#zfs-snapshots-table', snaps, '#zfs-snapshots-empty', (snap) => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${snap.name}</td><td>${snap.created}</td>
          <td>
            <button class="btn" data-action="snapshot-destroy" data-name="${snap.name}">Destroy</button>
            <button class="btn" data-action="snapshot-force-destroy" data-name="${snap.name}">Force Destroy</button>
          </td>`;
        return tr;
      });
    };

    const updatePreview = () => {
      if (!preview) return;
      const dataset = picker.getSelected() || 'dataset';
      const prefix = document.getElementById('snapshot-prefix').value.trim() || 'raidraccoon';
      const name = document.getElementById('snapshot-name').value.trim();
      const recursive = document.getElementById('snapshot-recursive').checked;
      const finalName = name === '' || name === 'auto' ? `${prefix}-YYYYMMDD-HHMMSS` : name;
      preview.textContent = `Preview: ${dataset}@${finalName}${recursive ? ' (recursive)' : ''}`;
    };

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      if (btn.dataset.action === 'datasets-refresh') {
        loadDatasets();
      }
      if (btn.dataset.action === 'refresh-snapshots') {
        loadSnapshots();
      }
      if (btn.dataset.action === 'snapshot-destroy') {
        const name = btn.dataset.name;
        const ok = await confirmModal('Destroy snapshot', `Destroy ${name}?`);
        if (!ok) return;
        await withBusy(btn, () => api('DELETE', '/api/zfs/snapshots', { name, confirm: true }));
        showToast('Snapshot destroyed');
        loadSnapshots();
      }
      if (btn.dataset.action === 'snapshot-force-destroy') {
        const name = btn.dataset.name;
        const ok = await confirmModal('Force destroy snapshot', `Force destroy ${name} recursively (deferred if busy)?`);
        if (!ok) return;
        await withBusy(btn, () => api('DELETE', '/api/zfs/snapshots', { name, confirm: true, force: true }));
        showToast('Snapshot force-destroy requested');
        loadSnapshots();
      }
    });

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      clearBanner();
      const dataset = picker.getSelected();
      if (!dataset) {
        showBanner('select a dataset first');
        return;
      }
      const prefix = document.getElementById('snapshot-prefix').value.trim();
      const name = document.getElementById('snapshot-name').value.trim();
      const recursive = document.getElementById('snapshot-recursive').checked;
      try {
        const btn = form.querySelector('button[type="submit"]');
        await withBusy(btn, () => api('POST', '/api/zfs/snapshots', { dataset, prefix, name, recursive }));
        showToast('Snapshot created');
        loadSnapshots();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    document.getElementById('snapshot-prefix').addEventListener('input', updatePreview);
    document.getElementById('snapshot-name').addEventListener('input', updatePreview);
    document.getElementById('snapshot-recursive').addEventListener('change', updatePreview);
    loadDatasets()
      .then(() => {
        loadSnapshots();
        updatePreview();
      })
      .catch((err) => showBanner(err.message, err.details));
  };

  const bindZFSDatasets = () => {
    const treeEl = document.getElementById('datasets-tree');
    if (!treeEl) return;
    const filterInput = document.getElementById('datasets-filter');
    const emptyEl = document.getElementById('datasets-empty');
    const form = document.getElementById('dataset-form');
    const formTitle = document.getElementById('dataset-form-title');
    const formNote = document.getElementById('dataset-form-note');
    const modeInput = document.getElementById('dataset-mode');
    const currentInput = document.getElementById('dataset-current');
    const nameInput = document.getElementById('dataset-name');
    const newNameInput = document.getElementById('dataset-new-name');
    const kindInput = document.getElementById('dataset-kind');
    const sizeInput = document.getElementById('dataset-size');
    const sizeMaxBtn = document.getElementById('dataset-size-max');
    const sizeHint = document.getElementById('dataset-size-hint');
    const mountpointInput = document.getElementById('dataset-mountpoint');
    const canmountInput = document.getElementById('dataset-canmount');
    const compressionInput = document.getElementById('dataset-compression');
    const atimeInput = document.getElementById('dataset-atime');
    const quotaInput = document.getElementById('dataset-quota');
    const quotaMaxBtn = document.getElementById('dataset-quota-max');
    const details = document.getElementById('dataset-details');
    const resetBtn = document.getElementById('dataset-reset');

    let selectedData = null;
    let poolSizes = {};

    const picker = initDatasetTree({
      container: treeEl,
      filterInput,
      emptyEl,
      showMeta: true,
      onSelect: (_, data) => {
        selectedData = data || null;
        if (!details) return;
        details.innerHTML = '';
        if (!data) {
          details.textContent = 'Select a dataset to see details.';
          updateSizeControls();
          return;
        }
        const usedBytes = parseSize(data.used);
        const availBytes = parseSize(data.available);
        const maxBytes = usedBytes !== null && availBytes !== null ? usedBytes + availBytes : null;
        const rows = [
          ['Name', data.name],
          ['Type', data.type || '-'],
          ['Used', data.used || '-'],
          ['Available', data.available || '-'],
          ['Max (approx)', maxBytes === null ? '-' : formatSize(maxBytes)],
          ['Referenced', data.referenced || '-'],
          ['Mountpoint', data.mountpoint || '-'],
        ];
        rows.forEach(([label, value]) => {
          const line = document.createElement('div');
          line.textContent = `${label}: ${value}`;
          details.appendChild(line);
        });
        updateSizeControls();
      },
      onEdit: (name, data) => {
        selectedData = data || null;
        modeInput.value = 'edit';
        currentInput.value = name;
        nameInput.value = name;
        nameInput.readOnly = true;
        if (newNameInput) {
          newNameInput.value = '';
        }
        kindInput.value = data && data.type ? data.type : 'filesystem';
        kindInput.disabled = true;
        updateKindUI();
        sizeInput.value = '';
        mountpointInput.value = '';
        canmountInput.value = '';
        compressionInput.value = '';
        atimeInput.value = '';
        quotaInput.value = '';
        if (formTitle) formTitle.textContent = `Edit Dataset: ${name}`;
        if (formNote) formNote.textContent = 'Leave fields blank to keep current values. Use New Name to rename.';
        document.querySelectorAll('.dataset-edit-only').forEach((el) => el.classList.remove('hidden'));
        updateSizeControls();
      },
      onDestroy: async (name) => {
        const ok = await confirmModal('Destroy dataset', `Destroy ${name}?`);
        if (!ok) return;
        const recursive = await confirmModal('Recursive destroy', 'Also destroy child datasets?');
        try {
          await api('DELETE', `/api/zfs/datasets/${encodeURIComponent(name)}`, { confirm: true, recursive });
          showToast('Dataset destroyed');
          await loadDatasets();
        } catch (err) {
          showBanner(err.message, err.details);
        }
      },
    });

    const maxSizeBytes = (data) => {
      if (!data) return null;
      const usedBytes = parseSize(data.used);
      const availBytes = parseSize(data.available);
      if (usedBytes === null || availBytes === null) return null;
      return usedBytes + availBytes;
    };

    const maxQuotaBytes = (data) => {
      if (!data || !data.name) return null;
      const pool = data.name.split('/')[0];
      if (pool && poolSizes[pool] !== undefined) {
        return poolSizes[pool];
      }
      return maxSizeBytes(data);
    };

    const updateSizeControls = () => {
      const isVolume = kindInput.value === 'volume';
      const maxBytes = maxSizeBytes(selectedData);
      sizeInput.disabled = !isVolume;
      if (sizeMaxBtn) {
        sizeMaxBtn.disabled = !isVolume || maxBytes === null;
      }
      if (quotaMaxBtn) {
        quotaMaxBtn.disabled = maxQuotaBytes(selectedData) === null;
      }
      if (sizeHint) {
        if (!isVolume) {
          sizeHint.textContent = 'Volume size applies to zvols only (use quota for filesystems).';
        } else if (maxBytes === null) {
          sizeHint.textContent = 'Max size unavailable.';
        } else {
          sizeHint.textContent = `Max approx: ${formatSize(maxBytes)}.`;
        }
      }
    };

    const updateKindUI = () => {
      const isVolume = kindInput.value === 'volume';
      sizeInput.placeholder = isVolume ? '10G' : '';
      updateSizeControls();
    };

    const resetForm = () => {
      form.reset();
      modeInput.value = 'create';
      currentInput.value = '';
      nameInput.readOnly = false;
      kindInput.disabled = false;
      updateKindUI();
      document.querySelectorAll('.dataset-edit-only').forEach((el) => el.classList.add('hidden'));
      if (formTitle) formTitle.textContent = 'Create Dataset';
      if (formNote) formNote.textContent = 'Leave fields blank to keep defaults.';
      selectedData = null;
    };

    const collectProps = (includeVolsize) => {
      const props = {};
      if (mountpointInput.value.trim()) props.mountpoint = mountpointInput.value.trim();
      if (canmountInput.value.trim()) props.canmount = canmountInput.value.trim();
      if (compressionInput.value.trim()) props.compression = compressionInput.value.trim();
      if (atimeInput.value.trim()) props.atime = atimeInput.value.trim();
      if (quotaInput.value.trim()) props.quota = quotaInput.value.trim();
      if (includeVolsize && sizeInput.value.trim()) props.volsize = sizeInput.value.trim();
      return props;
    };

    const loadPools = async () => {
      const pools = await api('GET', '/api/zfs/pools');
      const next = {};
      pools.forEach((pool) => {
        const bytes = parseSize(pool.size);
        if (bytes !== null) {
          next[pool.name] = bytes;
        }
      });
      poolSizes = next;
    };

    const loadDatasets = async () => {
      const datasets = await api('GET', '/api/zfs/datasets');
      picker.setDatasets(datasets);
      if (datasets.length) {
        if (!picker.getSelected()) {
          picker.setSelected(datasets[0].name);
        }
      } else {
        picker.setSelected('');
      }
    };

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      clearBanner();
      const name = nameInput.value.trim();
      const newName = newNameInput ? newNameInput.value.trim() : '';
      const kind = kindInput.value;
      const size = sizeInput.value.trim();
      const props = collectProps(modeInput.value === 'edit');
      try {
        const btn = document.getElementById('dataset-save');
        if (modeInput.value === 'edit') {
          if (!currentInput.value) {
            showBanner('select a dataset to edit');
            return;
          }
          if (!newName && Object.keys(props).length === 0) {
            showBanner('provide a rename or property update');
            return;
          }
          await withBusy(btn, () => api('PUT', `/api/zfs/datasets/${encodeURIComponent(currentInput.value)}`, { new_name: newName, properties: props }));
          showToast(newName ? 'Dataset renamed' : 'Dataset updated');
        } else {
          if (!name) {
            showBanner('dataset name required');
            return;
          }
          if (kind === 'volume' && !size) {
            showBanner('volume size required');
            return;
          }
          await withBusy(btn, () => api('POST', '/api/zfs/datasets', { name, kind, size, properties: props }));
          showToast('Dataset created');
        }
        resetForm();
        loadDatasets();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    document.addEventListener('click', (e) => {
      const btn = e.target.closest('[data-action="datasets-refresh"]');
      if (!btn) return;
      Promise.all([loadPools(), loadDatasets()])
        .then(() => updateSizeControls())
        .catch((err) => showBanner(err.message, err.details));
    });

    if (kindInput) {
      kindInput.addEventListener('change', updateKindUI);
    }
    if (sizeMaxBtn) {
      sizeMaxBtn.addEventListener('click', () => {
        const maxBytes = maxSizeBytes(selectedData);
        if (maxBytes === null) return;
        sizeInput.value = formatSize(maxBytes);
      });
    }
    if (quotaMaxBtn) {
      quotaMaxBtn.addEventListener('click', () => {
        const maxBytes = maxQuotaBytes(selectedData);
        if (maxBytes === null) return;
        quotaInput.value = formatSize(maxBytes);
      });
    }
    if (resetBtn) {
      resetBtn.addEventListener('click', resetForm);
    }

    resetForm();
    Promise.all([loadPools(), loadDatasets()])
      .then(() => updateSizeControls())
      .catch((err) => showBanner(err.message, err.details));
  };

  const bindSchedules = () => {
    const table = document.getElementById('schedules-table');
    if (!table) return;
    const form = document.getElementById('schedule-form');
    const cronUpdated = document.getElementById('cron-updated');
    const datasetTree = document.getElementById('schedule-dataset-tree');
    const datasetFilter = document.getElementById('schedule-dataset-filter');
    const datasetEmpty = document.getElementById('schedule-dataset-empty');
    const selectedLabel = document.getElementById('schedule-selected');
    const schedId = document.getElementById('sched-id');
    const schedRetention = document.getElementById('sched-retention');
    const schedPrefix = document.getElementById('sched-prefix');
    const schedEnabled = document.getElementById('sched-enabled');
    const schedMode = document.getElementById('sched-mode');
    const schedFrequency = document.getElementById('sched-frequency');
    const schedInterval = document.getElementById('sched-interval');
    const schedTime = document.getElementById('sched-time');
    const schedDay = document.getElementById('sched-day');
    const schedMinute = document.getElementById('sched-minute');
    const schedHour = document.getElementById('sched-hour');
    const schedDom = document.getElementById('sched-dom');
    const schedMonth = document.getElementById('sched-month');
    const schedDow = document.getElementById('sched-dow');
    const schedPreview = document.getElementById('sched-preview');
    const schedReset = document.getElementById('sched-reset');

    const state = {
      items: [],
    };

    const picker = initDatasetTree({
      container: datasetTree,
      filterInput: datasetFilter,
      emptyEl: datasetEmpty,
      showMeta: true,
      onSelect: (name) => {
        if (selectedLabel) {
          selectedLabel.textContent = name ? `Selected: ${name}` : 'Selected: -';
        }
      },
    });

    const setMode = (mode) => {
      const quick = document.querySelectorAll('.sched-quick');
      const adv = document.querySelectorAll('.sched-advanced');
      quick.forEach((el) => el.classList.toggle('hidden', mode !== 'quick'));
      adv.forEach((el) => el.classList.toggle('hidden', mode !== 'advanced'));
      updatePreview();
    };

    const summarizeCron = (spec, raw) => {
      if (!spec) return raw || '';
      const minute = spec.minute || '*';
      const hour = spec.hour || '*';
      const dom = spec.dom || '*';
      const month = spec.month || '*';
      const dow = spec.dow || '*';
      if (minute === '0' && hour.startsWith('*/') && dom === '*' && month === '*' && dow === '*') {
        return `Every ${hour.slice(2)} hours`;
      }
      if (minute.startsWith('*/') && hour === '*' && dom === '*' && month === '*' && dow === '*') {
        return `Every ${minute.slice(2)} minutes`;
      }
      if (dom === '*' && month === '*' && dow === '*') {
        return `Daily at ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
      }
      if (dow !== '*' && dom === '*' && month === '*') {
        return `Weekly (dow ${dow}) at ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
      }
      if (dom !== '*' && month === '*') {
        return `Monthly (day ${dom}) at ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
      }
      return raw || `${minute} ${hour} ${dom} ${month} ${dow}`;
    };

    const loadSchedules = async () => {
      const data = await api('GET', '/api/zfs/schedules');
      cronUpdated.textContent = data.updated ? `cron updated ${data.updated}` : '';
      state.items = data.items || [];
      renderTable('#schedules-table', state.items, '#schedules-empty', (item) => {
        const summary = summarizeCron(item.schedule, item.cron);
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${item.id}</td><td>${item.dataset}</td><td>${summary}</td><td>${item.cron}</td><td>${item.retention}</td><td>${item.prefix}</td><td>${item.enabled}</td>
          <td>
            <button class="btn" data-action="schedule-toggle" data-id="${item.id}">${item.enabled ? 'Disable' : 'Enable'}</button>
            <button class="btn" data-action="schedule-edit" data-id="${item.id}">Edit</button>
            <button class="btn" data-action="schedule-delete" data-id="${item.id}">Delete</button>
          </td>`;
        return tr;
      });
    };

    const buildQuickSchedule = () => {
      const frequency = schedFrequency.value;
      const time = schedTime.value || '03:00';
      const [hourRaw, minuteRaw] = time.split(':');
      const hour = hourRaw || '0';
      const minute = minuteRaw || '0';
      const interval = Math.max(1, parseInt(schedInterval.value, 10) || 1);
      const day = schedDay.value;
      let schedule = { minute, hour, dom: '*', month: '*', dow: '*' };
      if (frequency === 'hourly') {
        schedule = { minute: minute || '0', hour: '*', dom: '*', month: '*', dow: '*' };
      }
      if (frequency === 'weekly') {
        schedule.dow = day || '0';
      }
      if (frequency === 'monthly') {
        schedule.dom = day || '1';
      }
      if (frequency === 'interval-hours') {
        schedule = { minute: minute || '0', hour: `*/${interval}`, dom: '*', month: '*', dow: '*' };
      }
      if (frequency === 'interval-minutes') {
        schedule = { minute: `*/${interval}`, hour: '*', dom: '*', month: '*', dow: '*' };
      }
      return schedule;
    };

    const buildAdvancedSchedule = () => {
      const minute = schedMinute.value.trim();
      const hour = schedHour.value.trim();
      const dom = schedDom.value.trim();
      const month = schedMonth.value.trim();
      const dow = schedDow.value.trim();
      if (!minute || !hour || !dom || !month || !dow) {
        return null;
      }
      return { minute, hour, dom, month, dow };
    };

    const updatePreview = () => {
      if (!schedPreview) return;
      const mode = schedMode.value;
      const schedule = mode === 'advanced' ? buildAdvancedSchedule() : buildQuickSchedule();
      if (!schedule) {
        schedPreview.textContent = 'Cron: invalid (fill all fields)';
        return;
      }
      schedPreview.textContent = `Cron: ${schedule.minute} ${schedule.hour} ${schedule.dom} ${schedule.month} ${schedule.dow}`;
    };

    const resetForm = () => {
      form.reset();
      schedId.value = '';
      setMode('quick');
      if (schedPreview) schedPreview.textContent = 'Cron: -';
    };

    const enterEdit = (item) => {
      schedId.value = item.id;
      schedRetention.value = item.retention;
      schedPrefix.value = item.prefix || '';
      schedEnabled.value = item.enabled ? 'true' : 'false';
      schedMode.value = 'advanced';
      schedMinute.value = item.schedule.minute;
      schedHour.value = item.schedule.hour;
      schedDom.value = item.schedule.dom;
      schedMonth.value = item.schedule.month;
      schedDow.value = item.schedule.dow;
      setMode('advanced');
      picker.setSelected(item.dataset);
    };

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      clearBanner();
      const dataset = picker.getSelected();
      if (!dataset) {
        showBanner('select a dataset first');
        return;
      }
      const retention = parseInt(schedRetention.value, 10);
      const prefix = schedPrefix.value.trim();
      const enabled = schedEnabled.value === 'true';
      const mode = schedMode.value;
      const schedule = mode === 'advanced' ? buildAdvancedSchedule() : buildQuickSchedule();
      if (!schedule) {
        showBanner('invalid cron fields');
        return;
      }
      try {
        const btn = document.getElementById('sched-save');
        if (schedId.value) {
          await withBusy(btn, () => api('PUT', `/api/zfs/schedules/${schedId.value}`, { dataset, retention, prefix, enabled, schedule }));
          showToast('Schedule updated');
        } else {
          await withBusy(btn, () => api('POST', '/api/zfs/schedules', { dataset, retention, prefix, enabled, schedule }));
          showToast('Schedule saved');
        }
        resetForm();
        loadSchedules();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action^="schedule-"]');
      if (!btn) return;
      const id = btn.dataset.id;
      try {
        if (btn.dataset.action === 'schedule-delete') {
          const ok = await confirmModal('Delete schedule', `Delete schedule ${id}?`);
          if (!ok) return;
          await withBusy(btn, () => api('DELETE', `/api/zfs/schedules/${id}`, { confirm: true }));
          showToast('Schedule deleted');
        }
        if (btn.dataset.action === 'schedule-toggle') {
          await withBusy(btn, () => api('PUT', `/api/zfs/schedules/${id}`, { toggle: true }));
          showToast('Schedule updated');
        }
        if (btn.dataset.action === 'schedule-edit') {
          const item = state.items.find((entry) => entry.id === id);
          if (item) {
            enterEdit(item);
          }
        }
        loadSchedules();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    document.addEventListener('click', (e) => {
      const btn = e.target.closest('[data-action="datasets-refresh"]');
      if (btn) {
        api('GET', '/api/zfs/datasets')
          .then((datasets) => {
            picker.setDatasets(datasets);
            if (datasets.length) {
              if (!picker.getSelected()) {
                picker.setSelected(datasets[0].name);
              }
            } else {
              picker.setSelected('');
            }
          })
          .catch((err) => showBanner(err.message, err.details));
      }
    });

    if (schedReset) {
      schedReset.addEventListener('click', resetForm);
    }
    [schedMode, schedFrequency, schedInterval, schedTime, schedDay, schedMinute, schedHour, schedDom, schedMonth, schedDow].forEach((el) => {
      if (!el) return;
      el.addEventListener('input', updatePreview);
      el.addEventListener('change', updatePreview);
    });
    if (schedMode) {
      schedMode.addEventListener('change', () => setMode(schedMode.value));
    }

    api('GET', '/api/zfs/datasets')
      .then((datasets) => {
        picker.setDatasets(datasets);
        if (datasets.length) {
          picker.setSelected(datasets[0].name);
        } else {
          picker.setSelected('');
        }
      })
      .catch((err) => showBanner(err.message, err.details));

    setMode(schedMode.value);
    loadSchedules();
  };

  const bindReplicationJobs = () => {
    const replTable = document.getElementById('repl-table');
    const rsyncTable = document.getElementById('rsync-table');
    if (!replTable && !rsyncTable) return;

    const summarizeCron = (spec, raw) => {
      if (!spec) return raw || '';
      const minute = spec.minute || '*';
      const hour = spec.hour || '*';
      const dom = spec.dom || '*';
      const month = spec.month || '*';
      const dow = spec.dow || '*';
      if (minute === '0' && hour.startsWith('*/') && dom === '*' && month === '*' && dow === '*') {
        return `Every ${hour.slice(2)} hours`;
      }
      if (minute.startsWith('*/') && hour === '*' && dom === '*' && month === '*' && dow === '*') {
        return `Every ${minute.slice(2)} minutes`;
      }
      if (dom === '*' && month === '*' && dow === '*') {
        return `Daily at ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
      }
      if (dow !== '*' && dom === '*' && month === '*') {
        return `Weekly (dow ${dow}) at ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
      }
      if (dom !== '*' && month === '*') {
        return `Monthly (day ${dom}) at ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
      }
      return raw || `${minute} ${hour} ${dom} ${month} ${dow}`;
    };

    const replForm = document.getElementById('repl-form');
    const replUpdated = document.getElementById('repl-updated');
    const replId = document.getElementById('repl-id');
    const replSourceSelect = document.getElementById('repl-source-select');
    const replTargetPool = document.getElementById('repl-target-pool');
    const replTargetSuffix = document.getElementById('repl-target-suffix');
    const replPrefix = document.getElementById('repl-prefix');
    const replRetention = document.getElementById('repl-retention');
    const replRecursive = document.getElementById('repl-recursive');
    const replForce = document.getElementById('repl-force');
    const replEnabled = document.getElementById('repl-enabled');
    const replMode = document.getElementById('repl-mode');
    const replFrequency = document.getElementById('repl-frequency');
    const replInterval = document.getElementById('repl-interval');
    const replTime = document.getElementById('repl-time');
    const replDay = document.getElementById('repl-day');
    const replMinute = document.getElementById('repl-minute');
    const replHour = document.getElementById('repl-hour');
    const replDom = document.getElementById('repl-dom');
    const replMonth = document.getElementById('repl-month');
    const replDow = document.getElementById('repl-dow');
    const replPreview = document.getElementById('repl-preview');
    const replTargetPreview = document.getElementById('repl-target-preview');
    const replReset = document.getElementById('repl-reset');
    const replDatasetTree = document.getElementById('repl-dataset-tree');
    const replDatasetFilter = document.getElementById('repl-dataset-filter');
    const replDatasetEmpty = document.getElementById('repl-dataset-empty');
    const replSelected = document.getElementById('repl-selected');

    const replState = { items: [] };
    let replPicker = null;
    let replSourceList = [];

    if (replDatasetTree) {
      replPicker = initDatasetTree({
        container: replDatasetTree,
        filterInput: replDatasetFilter,
        emptyEl: replDatasetEmpty,
        showMeta: true,
        onSelect: (name) => {
          if (replSelected) {
            replSelected.textContent = name ? `Selected: ${name}` : 'Selected: -';
          }
          if (replSourceSelect && name && replSourceSelect.value !== name) {
            replSourceSelect.value = name;
          }
          if (replTargetSuffix && !replTargetSuffix.value) {
            const parts = (name || '').split('/');
            replTargetSuffix.value = parts.length > 1 ? parts.slice(1).join('/') : '';
          }
          updateReplTargetPreview();
        },
      });
    }

    const setReplSource = (name) => {
      if (replPicker && name) {
        replPicker.setSelected(name);
      }
      if (replSourceSelect) {
        replSourceSelect.value = name || '';
      }
      if (replSelected) {
        replSelected.textContent = name ? `Selected: ${name}` : 'Selected: -';
      }
      if (replTargetSuffix && !replTargetSuffix.value && name) {
        const parts = name.split('/');
        replTargetSuffix.value = parts.length > 1 ? parts.slice(1).join('/') : '';
      }
      updateReplTargetPreview();
    };

    const setReplMode = (mode) => {
      document.querySelectorAll('.repl-quick').forEach((el) => el.classList.toggle('hidden', mode !== 'quick'));
      document.querySelectorAll('.repl-advanced').forEach((el) => el.classList.toggle('hidden', mode !== 'advanced'));
      updateReplPreview();
    };

    const buildReplQuickSchedule = () => {
      const frequency = replFrequency.value;
      const time = replTime.value || '03:00';
      const [hourRaw, minuteRaw] = time.split(':');
      const hour = hourRaw || '0';
      const minute = minuteRaw || '0';
      const interval = Math.max(1, parseInt(replInterval.value, 10) || 1);
      const day = replDay.value;
      let schedule = { minute, hour, dom: '*', month: '*', dow: '*' };
      if (frequency === 'hourly') {
        schedule = { minute: minute || '0', hour: '*', dom: '*', month: '*', dow: '*' };
      }
      if (frequency === 'weekly') {
        schedule.dow = day || '0';
      }
      if (frequency === 'monthly') {
        schedule.dom = day || '1';
      }
      if (frequency === 'interval-hours') {
        schedule = { minute: minute || '0', hour: `*/${interval}`, dom: '*', month: '*', dow: '*' };
      }
      if (frequency === 'interval-minutes') {
        schedule = { minute: `*/${interval}`, hour: '*', dom: '*', month: '*', dow: '*' };
      }
      return schedule;
    };

    const buildReplAdvancedSchedule = () => {
      const minute = replMinute.value.trim();
      const hour = replHour.value.trim();
      const dom = replDom.value.trim();
      const month = replMonth.value.trim();
      const dow = replDow.value.trim();
      if (!minute || !hour || !dom || !month || !dow) {
        return null;
      }
      return { minute, hour, dom, month, dow };
    };

    const updateReplPreview = () => {
      if (!replPreview) return;
      const mode = replMode.value;
      const schedule = mode === 'advanced' ? buildReplAdvancedSchedule() : buildReplQuickSchedule();
      if (!schedule) {
        replPreview.textContent = 'Cron: invalid (fill all fields)';
        return;
      }
      replPreview.textContent = `Cron: ${schedule.minute} ${schedule.hour} ${schedule.dom} ${schedule.month} ${schedule.dow}`;
    };

    const updateReplTargetPreview = () => {
      if (!replTargetPreview || !replTargetPool) return;
      const pool = replTargetPool.value;
      const suffix = (replTargetSuffix && replTargetSuffix.value || '').trim();
      const target = pool ? (suffix ? `${pool}/${suffix}` : pool) : '';
      replTargetPreview.textContent = target ? `Target dataset: ${target}` : 'Target dataset: -';
    };

    const loadReplPools = async () => {
      if (!replTargetPool) return;
      const pools = await api('GET', '/api/zfs/pools');
      const current = replTargetPool.value;
      replTargetPool.innerHTML = '';
      pools.forEach((pool) => {
        const opt = document.createElement('option');
        opt.value = pool.name;
        opt.textContent = pool.name;
        replTargetPool.appendChild(opt);
      });
      if (current) {
        replTargetPool.value = current;
      }
      updateReplTargetPreview();
    };

    const loadReplDatasets = async () => {
      const datasets = await api('GET', '/api/zfs/datasets');
      replSourceList = datasets.map((ds) => ds.name);
      if (replPicker) {
        replPicker.setDatasets(datasets);
      }
      if (replSourceSelect) {
        const current = replSourceSelect.value;
        replSourceSelect.innerHTML = '';
        replSourceList.forEach((name) => {
          const opt = document.createElement('option');
          opt.value = name;
          opt.textContent = name;
          replSourceSelect.appendChild(opt);
        });
        if (current && replSourceList.includes(current)) {
          replSourceSelect.value = current;
        }
      }
      const selected = replPicker ? replPicker.getSelected() : '';
      if (datasets.length && !selected && replSourceList.length) {
        setReplSource(replSourceList[0]);
      }
    };

    const loadReplication = async () => {
      if (!replTable) return;
      const data = await api('GET', '/api/zfs/replication');
      if (replUpdated) {
        replUpdated.textContent = data.updated ? `cron updated ${data.updated}` : '';
      }
      replState.items = data.items || [];
      renderTable('#repl-table', replState.items, '#repl-empty', (item) => {
        const summary = summarizeCron(item.schedule, item.cron);
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${item.id}</td><td>${item.source}</td><td>${item.target}</td><td>${summary}</td><td>${item.cron}</td><td>${item.retention}</td><td>${item.prefix || ''}</td><td>${item.enabled}</td>
          <td>
            <button class="btn" data-action="repl-toggle" data-id="${item.id}">${item.enabled ? 'Disable' : 'Enable'}</button>
            <button class="btn" data-action="repl-edit" data-id="${item.id}">Edit</button>
            <button class="btn" data-action="repl-delete" data-id="${item.id}">Delete</button>
          </td>`;
        return tr;
      });
    };

    const replTargetValue = () => {
      if (!replTargetPool) return '';
      const pool = replTargetPool.value;
      const suffix = (replTargetSuffix && replTargetSuffix.value || '').trim();
      if (!pool) return '';
      return suffix ? `${pool}/${suffix}` : pool;
    };

    const resetReplForm = () => {
      if (!replForm) return;
      replForm.reset();
      replId.value = '';
      if (replRecursive) replRecursive.checked = false;
      if (replForce) replForce.checked = false;
      setReplMode('quick');
      if (replSourceList.length) {
        setReplSource(replSourceList[0]);
      }
      updateReplTargetPreview();
    };

    const enterReplEdit = (item) => {
      replId.value = item.id;
      replRetention.value = item.retention || 0;
      replPrefix.value = item.prefix || '';
      replEnabled.value = item.enabled ? 'true' : 'false';
      if (replRecursive) replRecursive.checked = !!item.recursive;
      if (replForce) replForce.checked = !!item.force;
      replMode.value = 'advanced';
      replMinute.value = item.schedule.minute;
      replHour.value = item.schedule.hour;
      replDom.value = item.schedule.dom;
      replMonth.value = item.schedule.month;
      replDow.value = item.schedule.dow;
      setReplMode('advanced');
      setReplSource(item.source);
      if (replTargetPool && item.target) {
        const parts = item.target.split('/');
        replTargetPool.value = parts[0] || '';
        if (replTargetSuffix) {
          replTargetSuffix.value = parts.slice(1).join('/');
        }
      }
      updateReplTargetPreview();
    };

    if (replForm) {
      replForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        clearBanner();
        const source = replSourceSelect ? replSourceSelect.value : (replPicker ? replPicker.getSelected() : '');
        if (!source) {
          showBanner('select a source dataset first');
          return;
        }
        const target = replTargetValue();
        if (!target) {
          showBanner('select a target pool');
          return;
        }
        const retention = parseInt(replRetention.value, 10) || 0;
        const prefix = replPrefix.value.trim();
        const enabled = replEnabled.value === 'true';
        const recursive = !!(replRecursive && replRecursive.checked);
        const force = !!(replForce && replForce.checked);
        const mode = replMode.value;
        const schedule = mode === 'advanced' ? buildReplAdvancedSchedule() : buildReplQuickSchedule();
        if (!schedule) {
          showBanner('invalid cron fields');
          return;
        }
        try {
          const btn = document.getElementById('repl-save');
          if (replId.value) {
            await withBusy(btn, () => api('PUT', `/api/zfs/replication/${replId.value}`, {
              source,
              target,
              retention,
              prefix,
              enabled,
              recursive,
              force,
              schedule,
            }));
            showToast('Replication updated');
          } else {
            await withBusy(btn, () => api('POST', '/api/zfs/replication', {
              source,
              target,
              retention,
              prefix,
              enabled,
              recursive,
              force,
              schedule,
            }));
            showToast('Replication saved');
          }
          resetReplForm();
          loadReplication();
        } catch (err) {
          showBanner(err.message, err.details);
        }
      });
    }

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      const id = btn.dataset.id;
      try {
        if (btn.dataset.action === 'repl-delete') {
          const ok = await confirmModal('Delete replication job', `Delete job ${id}?`);
          if (!ok) return;
          await withBusy(btn, () => api('DELETE', `/api/zfs/replication/${id}`, { confirm: true }));
          showToast('Replication deleted');
        }
        if (btn.dataset.action === 'repl-toggle') {
          await withBusy(btn, () => api('PUT', `/api/zfs/replication/${id}`, { toggle: true }));
          showToast('Replication updated');
        }
        if (btn.dataset.action === 'repl-edit') {
          const item = replState.items.find((entry) => entry.id === id);
          if (item) {
            enterReplEdit(item);
          }
        }
        if (btn.dataset.action === 'repl-datasets-refresh') {
          await loadReplDatasets();
        }
        loadReplication();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    if (replReset) {
      replReset.addEventListener('click', resetReplForm);
    }
    [replMode, replFrequency, replInterval, replTime, replDay, replMinute, replHour, replDom, replMonth, replDow].forEach((el) => {
      if (!el) return;
      el.addEventListener('input', updateReplPreview);
      el.addEventListener('change', updateReplPreview);
    });
    if (replMode) {
      replMode.addEventListener('change', () => setReplMode(replMode.value));
    }
    if (replTargetPool) {
      replTargetPool.addEventListener('change', updateReplTargetPreview);
    }
    if (replTargetSuffix) {
      replTargetSuffix.addEventListener('input', updateReplTargetPreview);
    }
    if (replSourceSelect) {
      replSourceSelect.addEventListener('change', () => {
        const value = replSourceSelect.value;
        setReplSource(value);
      });
    }

    const rsyncForm = document.getElementById('rsync-form');
    const rsyncUpdated = document.getElementById('rsync-updated');
    const rsyncId = document.getElementById('rsync-id');
    const rsyncSource = document.getElementById('rsync-source');
    const rsyncTarget = document.getElementById('rsync-target');
    const rsyncMode = document.getElementById('rsync-mode');
    const rsyncFlags = document.getElementById('rsync-flags');
    const rsyncFlagsWrap = document.getElementById('rsync-flags-wrap');
    const rsyncEnabled = document.getElementById('rsync-enabled');
    const rsyncModeCron = document.getElementById('rsync-mode-cron');
    const rsyncFrequency = document.getElementById('rsync-frequency');
    const rsyncInterval = document.getElementById('rsync-interval');
    const rsyncTime = document.getElementById('rsync-time');
    const rsyncDay = document.getElementById('rsync-day');
    const rsyncMinute = document.getElementById('rsync-minute');
    const rsyncHour = document.getElementById('rsync-hour');
    const rsyncDom = document.getElementById('rsync-dom');
    const rsyncMonth = document.getElementById('rsync-month');
    const rsyncDow = document.getElementById('rsync-dow');
    const rsyncPreview = document.getElementById('rsync-preview');
    const rsyncReset = document.getElementById('rsync-reset');

    const rsyncState = { items: [] };

    const setRsyncScheduleMode = (mode) => {
      document.querySelectorAll('.rsync-quick').forEach((el) => el.classList.toggle('hidden', mode !== 'quick'));
      document.querySelectorAll('.rsync-advanced').forEach((el) => el.classList.toggle('hidden', mode !== 'advanced'));
      updateRsyncPreview();
    };

    const setRsyncFlagsMode = (mode) => {
      if (!rsyncFlagsWrap) return;
      rsyncFlagsWrap.classList.toggle('hidden', mode !== 'custom');
    };

    const buildRsyncQuickSchedule = () => {
      const frequency = rsyncFrequency.value;
      const time = rsyncTime.value || '03:00';
      const [hourRaw, minuteRaw] = time.split(':');
      const hour = hourRaw || '0';
      const minute = minuteRaw || '0';
      const interval = Math.max(1, parseInt(rsyncInterval.value, 10) || 1);
      const day = rsyncDay.value;
      let schedule = { minute, hour, dom: '*', month: '*', dow: '*' };
      if (frequency === 'hourly') {
        schedule = { minute: minute || '0', hour: '*', dom: '*', month: '*', dow: '*' };
      }
      if (frequency === 'weekly') {
        schedule.dow = day || '0';
      }
      if (frequency === 'monthly') {
        schedule.dom = day || '1';
      }
      if (frequency === 'interval-hours') {
        schedule = { minute: minute || '0', hour: `*/${interval}`, dom: '*', month: '*', dow: '*' };
      }
      if (frequency === 'interval-minutes') {
        schedule = { minute: `*/${interval}`, hour: '*', dom: '*', month: '*', dow: '*' };
      }
      return schedule;
    };

    const buildRsyncAdvancedSchedule = () => {
      const minute = rsyncMinute.value.trim();
      const hour = rsyncHour.value.trim();
      const dom = rsyncDom.value.trim();
      const month = rsyncMonth.value.trim();
      const dow = rsyncDow.value.trim();
      if (!minute || !hour || !dom || !month || !dow) {
        return null;
      }
      return { minute, hour, dom, month, dow };
    };

    const updateRsyncPreview = () => {
      if (!rsyncPreview) return;
      const mode = rsyncModeCron.value;
      const schedule = mode === 'advanced' ? buildRsyncAdvancedSchedule() : buildRsyncQuickSchedule();
      if (!schedule) {
        rsyncPreview.textContent = 'Cron: invalid (fill all fields)';
        return;
      }
      rsyncPreview.textContent = `Cron: ${schedule.minute} ${schedule.hour} ${schedule.dom} ${schedule.month} ${schedule.dow}`;
    };

    const loadRsync = async () => {
      if (!rsyncTable) return;
      const data = await api('GET', '/api/rsync');
      if (rsyncUpdated) {
        rsyncUpdated.textContent = data.updated ? `cron updated ${data.updated}` : '';
      }
      rsyncState.items = data.items || [];
      renderTable('#rsync-table', rsyncState.items, '#rsync-empty', (item) => {
        const summary = summarizeCron(item.schedule, item.cron);
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${item.id}</td><td>${item.source}</td><td>${item.target}</td><td>${summary}</td><td>${item.cron}</td><td>${item.mode || ''}</td><td>${item.flags || ''}</td><td>${item.enabled}</td>
          <td>
            <button class="btn" data-action="rsync-toggle" data-id="${item.id}">${item.enabled ? 'Disable' : 'Enable'}</button>
            <button class="btn" data-action="rsync-edit" data-id="${item.id}">Edit</button>
            <button class="btn" data-action="rsync-delete" data-id="${item.id}">Delete</button>
          </td>`;
        return tr;
      });
    };

    const resetRsyncForm = () => {
      if (!rsyncForm) return;
      rsyncForm.reset();
      rsyncId.value = '';
      setRsyncScheduleMode('quick');
      setRsyncFlagsMode(rsyncMode.value);
    };

    const enterRsyncEdit = (item) => {
      rsyncId.value = item.id;
      rsyncSource.value = item.source || '';
      rsyncTarget.value = item.target || '';
      rsyncMode.value = item.mode || 'mirror';
      rsyncEnabled.value = item.enabled ? 'true' : 'false';
      rsyncFlags.value = item.flags || '';
      rsyncModeCron.value = 'advanced';
      rsyncMinute.value = item.schedule.minute;
      rsyncHour.value = item.schedule.hour;
      rsyncDom.value = item.schedule.dom;
      rsyncMonth.value = item.schedule.month;
      rsyncDow.value = item.schedule.dow;
      setRsyncScheduleMode('advanced');
      setRsyncFlagsMode(rsyncMode.value);
    };

    if (rsyncForm) {
      rsyncForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        clearBanner();
        const source = rsyncSource.value.trim();
        const target = rsyncTarget.value.trim();
        if (!source || !target) {
          showBanner('source and target required');
          return;
        }
        const mode = rsyncMode.value;
        const enabled = rsyncEnabled.value === 'true';
        let flags = rsyncFlags.value.trim();
        if (mode === 'custom' && !flags) {
          showBanner('custom flags required');
          return;
        }
        const schedule = rsyncModeCron.value === 'advanced' ? buildRsyncAdvancedSchedule() : buildRsyncQuickSchedule();
        if (!schedule) {
          showBanner('invalid cron fields');
          return;
        }
        try {
          const btn = document.getElementById('rsync-save');
          if (rsyncId.value) {
            await withBusy(btn, () => api('PUT', `/api/rsync/${rsyncId.value}`, {
              source,
              target,
              mode,
              flags,
              enabled,
              schedule,
            }));
            showToast('Rsync updated');
          } else {
            await withBusy(btn, () => api('POST', '/api/rsync', {
              source,
              target,
              mode,
              flags,
              enabled,
              schedule,
            }));
            showToast('Rsync saved');
          }
          resetRsyncForm();
          loadRsync();
        } catch (err) {
          showBanner(err.message, err.details);
        }
      });
    }

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action^="rsync-"]');
      if (!btn) return;
      const id = btn.dataset.id;
      try {
        if (btn.dataset.action === 'rsync-delete') {
          const ok = await confirmModal('Delete rsync job', `Delete job ${id}?`);
          if (!ok) return;
          await withBusy(btn, () => api('DELETE', `/api/rsync/${id}`, { confirm: true }));
          showToast('Rsync deleted');
        }
        if (btn.dataset.action === 'rsync-toggle') {
          await withBusy(btn, () => api('PUT', `/api/rsync/${id}`, { toggle: true }));
          showToast('Rsync updated');
        }
        if (btn.dataset.action === 'rsync-edit') {
          const item = rsyncState.items.find((entry) => entry.id === id);
          if (item) {
            enterRsyncEdit(item);
          }
        }
        loadRsync();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    if (rsyncReset) {
      rsyncReset.addEventListener('click', resetRsyncForm);
    }
    [rsyncModeCron, rsyncFrequency, rsyncInterval, rsyncTime, rsyncDay, rsyncMinute, rsyncHour, rsyncDom, rsyncMonth, rsyncDow].forEach((el) => {
      if (!el) return;
      el.addEventListener('input', updateRsyncPreview);
      el.addEventListener('change', updateRsyncPreview);
    });
    if (rsyncModeCron) {
      rsyncModeCron.addEventListener('change', () => setRsyncScheduleMode(rsyncModeCron.value));
    }
    if (rsyncMode) {
      rsyncMode.addEventListener('change', () => setRsyncFlagsMode(rsyncMode.value));
    }

    Promise.all([loadReplPools(), loadReplDatasets(), loadReplication(), loadRsync()])
      .catch((err) => showBanner(err.message, err.details));

    if (replMode) setReplMode(replMode.value);
    if (rsyncModeCron) setRsyncScheduleMode(rsyncModeCron.value);
    if (rsyncMode) setRsyncFlagsMode(rsyncMode.value);
  };

  const bindSettings = () => {
    const form = document.getElementById('settings-form');
    if (!form) return;

    const configPath = document.getElementById('settings-config-path');
    const passwordStatus = document.getElementById('settings-password-status');
    const restartBadge = document.getElementById('settings-restart');
    const saveBtn = document.getElementById('settings-save');

    const listenAddr = document.getElementById('settings-listen-addr');
    const authUsername = document.getElementById('settings-auth-username');
    const authPassword = document.getElementById('settings-auth-password');
    const authPasswordConfirm = document.getElementById('settings-auth-password-confirm');
    const authSaveBtn = document.getElementById('settings-auth-save');

    const pathZfs = document.getElementById('settings-path-zfs');
    const pathZpool = document.getElementById('settings-path-zpool');
    const pathGeom = document.getElementById('settings-path-geom');
    const pathService = document.getElementById('settings-path-service');
    const pathSmbpasswd = document.getElementById('settings-path-smbpasswd');
    const pathPdbedit = document.getElementById('settings-path-pdbedit');
    const pathTestparm = document.getElementById('settings-path-testparm');
    const pathRsync = document.getElementById('settings-path-rsync');
    const pathSysctl = document.getElementById('settings-path-sysctl');
    const pathSysrc = document.getElementById('settings-path-sysrc');
    const pathShutdown = document.getElementById('settings-path-shutdown');

    const sambaInclude = document.getElementById('settings-samba-include');
    const sambaReload = document.getElementById('settings-samba-reload');
    const sambaTestparm = document.getElementById('settings-samba-testparm');

    const zfsSnapPrefix = document.getElementById('settings-zfs-snap-prefix');

    const cronFile = document.getElementById('settings-cron-file');
    const cronUser = document.getElementById('settings-cron-user');

    const terminalAliases = document.getElementById('settings-terminal-aliases');
    const terminalFavorites = document.getElementById('settings-terminal-favorites');
    const terminalHistory = document.getElementById('settings-terminal-history');

    const limitRequest = document.getElementById('settings-limit-request');
    const limitOutput = document.getElementById('settings-limit-output');
    const limitRuntime = document.getElementById('settings-limit-runtime');

    const auditFile = document.getElementById('settings-audit-file');
    const allowedCmds = document.getElementById('settings-allowed-cmds');
    const binaryPath = document.getElementById('settings-binary-path');

    const autostartToggle = document.getElementById('settings-autostart');
    const autostartStatus = document.getElementById('settings-autostart-status');
    const autostartNote = document.getElementById('settings-autostart-note');

    let restartRequired = false;
    const setRestart = (flag) => {
      if (!restartBadge) return;
      if (flag) {
        restartRequired = true;
      }
      restartBadge.classList.toggle('hidden', !restartRequired);
    };

    const fillForm = (data) => {
      const cfg = data.config || {};
      const meta = data.meta || {};
      const serverCfg = cfg.server || {};
      const authCfg = cfg.auth || {};
      const pathsCfg = cfg.paths || {};
      const sambaCfg = cfg.samba || {};
      const zfsCfg = cfg.zfs || {};
      const cronCfg = cfg.cron || {};
      const terminalCfg = cfg.terminal || {};
      const limitsCfg = cfg.limits || {};
      const auditCfg = cfg.audit || {};
      if (configPath) configPath.textContent = meta.config_path || '-';
      if (passwordStatus) {
        passwordStatus.textContent = meta.password_set ? 'Password set' : 'Password not set';
      }
      setRestart(!!data.restart_required);

      listenAddr.value = serverCfg.listen_addr || '';
      authUsername.value = authCfg.username || '';

      pathZfs.value = pathsCfg.zfs || '';
      pathZpool.value = pathsCfg.zpool || '';
      pathGeom.value = pathsCfg.geom || '';
      pathService.value = pathsCfg.service || '';
      pathSmbpasswd.value = pathsCfg.smbpasswd || '';
      pathPdbedit.value = pathsCfg.pdbedit || '';
      pathTestparm.value = pathsCfg.testparm || '';
      if (pathRsync) pathRsync.value = pathsCfg.rsync || '';
      pathSysctl.value = pathsCfg.sysctl || '';
      pathSysrc.value = pathsCfg.sysrc || '';
      pathShutdown.value = pathsCfg.shutdown || '';

      sambaInclude.value = sambaCfg.include_file || '';
      sambaReload.value = (sambaCfg.reload_args || []).join(' ');
      sambaTestparm.value = (sambaCfg.testparm_args || []).join(' ');

      zfsSnapPrefix.value = zfsCfg.snapshot_prefix || '';

      cronFile.value = cronCfg.cron_file || '';
      cronUser.value = cronCfg.cron_user || '';

      terminalAliases.value = formatAliases(terminalCfg.aliases || {});
      terminalFavorites.value = (terminalCfg.favorites || []).join('\n');
      terminalHistory.value = terminalCfg.history_limit || 20;

      limitRequest.value = limitsCfg.max_request_bytes || 0;
      limitOutput.value = limitsCfg.max_output_bytes || 0;
      limitRuntime.value = limitsCfg.max_runtime_seconds || 0;

      auditFile.value = auditCfg.log_file || '';
      allowedCmds.value = (cfg.allowed_cmds || []).join('\n');
      binaryPath.value = cfg.binary_path || '';

      if (autostartToggle) {
        const hasError = !!meta.autostart_error;
        autostartToggle.checked = !!meta.autostart_enabled;
        autostartToggle.disabled = !meta.rc_script_present || hasError;
      }
      if (autostartStatus) {
        if (!meta.rc_script_present) {
          autostartStatus.textContent = 'Service not installed';
        } else {
          autostartStatus.textContent = meta.autostart_enabled ? 'Enabled' : 'Disabled';
        }
      }
      if (autostartNote) {
        if (!meta.rc_script_present) {
          autostartNote.textContent = 'Install /usr/local/etc/rc.d/raidraccoon (install.sh can do this).';
        } else {
          autostartNote.textContent = meta.autostart_error || '';
        }
      }
    };

    const loadSettings = async () => {
      const data = await api('GET', '/api/settings');
      fillForm(data || {});
    };

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      clearBanner();
      const payload = {
        server: { listen_addr: listenAddr.value.trim() },
        auth: { username: authUsername.value.trim() },
        paths: {
          zfs: pathZfs.value.trim(),
          zpool: pathZpool.value.trim(),
          geom: pathGeom.value.trim(),
          service: pathService.value.trim(),
          smbpasswd: pathSmbpasswd.value.trim(),
          pdbedit: pathPdbedit.value.trim(),
          testparm: pathTestparm.value.trim(),
          rsync: pathRsync ? pathRsync.value.trim() : '',
          sysctl: pathSysctl.value.trim(),
          sysrc: pathSysrc.value.trim(),
          shutdown: pathShutdown.value.trim(),
        },
        samba: {
          include_file: sambaInclude.value.trim(),
          reload_args: parseArgs(sambaReload.value),
          testparm_args: parseArgs(sambaTestparm.value),
        },
        zfs: {
          snapshot_prefix: zfsSnapPrefix.value.trim(),
        },
        cron: {
          cron_file: cronFile.value.trim(),
          cron_user: cronUser.value.trim(),
        },
        terminal: {
          aliases: parseAliases(terminalAliases.value),
          favorites: parseLines(terminalFavorites.value),
          history_limit: parseInt(terminalHistory.value, 10) || 0,
        },
        limits: {
          max_request_bytes: parseInt(limitRequest.value, 10) || 0,
          max_output_bytes: parseInt(limitOutput.value, 10) || 0,
          max_runtime_seconds: parseInt(limitRuntime.value, 10) || 0,
        },
        audit: { log_file: auditFile.value.trim() },
        allowed_cmds: parseLines(allowedCmds.value),
        binary_path: binaryPath.value.trim(),
      };
      try {
        const res = await withBusy(saveBtn, () => api('PUT', '/api/settings', payload));
        showToast('Settings saved');
        setRestart(!!res.restart_required);
        loadSettings();
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    if (authSaveBtn) {
      authSaveBtn.addEventListener('click', async () => {
        clearBanner();
        const pw = authPassword.value;
        const confirm = authPasswordConfirm.value;
        if (!pw || pw !== confirm) {
          showBanner('passwords do not match');
          return;
        }
        const ok = await confirmModal('Update password', 'Update the admin password?');
        if (!ok) return;
        try {
          const res = await withBusy(authSaveBtn, () => api('POST', '/api/settings/password', { password: pw, password_confirm: confirm, confirm: true }));
          showToast('Password updated');
          authPassword.value = '';
          authPasswordConfirm.value = '';
          setRestart(!!res.restart_required);
        } catch (err) {
          showBanner(err.message, err.details);
        }
      });
    }

    if (autostartToggle) {
      autostartToggle.addEventListener('change', async (e) => {
        clearBanner();
        const enable = autostartToggle.checked;
        const ok = await confirmModal('Update autostart', enable ? 'Enable autostart at boot?' : 'Disable autostart at boot?');
        if (!ok) {
          autostartToggle.checked = !enable;
          return;
        }
        autostartToggle.disabled = true;
        try {
          await api('POST', '/api/system/autostart', { enable, confirm: true });
          showToast(enable ? 'Autostart enabled' : 'Autostart disabled');
          loadSettings();
        } catch (err) {
          autostartToggle.checked = !enable;
          showBanner(err.message, err.details);
        } finally {
          autostartToggle.disabled = false;
        }
      });
    }

    document.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      try {
        if (btn.dataset.action === 'system-reboot') {
          const ok = await confirmModal('Reboot system', 'Reboot the host now?');
          if (!ok) return;
          await withBusy(btn, () => api('POST', '/api/system/reboot', { confirm: true }));
          showToast('Reboot initiated');
        }
        if (btn.dataset.action === 'system-shutdown') {
          const ok = await confirmModal('Shutdown system', 'Shutdown the host now?');
          if (!ok) return;
          await withBusy(btn, () => api('POST', '/api/system/shutdown', { confirm: true }));
          showToast('Shutdown initiated');
        }
      } catch (err) {
        showBanner(err.message, err.details);
      }
    });

    loadSettings().catch((err) => showBanner(err.message, err.details));
  };

  document.addEventListener('DOMContentLoaded', () => {
    bindNavButtons();
    bindZpoolImportWatcher();
    bindDashboard();
    bindTerminal();
    bindSambaUsers();
    bindSambaShares();
    bindZFSPools();
    bindZFSMounts();
    bindZFSDatasets();
    bindZFSSnapshots();
    bindSchedules();
    bindReplicationJobs();
    bindSettings();
  });
})();
