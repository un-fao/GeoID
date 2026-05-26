// Dashboard application logic.
//
// Operator-facing surface: stats, logs, tasks. Tabs (.tab-btn / .tab-panel)
// + context selector + manual / 30 s auto refresh. All server-derived
// strings are rendered via textContent / createElement — never innerHTML —
// since logs and task names carry untrusted text.

const app = {
    state: {
        activeTab: 'overview',
        catalogId: null,
        collectionId: null,
    },

    // --- Initialization ---
    init() {
        this.contextSelector = new ContextSelector({
            containerId: 'context-selector-container',
            enableCollection: true,
            enableVirtualCollections: false,
            enableAssets: false,
            enableSearch: true,
            defaultCatalog: '_system_',
            onChange: (context) => {
                this.state.catalogId = context.catalogId;
                this.state.collectionId = context.collectionId;
                this.refreshAll();
            }
        });

        this.bindEvents();
        this.refreshAll();
        // Auto-refresh every 30 s.
        setInterval(() => this.refreshAll(), 30000);
    },

    bindEvents() {
        // Tab switching (atlas .tab-btn / .tab-panel idiom).
        document.querySelectorAll('.tab-btn').forEach((el) => {
            el.addEventListener('click', (e) => {
                const target = e.currentTarget.dataset.tab;
                if (target) {
                    e.preventDefault();
                    this.switchTab(target);
                }
            });
        });

        const refreshBtn = document.getElementById('refresh-btn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.refreshAll());
        }

        const logsFilterBtn = document.getElementById('logs-filter-btn');
        if (logsFilterBtn) {
            logsFilterBtn.addEventListener('click', () => this.loadLogs());
        }

        // Task-filter pill toolbar (cosmetic — backend doesn't filter yet).
        document.querySelectorAll('.tasks-toolbar .pill').forEach((el) => {
            el.addEventListener('click', (e) => {
                document.querySelectorAll('.tasks-toolbar .pill').forEach(
                    (p) => p.classList.remove('active')
                );
                e.currentTarget.classList.add('active');
            });
        });
    },

    switchTab(tabId) {
        document.querySelectorAll('.tab-btn').forEach((el) => {
            const isActive = el.dataset.tab === tabId;
            el.classList.toggle('active', isActive);
            el.setAttribute('aria-selected', isActive ? 'true' : 'false');
        });

        document.querySelectorAll('.tab-panel').forEach((el) => {
            const isActive = el.id === `tab-${tabId}`;
            el.classList.toggle('active', isActive);
            if (isActive) { el.removeAttribute('hidden'); }
            else { el.setAttribute('hidden', ''); }
        });

        const titleEl = document.getElementById('page-title');
        if (titleEl) {
            const titles = {
                overview: 'System overview',
                logs: 'Logs explorer',
                tasks: 'Task monitor',
            };
            titleEl.textContent = titles[tabId] || tabId;
        }

        this.state.activeTab = tabId;
        this.refreshActiveView();
    },

    refreshAll() {
        const now = new Date().toLocaleTimeString();
        const updatedEl = document.getElementById('last-updated');
        if (updatedEl) { updatedEl.textContent = `Updated: ${now}`; }

        if (this.state.activeTab === 'overview') { this.loadOverview(); }
        if (this.state.activeTab === 'logs') { this.loadLogs(); }
        if (this.state.activeTab === 'tasks') { this.loadTasks(); }
    },

    refreshActiveView() {
        this.refreshAll();
    },

    // --- Data fetching ---

    async loadOverview() {
        try {
            const collectionId = this.state.collectionId;
            // catalog_id sits in the page URL: /web/dashboard/catalogs/{cat}/.
            // Relative URLs resolve against it automatically.
            const url = collectionId
                ? `collections/${encodeURIComponent(collectionId)}/stats`
                : 'stats';

            const res = await fetch(url);
            const data = await res.json();

            const setText = (id, value) => {
                const el = document.getElementById(id);
                if (el) { el.textContent = value; }
            };

            setText('stat-catalogs', data.total_catalogs ?? 0);

            const assets = (data.total_assets ?? data.asset_count);
            setText('stat-assets', assets == null ? '—' : assets);

            setText(
                'stat-req',
                data.average_latency_ms
                    ? (1000 / data.average_latency_ms).toFixed(1)
                    : '—'
            );
        } catch (e) {
            console.error('Failed to load stats', e);
        }
    },

    async loadLogs() {
        try {
            const collectionId = this.state.collectionId;
            // Path-based scope: catalog_id from /web/dashboard/catalogs/{cat}/;
            // collection_id (when set) goes in the path too.
            const base = collectionId
                ? `collections/${encodeURIComponent(collectionId)}/logs`
                : 'logs';
            const url = `${base}?limit=20`;

            const res = await fetch(url);
            const logs = await res.json();

            const tbody = document.querySelector('#logs-table tbody');
            if (!tbody) { return; }

            // Drop existing children safely (no innerHTML).
            while (tbody.firstChild) { tbody.removeChild(tbody.firstChild); }

            if (!Array.isArray(logs) || logs.length === 0) {
                const tr = document.createElement('tr');
                tr.className = 'empty-row';
                const td = document.createElement('td');
                td.colSpan = 4;
                td.textContent = 'No logs found.';
                tr.appendChild(td);
                tbody.appendChild(tr);
                return;
            }

            const queryEl = document.getElementById('logs-query');
            const levelEl = document.getElementById('logs-level');
            const queryFilter = (queryEl?.value || '').toLowerCase().trim();
            const levelFilter = levelEl?.value || 'ALL';

            const visible = logs.filter((log) => {
                if (levelFilter !== 'ALL' && log.level !== levelFilter) {
                    return false;
                }
                if (queryFilter && !(log.message || '').toLowerCase().includes(queryFilter)) {
                    return false;
                }
                return true;
            });

            if (visible.length === 0) {
                const tr = document.createElement('tr');
                tr.className = 'empty-row';
                const td = document.createElement('td');
                td.colSpan = 4;
                td.textContent = 'No logs match the current filter.';
                tr.appendChild(td);
                tbody.appendChild(tr);
                return;
            }

            visible.forEach((log) => {
                const tr = document.createElement('tr');

                const tsRaw = log.timestamp || log.created_at;
                const tsCell = document.createElement('td');
                tsCell.textContent = tsRaw
                    ? new Date(tsRaw).toLocaleString()
                    : 'Just now';
                tr.appendChild(tsCell);

                const levelCell = document.createElement('td');
                const span = document.createElement('span');
                span.className = `log-level level-${(log.level || 'info').toLowerCase()}`;
                span.textContent = log.level || 'INFO';
                levelCell.appendChild(span);
                tr.appendChild(levelCell);

                const svcCell = document.createElement('td');
                svcCell.textContent = log.service || 'system';
                tr.appendChild(svcCell);

                const msgCell = document.createElement('td');
                msgCell.textContent = log.message || '';
                tr.appendChild(msgCell);

                tbody.appendChild(tr);
            });
        } catch (e) {
            console.error('Failed to load logs', e);
        }
    },

    async loadTasks() {
        try {
            const res = await fetch('tasks');
            const tasks = await res.json();

            const grid = document.getElementById('tasks-grid');
            if (!grid) { return; }

            while (grid.firstChild) { grid.removeChild(grid.firstChild); }

            if (!Array.isArray(tasks) || tasks.length === 0) {
                const empty = document.createElement('p');
                empty.className = 'empty-cell';
                empty.textContent = 'No active background tasks.';
                grid.appendChild(empty);
                return;
            }

            tasks.forEach((task) => {
                const card = document.createElement('div');
                card.className = 'task-card';

                const header = document.createElement('div');
                header.className = 'task-header';

                const title = document.createElement('div');
                title.className = 'task-title';
                title.textContent = task.name || 'task';
                header.appendChild(title);

                const status = document.createElement('span');
                const statusLabel = (task.status || 'unknown').toUpperCase();
                const statusKind = (() => {
                    const s = (task.status || '').toLowerCase();
                    if (s === 'failed' || s === 'error') { return 'effect-DENY'; }
                    if (s === 'completed' || s === 'success') { return 'effect-ALLOW'; }
                    return '';
                })();
                status.className = `chip ${statusKind}`;
                status.textContent = statusLabel;
                header.appendChild(status);

                card.appendChild(header);

                const idLine = document.createElement('div');
                idLine.className = 'task-id';
                idLine.textContent = `ID ${task.id || '—'}`;
                card.appendChild(idLine);

                const progress = Math.max(0, Math.min(100, Number(task.progress) || 0));
                const bar = document.createElement('div');
                bar.className = 'progress-bar';
                const fill = document.createElement('div');
                fill.className = 'progress-fill';
                fill.style.width = `${progress}%`;
                bar.appendChild(fill);
                card.appendChild(bar);

                const pct = document.createElement('div');
                pct.className = 'task-progress-pct';
                pct.textContent = `${progress}%`;
                card.appendChild(pct);

                grid.appendChild(card);
            });
        } catch (e) {
            console.error('Failed to load tasks', e);
        }
    },
};

// Start
document.addEventListener('DOMContentLoaded', () => app.init());
