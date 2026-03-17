// Dashboard Application Logic
const app = {
    state: {
        activeTab: 'overview'
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
        // Auto-refresh every 30s
        setInterval(() => this.refreshAll(), 30000);
    },

    bindEvents() {
        // Tab Switching
        document.querySelectorAll('.nav-item').forEach(el => {
            el.addEventListener('click', (e) => {
                const target = e.currentTarget.dataset.tab;
                if (target) {
                    e.preventDefault();
                    this.switchTab(target);
                }
                // If no data-tab, let the default link behavior happen (e.g. navigation)
            });
        });
    },

    switchTab(tabId) {
        // UI Updates
        document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
        document.querySelector(`.nav-item[data-tab="${tabId}"]`).classList.add('active');
        
        document.querySelectorAll('.view').forEach(el => el.classList.remove('active'));
        document.getElementById(`view-${tabId}`).classList.add('active');
        
        document.getElementById('page-title').innerText = 
            tabId.charAt(0).toUpperCase() + tabId.slice(1);
            
        this.state.activeTab = tabId;
        this.refreshActiveView();
    },

    refreshAll() {
        const now = new Date().toLocaleTimeString();
        document.getElementById('last-updated').innerText = `Updated: ${now}`;
        
        if (this.state.activeTab === 'overview') this.loadOverview();
        if (this.state.activeTab === 'logs') this.loadLogs();
        if (this.state.activeTab === 'tasks') this.loadTasks();
    },
    
    refreshActiveView() {
        this.refreshAll(); // Simple enough for now
    },

    // --- Data Fetching ---

    async loadOverview() {
        try {
            const catalogId = this.state.catalogId;
            const collectionId = this.state.collectionId;
            let url = `/web/dashboard/stats?catalog_id=${catalogId}`;
            if (collectionId) url += `&collection_id=${collectionId}`;
            
            const res = await fetch(url);
            const data = await res.json();
            
            // Populate Cards
            document.getElementById('stat-catalogs').innerText = data.total_catalogs || 0;
            document.getElementById('stat-assets').innerText = (data.total_collections || 0) * 150; // Mock estimate
            document.getElementById('stat-req').innerText = (data.average_latency_ms ? (1000/data.average_latency_ms).toFixed(1) : "0.0");

            // Charts
            this.renderCharts(data);
        } catch (e) {
            console.error("Failed to load stats", e);
        }
    },

    async loadLogs() {
        try {
            const catalogId = this.state.catalogId;
            const collectionId = this.state.collectionId;
            
            let url = `/web/dashboard/logs?limit=20&catalog_id=${catalogId}`;
            if (collectionId) url += `&collection_id=${collectionId}`;
            
            const levelFilter = document.querySelector('.filter-select').value; // Assuming the first .filter-select is level (if they have unique ids it's better)
            
            const res = await fetch(url);
            const logs = await res.json();
            
            const tbody = document.querySelector('#logs-table tbody');
            tbody.innerHTML = '';
            
            if (logs.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; padding: 2rem;">No logs found.</td></tr>';
                return;
            }

            logs.forEach(log => {
                const tr = document.createElement('tr');
                // Colorize level
                let color = '#64748b';
                if(log.level === 'ERROR') color = '#ef4444';
                if(log.level === 'WARNING') color = '#f59e0b';
                
                const timeStr = log.timestamp ? new Date(log.timestamp).toLocaleString() : (log.created_at ? new Date(log.created_at).toLocaleString() : 'Just now');
                tr.innerHTML = `
                    <td>${timeStr}</td>
                    <td><span style="color:${color}; font-weight:700;">${log.level}</span></td>
                    <td>${log.service || 'system'}</td>
                    <td>${log.message}</td>
                `;
                tbody.appendChild(tr);
            });
        } catch (e) {
            console.error("Failed to load logs", e);
        }
    },
    
    async loadTasks() {
        try {
            const res = await fetch('/web/dashboard/tasks');
            const tasks = await res.json();
            
            const grid = document.getElementById('tasks-grid');
            grid.innerHTML = '';
            
            if (tasks.length === 0) {
                grid.innerHTML = '<p style="color:#64748b; padding:2rem;">No active background tasks.</p>';
                return;
            }
            
            tasks.forEach(task => {
                const el = document.createElement('div');
                el.className = 'task-card';
                el.innerHTML = `
                    <div class="task-header">
                        <div class="task-title">${task.name}</div>
                        <div class="task-status status-${task.status.toLowerCase()}">${task.status}</div>
                    </div>
                    <div style="font-size:0.85rem; color:#64748b; margin-bottom:0.5rem;">ID: ${task.id}</div>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: ${task.progress || 0}%"></div>
                    </div>
                    <div style="font-size:0.8rem; text-align:right;">${task.progress || 0}%</div>
                `;
                grid.appendChild(el);
            });
            
        } catch (e) {
            console.error("Failed to load tasks", e);
        }
    },

    // --- Charts ---
    renderCharts(stats) {
        const ctx = document.getElementById('trafficChart');
        if(!ctx) return;
        
        // Mock data for demo purposes if not available
        if (window.trafficChartInstance) window.trafficChartInstance.destroy();
        
        window.trafficChartInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00'],
                datasets: [{
                    label: 'Requests',
                    data: [12, 19, 3, 5, 2, 30], // Connect to real historic stats if available
                    borderColor: '#2563eb',
                    tension: 0.4
                }]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }
};

// Start
document.addEventListener('DOMContentLoaded', () => app.init());
