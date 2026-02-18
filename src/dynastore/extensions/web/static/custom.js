/**
 * DynaStore Web Interface Logic
 * Handles Navigation, Documentation System, Dashboard, Extensions, and I18n
 */

// Global State
let docsManifest = null;
let docsLoaded = false;
let dashboardPulse = null;
let dashboardLastUpdate = 0;
let currentLocale = 'en';

// --- Navigation Logic ---
function switchTab(tabId) {
    // 1. Hide all main sections
    document.querySelectorAll('[id^="section-"]').forEach(el => {
        // Don't hide the app viewer if we are just switching tabs unless we explicitly close it
        if(el.id !== 'section-app-viewer') {
            el.classList.add('hidden-section');
        }
    });
    
    // 2. Show selected section with animation
    const target = document.getElementById(`section-${tabId}`);
    if(target) {
        target.classList.remove('hidden-section');
        target.classList.remove('fade-in');
        void target.offsetWidth; // Force reflow
        target.classList.add('fade-in');
    }

    // 3. Update Nav State
    document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active-tab'));
    const navBtn = document.getElementById(`nav-${tabId}`);
    if (navBtn) navBtn.classList.add('active-tab');
    
    // 4. Component-Specific Loaders
    if (tabId === 'docs' && !docsLoaded) initDocs();
    if (tabId === 'dashboard') {
        initDashboard();
        if (!dashboardPulse) dashboardPulse = setInterval(updateDashboard, 5000);
    } else {
        if (dashboardPulse) { clearInterval(dashboardPulse); dashboardPulse = null; }
    }
    if (tabId === 'extensions') {
        loadExtensions();
    }

    // 5. Scroll to top
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

// --- I18n Logic ---
function toggleLangDropdown() {
    const dd = document.getElementById('lang-dropdown');
    if (dd) dd.classList.toggle('show');
}

function setLanguage(lang) {
    currentLocale = lang;
    
    // Update active state in UI
    document.querySelectorAll('.lang-btn').forEach(btn => {
        if (btn.dataset.lang === lang) btn.classList.add('bg-blue-500/10', 'text-blue-400', 'font-bold');
        else btn.classList.remove('bg-blue-500/10', 'text-blue-400', 'font-bold');
    });

    // Update Label
    const label = document.getElementById('current-lang-label');
    if (label) {
        const names = { 'en': 'English', 'es': 'Español', 'fr': 'Français' };
        label.innerText = names[lang] || lang.toUpperCase();
    }

    // Close Dropdown
    const dd = document.getElementById('lang-dropdown');
    if (dd) dd.classList.remove('show');

    // Reload active Extension if visible
    const viewer = document.getElementById('section-app-viewer');
    if (viewer && !viewer.classList.contains('hidden-section')) {
        const frame = document.getElementById('app-viewer-frame');
        if (frame && frame.src) {
            const url = new URL(frame.src);
            url.searchParams.set('language', lang);
            frame.src = url.toString();
        }
    }
    
    // Reload Extensions list if active
    const extSection = document.getElementById('section-extensions');
    if (extSection && !extSection.classList.contains('hidden-section')) {
        loadExtensions();
    }
}

// --- Layout Logic ---
function toggleSidebar() {
    const sidebar = document.querySelector('aside');
    const icon = document.getElementById('sidebar-toggle-icon');
    const span = icon.nextElementSibling;
    
    sidebar.classList.toggle('sidebar-collapsed');
    
    if (sidebar.classList.contains('sidebar-collapsed')) {
        icon.classList.remove('fa-angles-left');
        icon.classList.add('fa-angles-right');
        if (span) span.innerText = '';
    } else {
        icon.classList.remove('fa-angles-right');
        icon.classList.add('fa-angles-left');
        if (span) span.innerText = 'Collapse';
    }
}

// --- Extensions Logic ---
async function loadExtensions() {
    const container = document.getElementById('extensions-grid');
    if (!container) return;
    
    container.innerHTML = '<div class="col-span-full text-center py-20 text-slate-500"><i class="fa-solid fa-circle-notch fa-spin text-2xl"></i><p class="mt-2">Loading extensions...</p></div>';

    try {
        // Use relative path to 'config/pages' to support proxy prefixes
        const res = await fetch(`config/pages?language=${currentLocale}`);
        if (!res.ok) throw new Error("Failed to load extensions config");
        const pages = await res.json();
        
        if (pages.length === 0) {
            container.innerHTML = '<div class="col-span-full text-center py-20 text-slate-500">No extensions found.</div>';
            return;
        }

        container.innerHTML = pages.map(page => `
            <div class="glass-card p-6 rounded-xl hover:bg-white/5 transition-colors group cursor-pointer border border-white/5 hover:border-blue-500/30" onclick="openExtension('${page.id}')">
                <div class="flex items-start justify-between mb-4">
                    <div class="w-12 h-12 bg-blue-500/10 rounded-lg flex items-center justify-center text-blue-400 group-hover:scale-110 transition-transform">
                        <i class="fa-solid ${page.icon} text-xl"></i>
                    </div>
                </div>
                <h3 class="text-lg font-bold text-white mb-2">${page.title}</h3>
                <p class="text-sm text-slate-400 mb-6 h-10 overflow-hidden text-ellipsis line-clamp-2">${page.description || 'No description available.'}</p>
                <div class="flex items-center text-blue-400 text-xs font-mono font-bold group-hover:underline uppercase tracking-wider">
                    Launch App <i class="fa-solid fa-arrow-right ml-2 opacity-0 group-hover:opacity-100 transition-opacity"></i>
                </div>
            </div>
        `).join('');

    } catch (e) {
        console.error("Extensions load error:", e);
        container.innerHTML = '<div class="col-span-full text-center py-20 text-red-400 border border-red-500/20 bg-red-500/5 rounded-xl">Failed to load extensions.</div>';
    }
}

function openExtension(pageId) {
    let viewer = document.getElementById('section-app-viewer');
    
    if (!viewer) {
        console.error("Viewer element not found!");
        return;
    }
    
    const frame = document.getElementById('app-viewer-frame');
    // Use relative path for pages
    frame.src = `pages/${pageId}?language=${currentLocale}`;
    
    viewer.classList.remove('hidden-section');
    document.body.style.overflow = 'hidden'; // Lock body scroll

    // Update hash for back button support
    if (window.location.hash !== `#ext:${pageId}`) {
        window.history.pushState(null, null, `#ext:${pageId}`);
    }
}

function closeExtension(skipHistory = false) {
    const viewer = document.getElementById('section-app-viewer');
    if (viewer) {
        viewer.classList.add('hidden-section');
        const frame = document.getElementById('app-viewer-frame');
        frame.src = 'about:blank';
        document.body.style.overflow = ''; // Restore body scroll

        if (!skipHistory && window.location.hash.startsWith('#ext:')) {
            window.history.pushState(null, null, '#extensions');
        }
    }
}

// --- Documentation System Logic ---
async function initDocs() {
    docsLoaded = true;
    const contentDiv = document.getElementById('docs-content');
    const sidebarDiv = document.getElementById('docs-sidebar-content');
    
    if (sidebarDiv) sidebarDiv.innerHTML = '<div class="py-4 text-center text-slate-500"><i class="fa-solid fa-spinner fa-spin"></i> <span class="text-xs ml-2">Loading docs...</span></div>';
    
    try {
        // Relative path
        const response = await fetch('docs-manifest');
        if (!response.ok) throw new Error('Failed to load docs manifest');
        docsManifest = await response.json();
        renderSidebar(docsManifest);
        if (contentDiv) contentDiv.innerHTML = '<div class="py-12 text-center text-slate-600"><p>Select a document from the sidebar to view</p></div>';
        handleHashChange();
    } catch (e) {
        console.error(e);
        if (contentDiv) contentDiv.innerHTML = `<div class="p-6 border border-red-500/20 bg-red-500/10 rounded-lg text-red-400"><h3>Error</h3><p>${e.message}</p></div>`;
        if (sidebarDiv) sidebarDiv.innerHTML = '<div class="text-red-400 text-xs p-4">Error loading docs</div>';
    }
}

function renderSidebar(manifest) {
    const sidebar = document.getElementById('docs-sidebar-content');
    if (!sidebar) return;

    let html = '';
    const categoryMapping = {
        'root': { title: 'Architecture', icon: 'fa-sitemap' },
        'modules': { title: 'Modules', icon: 'fa-cubes' },
        'extensions': { title: 'Extensions', icon: 'fa-puzzle-piece' },
        'tasks': { title: 'Tasks', icon: 'fa-gears' }
    };

    // Ensure 'root' is always processed first
    const sortedKeys = Object.keys(manifest).sort((a, b) => {
        if (a === 'root') return -1;
        if (b === 'root') return 1;
        return a.localeCompare(b);
    });

    sortedKeys.forEach(catKey => {
        const items = manifest[catKey];
        if (items && items.length > 0) {
            const info = categoryMapping[catKey] || { title: catKey.charAt(0).toUpperCase() + catKey.slice(1), icon: 'fa-circle-info' };
            html += `
                <div class="mb-6">
                    <h3 class="px-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2 flex items-center gap-2">
                        <i class="fa-solid ${info.icon} text-slate-600"></i> ${info.title}
                    </h3>
                    <ul class="space-y-0.5">
            `;
            items.forEach(doc => {
                const displayName = doc.title || doc.id;
                html += `
                    <li>
                        <a href="#docs:${doc.id}" class="doc-sidebar-link group flex items-center px-4 py-2 text-sm text-slate-400 hover:text-white hover:bg-white/5 transition-all border-l-2 border-transparent" data-doc-id="${doc.id}">
                            <span class="truncate">${displayName}</span>
                        </a>
                    </li>
                `;
            });
            html += `</ul></div>`;
        }
    });

    sidebar.innerHTML = html;
    
    document.querySelectorAll('.doc-sidebar-link').forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const docId = this.getAttribute('data-doc-id');
            loadDocContent(docId);
            window.location.hash = `#docs:${docId}`;
        });
    });
}

async function loadDocContent(docId) {
    const contentDiv = document.getElementById('docs-content');
    if (!contentDiv) return;
    contentDiv.innerHTML = '<div class="py-20 text-center"><i class="fa-solid fa-circle-notch fa-spin text-2xl text-blue-500"></i><p class="text-slate-500 mt-2 text-sm">Loading...</p></div>';
    
    document.querySelectorAll('.doc-sidebar-link').forEach(l => {
        l.classList.remove('active', 'text-blue-400', 'bg-blue-500/10', 'border-blue-500', 'border-l-2');
        l.classList.add('border-transparent');
    });
    const activeLink = document.querySelector(`.doc-sidebar-link[data-doc-id="${docId}"]`);
    if (activeLink) {
        activeLink.classList.remove('border-transparent');
        activeLink.classList.add('active', 'text-blue-400', 'bg-blue-500/10', 'border-blue-500', 'border-l-2');
    }

    try {
        const res = await fetch(`docs-content/${docId}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const html = await res.text();
        contentDiv.innerHTML = `<div class="prose prose-invert max-w-3xl mx-auto">${html}</div>`;
        
        // Generate IDs for headers
        contentDiv.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach(header => {
            if (!header.id) header.id = header.textContent.toLowerCase().trim().replace(/[^\w\s-]/g, '').replace(/\s/g, '-');
        });
    } catch (e) {
        contentDiv.innerHTML = `<div class="p-8 text-center text-red-400 border border-red-500/20 bg-red-500/5 rounded-xl">Document not found: ${docId}</div>`;
    }
}

function handleHashChange() {
    const hash = window.location.hash;
    
    if (hash.startsWith('#ext:')) {
        const pageId = hash.substring(5);
        openExtension(pageId);
        return;
    } else {
        // If we were in an extension and shifted to something else, close it
        const viewer = document.getElementById('section-app-viewer');
        if (viewer && !viewer.classList.contains('hidden-section')) {
            closeExtension(true);
        }
    }

    if (!docsLoaded && hash.startsWith('#docs')) return;
    
    if (hash.startsWith('#docs:')) {
        loadDocContent(hash.substring(6));
    } else if (hash === '#docs' && docsManifest) {
        const firstCat = Object.keys(docsManifest)[0];
        if (firstCat && docsManifest[firstCat].length > 0) {
            loadDocContent(docsManifest[firstCat][0].id);
        }
    }
}

// --- Dashboard Logic ---
async function initDashboard() { updateDashboard(); }
async function updateDashboard() {
    if (Date.now() - dashboardLastUpdate < 2000) return;
    dashboardLastUpdate = Date.now();
    await Promise.all([fetchDashboardStats(), fetchDashboardLogs(), fetchDashboardTasks()]);
}

async function fetchDashboardStats() {
    try {
        const res = await fetch('dashboard/stats');
        if (!res.ok) return;
        const stats = await res.json();
        if(document.getElementById('stat-total-requests')) document.getElementById('stat-total-requests').innerText = stats.total_requests.toLocaleString() || '0';
        if(document.getElementById('stat-avg-latency')) document.getElementById('stat-avg-latency').innerText = `${Math.round(stats.average_latency_ms || 0)}ms`;
        const rate = stats.total_requests > 0 ? 100 : 0;
        if(document.getElementById('stat-success-rate')) document.getElementById('stat-success-rate').innerText = `${rate}%`;
    } catch(e) {}
}

async function fetchDashboardLogs() {
    try {
        const level = document.getElementById('log-filter-level')?.value || 'INFO';
        const res = await fetch(`dashboard/logs?limit=50&level=${level}`);
        if (!res.ok) return;
        const logs = await res.json();
        const container = document.getElementById('dashboard-logs');
        if (container) {
             container.innerHTML = logs.length ? logs.map(l => `<div class="text-xs font-mono p-1 hover:bg-white/5 flex gap-2"><span class="text-slate-500 shrink-0">${new Date(l.created_at).toLocaleTimeString()}</span> <span class="font-bold ${l.level=='ERROR'?'text-red-400':l.level=='WARN'?'text-yellow-400':'text-blue-400'} w-12 shrink-0">${l.level}</span> <span class="break-all">${l.message}</span></div>`).join('') : '<div class="text-slate-500 text-center py-4">No logs</div>';
        }
    } catch(e) {}
}

async function fetchDashboardTasks() {
    try {
        const res = await fetch('dashboard/tasks');
        if(!res.ok) return;
        const tasks = await res.json();
        const container = document.getElementById('dashboard-tasks');
        if(container) container.innerHTML = tasks.length ? tasks.map(t => `<div class="p-2 border-b border-white/5"><div class="flex justify-between"><div class="font-bold text-sm">${t.type}</div><div class="text-xs text-purple-300 bg-purple-500/10 px-1 rounded">${t.status}</div></div><div class="text-[10px] text-slate-500">${t.id}</div></div>`).join('') : '<div class="text-slate-500 text-center py-4">No active tasks</div>';
        if(document.getElementById('stat-active-tasks')) document.getElementById('stat-active-tasks').innerText = tasks.length;
    } catch(e){}
}

// --- Initialization ---
document.addEventListener('DOMContentLoaded', () => {
    window.addEventListener('hashchange', handleHashChange);
    
    const hash = window.location.hash;
    if (hash.startsWith('#docs')) { switchTab('docs'); }
    else if (hash === '#dashboard') switchTab('dashboard');
    else if (hash === '#extensions') switchTab('extensions');
    else switchTab('home');

    // Language Selector Listeners
    document.querySelectorAll('.lang-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            setLanguage(e.target.dataset.lang);
        });
    });

    // Test Badges Toggle Logic
    const badgeTrigger = document.getElementById('badge-trigger');
    const badgePanel = document.getElementById('badge-panel');
    
    if (badgeTrigger && badgePanel) {
        badgeTrigger.addEventListener('click', (e) => {
            e.stopPropagation();
            badgePanel.classList.toggle('hidden');
        });
        
        document.addEventListener('click', (e) => {
            if (!badgePanel.contains(e.target) && !badgeTrigger.contains(e.target)) {
                badgePanel.classList.add('hidden');
            }
        });
    }

    // Global click listener to close lang dropdown
    document.addEventListener('click', (e) => {
        const dd = document.getElementById('lang-dropdown');
        const btn = document.getElementById('lang-dropdown-btn');
        if (dd && btn && !dd.contains(e.target) && !btn.contains(e.target)) {
            dd.classList.remove('show');
        }
    });

});