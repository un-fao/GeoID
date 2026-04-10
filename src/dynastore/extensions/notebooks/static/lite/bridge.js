// JupyterLite Bridge Script
// Bridges communication between the host DynaStore application
// and the JupyterLite environment running in an iframe.
//
// Message types:
//   INIT_CONTEXT   — Auth token + catalog context injection
//   LOAD_NOTEBOOK  — Load a notebook into the current session

(function() {
    console.log("[Bridge] Initializing DynaStore Bridge...");

    // Store context
    window.DYNASTORE_CONTEXT = {
        token: null,
        catalogCode: null,
        baseUrl: null,
        stacItems: null
    };

    window.addEventListener('message', async (event) => {
        if (!event.data || !event.data.type) return;

        // --- INIT_CONTEXT: inject auth + catalog context ---
        if (event.data.type === 'INIT_CONTEXT') {
            console.log("[Bridge] Received INIT_CONTEXT", event.data);
            const { token, catalogCode, baseUrl, stacItems } = event.data;

            window.DYNASTORE_CONTEXT.token = token;
            window.DYNASTORE_CONTEXT.catalogCode = catalogCode;
            window.DYNASTORE_CONTEXT.baseUrl = baseUrl || '';
            window.DYNASTORE_CONTEXT.stacItems = stacItems;

            // Inject into Python when Pyodide is ready
            if (window.pyodideReadyPromise) {
                try {
                    const pyodide = await window.pyodideReadyPromise;

                    const stacJson = JSON.stringify(stacItems || {});
                    const safeBase = (baseUrl || '').replace(/'/g, "\\'");
                    const safeCatalog = (catalogCode || '').replace(/'/g, "\\'");

                    pyodide.runPython(`
import json
import os

# Patch HTTP for Pyodide (must run before httpx import)
try:
    import pyodide_http
    pyodide_http.patch_all()
except ImportError:
    pass

os.environ['DYNASTORE_BASE_URL'] = '${safeBase}'
os.environ['DYNASTORE_CATALOG'] = '${safeCatalog}'

try:
    STAC_CONTEXT = json.loads('${stacJson}')
    print(f"DynaStore Context: catalog={os.environ['DYNASTORE_CATALOG']}, base={os.environ['DYNASTORE_BASE_URL']}")
except Exception as e:
    STAC_CONTEXT = {}
    print(f"Warning: STAC context parse error: {e}")
`);
                    console.log("[Bridge] Python environment configured.");
                    window.parent.postMessage({ type: 'NOTEBOOK_READY' }, '*');
                } catch (e) {
                    console.error("[Bridge] Failed to configure Python environment", e);
                }
            } else {
                console.warn("[Bridge] pyodideReadyPromise not found. JupyterLite may not be fully loaded.");
                // Still signal readiness so the host can retry
                window.parent.postMessage({ type: 'NOTEBOOK_READY' }, '*');
            }
        }

        // --- LOAD_NOTEBOOK: inject a notebook into JupyterLite ---
        if (event.data.type === 'LOAD_NOTEBOOK') {
            console.log("[Bridge] Received LOAD_NOTEBOOK", event.data.notebookId);
            const { notebook, notebookId } = event.data;

            if (!notebook || !notebook.cells) {
                console.error("[Bridge] Invalid notebook content");
                return;
            }

            // Attempt to load via JupyterLite Contents API
            try {
                const filename = (notebookId || 'notebook') + '.ipynb';
                const contentsUrl = '/api/contents/' + encodeURIComponent(filename);

                // Write the notebook to JupyterLite's virtual filesystem
                const response = await fetch(contentsUrl, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        type: 'notebook',
                        format: 'json',
                        content: notebook
                    })
                });

                if (response.ok) {
                    console.log("[Bridge] Notebook written to virtual FS:", filename);
                    // Navigate JupyterLite to open the notebook
                    // This uses JupyterLab's command system if available
                    if (window.jupyterapp) {
                        window.jupyterapp.commands.execute('docmanager:open', {
                            path: filename
                        });
                    }
                } else {
                    console.warn("[Bridge] Contents API write failed:", response.status);
                    // Fallback: store in sessionStorage for manual load
                    sessionStorage.setItem('dynastore_notebook', JSON.stringify(notebook));
                    sessionStorage.setItem('dynastore_notebook_id', notebookId || 'notebook');
                    console.log("[Bridge] Notebook stored in sessionStorage as fallback.");
                }
            } catch (e) {
                console.warn("[Bridge] Contents API not available, using sessionStorage fallback:", e.message);
                sessionStorage.setItem('dynastore_notebook', JSON.stringify(notebook));
                sessionStorage.setItem('dynastore_notebook_id', notebookId || 'notebook');
            }
        }
    });

    // Custom Save Handler: persist notebooks back to DynaStore
    window.saveNotebookToDynaStore = async function(name, content) {
        console.log(`[Bridge] Saving notebook ${name}...`);

        const { token, catalogCode, baseUrl } = window.DYNASTORE_CONTEXT;

        if (!catalogCode) {
            console.error("[Bridge] Cannot save: no catalog selected");
            return;
        }

        try {
            const notebookId = name.replace('.ipynb', '');
            const apiBase = baseUrl || '';
            const url = `${apiBase}/notebooks/${encodeURIComponent(catalogCode)}/${encodeURIComponent(notebookId)}`;

            const headers = { 'Content-Type': 'application/json' };
            if (token) {
                headers['Authorization'] = `Bearer ${token}`;
            }

            const response = await fetch(url, {
                method: 'PUT',
                headers: headers,
                body: JSON.stringify(content)
            });

            if (response.ok) {
                console.log("[Bridge] Notebook saved successfully");
            } else {
                console.error("[Bridge] Save failed", response.status, await response.text());
            }
        } catch (e) {
            console.error("[Bridge] Network error during save", e);
        }
    };

})();
