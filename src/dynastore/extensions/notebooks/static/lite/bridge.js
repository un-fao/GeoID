// JupyterLite Bridge Script
// This script bridges the communication between the host application (DynaStore) 
// and the isolated JupyterLite environment running in an iframe.

(function() {
    console.log("[Bridge] Initializing DynaStore Bridge...");

    // Store context
    window.DYNASTORE_CONTEXT = {
        token: null,
        catalogCode: null,
        stacItems: null
    };

    // 1. Listen for initialization message from parent
    window.addEventListener('message', async (event) => {
        // Security check: You might want to check event.origin here
        
        if (event.data.type === 'INIT_CONTEXT') {
            console.log("[Bridge] Received INIT_CONTEXT", event.data);
            const { token, catalogCode, stacItems } = event.data;
            
            // Store in memory
            window.DYNASTORE_CONTEXT.token = token;
            window.DYNASTORE_CONTEXT.catalogCode = catalogCode;
            window.DYNASTORE_CONTEXT.stacItems = stacItems;

            // Wait for Pyodide to be ready
            // Note: This promise needs to be exposed by the JupyterLite initialization process
            // or we need to poll for it.
            if (window.pyodideReadyPromise) {
                try {
                    const pyodide = await window.pyodideReadyPromise;
                    
                    // Inject variables into Python namespace
                    // We use JSON to pass complex objects safely
                    const stacJson = JSON.stringify(stacItems || {});
                    
                    pyodide.runPython(`
                        import json
                        import os
                        
                        # Set environment variables for the session
                        os.environ['DYNASTORE_CATALOG'] = '${catalogCode}'
                        
                        # Inject STAC Items
                        try:
                            STAC_CONTEXT = json.loads('${stacJson}')
                            print(f"DynaStore Context Loaded: {len(STAC_CONTEXT.get('features', []))} features available in STAC_CONTEXT")
                        except Exception as e:
                            print(f"Error loading STAC context: {e}")
                    `);
                    
                    console.log("[Bridge] Python environment configured.");
                    
                    // Notify parent that we are ready
                    window.parent.postMessage({ type: 'NOTEBOOK_READY' }, '*');
                    
                } catch (e) {
                    console.error("[Bridge] Failed to configure Python environment", e);
                }
            } else {
                console.warn("[Bridge] pyodideReadyPromise not found. Is JupyterLite fully loaded?");
            }
        }
    });

    // 2. Custom Save Handler
    // We override the default save mechanism to persist to DynaStore
    window.saveNotebookToDynaStore = async function(name, content) {
        console.log(`[Bridge] Saving notebook ${name}...`);
        
        const { token, catalogCode } = window.DYNASTORE_CONTEXT;
        
        if (!token || !catalogCode) {
            console.error("[Bridge] Cannot save: Missing auth token or catalog code");
            alert("Error: Missing authentication context. Please refresh the page.");
            return;
        }
        
        try {
            const notebookId = name.replace('.ipynb', ''); // Simple slugification
            
            const response = await fetch(`/api/extensions/notebooks/${catalogCode}/${notebookId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${token}`
                },
                body: JSON.stringify(content)
            });
            
            if (response.ok) {
                console.log("[Bridge] Notebook saved successfully");
                // Optional: Show a toast notification
            } else {
                console.error("[Bridge] Save failed", response.status, await response.text());
                alert("Failed to save notebook. Check console for details.");
            }
        } catch (e) {
            console.error("[Bridge] Network error during save", e);
            alert("Network error while saving.");
        }
    };

})();
