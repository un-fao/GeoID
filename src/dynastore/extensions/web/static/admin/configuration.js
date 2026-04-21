// Configuration Hub — page glue.
// Wires the reusable common/ modules together. No plugin-specific code.

import {
  fetchSchemas,
  fetchConfigSet,
  patchConfigSet,
  fetchMe,
} from "../common/api.js";
import { mountContextBar } from "../common/context-bar.js";
import { mountSchemaList } from "../common/schema-list.js";
import { mountSchemaForm } from "../common/schema-form.js";

const $ = (sel) => document.querySelector(sel);

const state = {
  schemas: {},            // class_key -> { json_schema, description, scope }
  scope: { kind: "platform" },
  selectedClassKey: null,
  resolvedSet: {},        // class_key -> config dict (inherited)
  explicitSet: {},        // class_key -> config dict (overrides only)
  form: null,
};

function setStatus(msg, cls = "") {
  const s = $("#status");
  s.textContent = msg || "";
  s.className = "status " + cls;
}

// Normalize composed response to { class_key -> value }.
function normalizeConfigSet(payload) {
  if (!payload) return {};
  if (Array.isArray(payload.items)) {
    const out = {};
    for (const e of payload.items) {
      const key = e.class_key || e.plugin_id || e.key;
      if (key) out[key] = e.value || e.config_data || e;
    }
    return out;
  }
  if (payload.configs && typeof payload.configs === "object") return payload.configs;
  return payload;
}

async function refreshConfigsForScope() {
  const [resolved, explicit] = await Promise.all([
    fetchConfigSet(state.scope, { resolved: true }),
    fetchConfigSet(state.scope, { resolved: false }),
  ]);
  state.resolvedSet = normalizeConfigSet(resolved);
  state.explicitSet = normalizeConfigSet(explicit);
}

function renderForm() {
  const host = $("#schema-form");
  const hdr = $("#form-header");
  const preview = $("#preview-json");
  $("#apply").disabled = true;
  $("#revert").disabled = true;
  setStatus("");

  if (!state.selectedClassKey) {
    host.replaceChildren();
    preview.textContent = "";
    hdr.textContent = "Editor";
    const p = document.createElement("div");
    p.className = "empty-hint";
    p.textContent = "Select a plugin from the list to edit its configuration.";
    host.appendChild(p);
    return;
  }

  const entry = state.schemas[state.selectedClassKey];
  if (!entry) return;

  hdr.textContent = `${state.selectedClassKey} — ${state.scope.kind}`
    + (state.scope.catalogId ? ` / ${state.scope.catalogId}` : "")
    + (state.scope.collectionId ? ` / ${state.scope.collectionId}` : "");

  const resolved = state.resolvedSet[state.selectedClassKey] || {};
  const explicit = state.explicitSet[state.selectedClassKey] || {};
  preview.textContent = JSON.stringify(resolved, null, 2);

  state.form = mountSchemaForm(host, {
    schema: entry.json_schema,
    resolved,
    explicit,
    allowInherit: state.scope.kind !== "platform",
    onDirty: (dirty) => {
      $("#apply").disabled = !dirty;
      $("#revert").disabled = !dirty;
    },
  });
}

async function onSave() {
  if (!state.form || !state.selectedClassKey) return;
  const patch = state.form.getPatch();
  if (!Object.keys(patch).length) {
    setStatus("No changes.", "");
    return;
  }
  const body = { [state.selectedClassKey]: patch };
  setStatus("Saving…", "");
  try {
    await patchConfigSet(state.scope, body);
    await refreshConfigsForScope();
    renderForm();
    setStatus("Saved.", "ok");
  } catch (e) {
    setStatus(`Save failed: ${e.message || e}`, "err");
  }
}

function onRevert() {
  if (state.form) state.form.reset();
  renderForm();
}

async function boot() {
  // Gate the page: require sysadmin.
  const me = await fetchMe();
  const isSysadmin = (me.roles || []).includes("sysadmin");
  if (!isSysadmin) {
    const main = document.querySelector("main");
    main.replaceChildren();
    const p = document.createElement("p");
    p.className = "empty-hint";
    p.textContent = "You need the 'sysadmin' role to access the Configuration Hub.";
    main.appendChild(p);
    return;
  }

  setStatus("Loading schemas…", "");
  try {
    state.schemas = await fetchSchemas();
  } catch (e) {
    setStatus(`Could not load schemas: ${e.message}`, "err");
    return;
  }

  mountSchemaList($("#schema-list"), {
    schemas: state.schemas,
    onSelect: async (classKey) => {
      state.selectedClassKey = classKey;
      renderForm();
    },
  });

  mountContextBar($("#context-bar"), {
    onChange: async (scope) => {
      state.scope = scope;
      try {
        setStatus("Loading configs…", "");
        await refreshConfigsForScope();
        setStatus("", "");
      } catch (e) {
        setStatus(`Load failed: ${e.message}`, "err");
      }
      renderForm();
    },
  });

  $("#apply").addEventListener("click", onSave);
  $("#revert").addEventListener("click", onRevert);
}

boot();
