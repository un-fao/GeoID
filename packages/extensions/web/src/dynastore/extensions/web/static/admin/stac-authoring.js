// STAC authoring — catalog + collection creation forms.
// Sysadmins create catalogs; anyone with write on a catalog creates
// collections under it. Gating relies on the server — we just show a
// friendly message if the POST comes back 403.

import {
  fetchMe, fetchCatalogs, fetchMyCatalogs,
  createStacCatalog, createStacCollection,
} from "../common/api.js";

const $ = (s) => document.querySelector(s);

const state = {
  isSysadmin: false,
  myCatalogs: [],
  allCatalogs: [],
};

function csv(s) {
  return String(s || "").split(",").map((t) => t.trim()).filter(Boolean);
}

function clearNode(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function setStatus(sel, msg, cls = "") {
  const el = $(sel);
  if (!el) return;
  el.textContent = msg || "";
  el.className = "status " + cls;
}

function switchTab(name) {
  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === name);
  });
  document.querySelectorAll(".tab-panel").forEach((p) => {
    p.classList.toggle("active", p.id === `tab-${name}`);
  });
}

async function refreshCatalogs() {
  const tbody = $("#catalogs-table tbody");
  clearNode(tbody);
  const loading = document.createElement("tr");
  loading.className = "empty-row";
  const td = document.createElement("td");
  td.colSpan = 3;
  td.textContent = "Loading…";
  loading.appendChild(td);
  tbody.appendChild(loading);

  try {
    state.allCatalogs = await fetchCatalogs();
  } catch (e) {
    clearNode(tbody);
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 3;
    td.textContent = `Load failed: ${e.message}`;
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  clearNode(tbody);
  // /catalogs may return a bare list or {catalogs: [...]}.
  const items = Array.isArray(state.allCatalogs)
    ? state.allCatalogs
    : (state.allCatalogs.catalogs || state.allCatalogs.items || []);
  if (!items.length) {
    const tr = document.createElement("tr");
    tr.className = "empty-row";
    const td = document.createElement("td");
    td.colSpan = 3;
    td.textContent = "No catalogs yet — charter one above.";
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    for (const c of items) {
      const tr = document.createElement("tr");
      const id = document.createElement("td");
      id.textContent = c.id || "—";
      const title = document.createElement("td");
      title.textContent =
        typeof c.title === "string"
          ? c.title
          : (c.title && (c.title.en || Object.values(c.title)[0])) || "—";
      const lic = document.createElement("td");
      lic.className = "muted";
      lic.textContent = c.license || "—";
      tr.append(id, title, lic);
      tbody.appendChild(tr);
    }
  }

  // Repopulate the catalog <select> for collection creation.
  const sel = $("#col-catalog");
  clearNode(sel);
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = "Choose catalog…";
  sel.appendChild(opt0);

  const allowed = state.isSysadmin
    ? items.map((c) => c.id).filter(Boolean)
    : state.myCatalogs.map((c) => c.catalog_id);
  const allowedSet = new Set(allowed);
  for (const c of items) {
    if (!c.id || !allowedSet.has(c.id)) continue;
    const o = document.createElement("option");
    o.value = c.id;
    o.textContent = c.id;
    sel.appendChild(o);
  }
}

async function onCreateCatalog(e) {
  e.preventDefault();
  if (!state.isSysadmin) {
    setStatus("#cat-status", "Only sysadmins can charter catalogs.", "err");
    return;
  }
  const body = {
    id: $("#cat-id").value.trim(),
    title: $("#cat-title").value.trim(),
    description: $("#cat-description").value.trim() || undefined,
    license: $("#cat-license").value.trim() || undefined,
    keywords: csv($("#cat-keywords").value),
  };
  if (!body.id || !body.title) {
    setStatus("#cat-status", "ID and title are required.", "err");
    return;
  }
  setStatus("#cat-status", "Chartering…", "");
  try {
    await createStacCatalog(body);
    setStatus("#cat-status", `Catalog "${body.id}" chartered.`, "ok");
    $("#catalog-create").reset();
    refreshCatalogs();
  } catch (e) {
    setStatus("#cat-status", `Failed: ${e.message}`, "err");
  }
}

function parseBbox(s) {
  if (!s) return undefined;
  const parts = s.split(",").map((t) => Number(t.trim())).filter((n) => Number.isFinite(n));
  if (parts.length !== 4) return undefined;
  return [parts];
}

function parseTemporal(s) {
  if (!s) return undefined;
  const [a, b] = s.split("/");
  const norm = (t) => (t && t.trim() && t.trim() !== "null" ? t.trim() : null);
  return [[norm(a), norm(b)]];
}

async function onCreateCollection(e) {
  e.preventDefault();
  const catalogId = $("#col-catalog").value.trim();
  if (!catalogId) {
    setStatus("#col-status", "Pick a catalog.", "err");
    return;
  }
  const id = $("#col-id").value.trim();
  const title = $("#col-title").value.trim();
  const description = $("#col-description").value.trim();
  if (!id || !title || !description) {
    setStatus("#col-status", "ID, title and description are required.", "err");
    return;
  }

  const spatial = parseBbox($("#col-bbox").value);
  const temporal = parseTemporal($("#col-temporal").value);
  const body = {
    id, title, description,
    type: "Collection",
    stac_version: "1.0.0",
    license: $("#col-license").value.trim() || "proprietary",
    extent: {
      spatial: { bbox: spatial || [[-180, -90, 180, 90]] },
      temporal: { interval: temporal || [[null, null]] },
    },
  };

  setStatus("#col-status", "Commissioning…", "");
  try {
    await createStacCollection(catalogId, body);
    setStatus("#col-status", `Collection "${id}" commissioned under "${catalogId}".`, "ok");
    $("#collection-create").reset();
  } catch (e) {
    setStatus("#col-status", `Failed: ${e.message}`, "err");
  }
}

async function boot() {
  const me = await fetchMe();
  const roles = me.roles || [];
  state.isSysadmin = roles.includes("sysadmin");

  try {
    state.myCatalogs = await fetchMyCatalogs();
  } catch (_) {
    state.myCatalogs = [];
  }

  if (!state.isSysadmin && !state.myCatalogs.length) {
    const main = document.querySelector("main");
    clearNode(main);
    const p = document.createElement("p");
    p.className = "subtitle";
    p.textContent =
      "You need sysadmin, or a role on at least one catalog, to author STAC resources.";
    main.appendChild(p);
    return;
  }

  // Disable the catalog-create form for non-sysadmins (backend will also block).
  if (!state.isSysadmin) {
    $("#catalog-create").querySelectorAll("input,button,textarea").forEach((el) => {
      el.disabled = true;
    });
    setStatus("#cat-status", "Sysadmin only.", "warn");
  }

  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.addEventListener("click", () => switchTab(b.dataset.tab));
  });

  $("#catalog-create").addEventListener("submit", onCreateCatalog);
  $("#collection-create").addEventListener("submit", onCreateCollection);

  refreshCatalogs();
}

boot();
