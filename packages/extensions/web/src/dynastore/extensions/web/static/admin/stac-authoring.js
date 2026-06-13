// STAC authoring — catalog + collection creation forms.
// Sysadmins create catalogs; anyone with write on a catalog creates
// collections under it. Gating relies on the server — we just show a
// friendly message if the POST comes back 403.

import {
  fetchMe, fetchCatalogs, fetchCatalogOptions,
  createStacCatalog, createStacCollection,
} from "../common/api.js";

const $ = (s) => document.querySelector(s);

const state = {
  isSysadmin: false,
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

// Navigate to an admin page by id.  Works both embedded in the shell (via
// parent.switchTab) and standalone (hash navigation falls back to the server).
function goToAdminPage(pageId) {
  if (
    window.parent !== window &&
    typeof window.parent.switchTab === "function"
  ) {
    window.parent.switchTab(pageId);
  } else {
    window.location.href = "#" + pageId;
  }
}

// Render a "Next steps" affordance block inserted after the given status span.
// catalogId is user-supplied; it is set via textContent only — never innerHTML.
function showNextSteps(afterStatusSel, catalogId) {
  const statusEl = $(afterStatusSel);
  if (!statusEl) return;

  // Remove any previously-injected block so repeated submissions stay clean.
  const prev = statusEl.parentElement.querySelector(".next-steps-block");
  if (prev) prev.remove();

  const block = document.createElement("div");
  block.className = "next-steps-block";
  block.style.cssText =
    "margin-top:1rem;padding:0.75rem 1rem;" +
    "border:1px solid rgba(59,130,246,0.2);border-radius:0.5rem;" +
    "background:rgba(59,130,246,0.05);font-size:0.85rem;";

  // Heading — catalogId set via textContent, never concatenated into HTML.
  const heading = document.createElement("p");
  heading.style.cssText = "font-weight:600;margin-bottom:0.5rem;color:#93c5fd;";
  heading.textContent = "Next steps for ";
  const code = document.createElement("code");
  code.style.color = "#e2e8f0";
  code.textContent = catalogId;
  heading.appendChild(code);
  block.appendChild(heading);

  const row = document.createElement("div");
  row.style.cssText = "display:flex;flex-wrap:wrap;gap:0.5rem;";

  const steps = [
    { icon: "fa-shield-halved", label: "Set up access (Governance)", page: "governance" },
    { icon: "fa-sliders",        label: "Apply a preset (Presets)",   page: "presets"    },
    { icon: "fa-broadcast-tower", label: "Enable services (Exposure)", page: "exposure"  },
  ];
  steps.forEach(({ icon, label, page }) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.style.cssText =
      "display:inline-flex;align-items:center;gap:0.4rem;" +
      "padding:0.35rem 0.75rem;border-radius:0.4rem;font-size:0.78rem;" +
      "background:rgba(30,41,59,0.8);border:1px solid rgba(255,255,255,0.1);" +
      "color:#e2e8f0;cursor:pointer;";
    // icon and label are hardcoded constants, not user input.
    const ic = document.createElement("i");
    ic.className = "fa-solid " + icon + " text-blue-400";
    ic.style.fontSize = "0.75rem";
    btn.appendChild(ic);
    btn.appendChild(document.createTextNode(" " + label));
    btn.addEventListener("click", () => goToAdminPage(page));
    row.appendChild(btn);
  });
  block.appendChild(row);

  statusEl.parentElement.appendChild(block);
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

  let raw;
  try {
    raw = await fetchCatalogs();
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
  const items = Array.isArray(raw) ? raw : (raw.catalogs || raw.items || []);
  state.allCatalogs = items;
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

  // Repopulate the catalog <select> for collection creation using the
  // grant-filtered list so non-sysadmin catalog admins see only their tenants.
  const sel = $("#col-catalog");
  clearNode(sel);
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = "Choose catalog…";
  sel.appendChild(opt0);

  let allowed = [];
  try {
    allowed = await fetchCatalogOptions();
  } catch (_) { /* leave the select empty on error */ }

  for (const c of allowed) {
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
    showNextSteps("#cat-status", body.id);
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
    showNextSteps("#col-status", catalogId);
    $("#collection-create").reset();
  } catch (e) {
    setStatus("#col-status", `Failed: ${e.message}`, "err");
  }
}

async function boot() {
  const me = await fetchMe();
  const roles = me.roles || [];
  state.isSysadmin = roles.includes("sysadmin");

  // Gate: the user needs sysadmin or at least one accessible catalog to author resources.
  if (!state.isSysadmin) {
    let accessible = [];
    try {
      accessible = await fetchCatalogOptions();
    } catch (_) { /* deny on error */ }

    if (!accessible.length) {
      const main = document.querySelector("main");
      clearNode(main);
      const p = document.createElement("p");
      p.className = "subtitle";
      p.textContent =
        "You need sysadmin, or a role on at least one catalog, to author STAC resources.";
      main.appendChild(p);
      return;
    }
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
