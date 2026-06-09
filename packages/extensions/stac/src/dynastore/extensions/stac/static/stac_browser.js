// stac_browser.js — STAC browser page logic.
// Browse catalogs → collections → items; inline map preview; create catalog/collection.

import { getJSON, postJSON } from "../static/common/api.js";
import { register, t, lang } from "../static/common/i18n.js";
import { initMap, showGeoJSON } from "../static/common/leaflet-map.js";

// ---------------------------------------------------------------------------
// i18n dictionaries
// ---------------------------------------------------------------------------

register({
  en: {
    "nav.catalogs":        "Catalogs",
    "nav.collections":     "Collections",
    "nav.items":           "Items",
    "nav.item_detail":     "Item detail",

    "sidebar.filter":      "Filter…",
    "sidebar.no_results":  "No results.",
    "sidebar.loading":     "Loading…",
    "sidebar.back":        "Back",

    "detail.placeholder":  "Select a catalog or collection from the sidebar to explore it.",
    "detail.spatial":      "Spatial extent",
    "detail.temporal":     "Temporal extent",
    "detail.west_south":   "West / South",
    "detail.east_north":   "East / North",
    "detail.from":         "From",
    "detail.to":           "To",
    "detail.collections":  "Collections",
    "detail.items":        "Items",
    "detail.links":        "Links",
    "detail.properties":   "Properties",
    "detail.geometry":     "Geometry preview",

    "item.id":             "ID",
    "item.date":           "Date",
    "item.view_on_map":    "View on map",

    "create.catalog":      "Create catalog",
    "create.collection":   "Create collection",
    "create.id":           "ID",
    "create.title":        "Title",
    "create.description":  "Description",
    "create.cancel":       "Cancel",
    "create.submit":       "Create",
    "create.ok_catalog":   "Catalog created.",
    "create.ok_collection":"Collection created.",

    "err.forbidden":       "You do not have permission for this action.",
    "err.load":            "Failed to load: {msg}",
    "err.create":          "Create failed: {msg}",
  },
  fr: {
    "nav.catalogs":        "Catalogues",
    "nav.collections":     "Collections",
    "nav.items":           "Éléments",
    "nav.item_detail":     "Détail de l'élément",

    "sidebar.filter":      "Filtrer…",
    "sidebar.no_results":  "Aucun résultat.",
    "sidebar.loading":     "Chargement…",
    "sidebar.back":        "Retour",

    "detail.placeholder":  "Sélectionnez un catalogue ou une collection dans la barre latérale.",
    "detail.spatial":      "Étendue spatiale",
    "detail.temporal":     "Étendue temporelle",
    "detail.west_south":   "Ouest / Sud",
    "detail.east_north":   "Est / Nord",
    "detail.from":         "De",
    "detail.to":           "À",
    "detail.collections":  "Collections",
    "detail.items":        "Éléments",
    "detail.links":        "Liens",
    "detail.properties":   "Propriétés",
    "detail.geometry":     "Aperçu géométrique",

    "item.id":             "ID",
    "item.date":           "Date",
    "item.view_on_map":    "Voir sur la carte",

    "create.catalog":      "Créer un catalogue",
    "create.collection":   "Créer une collection",
    "create.id":           "ID",
    "create.title":        "Titre",
    "create.description":  "Description",
    "create.cancel":       "Annuler",
    "create.submit":       "Créer",
    "create.ok_catalog":   "Catalogue créé.",
    "create.ok_collection":"Collection créée.",

    "err.forbidden":       "Vous n'avez pas la permission d'effectuer cette action.",
    "err.load":            "Échec du chargement : {msg}",
    "err.create":          "Échec de la création : {msg}",
  },
  es: {
    "nav.catalogs":        "Catálogos",
    "nav.collections":     "Colecciones",
    "nav.items":           "Elementos",
    "nav.item_detail":     "Detalle del elemento",

    "sidebar.filter":      "Filtrar…",
    "sidebar.no_results":  "Sin resultados.",
    "sidebar.loading":     "Cargando…",
    "sidebar.back":        "Volver",

    "detail.placeholder":  "Seleccione un catálogo o colección en la barra lateral.",
    "detail.spatial":      "Extensión espacial",
    "detail.temporal":     "Extensión temporal",
    "detail.west_south":   "Oeste / Sur",
    "detail.east_north":   "Este / Norte",
    "detail.from":         "Desde",
    "detail.to":           "Hasta",
    "detail.collections":  "Colecciones",
    "detail.items":        "Elementos",
    "detail.links":        "Vínculos",
    "detail.properties":   "Propiedades",
    "detail.geometry":     "Vista previa de geometría",

    "item.id":             "ID",
    "item.date":           "Fecha",
    "item.view_on_map":    "Ver en el mapa",

    "create.catalog":      "Crear catálogo",
    "create.collection":   "Crear colección",
    "create.id":           "ID",
    "create.title":        "Título",
    "create.description":  "Descripción",
    "create.cancel":       "Cancelar",
    "create.submit":       "Crear",
    "create.ok_catalog":   "Catálogo creado.",
    "create.ok_collection":"Colección creada.",

    "err.forbidden":       "No tiene permiso para realizar esta acción.",
    "err.load":            "Error al cargar: {msg}",
    "err.create":          "Error al crear: {msg}",
  },
});

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let _catalogId = null;   // currently selected catalog
let _collectionId = null; // currently selected collection

// Leaflet map + layer ref for item geometry preview
let _map = null;
const _layerRef = { current: null };

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

function el(id) { return document.getElementById(id); }

function setLoading(msg) {
  const list = el("items-list");
  list.innerHTML = "";
  const wrap = document.createElement("div");
  wrap.className = "loading-spinner";
  // Static author-controlled icon markup — no API data interpolated.
  const icon = document.createElement("i");
  icon.className = "fa-solid fa-circle-notch fa-spin text-xl";
  const p = document.createElement("p");
  p.className = "mt-2 text-sm";
  // msg is always a translation string (t(...)) or undefined — plain text only.
  p.textContent = msg || t("sidebar.loading");
  wrap.appendChild(icon);
  wrap.appendChild(p);
  list.appendChild(wrap);
}

function setListError(msg) {
  // msg is always produced by t(...) with e.message substituted — plain text.
  const list = el("items-list");
  list.innerHTML = "";
  const wrap = document.createElement("div");
  wrap.className = "error-box m-2";
  // Static author-controlled icon markup — no API data interpolated.
  const icon = document.createElement("i");
  icon.className = "fa-solid fa-triangle-exclamation mr-2";
  const span = document.createElement("span");
  span.textContent = msg;
  wrap.appendChild(icon);
  wrap.appendChild(span);
  list.appendChild(wrap);
}

function showNotice(container, text, isError) {
  const div = document.createElement("div");
  div.className = isError ? "error-box" : "success-box";
  div.textContent = text;
  container.prepend(div);
  setTimeout(() => div.remove(), 5000);
}

function setBreadcrumb(parts) {
  const bc = el("breadcrumb");
  if (!bc) return;
  bc.innerHTML = parts
    .map((p, i) =>
      i < parts.length - 1
        ? `<span class="bc-link" data-idx="${i}">${_escHtml(p.label)}</span> <span class="mx-1">/</span>`
        : `<span class="text-slate-400">${_escHtml(p.label)}</span>`
    )
    .join("");
  bc.querySelectorAll(".bc-link").forEach((node) => {
    const idx = parseInt(node.dataset.idx, 10);
    node.addEventListener("click", () => parts[idx].action());
  });
}

function _escHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ---------------------------------------------------------------------------
// Auth — gate create buttons client-side (API enforces regardless)
// ---------------------------------------------------------------------------

async function resolveCanWrite() {
  try {
    const me = await getJSON("/iam/me");
    const roles = me.roles || [];
    // Treat any authenticated principal with a role as potentially allowed;
    // the API will return 403 if the specific action is denied.
    return roles.length > 0;
  } catch (e) {
    if (e.status === 404 || e.status === 401) {
      // No-IAM deployment or anonymous — show forms and let the API decide.
      return true;
    }
    // Other errors (network, 500) — default to showing forms; API enforces.
    return true;
  }
}

// ---------------------------------------------------------------------------
// Sidebar rendering — catalog list
// ---------------------------------------------------------------------------

async function loadCatalogs() {
  setLoading();
  _catalogId = null;
  _collectionId = null;
  setBreadcrumb([{ label: t("nav.catalogs"), action: loadCatalogs }]);
  el("btn-create-catalog").style.display = "";
  el("btn-create-collection").style.display = "none";

  try {
    const data = await getJSON("/stac/catalogs?language=" + lang());
    const list = Array.isArray(data) ? data : (data.catalogs || data.collections || []);
    if (list.length === 0) {
      setLoading(t("sidebar.no_results"));
      return;
    }
    renderSidebarItems(
      list.map((c) => ({
        id: c.id,
        label: c.title || c.id,
        sub: "catalog",
        icon: "fa-database",
        action: () => loadCollections(c.id, c.title || c.id),
      }))
    );
  } catch (e) {
    setListError(t("err.load", { msg: e.message }));
  }
}

// ---------------------------------------------------------------------------
// Sidebar rendering — collection list
// ---------------------------------------------------------------------------

async function loadCollections(catalogId, catalogLabel) {
  setLoading();
  _catalogId = catalogId;
  _collectionId = null;
  setBreadcrumb([
    { label: t("nav.catalogs"), action: loadCatalogs },
    { label: catalogLabel || catalogId, action: () => loadCollections(catalogId, catalogLabel) },
  ]);
  el("btn-create-catalog").style.display = "none";
  el("btn-create-collection").style.display = "";

  try {
    const data = await getJSON(
      "/stac/catalogs/" + encodeURIComponent(catalogId) + "/collections?language=" + lang()
    );
    const list = Array.isArray(data)
      ? data
      : (data.collections || data.items || []);
    if (list.length === 0) {
      setLoading(t("sidebar.no_results"));
      return;
    }
    renderSidebarItems(
      list.map((c) => ({
        id: c.id,
        label: c.title || c.id,
        sub: "collection",
        icon: "fa-folder-open",
        action: () => loadItems(catalogId, c.id, catalogLabel, c.title || c.id),
      }))
    );

    // Show catalog summary in detail pane
    showDetailPlaceholder(catalogLabel || catalogId, t("nav.collections"));
  } catch (e) {
    setListError(t("err.load", { msg: e.message }));
  }
}

// ---------------------------------------------------------------------------
// Sidebar rendering — item list
// ---------------------------------------------------------------------------

async function loadItems(catalogId, collectionId, catalogLabel, collectionLabel) {
  setLoading();
  _catalogId = catalogId;
  _collectionId = collectionId;
  setBreadcrumb([
    { label: t("nav.catalogs"), action: loadCatalogs },
    { label: catalogLabel || catalogId, action: () => loadCollections(catalogId, catalogLabel) },
    { label: collectionLabel || collectionId, action: () => loadItems(catalogId, collectionId, catalogLabel, collectionLabel) },
  ]);
  el("btn-create-catalog").style.display = "none";
  el("btn-create-collection").style.display = "none";

  try {
    const data = await getJSON(
      "/stac/catalogs/" +
        encodeURIComponent(catalogId) +
        "/collections/" +
        encodeURIComponent(collectionId) +
        "/items?language=" +
        lang()
    );
    const features = Array.isArray(data) ? data : (data.features || data.items || []);
    if (features.length === 0) {
      setLoading(t("sidebar.no_results"));
      return;
    }
    renderSidebarItems(
      features.map((f) => ({
        id: f.id,
        label: f.id,
        sub: f.properties?.datetime || f.properties?.start_datetime || "",
        icon: "fa-file",
        action: () => showItemDetail(f),
      }))
    );

    // Show collection summary in detail pane
    showDetailPlaceholder(collectionLabel || collectionId, t("nav.items") + " (" + features.length + ")");
  } catch (e) {
    setListError(t("err.load", { msg: e.message }));
  }
}

// ---------------------------------------------------------------------------
// Sidebar item rendering
// ---------------------------------------------------------------------------

let _sidebarItems = [];

function renderSidebarItems(items) {
  _sidebarItems = items;
  applySidebarFilter("");
  const box = el("search-box");
  if (box) box.value = "";
}

function applySidebarFilter(q) {
  const filtered = q
    ? _sidebarItems.filter((i) => (i.label + " " + i.id).toLowerCase().includes(q.toLowerCase()))
    : _sidebarItems;

  const list = el("items-list");
  if (filtered.length === 0) {
    setLoading(t("sidebar.no_results"));
    return;
  }
  // All API-sourced values (item.id, item.label, item.sub) pass through _escHtml().
  // item.icon is a hardcoded FontAwesome class string set by this module, not from API data.
  // Click handlers are wired via addEventListener below, not via onclick= attributes.
  list.innerHTML = filtered
    .map(
      (item) => `
    <div class="item-card" data-id="${_escHtml(item.id)}" role="button" tabindex="0">
      <span class="icon"><i class="fa-solid ${_escHtml(item.icon)}"></i></span>
      <div style="min-width:0">
        <div class="label">${_escHtml(item.label)}</div>
        ${item.sub ? `<div class="sublabel">${_escHtml(item.sub)}</div>` : ""}
      </div>
    </div>
  `
    )
    .join("");

  list.querySelectorAll(".item-card").forEach((card) => {
    const id = card.dataset.id;
    const item = filtered.find((i) => i.id === id);
    if (!item) return;
    card.addEventListener("click", () => {
      list.querySelectorAll(".item-card").forEach((c) => c.classList.remove("active"));
      card.classList.add("active");
      item.action();
    });
    card.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") card.click();
    });
  });
}

// ---------------------------------------------------------------------------
// Detail pane — placeholder
// ---------------------------------------------------------------------------

function showDetailPlaceholder(title, subtitle) {
  const pane = el("detail-pane");
  pane.innerHTML = "";
  const h = document.createElement("div");
  h.id = "detail-title";
  h.textContent = title;
  const s = document.createElement("div");
  s.id = "detail-desc";
  s.className = "detail-meta";
  s.textContent = subtitle;
  pane.appendChild(h);
  pane.appendChild(s);
}

// ---------------------------------------------------------------------------
// Detail pane — item detail with geometry preview
// ---------------------------------------------------------------------------

function showItemDetail(feature) {
  const pane = el("detail-pane");
  const props = feature.properties || {};
  const datetime = props.datetime || props.start_datetime || "—";

  // All API-sourced values (feature.id, datetime, property keys and values) are
  // passed through _escHtml() before being interpolated into innerHTML. The only
  // unescaped strings are t(...) translation constants and hardcoded CSS/HTML
  // structure, both of which are author-controlled.
  let propsRows = "";
  for (const [k, v] of Object.entries(props)) {
    propsRows += `<tr><td>${_escHtml(k)}</td><td>${_escHtml(JSON.stringify(v))}</td></tr>`;
  }

  pane.innerHTML = `
    <div id="detail-title">${_escHtml(feature.id)}</div>
    <div class="detail-meta">${_escHtml(datetime)}</div>

    ${feature.geometry ? `
      <div class="detail-section-title">${t("detail.geometry")}</div>
      <div id="stac-map" style="height:260px;border-radius:0.5rem;overflow:hidden;margin-bottom:1rem;"></div>
    ` : ""}

    ${propsRows ? `
      <div class="detail-section-title">${t("detail.properties")}</div>
      <table class="prop-table">${propsRows}</table>
    ` : ""}
  `;

  // Initialize or re-init the Leaflet map inside the detail pane
  if (feature.geometry) {
    if (_map) {
      _map.remove();
      _map = null;
      _layerRef.current = null;
    }
    _map = initMap("stac-map");
    showGeoJSON(_map, _layerRef, feature.geometry);
  }
}

// ---------------------------------------------------------------------------
// Create catalog form
// ---------------------------------------------------------------------------

function toggleCreateCatalogForm(show) {
  const form = el("form-create-catalog");
  if (form) form.hidden = !show;
  if (show) el("input-catalog-id")?.focus();
}

async function submitCreateCatalog(e) {
  e.preventDefault();
  const id = el("input-catalog-id").value.trim();
  const title = el("input-catalog-title").value.trim();
  const description = el("input-catalog-description").value.trim();
  if (!id) return;

  const btn = el("btn-submit-catalog");
  btn.disabled = true;
  try {
    await postJSON("/stac/catalogs", {
      id,
      type: "Catalog",
      stac_version: "1.0.0",
      ...(title ? { title } : {}),
      ...(description ? { description } : {}),
      links: [],
    });
    showNotice(el("form-create-catalog"), t("create.ok_catalog"), false);
    toggleCreateCatalogForm(false);
    el("form-create-catalog").reset();
    await loadCatalogs();
  } catch (e) {
    if (e.status === 401 || e.status === 403) {
      showNotice(el("form-create-catalog"), t("err.forbidden"), true);
    } else {
      showNotice(el("form-create-catalog"), t("err.create", { msg: e.message }), true);
    }
  } finally {
    btn.disabled = false;
  }
}

// ---------------------------------------------------------------------------
// Create collection form
// ---------------------------------------------------------------------------

function toggleCreateCollectionForm(show) {
  const form = el("form-create-collection");
  if (form) form.hidden = !show;
  if (show) el("input-coll-id")?.focus();
}

async function submitCreateCollection(e) {
  e.preventDefault();
  if (!_catalogId) return;
  const id = el("input-coll-id").value.trim();
  const title = el("input-coll-title").value.trim();
  const description = el("input-coll-description").value.trim();
  if (!id) return;

  const btn = el("btn-submit-coll");
  btn.disabled = true;
  try {
    await postJSON(
      "/stac/catalogs/" + encodeURIComponent(_catalogId) + "/collections",
      {
        id,
        type: "Collection",
        stac_version: "1.0.0",
        ...(title ? { title } : {}),
        ...(description ? { description } : {}),
        extent: {
          spatial: { bbox: [[-180, -90, 180, 90]] },
          temporal: { interval: [[null, null]] },
        },
        links: [],
      }
    );
    showNotice(el("form-create-collection"), t("create.ok_collection"), false);
    toggleCreateCollectionForm(false);
    el("form-create-collection").reset();
    // Refresh the collection list for the current catalog
    const catalogLabel = el("breadcrumb")?.querySelectorAll(".bc-link")[0]?.textContent || _catalogId;
    await loadCollections(_catalogId, catalogLabel);
  } catch (e) {
    if (e.status === 401 || e.status === 403) {
      showNotice(el("form-create-collection"), t("err.forbidden"), true);
    } else {
      showNotice(el("form-create-collection"), t("err.create", { msg: e.message }), true);
    }
  } finally {
    btn.disabled = false;
  }
}

// ---------------------------------------------------------------------------
// Wire-up labels (apply translated text to static DOM nodes)
// ---------------------------------------------------------------------------

function applyLabels() {
  const map = {
    "lbl-create-catalog":     "create.catalog",
    "lbl-create-collection":  "create.collection",
    "lbl-cat-id":             "create.id",
    "lbl-cat-title":          "create.title",
    "lbl-cat-desc":           "create.description",
    "lbl-coll-id":            "create.id",
    "lbl-coll-title":         "create.title",
    "lbl-coll-desc":          "create.description",
  };
  for (const [id, key] of Object.entries(map)) {
    const node = el(id);
    if (node) node.textContent = t(key);
  }
  const cancelBtns = document.querySelectorAll(".btn-cancel-form");
  cancelBtns.forEach((b) => { b.textContent = t("create.cancel"); });
  const submitBtns = document.querySelectorAll(".btn-submit-form");
  submitBtns.forEach((b) => { b.textContent = t("create.submit"); });
  const filterBox = el("search-box");
  if (filterBox) filterBox.placeholder = t("sidebar.filter");
  const btnCat = el("btn-create-catalog");
  if (btnCat) btnCat.textContent = "+ " + t("create.catalog");
  const btnColl = el("btn-create-collection");
  if (btnColl) btnColl.textContent = "+ " + t("create.collection");
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

async function init() {
  applyLabels();

  // Search / filter
  const searchBox = el("search-box");
  if (searchBox) {
    searchBox.addEventListener("input", (e) => applySidebarFilter(e.target.value));
  }

  // Create catalog button
  const btnCat = el("btn-create-catalog");
  if (btnCat) {
    btnCat.addEventListener("click", () => toggleCreateCatalogForm(true));
  }
  const formCat = el("form-create-catalog");
  if (formCat) {
    formCat.addEventListener("submit", submitCreateCatalog);
    formCat.querySelector(".btn-cancel-form")?.addEventListener("click", (e) => {
      e.preventDefault();
      toggleCreateCatalogForm(false);
    });
  }

  // Create collection button
  const btnColl = el("btn-create-collection");
  if (btnColl) {
    btnColl.addEventListener("click", () => toggleCreateCollectionForm(true));
  }
  const formColl = el("form-create-collection");
  if (formColl) {
    formColl.addEventListener("submit", submitCreateCollection);
    formColl.querySelector(".btn-cancel-form")?.addEventListener("click", (e) => {
      e.preventDefault();
      toggleCreateCollectionForm(false);
    });
  }

  // Gate create buttons based on auth
  const canWrite = await resolveCanWrite();
  if (!canWrite) {
    if (btnCat) btnCat.style.display = "none";
    if (btnColl) btnColl.style.display = "none";
  }

  // Load catalogs
  await loadCatalogs();
}

init();
