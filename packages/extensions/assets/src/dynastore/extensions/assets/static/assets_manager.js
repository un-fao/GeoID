// assets_manager.js — Assets Manager page logic.
// Imports shared helpers from the always-on common/ layer.
import { getJSON, postJSON, deleteJSON } from "../static/common/api.js";
import { mountContextBar } from "../static/common/context-bar.js";
import { register, t, lang } from "../static/common/i18n.js";
import { uploadAsset } from "./upload.js";

// ---------------------------------------------------------------------------
// i18n dictionaries
// ---------------------------------------------------------------------------
register({
  en: {
    "page.title": "Assets Manager",
    "back": "Back to Home",
    "assets.section": "Assets",
    "select.prompt": "Select a catalog to browse assets.",
    "upload.section": "Upload Asset",
    "upload.asset_id": "Asset ID",
    "upload.file": "File",
    "upload.btn": "Upload",
    "upload.ok": "Upload initiated. Asset will be registered shortly.",
    "upload.polling": "Polling upload status…",
    "upload.completed": "Asset registered successfully.",
    "upload.failed": "Upload failed: {msg}",
    "upload.no_file": "Please select a file to upload.",
    "upload.no_cat": "Please select a catalog first.",
    "virtual.section": "Register Virtual Asset",
    "virtual.asset_id": "Asset ID",
    "virtual.href": "External URL",
    "virtual.btn": "Register",
    "virtual.ok": "Virtual asset registered.",
    "virtual.failed": "Registration failed: {msg}",
    "virtual.no_cat": "Please select a catalog first.",
    "delete.ok": "Asset deleted.",
    "delete.failed": "Delete failed: {msg}",
    "delete.confirm": "Delete asset {id}?",
    "asset.id": "ID",
    "asset.type": "Type",
    "asset.status": "Status",
    "asset.filename": "Filename",
    "asset.actions": "Actions",
    "asset.delete": "Delete",
    "asset.no_assets": "No assets found.",
    "asset.load_failed": "Failed to load assets: {msg}",
    "err.forbidden": "You do not have permission to perform this action.",
    "err.no_assets_endpoint": "Assets not available for the selected scope.",
  },
  fr: {
    "page.title": "Gestionnaire de ressources",
    "back": "Retour à l'accueil",
    "assets.section": "Ressources",
    "select.prompt": "Sélectionnez un catalogue pour explorer les ressources.",
    "upload.section": "Téléverser une ressource",
    "upload.asset_id": "Identifiant de ressource",
    "upload.file": "Fichier",
    "upload.btn": "Téléverser",
    "upload.ok": "Téléversement initié. La ressource sera bientôt enregistrée.",
    "upload.polling": "Vérification du statut…",
    "upload.completed": "Ressource enregistrée avec succès.",
    "upload.failed": "Échec du téléversement : {msg}",
    "upload.no_file": "Veuillez sélectionner un fichier.",
    "upload.no_cat": "Veuillez sélectionner un catalogue.",
    "virtual.section": "Enregistrer une ressource virtuelle",
    "virtual.asset_id": "Identifiant",
    "virtual.href": "URL externe",
    "virtual.btn": "Enregistrer",
    "virtual.ok": "Ressource virtuelle enregistrée.",
    "virtual.failed": "Échec de l'enregistrement : {msg}",
    "virtual.no_cat": "Veuillez sélectionner un catalogue.",
    "delete.ok": "Ressource supprimée.",
    "delete.failed": "Échec de la suppression : {msg}",
    "delete.confirm": "Supprimer la ressource {id} ?",
    "asset.id": "Identifiant",
    "asset.type": "Type",
    "asset.status": "Statut",
    "asset.filename": "Nom de fichier",
    "asset.actions": "Actions",
    "asset.delete": "Supprimer",
    "asset.no_assets": "Aucune ressource trouvée.",
    "asset.load_failed": "Échec du chargement des ressources : {msg}",
    "err.forbidden": "Vous n'avez pas la permission d'effectuer cette action.",
    "err.no_assets_endpoint": "Ressources non disponibles pour cette sélection.",
  },
  es: {
    "page.title": "Gestor de recursos",
    "back": "Volver al inicio",
    "assets.section": "Recursos",
    "select.prompt": "Seleccione un catálogo para explorar los recursos.",
    "upload.section": "Cargar recurso",
    "upload.asset_id": "ID de recurso",
    "upload.file": "Archivo",
    "upload.btn": "Cargar",
    "upload.ok": "Carga iniciada. El recurso se registrará en breve.",
    "upload.polling": "Verificando estado de carga…",
    "upload.completed": "Recurso registrado correctamente.",
    "upload.failed": "Error al cargar: {msg}",
    "upload.no_file": "Seleccione un archivo para cargar.",
    "upload.no_cat": "Seleccione un catálogo primero.",
    "virtual.section": "Registrar recurso virtual",
    "virtual.asset_id": "ID de recurso",
    "virtual.href": "URL externa",
    "virtual.btn": "Registrar",
    "virtual.ok": "Recurso virtual registrado.",
    "virtual.failed": "Error de registro: {msg}",
    "virtual.no_cat": "Seleccione un catálogo primero.",
    "delete.ok": "Recurso eliminado.",
    "delete.failed": "Error al eliminar: {msg}",
    "delete.confirm": "¿Eliminar recurso {id}?",
    "asset.id": "ID",
    "asset.type": "Tipo",
    "asset.status": "Estado",
    "asset.filename": "Nombre de archivo",
    "asset.actions": "Acciones",
    "asset.delete": "Eliminar",
    "asset.no_assets": "No se encontraron recursos.",
    "asset.load_failed": "Error al cargar los recursos: {msg}",
    "err.forbidden": "No tiene permiso para realizar esta acción.",
    "err.no_assets_endpoint": "Recursos no disponibles para la selección actual.",
  },
});

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let _catalogId = null;
let _collectionId = null;

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------
function applyLabels() {
  const set = (id, key) => { const el = document.getElementById(id); if (el) el.textContent = t(key); };
  set("page-title", "page.title");
  set("lbl-back", "back");
  set("lbl-assets-section", "assets.section");
  set("lbl-select-prompt", "select.prompt");
  set("lbl-upload-section", "upload.section");
  set("lbl-asset-id", "upload.asset_id");
  set("lbl-file", "upload.file");
  set("lbl-upload-btn", "upload.btn");
  set("lbl-virtual-section", "virtual.section");
  set("lbl-v-asset-id", "virtual.asset_id");
  set("lbl-v-href", "virtual.href");
  set("lbl-virtual-btn", "virtual.btn");
}

function showNotice(elId, msg, type) {
  const el = document.getElementById(elId);
  if (!el) return;
  // Build via DOM so msg (which may include server error text) is never parsed as HTML.
  el.textContent = "";
  const div = document.createElement("div");
  div.className = `notice notice-${type}`;
  div.textContent = msg;
  el.appendChild(div);
}

function clearNotice(elId) {
  const el = document.getElementById(elId);
  if (el) el.textContent = "";
}

// ---------------------------------------------------------------------------
// Asset table
// ---------------------------------------------------------------------------
function renderAssets(assets) {
  const container = document.getElementById("asset-list-container");
  if (!assets || assets.length === 0) {
    container.textContent = "";
    const empty = document.createElement("div");
    empty.className = "loading-spinner";
    empty.textContent = t("asset.no_assets");
    container.appendChild(empty);
    return;
  }
  // All API-derived values are passed through escHtml/escAttr before
  // being embedded in the template string — no raw server data reaches innerHTML.
  const rows = assets.map((a) => {
    const assetId = escAttr(a.asset_id || a.id || "");
    const filename = escHtml(a.filename || "—");
    const atype = escHtml(a.asset_type || a.kind || "—");
    const astatus = escHtml(a.status || "—");
    return `<tr>
      <td style="font-family:monospace;font-size:0.78rem;">${escHtml(a.asset_id || a.id || "")}</td>
      <td><span class="badge badge-blue">${atype}</span></td>
      <td><span class="badge badge-yellow">${astatus}</span></td>
      <td style="color:#94a3b8;">${filename}</td>
      <td>
        <button class="btn btn-danger btn-sm delete-btn" data-id="${assetId}" title="${escAttr(t("asset.delete"))}">
          <i class="fa-solid fa-trash-can"></i>
        </button>
      </td>
    </tr>`;
  }).join("");

  // t() values are developer-controlled dictionary strings; escHtml applied for defence-in-depth.
  container.innerHTML = `
    <table class="asset-table">
      <thead>
        <tr>
          <th>${escHtml(t("asset.id"))}</th>
          <th>${escHtml(t("asset.type"))}</th>
          <th>${escHtml(t("asset.status"))}</th>
          <th>${escHtml(t("asset.filename"))}</th>
          <th>${escHtml(t("asset.actions"))}</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;

  container.querySelectorAll(".delete-btn").forEach((btn) => {
    btn.addEventListener("click", () => handleDelete(btn.dataset.id));
  });
}

function escAttr(s) {
  return String(s).replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/'/g, "&#39;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function escHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ---------------------------------------------------------------------------
// Load assets
// ---------------------------------------------------------------------------
async function loadAssets() {
  if (!_catalogId) return;
  const container = document.getElementById("asset-list-container");
  // Spinner via DOM — no innerHTML with dynamic content.
  container.textContent = "";
  const spinner = document.createElement("div");
  spinner.className = "loading-spinner";
  const icon = document.createElement("i");
  icon.className = "fa-solid fa-circle-notch fa-spin";
  spinner.appendChild(icon);
  container.appendChild(spinner);
  try {
    const url = _collectionId
      ? `/assets/catalogs/${_catalogId}/collections/${_collectionId}?language=${lang()}`
      : `/assets/catalogs/${_catalogId}?language=${lang()}`;
    const data = await getJSON(url);
    const assets = Array.isArray(data) ? data : (data.assets || data.items || data.features || []);
    renderAssets(assets);
  } catch (err) {
    const code = err?.status || 0;
    container.textContent = "";
    const notice = document.createElement("div");
    notice.className = code === 401 || code === 403 ? "notice notice-warn" : "notice notice-err";
    notice.textContent = code === 401 || code === 403
      ? t("err.forbidden")
      : t("asset.load_failed", { msg: String(err?.message || err) });
    container.appendChild(notice);
  }
}

// ---------------------------------------------------------------------------
// Upload handler
// ---------------------------------------------------------------------------
async function handleUpload(ev) {
  ev.preventDefault();
  clearNotice("upload-status");

  if (!_catalogId) {
    showNotice("upload-status", t("upload.no_cat"), "warn");
    return;
  }
  const fileInput = document.getElementById("upload-file");
  const file = fileInput.files && fileInput.files[0];
  if (!file) {
    showNotice("upload-status", t("upload.no_file"), "warn");
    return;
  }
  const assetId = (document.getElementById("upload-asset-id").value || "").trim() || file.name;

  try {
    const ticket = await uploadAsset(_catalogId, _collectionId || null, file, assetId);
    showNotice("upload-status", t("upload.ok"), "ok");
    // Poll status once to confirm registration (best-effort)
    if (ticket && ticket.ticket_id) {
      setTimeout(async () => {
        try {
          const statusUrl = `/assets/catalogs/${_catalogId}/upload/${ticket.ticket_id}/status`;
          const s = await getJSON(statusUrl);
          if (s && s.status === "completed") {
            showNotice("upload-status", t("upload.completed"), "ok");
            await loadAssets();
          }
        } catch (_) { /* polling is best-effort */ }
      }, 2500);
    } else {
      await loadAssets();
    }
    document.getElementById("upload-form").reset();
  } catch (err) {
    const code = err?.status || 0;
    if (code === 401 || code === 403) {
      showNotice("upload-status", t("err.forbidden"), "warn");
    } else {
      showNotice("upload-status", t("upload.failed", { msg: err.message || err }), "err");
    }
  }
}

// ---------------------------------------------------------------------------
// Virtual asset handler
// ---------------------------------------------------------------------------
async function handleVirtual(ev) {
  ev.preventDefault();
  clearNotice("virtual-status");

  if (!_catalogId) {
    showNotice("virtual-status", t("virtual.no_cat"), "warn");
    return;
  }
  const assetId = (document.getElementById("virtual-asset-id").value || "").trim();
  const href = (document.getElementById("virtual-href").value || "").trim();
  if (!assetId || !href) return;

  const url = _collectionId
    ? `/assets/catalogs/${_catalogId}/collections/${_collectionId}/virtual-assets`
    : `/assets/catalogs/${_catalogId}/virtual-assets`;

  try {
    await postJSON(url, {
      asset_id: assetId,
      href,
      kind: "VIRTUAL",
    });
    showNotice("virtual-status", t("virtual.ok"), "ok");
    await loadAssets();
    document.getElementById("virtual-form").reset();
  } catch (err) {
    const code = err?.status || 0;
    if (code === 401 || code === 403) {
      showNotice("virtual-status", t("err.forbidden"), "warn");
    } else {
      showNotice("virtual-status", t("virtual.failed", { msg: err.message || err }), "err");
    }
  }
}

// ---------------------------------------------------------------------------
// Delete handler
// ---------------------------------------------------------------------------
async function handleDelete(assetId) {
  clearNotice("delete-status");
  if (!_catalogId || !assetId) return;
  if (!window.confirm(t("delete.confirm", { id: assetId }))) return;

  try {
    await deleteJSON(`/assets/catalogs/${_catalogId}/assets/${encodeURIComponent(assetId)}`);
    showNotice("delete-status", t("delete.ok"), "ok");
    await loadAssets();
  } catch (err) {
    const code = err?.status || 0;
    if (code === 401 || code === 403) {
      showNotice("delete-status", t("err.forbidden"), "warn");
    } else {
      showNotice("delete-status", t("delete.failed", { msg: err.message || err }), "err");
    }
  }
}

// ---------------------------------------------------------------------------
// Initialise
// ---------------------------------------------------------------------------
function init() {
  applyLabels();

  document.getElementById("upload-form").addEventListener("submit", handleUpload);
  document.getElementById("virtual-form").addEventListener("submit", handleVirtual);

  mountContextBar(document.getElementById("context-bar-mount"), {
    mode: "select",
    onChange: ({ catalogId, collectionId }) => {
      _catalogId = catalogId || null;
      _collectionId = collectionId || null;
      clearNotice("upload-status");
      clearNotice("virtual-status");
      clearNotice("delete-status");
      loadAssets();
    },
  });
}

init();
