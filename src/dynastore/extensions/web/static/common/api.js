// Thin fetch helper for the admin Hub. Non-2xx responses throw with the
// response body attached. Handles the three config scopes (platform,
// catalog, collection) uniformly so page code never builds URLs by hand.
//
// Every absolute path passed in is run through apiUrl() so the proxy prefix
// (e.g. /geospatial/v2/api/catalog) is preserved when served behind a
// reverse proxy. Call sites can keep passing absolute paths like
// "/admin/roles" — the prefix is applied transparently.

import { apiUrl } from "./url.js";

export async function getJSON(url) {
  const target = apiUrl(url);
  const r = await fetch(target, { credentials: "same-origin" });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    const err = new Error(`${r.status} ${target}: ${text.slice(0, 400)}`);
    err.status = r.status;
    err.body = text;
    throw err;
  }
  return r.json();
}

export async function patchJSON(url, body) {
  const target = apiUrl(url);
  const r = await fetch(target, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    credentials: "same-origin",
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    const err = new Error(`${r.status} ${target}: ${text.slice(0, 400)}`);
    err.status = r.status;
    err.body = text;
    throw err;
  }
  return r.json();
}

// scope: {kind: "platform"} | {kind: "catalog", catalogId} | {kind: "collection", catalogId, collectionId}
export function scopeBasePath(scope) {
  if (!scope || scope.kind === "platform") return "/configs";
  if (scope.kind === "catalog") {
    return `/configs/catalogs/${encodeURIComponent(scope.catalogId)}`;
  }
  if (scope.kind === "collection") {
    return `/configs/catalogs/${encodeURIComponent(scope.catalogId)}`
      + `/collections/${encodeURIComponent(scope.collectionId)}`;
  }
  throw new Error(`unknown scope kind: ${scope.kind}`);
}

// GET the composed/resolved or explicit config set at a scope.
// resolved=true returns inherited + overrides; resolved=false returns only
// overrides set at this scope.
export function fetchConfigSet(scope, { resolved = true } = {}) {
  const base = scopeBasePath(scope);
  return getJSON(`${base}/config?resolved=${resolved ? "true" : "false"}`);
}

// PATCH the composed endpoint at a scope. body is a shallow
// {class_key: partial_dict | null} mapping. null deletes the override.
export function patchConfigSet(scope, body) {
  const base = scopeBasePath(scope);
  return patchJSON(`${base}/config`, body);
}

// Schema inventory endpoint. Response shape:
// { class_key: { json_schema, description, scope } }
export function fetchSchemas() {
  return getJSON("/configs/schemas");
}

// Identity — used to gate admin pages. Returns { principal, roles }.
export async function fetchMe() {
  try {
    return await getJSON("/me");
  } catch (e) {
    if (e.status === 401) return { principal: null, roles: [] };
    throw e;
  }
}

export function fetchCatalogs() {
  return getJSON("/catalogs");
}

// ----- HTTP verbs used by admin pages -----

async function sendJSON(method, url, body, { expectJSON = true } = {}) {
  const target = apiUrl(url);
  const init = {
    method,
    credentials: "same-origin",
    headers: body == null ? {} : { "Content-Type": "application/json" },
  };
  if (body != null) init.body = JSON.stringify(body);
  const r = await fetch(target, init);
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    const err = new Error(`${r.status} ${target}: ${text.slice(0, 400)}`);
    err.status = r.status;
    err.body = text;
    throw err;
  }
  if (r.status === 204 || !expectJSON) return null;
  const ct = r.headers.get("content-type") || "";
  if (!ct.includes("json")) return null;
  return r.json();
}

export const postJSON = (url, body) => sendJSON("POST", url, body);
export const putJSON = (url, body) => sendJSON("PUT", url, body);
export const deleteJSON = (url) => sendJSON("DELETE", url, null);

// ----- Governance: roles / policies / principals -----

const qs = (params) => {
  const out = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) {
    if (v != null && v !== "") out.set(k, v);
  }
  const s = out.toString();
  return s ? `?${s}` : "";
};

export const listRoles = (catalogId) =>
  getJSON(`/admin/roles${qs({ catalog_id: catalogId })}`);
export const createRole = (role, catalogId) =>
  postJSON(`/admin/roles${qs({ catalog_id: catalogId })}`, role);
export const updateRole = (name, patch, catalogId) =>
  putJSON(
    `/admin/roles/${encodeURIComponent(name)}${qs({ catalog_id: catalogId })}`,
    patch,
  );
export const deleteRole = (name, catalogId) =>
  deleteJSON(
    `/admin/roles/${encodeURIComponent(name)}${qs({ catalog_id: catalogId })}`,
  );

export const listPolicies = (catalogId) =>
  getJSON(`/admin/policies${qs({ catalog_id: catalogId })}`);
export const createPolicy = (policy, catalogId) =>
  postJSON(`/admin/policies${qs({ catalog_id: catalogId })}`, policy);
export const updatePolicy = (id, patch, catalogId) =>
  putJSON(
    `/admin/policies/${encodeURIComponent(id)}${qs({ catalog_id: catalogId })}`,
    patch,
  );
export const deletePolicy = (id, catalogId) =>
  deleteJSON(
    `/admin/policies/${encodeURIComponent(id)}${qs({ catalog_id: catalogId })}`,
  );

export const searchPrincipals = ({ identifier, role, catalogId, limit = 50, offset = 0 } = {}) =>
  getJSON(
    `/admin/principals${qs({ identifier, role, catalog_id: catalogId, limit, offset })}`,
  );

export const listCatalogUsers = (catalogId) =>
  getJSON(`/admin/catalogs/${encodeURIComponent(catalogId)}/users`);

export const assignGlobalRole = (principalId, role) =>
  postJSON(`/admin/principals/${encodeURIComponent(principalId)}/roles`, { role });
export const removeGlobalRole = (principalId, role) =>
  deleteJSON(
    `/admin/principals/${encodeURIComponent(principalId)}/roles/${encodeURIComponent(role)}`,
  );
export const assignCatalogRole = (principalId, catalogId, role) =>
  postJSON(
    `/admin/principals/${encodeURIComponent(principalId)}`
      + `/catalogs/${encodeURIComponent(catalogId)}/roles`,
    { role },
  );
export const removeCatalogRole = (principalId, catalogId, role) =>
  deleteJSON(
    `/admin/principals/${encodeURIComponent(principalId)}`
      + `/catalogs/${encodeURIComponent(catalogId)}`
      + `/roles/${encodeURIComponent(role)}`,
  );

// ----- STAC write endpoints (create catalog / collection / features) -----

export const createStacCatalog = (definition) => postJSON(`/catalogs`, definition);
export const createStacCollection = (catalogId, definition) =>
  postJSON(`/catalogs/${encodeURIComponent(catalogId)}/collections`, definition);

// POST features to a collection. Returns the 201 BulkCreationResponse on
// full success, or a 207 IngestionReport on partial failure (thrown as error
// with .body payload — caller can inspect). Always returns whatever the
// server sent as a parsed JSON object for a 2xx.
export async function postFeatures(catalogId, collectionId, payload) {
  const url = `/catalogs/${encodeURIComponent(catalogId)}`
    + `/collections/${encodeURIComponent(collectionId)}/items`;
  const target = apiUrl(url);
  const r = await fetch(target, {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const text = await r.text().catch(() => "");
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (_) { /* ignore */ }
  if (!r.ok) {
    const err = new Error(`${r.status} ${target}: ${text.slice(0, 400)}`);
    err.status = r.status;
    err.body = data ?? text;
    throw err;
  }
  return { status: r.status, body: data };
}

// /me/catalogs: returns list of {catalog_id, roles, ...} for the signed-in
// principal. Used to discover "catalog admin" scope.
export const fetchMyCatalogs = () => getJSON("/me/catalogs");
