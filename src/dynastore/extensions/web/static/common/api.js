// Thin fetch helper for the admin Hub. Non-2xx responses throw with the
// response body attached. Handles the three config scopes (platform,
// catalog, collection) uniformly so page code never builds URLs by hand.

export async function getJSON(url) {
  const r = await fetch(url, { credentials: "same-origin" });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    const err = new Error(`${r.status} ${url}: ${text.slice(0, 400)}`);
    err.status = r.status;
    err.body = text;
    throw err;
  }
  return r.json();
}

export async function patchJSON(url, body) {
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    credentials: "same-origin",
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const text = await r.text().catch(() => "");
    const err = new Error(`${r.status} ${url}: ${text.slice(0, 400)}`);
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
