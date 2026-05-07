// Proxy-prefix-aware URL helpers.
//
// The web UI is served under <prefix>/web/... where <prefix> is empty in
// dev/local but non-empty behind the FAO reverse proxy
// (e.g. /geospatial/v2/api/catalog). Absolute API paths like "/admin/roles"
// resolve against the origin and bypass the prefix; pages must prepend it.

export function apiBase() {
  const p = (typeof window !== "undefined" && window.location.pathname) || "";
  const idx = p.indexOf("/web/");
  return idx >= 0 ? p.substring(0, idx) : "";
}

// Prepend the discovered prefix to an absolute API path. Pass-through for
// relative paths, protocol-relative URLs (//host/...), and fully-qualified
// URLs (http(s)://...).
export function apiUrl(path) {
  if (typeof path !== "string") return path;
  if (path[0] !== "/") return path;
  if (path.startsWith("//")) return path;
  return apiBase() + path;
}
