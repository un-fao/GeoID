/**
 * Live OGC conformance matrix.
 *
 * Renders into any element with id="ogc-matrix". Each standard is shown as a
 * card; the card's status dot upgrades from "snapshot" (gray) to "live ok"
 * (green) or "empty" (yellow) once /conformance returns. Cached for 5 min in
 * sessionStorage to avoid hammering on reload.
 *
 * Built entirely with createElement + textContent + replaceChildren —
 * no HTML strings are inserted into the DOM, so the page is XSS-safe by
 * construction even if downstream operators replace the snapshot JSON.
 */
(function () {
  const CACHE_PREFIX = "ds_conf_";
  const CACHE_TTL_MS = 5 * 60 * 1000;
  const STATE_OK = "ok";
  const STATE_EMPTY = "empty";
  const STATE_FALLBACK = "fallback";

  function readCache(url) {
    try {
      const raw = sessionStorage.getItem(CACHE_PREFIX + url);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (Date.now() - parsed.ts > CACHE_TTL_MS) return null;
      return parsed.body;
    } catch (_) { return null; }
  }
  function writeCache(url, body) {
    try { sessionStorage.setItem(CACHE_PREFIX + url, JSON.stringify({ ts: Date.now(), body })); } catch (_) {}
  }

  function classify(body) {
    if (!body || typeof body !== "object") return { state: STATE_EMPTY, count: 0 };
    const arr = Array.isArray(body.conformsTo) ? body.conformsTo : [];
    return arr.length === 0 ? { state: STATE_EMPTY, count: 0 } : { state: STATE_OK, count: arr.length };
  }

  function getApiRoot() {
    if (typeof window !== "undefined" && window.platformConfig && window.platformConfig.root_path) {
      return window.platformConfig.root_path.replace(/\/$/, "");
    }
    return "";
  }

  function authHeaders() {
    const headers = { "Accept": "application/json" };
    try {
      const key = (typeof window !== "undefined" && window.DS_TOKEN_KEY) || "ds_token";
      const token = localStorage.getItem(key) || sessionStorage.getItem(key);
      if (token) headers["Authorization"] = "Bearer " + token;
    } catch (_) { /* storage unavailable (sandboxed embed) — probe anonymously */ }
    return headers;
  }

  async function probe(standard) {
    const url = getApiRoot() + standard.url;
    const cached = readCache(url);
    if (cached !== null) {
      if (cached.__denied) return { state: STATE_FALLBACK, count: standard.classes.length, denied: true };
      return classify(cached);
    }
    try {
      const res = await fetch(url, { headers: authHeaders() });
      if (res.status === 401 || res.status === 403) {
        // Conformance is access-gated for this principal. Remember the denial
        // so reloads inside the cache window stay quiet.
        writeCache(url, { __denied: true });
        return { state: STATE_FALLBACK, count: standard.classes.length, denied: true };
      }
      if (!res.ok) return { state: STATE_FALLBACK, count: standard.classes.length };
      const body = await res.json();
      writeCache(url, body);
      return classify(body);
    } catch (_) {
      return { state: STATE_FALLBACK, count: standard.classes.length };
    }
  }

  function buildBadge(standard, status) {
    const palette = {
      ok:       { color: "#10b981", note: "Live" },
      empty:    { color: "#f59e0b", note: "Empty" },
      fallback: { color: "#64748b", note: "Snapshot" },
    };
    const p = palette[status.state] || palette.fallback;

    const a = document.createElement("a");
    a.className = "glass-panel ds-conformance-badge";
    a.href = standard.spec;
    a.target = "_blank";
    a.rel = "noopener";
    a.title = standard.classes.join("\n");
    a.style.display = "flex";
    a.style.flexDirection = "column";
    a.style.gap = "0.25rem";
    a.style.padding = "1rem 1.25rem";
    a.style.textDecoration = "none";
    a.style.color = "inherit";

    const top = document.createElement("span");
    top.style.display = "flex";
    top.style.alignItems = "center";
    top.style.gap = "0.5rem";
    top.style.fontWeight = "700";

    const dot = document.createElement("span");
    dot.style.width = "0.5rem";
    dot.style.height = "0.5rem";
    dot.style.borderRadius = "50%";
    dot.style.background = p.color;
    dot.style.boxShadow = "0 0 8px " + p.color;
    top.appendChild(dot);

    const labelEl = document.createElement("span");
    labelEl.textContent = standard.label;
    top.appendChild(labelEl);

    const sub = document.createElement("span");
    sub.style.fontSize = "0.75rem";
    sub.style.color = "var(--color-text-dim, #94a3b8)";
    sub.style.textTransform = "uppercase";
    sub.style.letterSpacing = "0.08em";
    const noun = status.count === 1 ? "class" : "classes";
    sub.textContent = standard.service + " · " + status.count + " " + noun + " · " + p.note;

    a.appendChild(top);
    a.appendChild(sub);
    if (standard.status) {
      const pill = document.createElement("span");
      pill.textContent = standard.status === "stable" ? "Published" : "Draft spec";
      pill.style.alignSelf = "flex-start";
      pill.style.marginTop = "0.35rem";
      pill.style.padding = "0.1rem 0.55rem";
      pill.style.borderRadius = "9999px";
      pill.style.fontSize = "0.65rem";
      pill.style.fontWeight = "600";
      pill.style.letterSpacing = "0.06em";
      pill.style.textTransform = "uppercase";
      if (standard.status === "stable") {
        pill.style.background = "rgba(16,185,129,0.10)";
        pill.style.color = "#34d399";
        pill.style.border = "1px solid rgba(16,185,129,0.25)";
      } else {
        pill.style.background = "rgba(245,158,11,0.10)";
        pill.style.color = "#fbbf24";
        pill.style.border = "1px solid rgba(245,158,11,0.25)";
      }
      a.appendChild(pill);
    }
    a.dataset.state = status.state;
    return a;
  }

  function setFooterText(standardsCount, okCount, gated) {
    const footer = document.getElementById("ogc-matrix-footer");
    if (!footer) return;
    if (gated) {
      footer.textContent =
        standardsCount + " OGC API standards from the published snapshot — sign in for live conformance checks";
      return;
    }
    footer.textContent =
      okCount + " of " + standardsCount + " OGC API standards declared conformant — fetched live from /…/conformance";
  }

  async function render(rootEl, snapshot) {
    const standards = snapshot.standards;
    const initial = standards.map((s) => buildBadge(s, { state: STATE_FALLBACK, count: s.classes.length }));
    rootEl.replaceChildren(...initial);
    setFooterText(standards.length, 0, false);

    // Canary probe: if the first /conformance is access-gated for this
    // principal, the rest will be too — keep the snapshot badges and skip
    // the remaining requests instead of erroring once per standard.
    if (standards.length > 0) {
      const canary = await probe(standards[0]);
      if (canary.denied) {
        setFooterText(standards.length, 0, true);
        return;
      }
      const firstBadge = buildBadge(standards[0], canary);
      const oldFirst = rootEl.children[0];
      if (oldFirst && oldFirst.parentNode === rootEl) {
        rootEl.replaceChild(firstBadge, oldFirst);
      }
      let okCount = canary.state === STATE_OK ? 1 : 0;
      await Promise.all(standards.slice(1).map(async (s, i) => {
        const status = await probe(s);
        const newBadge = buildBadge(s, status);
        const oldBadge = rootEl.children[i + 1];
        if (oldBadge && oldBadge.parentNode === rootEl) {
          rootEl.replaceChild(newBadge, oldBadge);
        }
        if (status.state === STATE_OK) okCount += 1;
      }));
      setFooterText(standards.length, okCount, false);
    }
  }

  async function init() {
    const root = document.getElementById("ogc-matrix");
    if (!root) return;
    const snapshotPath = root.dataset.snapshot || "/web/geoid/conformance-snapshot.json";
    let snapshot;
    try {
      const res = await fetch(snapshotPath);
      if (!res.ok) throw new Error("snapshot " + res.status);
      snapshot = await res.json();
    } catch (e) {
      const msg = document.createElement("div");
      msg.className = "glass-panel";
      msg.style.padding = "1rem";
      msg.style.color = "var(--color-text-dim, #94a3b8)";
      msg.textContent = "Unable to load OGC conformance snapshot.";
      root.replaceChildren(msg);
      return;
    }
    render(root, snapshot);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  if (typeof window !== "undefined") {
    window.__dsConformance = { render, classify, probe };
  }
})();
