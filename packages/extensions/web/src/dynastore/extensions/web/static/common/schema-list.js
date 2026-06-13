// Left-rail schema picker. Categorizes entries by the top-level
// `x-ui.category` emitted by Pydantic on each model's json_schema_extra;
// falls back to a "other" bucket when absent. Search filters by
// class_key, description, and scope. Each item carries a scope badge
// (derived from the registry's `scope` field) so operators immediately see
// which tiers a plugin applies at without opening it.

const CATEGORY_LABELS = {
  routing: "Routing",
  "storage-drivers": "Storage Drivers",
  "asset-drivers": "Asset Drivers",
  "metadata-drivers": "Metadata Drivers",
  observability: "Observability",
  auth: "Auth",
  other: "Other",
};

const CATEGORY_ORDER = [
  "routing",
  "storage-drivers",
  "asset-drivers",
  "metadata-drivers",
  "observability",
  "auth",
  "other",
];

// Human-readable labels for the `scope` values the registry emits.
// `platform_waterfall` means it applies at all tiers (platform → collection);
// `collection_intrinsic` means it is only authored at collection tier;
// `deployment_env`      means it is set by the deployment environment (env vars).
const SCOPE_LABELS = {
  platform_waterfall: "all tiers",
  collection_intrinsic: "collection",
  deployment_env: "env",
};

// CSS class suffix for each scope value.
const SCOPE_CSS = {
  platform_waterfall: "waterfall",
  collection_intrinsic: "intrinsic",
  deployment_env: "env",
};

function clearNode(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function categoryOf(entry) {
  const cat = entry?.json_schema?.["x-ui"]?.category;
  if (cat && CATEGORY_LABELS[cat]) return cat;
  if (cat) return cat;
  return "other";
}

function labelOf(entry, classKey) {
  const title = entry?.json_schema?.title;
  return title && typeof title === "string" ? title : classKey;
}

function descOf(entry) {
  return entry?.description || entry?.json_schema?.description || "";
}

function scopeOf(entry) {
  return entry?.scope || "platform_waterfall";
}

function scopeBadge(scope) {
  const label = SCOPE_LABELS[scope] || scope;
  const cls = SCOPE_CSS[scope] || "waterfall";
  const el = document.createElement("span");
  el.className = `ci-scope-badge ci-scope-${cls}`;
  el.textContent = label;
  el.title = `This config is authored at scope: ${scope}`;
  return el;
}

export function mountSchemaList(
  container,
  { schemas, refs, onSelect, onSelectRef, selected } = {},
) {
  container.classList.add("schema-list");
  clearNode(container);

  const search = document.createElement("input");
  search.type = "search";
  search.placeholder = "Search plugins…";
  search.className = "schema-list-search";
  container.appendChild(search);

  const list = document.createElement("div");
  list.className = "schema-list-items";
  container.appendChild(list);

  // `refs` (#1940): {class_key: [ref_key, ...]} — extra instance rows whose
  // ref_key diverges from the canonical class_key (set_config_by_ref). Scope-
  // dependent, so the host refreshes it via the returned `setRefs` handle.
  const state = { query: "", selected: selected || null, refs: refs || {} };

  const grouped = {};
  for (const [classKey, entry] of Object.entries(schemas || {})) {
    const cat = categoryOf(entry);
    if (!grouped[cat]) grouped[cat] = [];
    grouped[cat].push({ classKey, entry });
  }
  for (const cat of Object.keys(grouped)) {
    grouped[cat].sort((a, b) => a.classKey.localeCompare(b.classKey));
  }

  function render() {
    clearNode(list);
    const q = state.query.trim().toLowerCase();
    const orderedCats = [
      ...CATEGORY_ORDER.filter((c) => grouped[c]),
      ...Object.keys(grouped).filter((c) => !CATEGORY_ORDER.includes(c)),
    ];
    for (const cat of orderedCats) {
      const entries = grouped[cat].filter(({ classKey, entry }) => {
        if (!q) return true;
        const scope = SCOPE_LABELS[scopeOf(entry)] || scopeOf(entry);
        const hay = (classKey + " " + descOf(entry) + " " + scope).toLowerCase();
        return hay.includes(q);
      });
      if (!entries.length) continue;
      const group = document.createElement("div");
      group.className = "schema-list-group";
      const header = document.createElement("div");
      header.className = "schema-list-group-header";
      header.textContent = CATEGORY_LABELS[cat] || cat;
      group.appendChild(header);

      for (const { classKey, entry } of entries) {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "schema-list-item";
        item.dataset.classKey = classKey;
        if (state.selected === classKey) item.classList.add("selected");

        const titleRow = document.createElement("div");
        titleRow.className = "item-title-row";

        const title = document.createElement("span");
        title.className = "item-title";
        title.textContent = labelOf(entry, classKey);

        titleRow.appendChild(title);
        titleRow.appendChild(scopeBadge(scopeOf(entry)));

        const desc = document.createElement("div");
        desc.className = "item-desc";
        desc.textContent = descOf(entry).slice(0, 100);

        item.appendChild(titleRow);
        if (desc.textContent) item.appendChild(desc);

        item.addEventListener("click", () => {
          state.selected = classKey;
          render();
          if (typeof onSelect === "function") onSelect(classKey);
        });

        group.appendChild(item);

        // #1940 — render extra instance rows (ref_key != class_key) nested
        // under their canonical class entry. Canonical rows (ref_key ==
        // class_key) are already the `item` above, so they are skipped here.
        const classRefs = (state.refs[classKey] || [])
          .filter((refKey) => refKey !== classKey)
          .sort((a, b) => a.localeCompare(b));
        for (const refKey of classRefs) {
          const refItem = document.createElement("button");
          refItem.type = "button";
          refItem.className = "schema-list-item schema-list-item-ref";
          refItem.dataset.classKey = classKey;
          refItem.dataset.refKey = refKey;
          if (state.selected === refKey) refItem.classList.add("selected");

          const refTitle = document.createElement("span");
          refTitle.className = "item-title item-title-ref";
          refTitle.textContent = refKey;
          refItem.appendChild(refTitle);

          refItem.addEventListener("click", () => {
            state.selected = refKey;
            render();
            if (typeof onSelectRef === "function") onSelectRef(refKey, classKey);
          });

          group.appendChild(refItem);
        }
      }
      list.appendChild(group);
    }
    if (!list.firstChild) {
      const empty = document.createElement("div");
      empty.className = "schema-list-empty";
      empty.textContent = "No matching plugins.";
      list.appendChild(empty);
    }
  }

  search.addEventListener("input", () => {
    state.query = search.value;
    render();
  });

  render();

  return {
    setSelected: (key) => {
      state.selected = key;
      render();
    },
    // #1940 — host pushes the scope's {class_key: [ref_key,...]} map; the
    // class registry itself is scope-independent so only refs re-render.
    setRefs: (refsMap) => {
      state.refs = refsMap || {};
      render();
    },
  };
}
