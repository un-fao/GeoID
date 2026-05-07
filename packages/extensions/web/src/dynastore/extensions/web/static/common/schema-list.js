// Left-rail schema picker. Categorizes entries by the top-level
// `x-ui.category` emitted by Pydantic on each model's json_schema_extra;
// falls back to a "other" bucket when absent. Search filters by
// class_key and description.

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

export function mountSchemaList(container, { schemas, onSelect, selected }) {
  container.classList.add("schema-list");
  clearNode(container);

  const search = document.createElement("input");
  search.type = "search";
  search.placeholder = "Search plugins...";
  search.className = "schema-list-search";
  search.style.width = "100%";
  search.style.marginBottom = "12px";
  search.style.padding = "6px 10px";
  search.style.background = "var(--surface2, #21262d)";
  search.style.color = "inherit";
  search.style.border = "1px solid var(--border, #30363d)";
  search.style.borderRadius = "6px";
  container.appendChild(search);

  const list = document.createElement("div");
  list.className = "schema-list-items";
  container.appendChild(list);

  const state = { query: "", selected: selected || null };

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
        const hay = (classKey + " " + descOf(entry)).toLowerCase();
        return hay.includes(q);
      });
      if (!entries.length) continue;
      const group = document.createElement("div");
      group.className = "schema-list-group";
      const header = document.createElement("div");
      header.className = "schema-list-group-header";
      header.textContent = CATEGORY_LABELS[cat] || cat;
      header.style.fontSize = "0.7rem";
      header.style.fontWeight = "600";
      header.style.textTransform = "uppercase";
      header.style.letterSpacing = "0.05em";
      header.style.color = "var(--text-dim, #8b949e)";
      header.style.padding = "10px 4px 4px";
      group.appendChild(header);

      for (const { classKey, entry } of entries) {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "schema-list-item";
        item.dataset.classKey = classKey;
        item.style.display = "block";
        item.style.width = "100%";
        item.style.textAlign = "left";
        item.style.padding = "6px 10px";
        item.style.background = "transparent";
        item.style.color = "inherit";
        item.style.border = "1px solid transparent";
        item.style.borderRadius = "6px";
        item.style.marginBottom = "2px";
        item.style.cursor = "pointer";
        item.style.fontSize = "0.82rem";

        const title = document.createElement("div");
        title.textContent = labelOf(entry, classKey);
        title.style.fontWeight = "500";

        const desc = document.createElement("div");
        desc.textContent = descOf(entry).slice(0, 100);
        desc.style.fontSize = "0.7rem";
        desc.style.color = "var(--text-dim, #8b949e)";
        desc.style.marginTop = "2px";
        desc.style.whiteSpace = "nowrap";
        desc.style.overflow = "hidden";
        desc.style.textOverflow = "ellipsis";

        item.appendChild(title);
        if (desc.textContent) item.appendChild(desc);

        if (state.selected === classKey) {
          item.style.background = "var(--surface2, #21262d)";
          item.style.borderColor = "var(--primary, #58a6ff)";
        }

        item.addEventListener("click", () => {
          state.selected = classKey;
          render();
          if (typeof onSelect === "function") onSelect(classKey);
        });
        item.addEventListener("mouseover", () => {
          if (state.selected !== classKey) item.style.background = "var(--surface2, #21262d)";
        });
        item.addEventListener("mouseout", () => {
          if (state.selected !== classKey) item.style.background = "transparent";
        });

        group.appendChild(item);
      }
      list.appendChild(group);
    }
    if (!list.firstChild) {
      const empty = document.createElement("div");
      empty.textContent = "No matching plugins.";
      empty.style.color = "var(--text-dim, #8b949e)";
      empty.style.fontSize = "0.8rem";
      empty.style.padding = "16px 4px";
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
  };
}
