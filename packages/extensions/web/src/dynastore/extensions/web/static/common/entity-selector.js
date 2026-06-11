// common/entity-selector.js — pluggable list/selector widget.
// Renders a debounced search input, a paginated list, and a load-more button.
// All user-visible strings come from the i18n registry; all labels via
// textContent so no XSS surface exists.
import { register, t, lang } from "./i18n.js";

register({
  en: {
    "sel.search":  "Search…",
    "sel.loading": "Loading…",
    "sel.empty":   "Nothing found",
    "sel.error":   "Failed to load",
    "sel.more":    "Load more",
  },
  fr: {
    "sel.search":  "Rechercher…",
    "sel.loading": "Chargement…",
    "sel.empty":   "Rien trouvé",
    "sel.error":   "Échec du chargement",
    "sel.more":    "Charger plus",
  },
  es: {
    "sel.search":  "Buscar…",
    "sel.loading": "Cargando…",
    "sel.empty":   "Sin resultados",
    "sel.error":   "Error al cargar",
    "sel.more":    "Cargar más",
  },
});

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function resolveRoot(rootOpt) {
  if (!rootOpt) throw new Error("entity-selector: root is required");
  if (typeof rootOpt === "string") {
    const el = document.querySelector(rootOpt);
    if (!el) throw new Error(`entity-selector: no element matches "${rootOpt}"`);
    return el;
  }
  return rootOpt;
}

function debounce(fn, ms) {
  let timer;
  return function (...args) {
    clearTimeout(timer);
    timer = setTimeout(() => fn.apply(this, args), ms);
  };
}

// ---------------------------------------------------------------------------
// mountEntitySelector(options) -> controller
// ---------------------------------------------------------------------------

/**
 * @param {object} options
 * @param {string|Element}  options.root
 * @param {object}          options.source          - adapter (see entity-sources.js)
 * @param {string}          [options.placeholder]
 * @param {number}          [options.pageSize=25]
 * @param {number}          [options.debounceMs=250]
 * @param {boolean}         [options.autoFirst=false]
 * @param {string}          [options.selectedId]
 * @param {function}        [options.onChange]
 * @param {function}        [options.labelOf]        - overrides source.labelOf
 * @returns {{ reload, setQuery, getSelected, setEnabled, destroy }}
 */
export function mountEntitySelector(options) {
  const {
    source,
    placeholder,
    pageSize    = 25,
    debounceMs  = 250,
    autoFirst   = false,
    selectedId  = null,
    onChange    = () => {},
    labelOf     = (source.labelOf || ((x) => String(x.title || x.id || x))),
  } = options;

  const root = resolveRoot(options.root);
  root.classList.add("ds-entity-selector");

  // Build skeleton DOM.
  const input   = document.createElement("input");
  const status  = document.createElement("div");
  const list    = document.createElement("ul");
  const moreBtn = document.createElement("button");

  input.type = "text";
  input.className = "es-search";
  input.placeholder = placeholder || t("sel.search");
  input.setAttribute("aria-label", input.placeholder);

  status.className = "es-status";

  list.className = "es-list";
  list.setAttribute("role", "listbox");
  list.setAttribute("aria-label", input.placeholder);

  moreBtn.className = "es-more";
  moreBtn.type = "button";
  moreBtn.textContent = t("sel.more");
  moreBtn.style.display = "none";

  root.appendChild(input);
  root.appendChild(status);
  root.appendChild(list);
  root.appendChild(moreBtn);

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------

  let _query     = "";
  let _offset    = 0;
  let _items     = [];        // currently rendered items (may grow with load-more)
  let _selected  = null;
  let _disabled  = false;
  let _token     = 0;         // monotonic request token — discard stale responses
  let _hasMore   = false;

  // ---------------------------------------------------------------------------
  // Keyboard navigation
  // ---------------------------------------------------------------------------

  let _focusIndex = -1;

  function _buttons() { return Array.from(list.querySelectorAll("button.es-item")); }

  function _moveFocus(delta) {
    const btns = _buttons();
    if (!btns.length) return;
    _focusIndex = Math.max(0, Math.min(btns.length - 1, _focusIndex + delta));
    btns[_focusIndex].focus();
    list.setAttribute("aria-activedescendant", btns[_focusIndex].id || "");
  }

  input.addEventListener("keydown", (e) => {
    if (e.key === "ArrowDown") { e.preventDefault(); _focusIndex = -1; _moveFocus(1); }
  });

  list.addEventListener("keydown", (e) => {
    if (e.key === "ArrowDown") { e.preventDefault(); _moveFocus(1); }
    else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (_focusIndex <= 0) { input.focus(); _focusIndex = -1; }
      else { _moveFocus(-1); }
    }
  });

  // ---------------------------------------------------------------------------
  // Rendering helpers
  // ---------------------------------------------------------------------------

  function _setStatus(text) { status.textContent = text; }

  function _appendItems(newItems, replace) {
    if (replace) {
      list.replaceChildren();
      _items = [];
      _focusIndex = -1;
    }
    for (const item of newItems) {
      const li  = document.createElement("li");
      li.setAttribute("role", "option");
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "es-item";
      btn.id = `es-item-${_items.length}`;
      btn.textContent = labelOf(item);                     // XSS-safe
      const isSelected = _selected && source.idOf(_selected) === source.idOf(item);
      btn.setAttribute("aria-selected", isSelected ? "true" : "false");
      if (isSelected) li.classList.add("es-active");
      btn.addEventListener("click", () => _selectItem(item, btn, li));
      li.appendChild(btn);
      list.appendChild(li);
      _items.push(item);
    }
  }

  function _selectItem(item, btn, li) {
    // Deselect old.
    list.querySelectorAll("button.es-item").forEach((b) => {
      b.setAttribute("aria-selected", "false");
      b.closest("li").classList.remove("es-active");
    });
    btn.setAttribute("aria-selected", "true");
    li.classList.add("es-active");
    _selected = item;
    onChange(item);
  }

  function _updateMoreBtn() {
    moreBtn.style.display = (_hasMore && source.paginated) ? "" : "none";
  }

  // ---------------------------------------------------------------------------
  // Fetch logic
  // ---------------------------------------------------------------------------

  async function _fetch(replace) {
    if (_disabled) return;
    const myToken = ++_token;
    _setStatus(t("sel.loading"));

    const fetchOpts = {
      q:      _query,
      limit:  pageSize,
      offset: _offset,
      lang:   lang(),
    };

    try {
      const { items: page, hasMore } = await _doFetch(fetchOpts);
      if (myToken !== _token) return;   // stale — discard

      _hasMore = hasMore;

      if (replace && page.length === 0) {
        _appendItems([], true);
        _setStatus(t("sel.empty"));
        _updateMoreBtn();
        return;
      }

      _appendItems(page, replace);
      _setStatus("");
      _updateMoreBtn();

      // Auto-select first item on initial load.
      if (replace && autoFirst && _items.length > 0 && !_selected) {
        const firstBtn = list.querySelector("button.es-item");
        const firstLi  = firstBtn && firstBtn.closest("li");
        if (firstBtn && firstLi) _selectItem(_items[0], firstBtn, firstLi);
      }

      // Restore previously selected item.
      if (replace && selectedId && !_selected) {
        const idx = _items.findIndex((i) => source.idOf(i) === selectedId);
        if (idx >= 0) {
          const btns = _buttons();
          const li   = btns[idx] && btns[idx].closest("li");
          if (btns[idx] && li) _selectItem(_items[idx], btns[idx], li);
        }
      }
    } catch (_err) {
      if (myToken !== _token) return;
      _appendItems([], true);
      _setStatus(t("sel.error"));
      moreBtn.style.display = "none";
    }
  }

  async function _doFetch(opts) {
    if (!source.supportsSearch && opts.q) {
      // Client-side filter: fetch all, then filter.
      const raw = await source.fetch({ ...opts, q: undefined });
      const filtered = raw.items.filter((i) => {
        const lbl = labelOf(i).toLowerCase();
        return lbl.includes(opts.q.toLowerCase());
      });
      return { items: filtered, hasMore: false };
    }
    return source.fetch(opts);
  }

  // ---------------------------------------------------------------------------
  // Debounced search input handler
  // ---------------------------------------------------------------------------

  const _onInput = debounce(() => {
    _query  = input.value.trim();
    _offset = 0;
    _fetch(true);
  }, debounceMs);

  input.addEventListener("input", _onInput);

  // Load-more button.
  moreBtn.addEventListener("click", () => {
    _offset += pageSize;
    _fetch(false);
  });

  // ---------------------------------------------------------------------------
  // Controller
  // ---------------------------------------------------------------------------

  function reload() {
    _query  = input.value.trim();
    _offset = 0;
    _selected = null;
    return _fetch(true);
  }

  function setQuery(text) {
    input.value = text;
    _query = text.trim();
    _offset = 0;
    return _fetch(true);
  }

  function getSelected() { return _selected; }

  function setEnabled(enabled) {
    _disabled = !enabled;
    input.disabled = _disabled;
    moreBtn.disabled = _disabled;
    list.querySelectorAll("button.es-item").forEach((b) => { b.disabled = _disabled; });
    root.classList.toggle("es-disabled", _disabled);
  }

  function destroy() {
    root.replaceChildren();
    root.classList.remove("ds-entity-selector");
  }

  // Initial load.
  _fetch(true);

  return { reload, setQuery, getSelected, setEnabled, destroy };
}
