// common/i18n.js — minimal string-dictionary i18n shared across extension web sections.
// Dictionaries are keyed by language code; missing keys fall back to English, then to the key itself.
// Active language resolution order: explicit setLang() > URL ?language= > <html lang> > "en".

const SUPPORTED = ["en", "fr", "es"]; // v1; add codes here to extend (e.g. ar, zh, ru)
let _lang = null;
let _dict = {};

export function detectLang() {
  const url = new URLSearchParams(window.location.search).get("language");
  if (url && SUPPORTED.includes(url)) return url;
  const htmlLang = (document.documentElement.lang || "").slice(0, 2);
  if (SUPPORTED.includes(htmlLang)) return htmlLang;
  return "en";
}

export function setLang(lang) {
  _lang = SUPPORTED.includes(lang) ? lang : "en";
}

export function lang() {
  if (!_lang) _lang = detectLang();
  return _lang;
}

// register({en:{key:val}, fr:{...}}) merges a section's dictionaries.
export function register(dicts) {
  for (const code of Object.keys(dicts)) {
    _dict[code] = Object.assign({}, _dict[code] || {}, dicts[code]);
  }
}

export function t(key, vars) {
  const code = lang();
  let s = (_dict[code] && _dict[code][key]) ?? (_dict.en && _dict.en[key]) ?? key;
  if (vars) for (const k of Object.keys(vars)) s = s.replace(new RegExp(`\\{${k}\\}`, "g"), vars[k]);
  return s;
}

export { SUPPORTED };
