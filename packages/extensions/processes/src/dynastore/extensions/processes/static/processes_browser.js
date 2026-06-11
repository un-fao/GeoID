// processes/processes_browser.js — entry point for the Processes Browser page.
// Lists registered OGC API - Processes definitions and submitted jobs;
// renders a read-only detail view for either. Data endpoints stay IAM-gated;
// this script only renders what the caller is allowed to fetch.
import { getJSON } from "../static/common/api.js";
import { register, t, lang } from "../static/common/i18n.js";

register({
  en: { "proc.loading": "Loading…", "proc.none": "Nothing to show", "proc.error": "Failed to load" },
  fr: { "proc.loading": "Chargement…", "proc.none": "Rien à afficher", "proc.error": "Échec du chargement" },
  es: { "proc.loading": "Cargando…", "proc.none": "Nada que mostrar", "proc.error": "Error al cargar" },
});

const navEl = document.getElementById("nav-list");
const bodyEl = document.getElementById("detail-body");
const tabProcesses = document.getElementById("tab-processes");
const tabJobs = document.getElementById("tab-jobs");

function setActive(tab) {
  for (const b of [tabProcesses, tabJobs]) b.classList.toggle("active", b === tab);
}

function renderList(rows, labelOf, onClick) {
  navEl.replaceChildren();
  if (!rows || rows.length === 0) {
    const p = document.createElement("p");
    p.textContent = t("proc.none");
    navEl.appendChild(p);
    return;
  }
  const ul = document.createElement("ul");
  for (const r of rows) {
    const li = document.createElement("li");
    const b = document.createElement("button");
    b.textContent = labelOf(r); // textContent => no XSS from API data
    b.addEventListener("click", () => onClick(r));
    li.appendChild(b);
    ul.appendChild(li);
  }
  navEl.appendChild(ul);
}

// Key/value table renderer; objects/arrays land in a <pre> block.
function renderDetail(title, obj) {
  bodyEl.replaceChildren();
  const h = document.createElement("h3");
  h.textContent = title;
  bodyEl.appendChild(h);
  const table = document.createElement("table");
  for (const [k, v] of Object.entries(obj || {})) {
    const tr = document.createElement("tr");
    const td1 = document.createElement("td");
    td1.textContent = k;
    const td2 = document.createElement("td");
    if (v !== null && typeof v === "object") {
      const pre = document.createElement("pre");
      pre.textContent = JSON.stringify(v, null, 2);
      td2.appendChild(pre);
    } else {
      td2.textContent = String(v ?? "");
    }
    tr.appendChild(td1);
    tr.appendChild(td2);
    table.appendChild(tr);
  }
  bodyEl.appendChild(table);
}

async function showProcesses() {
  setActive(tabProcesses);
  navEl.textContent = t("proc.loading");
  try {
    const res = await getJSON(`/processes/processes?language=${lang()}`);
    const procs = res.processes || res;
    renderList(procs, (p) => p.title || p.id, async (p) => {
      bodyEl.textContent = t("proc.loading");
      try {
        const detail = await getJSON(`/processes/processes/${encodeURIComponent(p.id)}`);
        renderDetail(detail.title || detail.id, detail);
      } catch (e) { bodyEl.textContent = t("proc.error"); }
    });
  } catch (e) { navEl.textContent = t("proc.error"); }
}

async function showJobs() {
  setActive(tabJobs);
  navEl.textContent = t("proc.loading");
  try {
    const res = await getJSON("/processes/jobs");
    const jobs = res.jobs || res;
    renderList(jobs, (j) => `${j.processID || j.type || "job"} · ${j.status || ""} · ${j.jobID || j.id}`, async (j) => {
      bodyEl.textContent = t("proc.loading");
      try {
        const id = j.jobID || j.id;
        const detail = await getJSON(`/processes/jobs/${encodeURIComponent(id)}`);
        renderDetail(`Job ${id}`, detail);
      } catch (e) { bodyEl.textContent = t("proc.error"); }
    });
  } catch (e) { navEl.textContent = t("proc.error"); }
}

tabProcesses.addEventListener("click", showProcesses);
tabJobs.addEventListener("click", showJobs);
showProcesses();
