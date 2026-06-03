// Client-side download helpers — dependency-free, no external libs.
// Provides Blob-based file download and RFC-4180 CSV serialization.

/**
 * Trigger a browser download for the given content.
 *
 * @param {string} filename  Suggested filename (sanitized by the browser).
 * @param {string} content   File contents as a string.
 * @param {string} mimeType  MIME type, e.g. "text/csv" or "application/json".
 */
export function downloadAsFile(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

/**
 * Serialize an array of rows to an RFC-4180 CSV string.
 *
 * @param {string[]} headers  Column header names.
 * @param {(string|number|null|undefined)[][]} rows  Data rows (one array per row).
 * @returns {string} Complete CSV including header row, CRLF line endings.
 */
export function rowsToCsv(headers, rows) {
  const escape = (v) => {
    const s = v == null ? "" : String(v);
    // Quote any field that contains a comma, double-quote, or newline.
    if (s.includes(",") || s.includes('"') || s.includes("\n") || s.includes("\r")) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  };
  const lines = [headers.map(escape).join(",")];
  for (const row of rows) {
    lines.push(row.map(escape).join(","));
  }
  return lines.join("\r\n");
}
