// upload.js — request an UploadTicket then deliver the file to the backend URL.
// UploadTicket response fields: ticket_id, upload_url, method, headers, expires_at, backend.
// UploadRequest body fields: filename, content_type (optional), asset { asset_id, asset_type, metadata }.
import { postJSON } from "../common/api.js";

/**
 * uploadAsset(cat, coll, file, assetId) -> UploadTicket JSON (or throws).
 *
 * Requests an upload ticket then delivers the file to the backend-specified
 * upload_url using the ticket's method (PUT for GCS/S3, POST for local).
 * Returns the ticket so the caller can poll /upload/{ticket_id}/status.
 *
 * @param {string}      cat     - catalog ID
 * @param {string|null} coll    - collection ID, or null for catalog-level upload
 * @param {File}        file    - the File object to upload
 * @param {string}      assetId - logical asset identifier (used in the ticket body)
 */
export async function uploadAsset(cat, coll, file, assetId) {
  const base = coll
    ? `/assets/catalogs/${cat}/collections/${coll}/upload`
    : `/assets/catalogs/${cat}/upload`;

  const ticket = await postJSON(base, {
    filename: file.name,
    content_type: file.type || "application/octet-stream",
    asset: {
      asset_id: assetId || file.name,
      asset_type: "ASSET",
      metadata: {},
    },
  });

  // Deliver the file to the backend.
  // ticket.upload_url  — pre-signed URL (GCS/S3) or server proxy path (local)
  // ticket.method      — "PUT" for GCS/S3, "POST" for local
  // ticket.headers     — required headers (e.g. Content-Type for GCS)
  const method = (ticket.method || "PUT").toUpperCase();
  let res;
  if (method === "POST") {
    const fd = new FormData();
    fd.append("file", file, file.name);
    res = await fetch(ticket.upload_url, {
      method,
      body: fd,
      headers: ticket.headers || {},
    });
  } else {
    res = await fetch(ticket.upload_url, {
      method,
      body: file,
      headers: Object.assign({ "Content-Type": file.type || "application/octet-stream" }, ticket.headers || {}),
    });
  }
  if (!res.ok) throw new Error(`upload failed: ${res.status}`);
  return ticket;
}
