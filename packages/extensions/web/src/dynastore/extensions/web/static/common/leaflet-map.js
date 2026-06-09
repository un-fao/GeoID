// common/leaflet-map.js — shared Leaflet initialiser. The page must load Leaflet 1.9.4 (unpkg) before importing this.

const BASEMAP = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png";
const ATTRIB = "&copy; OpenStreetMap, &copy; CARTO";

// initMap(elId) -> Leaflet map centred on the world.
export function initMap(elId) {
  const map = L.map(elId, { worldCopyJump: true }).setView([20, 0], 2);
  L.tileLayer(BASEMAP, { attribution: ATTRIB, subdomains: "abcd", maxZoom: 19 }).addTo(map);
  return map;
}

// showGeoJSON(map, layerRef, geojson) -> replaces layerRef.current with a new GeoJSON layer, fits bounds.
export function showGeoJSON(map, layerRef, geojson) {
  if (layerRef.current) {
    map.removeLayer(layerRef.current);
    layerRef.current = null;
  }
  if (!geojson) return;
  const layer = L.geoJSON(geojson);
  layer.addTo(map);
  layerRef.current = layer;
  try {
    const b = layer.getBounds();
    if (b.isValid()) map.fitBounds(b, { maxZoom: 12 });
  } catch (e) {
    /* empty geometry */
  }
}

// bboxFromMap(map) -> "minx,miny,maxx,maxy" for the current view, for OGC bbox= params.
export function bboxFromMap(map) {
  const b = map.getBounds();
  return [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].map((n) => n.toFixed(6)).join(",");
}
