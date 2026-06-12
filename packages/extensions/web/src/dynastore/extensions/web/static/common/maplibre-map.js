// common/maplibre-map.js — shared MapLibre GL initialiser for globe-projection pages.
// The page must load MapLibre GL JS (v5.x) and deck.gl as plain CDN scripts before
// importing this module — they expose window.maplibregl and window.deck respectively.

const OSM_STYLE = {
  version: 8,
  sources: {
    "osm-raster": {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxzoom: 19,
    },
  },
  layers: [
    {
      id: "osm-tiles",
      type: "raster",
      source: "osm-raster",
      minzoom: 0,
      maxzoom: 22,
    },
  ],
};

/**
 * createMapLibreGlobe(containerId, opts) -> { map, overlay }
 *
 * Builds a MapLibre GL map with globe projection, OSM raster basemap,
 * GlobeControl + NavigationControl, and a deck.MapboxOverlay (interleaved).
 *
 * @param {string} containerId  - id of the DOM element to mount into.
 * @param {object} [opts]       - optional overrides:
 *   opts.center  {[lng, lat]}  default [0, 20]
 *   opts.zoom    {number}      default 1.5
 *   opts.style   {object}      MapLibre style object; defaults to OSM raster
 * @returns {{ map: maplibregl.Map, overlay: deck.MapboxOverlay }}
 */
export function createMapLibreGlobe(containerId, opts = {}) {
  /* global maplibregl, deck */
  const map = new maplibregl.Map({
    container: containerId,
    style: opts.style || OSM_STYLE,
    center: opts.center || [0, 20],
    zoom: opts.zoom !== undefined ? opts.zoom : 1.5,
    projection: "globe",
  });

  map.addControl(new maplibregl.GlobeControl(), "top-right");
  map.addControl(new maplibregl.NavigationControl(), "top-right");

  const overlay = new deck.MapboxOverlay({ interleaved: true, layers: [] });
  map.addControl(overlay);

  return { map, overlay };
}
