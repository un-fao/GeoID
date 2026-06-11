// geovolumes-globe.js — OGC API 3D GeoVolumes globe browser.
// Depends on: maplibre-gl.js, deck.gl dist.min.js, common/i18n.js,
// common/maplibre-map.js — all loaded as plain scripts before this file.

(function () {
  "use strict";

  // ---------------------------------------------------------------------------
  // Constants
  // ---------------------------------------------------------------------------

  // Base path for the OGC API (same origin, rooted at /geovolumes).
  // The HTML page is served at /web/geovolumes/ so ../../geovolumes reaches
  // the API mount at /geovolumes.
  var API_BASE = "../../geovolumes";
  var FEATURES_BASE = "../../features";

  // 3D Tiles relationship URI per OGC API GeoVolumes spec.
  var REL_3DTILES = "http://www.opengis.net/def/rel/ogc/1.0/3dtiles";

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------

  var _map = null;
  var _overlay = null;
  var _activeLayers = [];
  var _catalogId = null;
  var _containers = [];       // flat list of loaded ThreeDContainer objects
  var _activeContainerId = null;

  // ---------------------------------------------------------------------------
  // DOM refs (populated after DOMContentLoaded)
  // ---------------------------------------------------------------------------

  var catalogSelect, containerTree, infoPanel, infoPropsDl, infoClose;

  // ---------------------------------------------------------------------------
  // Initialise globe after DOM is ready
  // ---------------------------------------------------------------------------

  document.addEventListener("DOMContentLoaded", function () {
    catalogSelect  = document.getElementById("catalog-select");
    containerTree  = document.getElementById("container-tree");
    infoPanel      = document.getElementById("info-panel");
    infoPropsDl    = document.getElementById("info-props");
    infoClose      = document.getElementById("info-close");

    var globe = createMapLibreGlobe("geovolumes-map");
    _map     = globe.map;
    _overlay = globe.overlay;

    infoClose.addEventListener("click", function () { infoPanel.style.display = "none"; });

    catalogSelect.addEventListener("change", function () {
      _catalogId = catalogSelect.value || null;
      if (_catalogId) loadContainers(_catalogId);
      else resetTree("Select a catalog to browse 3D containers.");
    });

    loadCatalogs();
  });

  // ---------------------------------------------------------------------------
  // Fetch helpers
  // ---------------------------------------------------------------------------

  function fetchJSON(url) {
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status + " for " + url);
      return r.json();
    });
  }

  // ---------------------------------------------------------------------------
  // Catalog selection
  // ---------------------------------------------------------------------------

  function loadCatalogs() {
    fetchJSON(API_BASE + "/catalogs")
      .then(function (data) {
        var cats = data.catalogs || data.collections || [];
        cats.forEach(function (c) {
          var opt = document.createElement("option");
          opt.value = c.id;
          opt.textContent = c.title || c.id;
          catalogSelect.appendChild(opt);
        });
      })
      .catch(function () {
        resetTree("Failed to load catalogs.");
      });
  }

  // ---------------------------------------------------------------------------
  // Container tree
  // ---------------------------------------------------------------------------

  function loadContainers(catalogId) {
    resetTree("Loading containers…");
    fetchJSON(API_BASE + "/catalogs/" + encodeURIComponent(catalogId) + "/collections")
      .then(function (data) {
        var containers = (data.collections || []).filter(function (c) {
          return c.collectionType === "3dcontainer";
        });
        _containers = containers;
        renderContainerTree(containers);
        renderVolumeLayers(containers);
      })
      .catch(function () {
        resetTree("Failed to load 3D containers.");
      });
  }

  function resetTree(msg) {
    var spinner = document.createElement("div");
    spinner.className = "loading-spinner";
    var p = document.createElement("p");
    p.textContent = msg;
    spinner.appendChild(p);
    containerTree.replaceChildren(spinner);
    _containers = [];
    _activeContainerId = null;
    clearOverlayLayers();
  }

  // Recursive tree render (handles children nesting).
  function renderContainerTree(containers) {
    containerTree.replaceChildren();
    if (!containers || containers.length === 0) {
      var spinner = document.createElement("div");
      spinner.className = "loading-spinner";
      var p = document.createElement("p");
      p.textContent = "No 3D containers found.";
      spinner.appendChild(p);
      containerTree.appendChild(spinner);
      return;
    }
    containers.forEach(function (c) {
      containerTree.appendChild(buildNodeEl(c, 0));
    });
  }

  function buildNodeEl(container, depth) {
    var wrapper = document.createElement("div");

    var node = document.createElement("div");
    node.className = "container-node";
    node.dataset.id = container.id;

    var titleSpan = document.createElement("span");
    titleSpan.textContent = container.title || container.id;

    var typeDiv = document.createElement("div");
    typeDiv.className = "node-type";
    typeDiv.textContent = container.collectionType || "";

    node.appendChild(titleSpan);
    node.appendChild(typeDiv);
    node.addEventListener("click", function () { selectContainer(container); });
    wrapper.appendChild(node);

    // Render nested children if present.
    var children = container.children || [];
    if (children.length > 0) {
      var childDiv = document.createElement("div");
      childDiv.className = "container-children";
      children.forEach(function (ch) {
        childDiv.appendChild(buildNodeEl(ch, depth + 1));
      });
      wrapper.appendChild(childDiv);
    }

    return wrapper;
  }

  // ---------------------------------------------------------------------------
  // Volume (extruded polygon) layers — one per container
  // ---------------------------------------------------------------------------

  function renderVolumeLayers(containers) {
    var polygonData = [];
    containers.forEach(function (c) {
      var ext = c.contentExtent || {};
      var bbox = ext.bbox;  // [w, s, zmin, e, n, zmax]
      if (!bbox || bbox.length < 6) return;
      var w = bbox[0], s = bbox[1], zmin = bbox[2], e = bbox[3], n = bbox[4], zmax = bbox[5];
      polygonData.push({
        id: c.id,
        contour: [[w, s], [e, s], [e, n], [w, n], [w, s]],
        elevation: zmin,
        extrudedHeight: zmax,
        container: c,
      });
    });

    var volumeLayer = new deck.PolygonLayer({
      id: "geovolumes-extents",
      data: polygonData,
      extruded: true,
      wireframe: true,
      getPolygon: function (d) { return d.contour; },
      getElevation: function (d) { return d.extrudedHeight - d.elevation; },
      getFillColor: [99, 102, 241, 64],   // indigo, ~0.25 opacity
      getLineColor: [99, 102, 241, 180],
      lineWidthMinPixels: 1,
      pickable: true,
      onClick: function (info) {
        if (info.object) selectContainer(info.object.container);
      },
    });

    setOverlayLayers([volumeLayer]);
  }

  // ---------------------------------------------------------------------------
  // Container selection: fly to + add 3D Tiles + footprints layer
  // ---------------------------------------------------------------------------

  function selectContainer(container) {
    // Mark active in sidebar.
    document.querySelectorAll(".container-node").forEach(function (el) {
      el.classList.toggle("active", el.dataset.id === container.id);
    });
    _activeContainerId = container.id;

    // Fly to contentExtent centre.
    var ext = container.contentExtent || {};
    var bbox = ext.bbox;
    if (bbox && bbox.length >= 5) {
      var lng = (bbox[0] + bbox[3]) / 2;
      var lat = (bbox[1] + bbox[4]) / 2;
      _map.flyTo({ center: [lng, lat], zoom: 14, pitch: 50, duration: 1500 });
    }

    // Build deck layers for this container.
    var layers = buildContainerLayers(container);
    setOverlayLayers(layers);
  }

  function buildContainerLayers(container) {
    var layers = [];

    // --- Re-render all volume footprints (so they stay visible) ---
    var polygonData = [];
    _containers.forEach(function (c) {
      var ext = c.contentExtent || {};
      var bbox = ext.bbox;
      if (!bbox || bbox.length < 6) return;
      var w = bbox[0], s = bbox[1], zmin = bbox[2], e = bbox[3], n = bbox[4], zmax = bbox[5];
      polygonData.push({
        id: c.id,
        contour: [[w, s], [e, s], [e, n], [w, n], [w, s]],
        elevation: zmin,
        extrudedHeight: zmax,
        container: c,
      });
    });

    layers.push(new deck.PolygonLayer({
      id: "geovolumes-extents",
      data: polygonData,
      extruded: true,
      wireframe: true,
      getPolygon: function (d) { return d.contour; },
      getElevation: function (d) { return d.extrudedHeight - d.elevation; },
      getFillColor: function (d) {
        return d.id === container.id ? [99, 102, 241, 100] : [99, 102, 241, 40];
      },
      getLineColor: [99, 102, 241, 180],
      lineWidthMinPixels: 1,
      pickable: true,
      onClick: function (info) {
        if (info.object) selectContainer(info.object.container);
      },
    }));

    // --- 3D Tiles layer (if a 3dtiles link is present) ---
    var tilesHref = find3dTilesHref(container);
    if (tilesHref) {
      layers.push(new deck.Tile3DLayer({
        id: "3dtiles-" + container.id,
        data: tilesHref,
        pickable: true,
        onTilesetLoad: function () {},
      }));
    }

    // --- GeoJSON footprints layer (items from Features API) ---
    if (_catalogId) {
      var itemsUrl =
        FEATURES_BASE +
        "/catalogs/" + encodeURIComponent(_catalogId) +
        "/collections/" + encodeURIComponent(container.id) +
        "/items?limit=200";

      layers.push(new deck.GeoJsonLayer({
        id: "footprints-" + container.id,
        data: itemsUrl,
        pickable: true,
        stroked: true,
        filled: true,
        getFillColor: [99, 200, 241, 40],
        getLineColor: [99, 200, 241, 180],
        lineWidthMinPixels: 1,
        onClick: function (info) {
          if (info.object) showBuildingPopup(info.object);
        },
      }));
    }

    return layers;
  }

  // ---------------------------------------------------------------------------
  // Building attribute popup
  // ---------------------------------------------------------------------------

  function showBuildingPopup(feature) {
    var props = feature.properties || {};
    infoPropsDl.innerHTML = "";

    var keys = Object.keys(props);
    if (keys.length === 0) {
      var dt = document.createElement("dt");
      dt.textContent = "(no properties)";
      infoPropsDl.appendChild(dt);
    } else {
      // Preferred keys first, then remainder.
      var preferred = ["citygml_type", "height", "lod"];
      var rest = keys.filter(function (k) { return preferred.indexOf(k) === -1; });
      var ordered = preferred.filter(function (k) { return k in props; }).concat(rest);

      ordered.forEach(function (key) {
        var dt = document.createElement("dt");
        dt.textContent = key;
        var dd = document.createElement("dd");
        dd.textContent = String(props[key]);
        infoPropsDl.appendChild(dt);
        infoPropsDl.appendChild(dd);
      });
    }

    infoPanel.style.display = "block";
  }

  // ---------------------------------------------------------------------------
  // Overlay layer management
  // ---------------------------------------------------------------------------

  function setOverlayLayers(layers) {
    _activeLayers = layers;
    _overlay.setProps({ layers: layers });
  }

  function clearOverlayLayers() {
    _activeLayers = [];
    _overlay.setProps({ layers: [] });
  }

  // ---------------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------------

  function find3dTilesHref(container) {
    var links = (container.content || []).concat(container.links || []);
    for (var i = 0; i < links.length; i++) {
      if (links[i].rel === REL_3DTILES) return links[i].href;
    }
    return null;
  }
})();
