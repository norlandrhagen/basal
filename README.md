# basal: an icechunk native catalog

A small, serverless data catalog built on [Icechunk](https://icechunk.io).

*basal* as in the bottom layer of an ice sheet, not the herb.

> **Warning: super experimental — may change at any time.**

## Install

```
uv add "basal @ git+https://github.com/norlandrhagen/basal"
```

Optional extras (basal is not on PyPI, so extras need the git URL too):

```
uv add "basal[search] @ git+https://github.com/norlandrhagen/basal"   # DuckDB SQL + similarity search
uv add "basal[stac] @ git+https://github.com/norlandrhagen/basal"     # STAC API server
```

## Web component (experimental)

`@carbonplan/basal` is a framework-free browser client + catalog dashboard. It
reads an icechunk catalog directly over HTTP (via `icechunk-js` + `zarrita`) — no
backend. Drop the `<basal-catalog>` custom element onto any page:

```html
<script type="module">
  import "@carbonplan/basal"; // auto-registers <basal-catalog>
</script>
<basal-catalog url="https://.../public_icechunk_stores"></basal-catalog>
```

It renders a searchable entry grid and an xarray-style detail view
(dims/coords/variables/chunks), lazily inspecting the store when the catalog lacks
schema metadata. Click-to-visualize is an opt-in extension point
(`registerVizProvider`) — wire in `@carbonplan/zarr-layer` to add live maps.

Source lives in [`js/`](js/); run the dashboard demo with
`cd js/demo && npm install && npm run dev` then open `/dashboard.html`.

## Docs

[norlandrhagen.github.io/basal](https://norlandrhagen.github.io/basal/)
