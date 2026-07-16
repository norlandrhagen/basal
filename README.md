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

## Docs

[norlandrhagen.github.io/basal](https://norlandrhagen.github.io/basal/)
