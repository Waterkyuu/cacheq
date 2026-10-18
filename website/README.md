# Build and publish the documentation site

The website uses Astro + Starlight and TypeScript. Go generates a local search index from the rendered documentation and public Go declarations. No search service or API key is required.

## Run locally

Requires Node.js 24, pnpm 11.10.0, and Go 1.22 or later.

```sh
pnpm --dir website install --frozen-lockfile
pnpm --dir website start
```

Astro serves the default English site. The same dev server serves Chinese at `/soroi/zh-CN/`. The search index is produced by the production build; use the built preview to test search.

## Check and preview the published build

```sh
pnpm --dir website lint
pnpm --dir website fmt:check
pnpm --dir website typecheck
pnpm --dir website test
pnpm --dir website build
pnpm --dir website test:build
pnpm --dir website serve
```

`pnpm --dir website fmt` formats TypeScript, TSX, CSS, and configuration with oxfmt. oxlint checks the frontend; `astro check` validates Astro templates and TypeScript, while `tsc` validates scripts. Repository Go checks also cover the search indexer.

## Edit documentation

Edit the named English and Chinese feature guides in `docs`. `website/scripts/prepare-docs.mts` creates ignored Starlight inputs, preserving code blocks and adapting source links. The generated documents, Astro cache, installed dependencies, search JSON, and rendered HTML are not committed.

The default locale is English, with the homepage at `/soroi/` and guides under `/soroi/docs/`. Chinese lives under `/soroi/zh-CN/`, with guides under `/soroi/zh-CN/docs/`. Go uses rendered page paths and heading IDs for documentation search, and indexes exported types, functions, methods, constants, and errors from the root Go package. API results open the exact source declaration.

## GitHub Pages deployment

The `Documentation` workflow installs from the pnpm lockfile, runs lint, formatting, type checks and tests, builds both locales, generates the Go search index, checks published links, and deploys the Pages artifact.

Pushes to `main` and `waterkyuu/feat/docs-site` publish changes. Pull requests targeting `main` run build checks without publishing. GitHub Pages uses the Actions publishing source, and the `github-pages` environment allows the publishing branches.

The public address is <https://waterkyuu.github.io/soroi/>. `DOCS_REF` selects the branch for source links during CI builds.
