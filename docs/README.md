# docs — the Redbull Workflows documentation site

A static site: the **workflows-orchestrator guide** as the homepage, plus one page per
workflow. No build step, no framework, no runtime dependencies — plain HTML served by
nginx, so the only thing that can break it is bad HTML.

Deployed by Argo CD into `redbull-workflows` as the `workflows-docs` service
(image built here → chart in `helm-charts-workflows-docs` → Argo app folder in
`redbull-platform/gitops/services/prod/workflows-docs/`).

```
docs/
  Dockerfile                       nginx-unprivileged + the site
  nginx.conf                       port 8080, /healthz, extensionless URLs
  docker-entrypoint.d/
    20-doc-version.sh              stamps DOC_VERSION into the header at start-up
  site/
    index.html                     the orchestrator guide (homepage)
    404.html
    segment-lifecycle/
      open-segment-rules.html      one page per workflow, under its domain
    assets/
      site.css                     every page's styling
      nav.js                       THE workflow catalogue + topbar menu behaviour
      favicon.svg
```

## Adding a workflow page

1. Create `site/<domain>/<workflow>.html`. Copy the `<head>` + `<header class="topbar">`
   block from an existing page verbatim and set `<body data-page="/<domain>/<workflow>.html">`
   — that value is what highlights the current entry in the menu.
2. Add the workflow to `CATALOGUE` in `site/assets/nav.js` with its `href`. That single
   entry drives both the topbar menu and the homepage catalogue cards, so there is
   nothing else to update.

A workflow that has no page yet gets `href: null`. It renders as a non-clickable
*planned* row in both places — visible, but never a dead link.

Domains and workflows in the catalogue that are not built yet
(`convert-segment`, the whole `segment-provisioner` domain) are there to show the shape
the catalogue takes as it grows; delete them once real ones replace them.

## Local preview

```sh
# plain python, no container
python3 -m http.server -d site 8081     # then http://localhost:8081
                                        # the header shows the literal
                                        # __DOC_VERSION__ placeholder

# or exactly what ships, version stamping included (build from the repo root)
docker build -f docs/Dockerfile -t workflows-docs:dev .
docker run --rm -p 8081:8080 -e DOC_VERSION=local workflows-docs:dev
```

## The size of everything

`site.css` sets one root font size and expresses every other length in `rem`, so a single
declaration scales the whole page — type, column width, spacing and the diagrams together:

```css
html { font-size: 106.25%; }    /* 17px. 112.5% = 18px, 100% = 16px */
```

Change that number and nothing else. It is a percentage rather than a px value so a reader
who has set a browser font-size preference still gets it honoured proportionally.

Two things stay in `px` on purpose. `svg text` sizes are viewBox **user units** — already
scaled by the browser when it fits a drawing to its container — so `rem` there would detach
the labels from the geometry they annotate and overflow their boxes. Borders, focus rings
and radii are chrome rather than type: a hairline that grows with the font reads as a
heavier border, not a bigger page.

There is no viewport-keyed sizing, deliberately. CSS cannot see physical screen size or
viewing distance, and the apparent proxies all lie — viewport width cannot tell a 13" laptop
from a small window on a 32" monitor. So the site ships one comfortable size for every
screen instead of guessing per-screen.

## The version in the header

`DOC_VERSION` comes from the chart, which sets it to the image tag CI just bumped, so the
number in the top-left is by construction the image being served. Running the image by
hand gives `dev`; serving `site/` directly gives the raw `__DOC_VERSION__` placeholder,
which is the honest answer for "nothing deployed this".
