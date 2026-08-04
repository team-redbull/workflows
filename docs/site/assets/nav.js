/* =============================================================================
   The workflow catalogue — the ONE place workflows are listed.
   Feeds both the topbar "Workflows" menu (every page) and the catalogue cards on
   the homepage, so the two can never disagree. Adding a workflow is one entry
   here plus its page; nothing else in the site needs touching.

   Grouping is by DOMAIN because that is what the repo is partitioned by (one
   activity deployment, queue and ConfigMap per domain, many workflows inside
   it) — see the "domain or workflow?" section of the guide. The bar itself
   holds a single button rather than one per domain: the panel scrolls, so a
   tenth domain costs nothing, whereas a tenth top-level menu wraps the bar.

   `href: null` means the page does not exist yet. Those entries render as
   non-clickable "planned" rows on purpose — a dead link is worse than no link.
   ========================================================================== */

(function () {
  "use strict";

  var CATALOGUE = [
    {
      domain: "segment-connectivity",
      queue: "segment-connectivity-activity",
      workflows: [
        {
          name: "open-segment-rules",
          href: "/segment-connectivity/open-segment-rules.html",
          blurb: "create → peer → approve → unlock"
        },
        {
          name: "convert-segment",
          href: null,
          blurb: "change a segment's type in place"
        }
      ]
    },
    {
      domain: "segment-provisioner",
      queue: "segment-provisioner-activity",
      workflows: [
        { name: "allocate-segment", href: null, blurb: "pick a free CIDR + VLAN for a site" },
        { name: "release-segment",  href: null, blurb: "tear down and return it to the pool" }
      ]
    }
  ];

  /* ---------------------------------------------------------------- utils */

  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }

  // Which page are we on? Set by each page as <body data-page="…">.
  var CURRENT = document.body.getAttribute("data-page") || "";

  function isCurrent(wf) {
    return wf.href !== null && wf.href === CURRENT;
  }

  function liveCount(domain) {
    var live = 0;
    domain.workflows.forEach(function (w) { if (w.href) live++; });
    return live;
  }

  /* ------------------------------------------------- the topbar dropdown */

  function buildMenuPanel(panel) {
    panel.textContent = "";

    CATALOGUE.forEach(function (d) {
      var group = el("div", "wfgroup");

      var head = el("div", "wfgroup-head");
      head.setAttribute("role", "presentation");
      head.appendChild(el("span", null, d.domain));
      head.appendChild(el("span", "count", liveCount(d) + "/" + d.workflows.length));
      group.appendChild(head);

      d.workflows.forEach(function (w) {
        var row;
        if (w.href) {
          row = el("a", "wfitem");
          row.href = w.href;
          row.setAttribute("role", "menuitem");
          if (isCurrent(w)) row.setAttribute("aria-current", "page");
        } else {
          row = el("div", "wfitem planned");
        }
        row.appendChild(el("span", "n", w.name));
        row.appendChild(el("span", "d", w.href ? w.blurb : "planned"));
        group.appendChild(row);
      });

      panel.appendChild(group);
    });

    var foot = el("div", "wfmenu-foot", "Grouped by domain — one activity deployment each.");
    panel.appendChild(foot);
  }

  function wireMenu() {
    var btn = document.getElementById("wfmenu-btn");
    var panel = document.getElementById("wfmenu-panel");
    if (!btn || !panel) return;

    buildMenuPanel(panel);

    function open() {
      panel.hidden = false;
      btn.setAttribute("aria-expanded", "true");
      document.addEventListener("click", onDocClick, true);
      document.addEventListener("keydown", onKeydown, true);
    }
    function close(refocus) {
      panel.hidden = true;
      btn.setAttribute("aria-expanded", "false");
      document.removeEventListener("click", onDocClick, true);
      document.removeEventListener("keydown", onKeydown, true);
      if (refocus) btn.focus();
    }
    function onDocClick(e) {
      if (!panel.contains(e.target) && !btn.contains(e.target)) close(false);
    }
    function onKeydown(e) {
      if (e.key === "Escape") { e.preventDefault(); close(true); return; }
      if (e.key !== "Tab") return;
      // Keep tabbing inside the open panel; tabbing off the end closes it.
      var items = panel.querySelectorAll("a.wfitem");
      if (!items.length) return;
      var last = items[items.length - 1];
      if (!e.shiftKey && e.target === last) close(false);
      if (e.shiftKey && e.target === items[0]) { e.preventDefault(); close(true); }
    }

    btn.addEventListener("click", function () {
      if (panel.hidden) {
        open();
        var first = panel.querySelector("a.wfitem");
        if (first) first.focus();
      } else {
        close(true);
      }
    });
  }

  /* ------------------------------------------- the homepage catalogue --- */

  function buildCatalogue(host) {
    CATALOGUE.forEach(function (d) {
      var card = el("div", "wfdomain");

      var head = el("div", "wfdomain-head");
      head.appendChild(el("span", "dn", d.domain));
      head.appendChild(el("span", "dq", d.queue));
      card.appendChild(head);

      var body = el("div", "wfdomain-body");
      d.workflows.forEach(function (w) {
        var row = w.href ? el("a", "wfrow") : el("div", "wfrow planned");
        if (w.href) row.href = w.href;
        row.appendChild(el("span", "n", w.name));
        row.appendChild(el("span", "d", w.blurb));
        row.appendChild(el("span", "go", w.href ? "read →" : "planned"));
        body.appendChild(row);
      });
      card.appendChild(body);

      host.appendChild(card);
    });
  }

  function wireCatalogue() {
    var host = document.getElementById("wf-catalogue");
    if (host) buildCatalogue(host);
  }

  /* ------------------------------------------------------------- start */

  wireMenu();
  wireCatalogue();
})();
