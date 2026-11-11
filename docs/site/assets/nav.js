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
      domain: "cluster-lifecycle",
      // One hub limb: the day1 git token (a push and the provider's merge-request
      // API) and the day2 sig repos' token; no Kubernetes credential. The
      // segment comes from allocate-segment, started as an awaited child.
      queue: "cluster-lifecycle-activity",
      workflows: [
        {
          name: "provision-cluster",
          href: "/cluster-lifecycle/provision-cluster.html",
          blurb: "a hosted cluster from its YAML: segment, day1 branch, merge request, day2"
        }
      ]
    },
    {
      domain: "segment-lifecycle",
      queue: "segment-lifecycle-activity",
      workflows: [
        {
          name: "initialize-segment",
          href: "/segment-lifecycle/initialize-segment.html",
          blurb: "create a segment — born Available, typeless"
        },
        {
          name: "allocate-segment",
          href: "/segment-lifecycle/allocate-segment.html",
          blurb: "reserve a VLAN for a cluster and verify it"
        },
        {
          name: "release-segment",
          href: "/segment-lifecycle/release-segment.html",
          blurb: "give the VLAN back to the pool"
        }
      ]
    },
    {
      domain: "server-lifecycle",
      // Per MCE: the activity queue carries the target cluster's name, so the
      // brain picks which cluster to write to purely by where it dispatches.
      queue: "server-lifecycle-activity-<mce>",
      workflows: [
        {
          name: "add-server-to-mce",
          href: "/server-lifecycle/add-server-to-mce.html",
          blurb: "put a physical server into an MCE InfraEnv"
        },
        {
          name: "publish-infraenv-to-pxe-server",
          href: "/server-lifecycle/publish-infraenv-to-pxe-server.html",
          blurb: "an InfraEnv's boot entry onto its PXE VM"
        },
        {
          name: "remove-server-from-mce",
          href: "/server-lifecycle/remove-server-from-mce.html",
          blurb: "take a server out of an MCE's inventory"
        },
        {
          name: "uninstall-server-from-hc",
          href: "/server-lifecycle/uninstall-server-from-hc.html",
          blurb: "take a server out of its hosted cluster and the MCE"
        }
      ]
    },
    {
      domain: "server-provisioning",
      // Not per MCE: this limb writes to no cluster at all, only outward to the
      // iDRACs, OME, the naming service and server-scan. One deployment serves
      // the whole estate.
      queue: "server-provisioning-activity",
      workflows: [
        {
          name: "provision-dell-server",
          href: "/server-provisioning/provision-dell-server.html",
          blurb: "rack-ready Dell → listed in server-scan"
        }
      ]
    },
    {
      domain: "cluster-upgrade",
      // Three limbs: the per-MCE cluster limb shown here (reads, Argo CD syncs,
      // watches), plus upgrade-gitops-activity on the hub (day1 git, the mirror)
      // and upgrade-agent-activity-<mce> (the agent). Built and tested on a fake
      // fleet; its routes are mounted, and a run waits on any limb not deployed yet.
      queue: "cluster-upgrade-activity-<mce>",
      workflows: [
        {
          name: "upgrade-cluster",
          href: "/cluster-upgrade/upgrade-cluster.html",
          blurb: "a HostedCluster to a newer release, via day1 git · sandbox only"
        },
        {
          name: "upgrade-targets",
          href: "/cluster-upgrade/upgrade-targets.html",
          blurb: "which releases a cluster can go to · one GET"
        }
      ]
    },
    {
      domain: "vm-lifecycle",
      // One hub limb for every zone: it holds no Kubernetes credential and
      // reaches each KubeVirt cluster through the two agents the platform
      // deploys there (kv-capacity, kv-vm-api), behind Routes.
      queue: "vm-lifecycle-activity",
      workflows: [
        {
          name: "create-vm",
          href: "/vm-lifecycle/create-vm.html",
          blurb: "one Halted VM, fixed MAC, on the best-fitting KubeVirt cluster of a zone"
        }
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
