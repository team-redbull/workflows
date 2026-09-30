# The Dell SCP template, as provision-dell-server deploys it

Researched 2026-09-30 for the `server-provisioning` domain, to answer four
questions the DC team raised: what an OME template actually contains, whether a
template is tied to the firmware it was captured on, which fields are dangerous
to deploy, and how to tell whether a deployment really applied.

**Every fact below names its source.** Unlike the sibling
[`dell-ome-idrac-api.md`](dell-ome-idrac-api.md) — written when dell.com was
unreachable from the build environment — Dell's published documentation *was*
reachable this time, so the primary source is the **Server Configuration
Profiles: Reference Guide** (whitepaper 456, iDRAC9 4.40, 70 pp., cited below as
*SCP-RG*), supported by Dell KB articles and the OME release notes.

An OME **template** is an SCP under a different name: OME captures one by
running an SCP Export against a reference server, and deploying it runs an SCP
Import on the target. So everything the SCP guide says about import behaviour is
what OME template deployment does.

## Anatomy

```xml
<SystemConfiguration Model="PowerEdge R740" ServiceTag="G123456" TimeStamp="Tue Jul 21 13:56:41 2020">
  <Component FQDD="NIC.Embedded.2-1-1">
    <Attribute Name="VirtMacAddr">F4:02:70:B4:13:DB</Attribute>
  </Component>
</SystemConfiguration>
```

Three levels, and no more: a root carrying `Model` / `ServiceTag` / `TimeStamp`,
one `Component` per device keyed by its **FQDD**, and flat `Attribute` elements
inside it. (*SCP-RG* §3)

### The components an R660 exports

`Component FQDD` values, with the export shorthand that selects them
(*SCP-RG* §3.1, "Available shorthand options"):

| shorthand | FQDD | what it carries |
| --- | --- | --- |
| `iDRAC` | `iDRAC.Embedded.1` | iDRAC users, network, services, alerting — **the dangerous one**, see below |
| `System` | `System.Embedded.1` | server OS hostname, asset tag, power policy |
| `LifecycleController` | `LifecycleController.Embedded.1` | LC enable/disable, part-replacement policy |
| `BIOS` | `BIOS.Setup.1-1` | every BIOS setup option: boot mode, virtualisation, SR-IOV, memory, profiles |
| `NIC` | `NIC.*` | per-port NIC config, including `VirtMacAddr` |
| `RAID` | *all storage devices* | controllers, physical disks, virtual disks |
| `AHCI` | `AHCI.*` | BOSS-S1 lives here |
| `Disk` | `Disk.*` | `Disk.Virtual.N:<controller>`, `Disk.Direct.N-N:<controller>` |
| `PCIeSSD` | `PCIeSSD.*` | NVMe |
| `FC` / `InfiniBand` | `FC.*` / `InfiniBand.*` | not present on our R660s |
| `EventFilters` | `EventFilters.*.1` | alert destinations and actions |
| `SupportAssist` | `SupportAssist.Embedded.1` | phone-home |

Shorthand works for both export and import, and `NIC` expands to every NIC
while `NIC.Integrated.1-1-1` selects exactly one. (*SCP-RG* §3.1)

## The firmware version is metadata, never a gate

**A template is not locked to the firmware it was captured on.** This was the
DC team's main question and the answer is unambiguous.

The root element carries `Model`, `ServiceTag` and `TimeStamp` — there is no
firmware attribute on it. The versions visible at the end of a template are
ordinary attributes such as `Info.1#Version`, and they are **ReadOnly**:

```xml
<!-- ReadOnly <Attribute Name="Info.1#Version">4.40.00.00</Attribute> -->
```
```json
{ "Name": "Info.1#Version", "Value": "4.40.00.00",
  "Set On Import": "False", "Comment": "Always Read Only" }
```

> "These attributes cannot be set during an SCP Import and are only available
> for informational purposes." — *SCP-RG* §3.2

ReadOnly attributes are excluded from a template by default and only appear at
all when the exporter passes *Include ReadOnly*; in XML they are commented out,
in JSON they carry `"Set On Import": "False"`. Either way they are inert.

So there is nothing to remove to make a template "version agnostic", and no
reason to generate one template per firmware version. Dell states the template
schema "has remained unchanged from release until iDRAC9 version 4.40" and that
"the template itself is backwards compatible throughout all generations of
iDRAC" (*SCP-RG* §2.5).

### What actually breaks across firmware: the attribute SET

Portability is limited by *which attributes exist*, not by a version stamp:

> "feature sets for individual devices can change over their lifetime. The
> possible values for attributes might be added or removed, or entire attributes
> might have been added or removed depending on the firmware version of a
> device." — *SCP-RG* §2.5

The concrete, documented case is
[KB 000326070](https://www.dell.com/support/kbdoc/en-au/000326070/idrac9-template-deployment-fails-or-import-system-config-profiles-complete-with-errors):
iDRAC firmware **7.20.30.00** added `SHA384v3Key` / `SHA512v3Key` to local user
accounts. A template captured before it fails on newer firmware with **SYS055**
(OME) or **SYS171**, ErrCode 10320, because an import containing SNMPv3-enabled
users requires every SNMPv3 key attribute to carry a value. Dell's resolution is
to re-enter the user passwords and **re-create the template**; the workaround is
to *"Remove or comment the user's SNMPv3 key attributes from the Template or
SCP."*

That failure is in `Users.*` — which is why this workflow stopped putting the
root password in the template at all (see *What this changed in the code*).
Removing the component removes the incompatibility, which is Dell's own
workaround arrived at from the other direction.

Because SCP Import is a **"continue on error"** operation (*SCP-RG* §2.5), a
single incompatible attribute does not fail the deployment — it fails that one
attribute and the rest still apply. That is precisely why a deployment reporting
success proves nothing, and why this workflow verifies attributes individually.

## Export type changes the file, silently

`Basic`, `Clone` and `Replace` do not merely select components — Clone and
Replace **rewrite** what they export (*SCP-RG* §3.4).

**Storage** (§3.4.2). Clone and Replace automatically set:

```xml
<Attribute Name="RAIDresetConfig">True</Attribute>
<Attribute Name="RAIDforeignConfig">Clear</Attribute>
```

and flip the virtual disk from `Update` to create, uncommenting every attribute
the create needs:

```xml
<Component FQDD="Disk.Virtual.0:RAID.Integrated.1-1">
  <Attribute Name="RAIDaction">Create</Attribute>
  <Attribute Name="RAIDTypes">RAID 1</Attribute>
  <Attribute Name="IncludedPhysicalDiskID">Disk.Bay.0:Enclosure.Internal.0-1:RAID.Integrated.1-1</Attribute>
  <Attribute Name="IncludedPhysicalDiskID">Disk.Bay.1:Enclosure.Internal.0-1:RAID.Integrated.1-1</Attribute>
  <!-- BootVD, RAIDinitOperation, StripeSize, SpanDepth, SpanLength, ... -->
</Component>
```

`RAIDresetConfig=True` means **every deploy wipes the controller** before
applying. On a machine that already has its array, that is data loss.

**Passwords** (§3.4.1). Without *Include Password Hashes*, Clone and Replace do
not omit user passwords — they **invent** them:

| iDRAC9 version | generated `Users.2#Password` |
| --- | --- |
| before 4.40.00.00 | `calvin` |
| 4.40.00.00 and later | `Calvin#SCP#CloneReplace1` |

A template captured this way sets root to *that*, not to `IDRAC_ROOT_PASSWORD`.
In a Basic export the field is present but commented out and obfuscated with
`******`, and a plaintext value can be filled in by hand (§3.3).

## What our templates must not contain

Three groups, each fatal for a different reason. `template_policy.py` enforces
them and the run refuses to deploy a template carrying any of them. An attribute
marked `IsIgnored` in OME is not deployed, so it is not a hazard.

**1 — iDRAC network settings.** `IPv4Static.1#Address`, `#Gateway`, `#Netmask`,
`IPv4.1#DHCPEnable`, `NIC.1#VLanEnable`/`#VLanID`, `NIC.1#DNSRacName` and their
IPv6 counterparts. These are the address the workflow reached the machine on. A
template carrying the reference server's address would move every target onto
one address, or reset it to DHCP — and the machine is then unreachable, with no
path back except the rack. This is the highest-consequence item on the page.

**2 — Storage and RAID.** Any `Disk.Virtual.*` or `Disk.Direct.*` component, and
the `RAIDresetConfig` / `RAIDforeignConfig` / `RAIDaction` attributes. Two
independent reasons:

- `RAIDresetConfig=True` wipes the controller on every deploy (above), and this
  workflow never destroys data (`storage_plan.py`).
- On OME **4.5.x** it does not even work:
  [KB 000384312](https://www.dell.com/support/kbdoc/en-us/000384312/deploying-a-template-with-the-virtual-disk-configuration-set-to-action-create-fails-in-openmanage-enterprise-4-5-x)
  — OME's template ingestion misidentifies the repeated `IncludedPhysicalDiskID`
  attribute as a duplicate and **drops every physical disk after the first**, so
  the deploy fails with *"Unable to create a virtual disk because an invalid
  combination of span count or number of disks was entered for the RAID level
  selected."* Our BOSS RAID 1 spans exactly two disks: the precise case that
  breaks. Not present in 4.3 or 4.4; fixed in **4.6.0**.

Storage is built over Redfish instead, where it is idempotent, verifiable and
never destructive.

**3 — User accounts.** `Users.*`. The root password is set over Redfish before
OME ever sees the machine, so the template does not need them — and carrying
them is what makes a template firmware-fragile (KB 000326070 above). Touching
any slot other than 2 is separately forbidden: the OME account lives on the
iDRAC too, and changing it cuts OME off from the machine (CLAUDE.md §4).

## Known issues worth knowing

| id | what | status |
| --- | --- | --- |
| [KB 000384312](https://www.dell.com/support/kbdoc/en-us/000384312/deploying-a-template-with-the-virtual-disk-configuration-set-to-action-create-fails-in-openmanage-enterprise-4-5-x) | OME 4.5.x drops every `IncludedPhysicalDiskID` after the first; RAID create fails | fixed in OME 4.6.0 |
| [KB 000326070](https://www.dell.com/support/kbdoc/en-au/000326070/idrac9-template-deployment-fails-or-import-system-config-profiles-complete-with-errors) | iDRAC 7.20.30.00 added SNMPv3 key attributes; older templates fail SYS055 / SYS171 | re-create the template, or strip `Users.*` |
| FLT02M-118 | after upgrading to OME 4.5, deployment fails on `Duplicate key SecurityCertificate.1#CertData` | open; recreate templates and baselines in 4.5 |
| 273437 | RAID Initialization Operation deployment fails on non-RAID servers | open; set Initialization to NONE |
| 282738 | template deployment carrying user passwords reverts the device credential type to discovery credentials | open; run an onboarding task afterwards |

(OME 4.5 release notes, *Known issues*.)

## Telling whether a deployment actually applied

A finished job is not evidence. SCP Import is "continue on error", and Dell's
own reference client treats `JobState == "Completed"` as insufficient —
`ImportSystemConfigurationLocalREDFISH.py` string-searches the job message for
`fail` / `error` / `unable` / `not` and reports FAIL when it finds one.

The authoritative per-attribute record is **ConfigResults**, logged to the
Lifecycle Controller log as the import progresses (*SCP-RG* §1.9):

```
racadm lclog viewconfigresult -j JID_952589510966
FQDD            = iDRAC.Embedded.1
Name            = WebServer.1#Timeout
OldValue        = 1805
NewValue        = 1801
Status          = Success
ErrCode         = 0
```

A failure carries `Status = Failure`, an `ErrCode` and a `MessageID` (e.g.
`RAC015`, "the input value is not one of the possible values for ...").

This workflow does not read ConfigResults, and the choice is deliberate: it
would mean correlating an OME job with the iDRAC-side LC job that implemented
it, and it only describes one deployment. Reading the **attribute values
themselves** back over Redfish is independent of how they got there, catches
drift from any source, and is the same read → plan → converge shape
`storage_plan.py` already uses. See `template_policy.py`.

## What this changed in the code

1. **Storage stays out of the template**, settled rather than suspected — the
   sibling doc listed it as unverified. Two independent reasons above.
2. **`Users.*` stays out too**, and root's password is set over Redfish
   (`Accounts/2`) *before* OME discovery. This removed the `rediscovering-in-ome`
   phase entirely: OME is discovered once, with the final password, so its
   credential can never go stale. It also removes the firmware fragility of
   KB 000326070 and the `Calvin#SCP#CloneReplace1` trap.
3. **iDRAC network settings are refused**, a hazard nothing previously checked.
4. **A template is audited before it is deployed** (`auditing-template`), and a
   deployment is verified attribute by attribute afterwards (`verifying-config`),
   with drift remediated by targeted Redfish PATCH rather than by redeploying
   the profile.
5. **`DELL_TEMPLATES` keeps its `(model, iDRAC firmware)` key.** A golden
   template is technically sound — the version is not a gate — but the decision
   is left to evidence: once `verifying-config` has reported what real R660
   batches differ by, allowing one template per model is a one-line change.

## Still unverified: check these on the first real R660

- The attribute ids OME reports under
  `TemplateService/Templates({id})/Views({view})/AttributeViewDetails`, and
  whether the live template carries anything in the three forbidden groups.
  Worth checking **before** the audit ships, so day one is not a wall of
  `TemplateUnsafeError`.
- Which OME view id holds the deployment attributes on this appliance.
- Whether BIOS attributes on an R660 accept a PATCH to
  `Systems/System.Embedded.1/Bios/Settings` with the values this workflow
  remediates, and that one config job applies them all.
- Whether the OME appliance is 4.5.x (KB 000384312 applies) or 4.6.0+.
