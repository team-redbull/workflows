# Dell OME and iDRAC APIs, as provision-dell-server uses them

Researched 2026-09-30 for the `server-provisioning` domain. **Every fact below
names its source.** Dell's documentation sites (dell.com, developer.dell.com)
were unreachable from the build environment, so the primary source is Dell's
own client code: the **`dellemc.openmanage` Ansible collection 9.12.3**, taken
from the `ansible` 12.3.0 wheel on PyPI. It is what Dell ships to drive OME and
iDRAC, so where it builds a request, that is the request the appliance accepts.

What the collection cannot tell us is how a real appliance behaves beyond the
shapes it sends and reads. Those items are listed at the end, and the first run
against real hardware is their test.

## OpenManage Enterprise

| fact | source (collection path) |
| --- | --- |
| Login is `POST SessionService/Sessions` with `UserName`, `Password`, `SessionType: "API"`. The token comes back in the `X-Auth-Token` header, the session `Id` in the body. Logout is `DELETE SessionService/Sessions('<Id>')`. | `plugins/module_utils/ome.py`, `RestOME.__enter__/__exit__` |
| Paged collections follow `@odata.nextLink`, cut at `/api`. | `ome.py`, `get_all_items_with_pagination` |
| Job status is `JobService/Jobs(<id>)` → `LastRunStatus.Id`: 2020 Scheduled, 2030 Queued, 2040 Starting, 2050 Running, **2060 Completed**, 2070 Failed, 2080 New, 2090 Warning, 2100 Aborted, **2101 Paused**, 2102 Stopped, 2103 Canceled. Dell treats 2070/2090/2100/2101/2102/2103 as failed and final. | `ome.py`, `get_job_info` |
| A device is found with `DeviceService/Devices?$filter=DeviceServiceTag eq '<tag>'`. | `ome.py`, `get_device_id_from_service_tag` |
| The SERVER device-type id is read from `DiscoveryConfigService/ProtocolToDeviceType` (`DeviceTypeName`/`DeviceTypeId`), not assumed. | `modules/ome_discovery.py`, `get_protocol_device_map` |
| Discovery is `POST DiscoveryConfigService/DiscoveryConfigGroups` with `DiscoveryConfigGroupName`, `DiscoveryConfigModels[{DiscoveryConfigTargets[{NetworkAddressDetail}], ConnectionProfile, DeviceType[]}]`, `Schedule{RunNow, RunLater, Cron:"startnow"}`. | `ome_discovery.py`, `create_discovery`, `get_schedule` |
| `ConnectionProfile` is a **JSON string**: `profileId 0`, `type DISCOVERY`, and the WS-Man credential (`username`, `password`, `port`, `retries`, `timeout`, `cnCheck`, `caCheck`, `certificateDetail null`, `isHttp false`, `keepAlive true`) **duplicated as `REDFISH`** ("as in GUI"). | `ome_discovery.py`, `get_connection_profile` |
| The discovery's job id is `DiscoveryConfigTaskParam[0].TaskId` in the POST answer. Existing groups are listed with `?$top=9999` and matched by name. | `ome_discovery.py`, `get_job_data`, `check_existing_discovery` |
| A discovery job can finish 2060 and still have failed per IP. The detail is under `JobService/Jobs(<id>)/ExecutionHistories/.../ExecutionHistoryDetails`. **The workflow checks that the device exists afterwards instead**, which covers the same case. | `ome_discovery.py`, `get_execution_details` |
| A template is found with `TemplateService/Templates?$filter=Name eq '<name>'`, then compared exactly on `Name`. | `modules/ome_template.py`, `get_template_by_name` |
| Deploy is `POST TemplateService/Actions/TemplateService.Deploy` with `{"Id": <template>, "TargetIds": [<device>]}` (plus optional `Options`, e.g. `ShutdownType 0`, `EndHostPowerState 1`). **The response body is the bare job id.** | `ome_template.py`, `get_deploy_payload`, `exit_module`, and the module's EXAMPLES |
| Before deploying, Dell skips a device whose profile has **`ProfileState > 0` and the same `TemplateId`**, and refuses one templated from another template ("Please unassign the profiles"). | `ome_template.py`, the `deploy` branch of `_get_resource_parameters` |
| `ProfileState`: 0 unassigned, 1 assigned (auto-deploy), 4 deployed. A profile also carries **`DeploymentTaskId`**, `TemplateId`, `TemplateName`, `TargetId`, `ProfileName`. | `modules/ome_profile.py` (`assign_profile`, `unassign_profile`, `migrate_profile`); `ome_profile_info.py` RETURN sample |
| A profile is renamed with `PUT ProfileService/Profiles(<id>)` carrying `Id` and **`Name`**. Reads return it as `ProfileName`. OME's own default names look like `Profile 00001`. | `ome_profile.py`, `modify_profile`; `ome_profile_info.py` sample |

## iDRAC (Redfish)

| fact | source |
| --- | --- |
| A volume is created with `POST Systems/System.Embedded.1/Storage/<controller>/Volumes` with `RAIDType` (iDRAC firmware > 3.0; older firmware uses `VolumeType`), `Drives[{@odata.id}]`, optional `@Redfish.OperationApplyTime`. | `modules/redfish_storage_volume.py`, `volume_payload`, `is_fw_ver_greater` |
| **BOSS-S1 and BOSS-N1 apply volume changes `OnReset`; PERC controllers default to `Immediate`.** | `redfish_storage_volume.py`, `apply_time` option doc |
| Supported RAID levels are `StorageControllers[0].SupportedRAIDTypes` on the controller's Storage resource. | `redfish_storage_volume.py`, `check_raid_type_supported` |
| Non-RAID is `POST Systems/System.Embedded.1/Oem/Dell/DellRaidService/Actions/DellRaidService.ConvertToNonRAID` with `{"PDArray": [<drive FQDD>...]}`. The job URL is in the `Location` header. Dell only converts drives whose RaidStatus is `Ready`. | `modules/idrac_redfish_storage_controller.py`, `convert_raid_status` |
| **RaidStatus is `Oem.Dell.DellPhysicalDisk.RaidStatus` for SAS/SATA and `Oem.Dell.DellPCIeSSD.RaidStatus` for NVMe.** | `idrac_redfish_storage_controller.py`, `validate_secure_erase` |
| A storage job is named **`Configure: <controller FQDD>`**. A PERC applying at once shows `JobType: RealTimeNoRebootConfiguration`; staged storage jobs are `RAIDConfiguration`. Pending jobs are found in the Jobs collection by `JobState` (Scheduled/New/Running) and `JobType`. | `idrac_redfish_storage_controller.py` RETURN sample; `module_utils/utils.py`, `get_scheduled_job_resp` |
| Controller and drive FQDDs: `BOSS.SL.14-1` (BOSS-N1), `AHCI.Slot.6-1` (BOSS-S1), `RAID.SL.3-1` / `RAID.Integrated.1-1` / `RAID.Slot.1-1` (PERC). Drives: `Disk.Direct.0-0:<BOSS>`, `Disk.Bay.<n>:Enclosure.Internal.0-1:<PERC>`. | examples throughout the collection; `ome_template.py` SCP sample |
| Reset is `POST Systems/System.Embedded.1/Actions/ComputerSystem.Reset {"ResetType": ...}`. Dell's own helpers use `GracefulRestart`, fall back to `ForceRestart`/`ForceOff`, and send `On` to a machine that is off. | `module_utils/utils.py`, `trigger_restart_operation`, `reset_host` |
| **Root's password is PATCHed on the MANAGER's account collection for iDRAC8/9** — `Managers/iDRAC.Embedded.1/Accounts/<id>` with `{"Password": ...}`. Only **iDRAC10** (17G and later) uses the standard `AccountService/Accounts/<id>`. Dell branches on the system Model: 12G/13G → iDRAC8, 14G/15G/16G → iDRAC9, else iDRAC10. An R660 is 16G, so the current fleet is on the manager collection. | `iDRAC-Redfish-Scripting`, `ChangeIdracUserPasswordREDFISH.py` |
| **BIOS attributes are staged and the job created in ONE PATCH** to `Systems/System.Embedded.1/Bios/Settings`, carrying `{"@Redfish.SettingsApplyTime": {"ApplyTime": "OnReset"}, "Attributes": {...}}`. The job id comes back in the `Location` header. Note this is `SettingsApplyTime`, NOT the `OperationApplyTime` a volume POST uses. | `iDRAC-Redfish-Scripting`, `GetSetBiosAttributesREDFISH.py`, `create_next_boot_config_job` |
| The **BIOS attribute registry** at `Systems/System.Embedded.1/Bios/BiosRegistry` maps `AttributeName` ↔ `DisplayName`, and carries `ReadOnly` plus a `Value[]` list of `ValueName`/`ValueDisplayName`. It is the only bridge between what OME reports about a template (display names) and what a Redfish PATCH accepts. | iDRAC9 Redfish API guide, *BIOS and boot settings* |
| **racadm's `System.*` objects are Dell ATTRIBUTES, not ComputerSystem properties.** `racadm set System.ServerOS.HostName` writes `ServerOS.1.HostName` via `PATCH Managers/iDRAC.Embedded.1/Oem/Dell/DellAttributes/System.Embedded.1 {"Attributes": {...}}`. The standard `ComputerSystem.HostName` is what Dell POPULATES from the OS through iSM. | `iDRAC-Redfish-Scripting`, `SetIdracLcSystemAttributesREDFISH.py` |

## What this changed in the code

Found by the research and fixed on `claude/ome-api-verification`:

1. **NVMe PERC drives read as "unknown state"** and failed the run with
   `StorageLayoutUnsupportedError`, because only `DellPhysicalDisk` was read.
   The e2e test fails on the old code (checked by putting the bug back).
2. **The reboot could interrupt a running PERC conversion.** A PERC runs
   Non-RAID immediately as a real-time job while the BOSS volume waits for the
   reset. The workflow now waits for every job to be finished or `Scheduled`
   before resetting, and `apply_staged` refuses to reset while any job runs.
3. **A retried template deploy skipped waiting for the deployment.** It now
   returns the profile's own `DeploymentTaskId`.
4. The "already templated" check now uses Dell's rule (`ProfileState > 0`,
   compared on `TemplateId`), so an unassigned leftover profile no longer
   blocks a deploy.
5. The discovery credential now matches Dell's shape (REDFISH duplicate,
   `profileId`, `keepAlive`, ...). The server type id is looked up, not assumed.
6. Job 2101 (Paused) now fails the wait instead of running into the deadline.
7. RAID 1 support is checked on the controller before the POST.
8. Pending storage jobs are matched on the exact name `Configure: <controller>`
   rather than a substring, so `BOSS.SL.14-1` cannot match `BOSS.SL.14-10`.

## How it is tested without an appliance

`tests/dell_simulator.py` is a stateful iDRAC + OME + naming service +
server-scan built from the table above, served over HTTPS. The iDRAC listens on
`127.0.0.2:443`, because the limb addresses an iDRAC by bare IP.
`tests/test_provision_dell_server_e2e.py` runs the real workflow and the real
activities against it:

- a factory-fresh server all the way to server-scan;
- a second run that changes nothing;
- an unknown root password (never more than 3 logins a round);
- a server a cluster is using (never touched);
- a BOSS without RAID 1 (refused before anything is staged).

The simulator also models iDRAC9's IP block, a PERC's real-time job, and OME
only managing a machine whose root password it holds.

## What the second research pass changed

The calls added for the DC team's review were written partly from search results
rather than from Dell's client code, and three of them were wrong. Found by
re-reading `dell/iDRAC-Redfish-Scripting` afterwards, which is why CLAUDE.md §4
says to change a request only against that code:

1. **Root's password went to `AccountService/Accounts/2`** — the iDRAC10 path.
   Every R660 is iDRAC9 and keeps its accounts under the Manager, so the call
   would have 404'd on the entire fleet.
2. **BIOS staging did a PATCH and then a separate job POST.** Dell does it in
   one PATCH; without `@Redfish.SettingsApplyTime` the pending values may have
   nothing scheduled to apply them at all.
3. **The OS hostname was written as `ComputerSystem.HostName`.** That property
   is what Dell FILLS IN from the OS via iSM; the DC team's
   `racadm set System.ServerOS.HostName` writes a Dell system attribute.

## Still unverified: check these on the first real server

- ~~**The template must not carry RAID/storage components.**~~ **Settled
  2026-09-30** — this suspicion was right, for two independent reasons, and the
  run now refuses such a template outright rather than trusting an operator to
  strip it. See [`dell-scp-template-r660.md`](dell-scp-template-r660.md), which
  also covers what else a template must not carry (iDRAC network settings, user
  accounts) and why a template is NOT tied to the firmware it was captured on.
- ~~The template must set the iDRAC root (user 2) password, and only that user.~~
  **Superseded 2026-09-30:** the template no longer sets it at all. Root's
  password is applied over Redfish (`Accounts/2`) before OME discovers the
  machine, which removed the `rediscovering-in-ome` phase and the firmware
  fragility of KB 000326070.
- Which `ProfileState` a deployed profile really reaches in this OME version,
  and whether the naming service renames with the same `PUT` as above.
- Whether this OME version answers the `$filter=TargetId eq <n>` on
  Profiles. The code re-filters client-side, so ignoring the filter is safe but
  slower.
- A PERC whose drives are NVMe behind the controller (H965i) versus
  CPU-attached NVMe: the simulator models both, the hardware decides which the
  R660s have.
- `ForceRestart` on a machine with no OS. Dell's helpers prefer `GracefulRestart`
  first; a new server has nothing to honour it.
- **`ServerOS.1.HostName` is the right attribute name.** It follows Dell's
  `<Group>.<Index>.<Name>` convention (as `Users.2.Password` and `IPv4.1.Address`
  do) and matches racadm's `System.ServerOS.HostName`, but no Dell sample writes
  this specific one. If the PATCH is refused, the fallback is the standard
  `PATCH Systems/System.Embedded.1 {"HostName": ""}`.
- **Which BIOS display names an R660's registry actually carries**, and whether
  any two share one — the registry is flat, so a duplicate display name cannot
  be disambiguated and the first wins (`compare_bios`).
- **Proxy:** httpx ignores CIDR entries in `NO_PROXY`. If the worker pod ever gets
  an `HTTPS_PROXY`, iDRAC traffic would go through the proxy unless the iDRAC
  addresses are listed in a form httpx matches.
