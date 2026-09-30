"""A stateful stand-in for one Dell server's iDRAC, OpenManage Enterprise, the naming
service and server-scan — served over real HTTPS so the limb's own httpx code runs
unchanged against it.

WHAT IT IS BUILT FROM. No OME appliance can run in CI (it is a multi-GB VM
image), and Dell's download site is unreachable from the build environment, so
every shape and rule below comes from Dell's own client code — the
`dellemc.openmanage` Ansible collection 9.12.3, which Dell ships to drive OME
and iDRAC — and nothing is invented where that code is specific:

  OME     sessions (`X-Auth-Token`, `SessionService/Sessions('<Id>')`), job
          LastRunStatus ids (module_utils/ome.py), ProtocolToDeviceType and the
          discovery ConnectionProfile (modules/ome_discovery.py), the deploy
          action answering with the bare job id and the ProfileState /
          TemplateId / DeploymentTaskId profile fields (ome_template.py,
          ome_profile.py, ome_profile_info.py's sample).
  iDRAC   `Configure: <controller>` job names and the RealTimeNoRebootConfiguration
          job type (idrac_redfish_storage_controller.py's sample), BOSS
          controllers applying OnReset only while PERCs apply Immediate
          (redfish_storage_volume.py), RaidStatus under DellPhysicalDisk or
          DellPCIeSSD, SupportedRAIDTypes on StorageControllers[0], the
          `Disk.Direct.0-0:BOSS.SL.14-1` / `Disk.Bay.N:Enclosure.Internal.0-1:RAID...`
          drive FQDDs, and the Location header carrying JID_ job ids.

What it simulates on top, because a single request cannot show it: iDRAC9's
3-failed-logins IP block, a job advancing a step each time it is read, a
reset running the staged jobs, OME only managing a machine whose root password
it knows, the template enforcing root's password, and server-scan listing the
server only after its next collection.

Time is counted in REQUESTS, never seconds: the workflow runs on durable
timers that the test environment skips, so a wall-clock rule here would never
advance.
"""

from __future__ import annotations

import base64
import itertools
import json
import re
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

SYSTEM = "/redfish/v1/Systems/System.Embedded.1"
MANAGER = "/redfish/v1/Managers/iDRAC.Embedded.1"
JOBS = f"{MANAGER}/Jobs"
ROOT_ACCOUNT = "/redfish/v1/AccountService/Accounts/2"
BOSS = "BOSS.SL.14-1"
PERC = "RAID.SL.3-1"
NVME = "CPU.1"

_ids = itertools.count(10_000)


def _error(status: int, message: str, message_id: str = "SYS000") -> JSONResponse:
    return JSONResponse(
        {"error": {"@Message.ExtendedInfo": [{"Message": message, "MessageId": message_id}]}},
        status_code=status,
    )


@dataclass
class Drive:
    fqdd: str
    controller: str
    status: str | None
    oem_key: str = "DellPhysicalDisk"

    @property
    def odata_id(self) -> str:
        return f"{SYSTEM}/Storage/{self.controller}/Drives/{self.fqdd}"


@dataclass
class Job:
    id: str
    name: str
    job_type: str
    state: str
    apply: Any = None  # called once when the job completes
    reads_left: int = 2

    def as_json(self) -> dict[str, Any]:
        return {
            "Id": self.id,
            "Name": self.name,
            "JobType": self.job_type,
            "JobState": self.state,
            "Message": "Job completed successfully." if self.state == "Completed" else "Task successfully scheduled.",
            "PercentComplete": 100 if self.state == "Completed" else 0,
        }


@dataclass
class IdracSim:
    """One PowerEdge R660: a BOSS-N1 pair, a PERC with three SAS drives and one
    NVMe drive, and a CPU-attached NVMe controller that has no RAID at all."""

    service_tag: str = "7XK2QF3"
    model: str = "PowerEdge R660"
    firmware: str = "7.10.70.00"
    root_password: str = "calvin"
    power: str = "On"
    # What a server carries off the pallet: the factory OS hostname that stops
    # OME showing the machine's address beside its profile.
    os_hostname: str = "Miniwinpc"
    lockout_after: int = 3
    lockout_requests: int = 5
    boss_raid_types: list[str] = field(default_factory=lambda: ["RAID1"])
    failures: int = 0
    blocked_for: int = 0
    resets: list[str] = field(default_factory=list)
    login_attempts: list[str] = field(default_factory=list)
    # Every password this machine's root account was set to, in order.
    password_writes: list[str] = field(default_factory=list)
    drives: list[Drive] = field(default_factory=list)
    volumes: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    jobs: dict[str, Job] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.drives:
            self.drives = [
                Drive(f"Disk.Direct.{n}-{n}:{BOSS}", BOSS, None, "DellPCIeSSD") for n in (0, 1)
            ] + [
                Drive(f"Disk.Bay.{n}:Enclosure.Internal.0-1:{PERC}", PERC, "Ready") for n in (0, 1, 2)
            ] + [
                Drive(f"Disk.Bay.3:Enclosure.Internal.0-1:{PERC}", PERC, "Ready", "DellPCIeSSD"),
                Drive(f"Disk.Direct.0-0:{NVME}", NVME, None, "DellPCIeSSD"),
            ]
        self.volumes = {BOSS: [], PERC: [], NVME: []}

    # --- auth: HTTP Basic, with iDRAC9's IP block ----------------------------

    def authenticate(self, request: Request) -> Response | None:
        header = request.headers.get("authorization", "")
        password = ""
        if header.startswith("Basic "):
            password = base64.b64decode(header[6:]).decode().split(":", 1)[-1]
        self.login_attempts.append(password)
        if self.blocked_for > 0:
            self.blocked_for -= 1
            return Response(status_code=401)
        if password != self.root_password:
            self.failures += 1
            if self.failures >= self.lockout_after:
                self.failures = 0
                self.blocked_for = self.lockout_requests
            return Response(status_code=401)
        self.failures = 0
        return None

    # --- jobs ------------------------------------------------------------------

    def new_job(self, controller: str, job_type: str, state: str, apply: Any) -> Job:
        job = Job(f"JID_{next(_ids)}", f"Configure: {controller}", job_type, state, apply)
        self.jobs[job.id] = job
        return job

    def read_job(self, job: Job) -> None:
        if job.state == "Running":
            job.reads_left -= 1
            if job.reads_left <= 0:
                job.state = "Completed"
                if job.apply:
                    job.apply()

    def pending_on(self, controller: str) -> Job | None:
        return next(
            (j for j in self.jobs.values() if j.name == f"Configure: {controller}" and j.state in ("Scheduled", "Running")),
            None,
        )

    def reset(self, reset_type: str) -> None:
        self.resets.append(reset_type)
        self.power = "On"
        for job in self.jobs.values():
            if job.state == "Scheduled":
                job.state = "Running"


@dataclass
class OmeSim:
    idrac: IdracSim
    idrac_ip: str
    username: str = "admin"
    password: str = "ome-pass"
    template_name: str = "ocp-r660-idrac-7.10.70.00"
    template_id: int = 25
    # The root password the template's iDRAC user-2 attribute carries.
    template_root_password: str = "Target-Pw1"
    device_id: int | None = None
    device_credential: str | None = None
    groups: list[dict[str, Any]] = field(default_factory=list)
    jobs: dict[int, dict[str, Any]] = field(default_factory=dict)
    profiles: list[dict[str, Any]] = field(default_factory=list)
    deploy_calls: int = 0
    discovery_posts: list[dict[str, Any]] = field(default_factory=list)
    open_sessions: set[str] = field(default_factory=set)

    def new_job(self, reads: int, on_done: Any) -> int:
        job_id = next(_ids)
        self.jobs[job_id] = {"reads_left": reads, "status": (2050, "Running"), "on_done": on_done}
        return job_id

    def read_job(self, job_id: int) -> dict[str, Any] | None:
        job = self.jobs.get(job_id)
        if job is None:
            return None
        if job["status"][0] == 2050:
            job["reads_left"] -= 1
            if job["reads_left"] <= 0:
                job["status"] = job["on_done"]()
        return {"Id": job_id, "LastRunStatus": {"Id": job["status"][0], "Name": job["status"][1]}}


@dataclass
class ScanSim:
    """server-scan: lists the server as OME names it, once a collection has run."""

    ome: OmeSim
    collect_after_reads: int = 3
    claimed_by: tuple[str, str] | None = None
    _reads_since_name: int = 0
    _last_name: str | None = None

    def current(self) -> dict[str, Any] | None:
        profile = next((p for p in self.ome.profiles if p["ProfileState"] > 0), None)
        name = profile["ProfileName"] if profile else None
        if name != self._last_name:
            self._last_name, self._reads_since_name = name, 0
        self._reads_since_name += 1
        if self.claimed_by:
            state, cluster = self.claimed_by
            return self._doc(name or "ocp-legacy", state, cluster)
        if name is None or self._reads_since_name < self.collect_after_reads:
            return None
        return self._doc(name, "AVAILABLE", None)

    def _doc(self, name: str, state: str, cluster: str | None) -> dict[str, Any]:
        return {
            "id": "srv_sim_1",
            "name": name,
            "identity": {"serial": self.ome.idrac.service_tag},
            "health": {"overall": "HEALTHY"},
            "reachable": True,
            "openshift": {"lifecycle_state": state, "cluster_name": cluster},
        }


def _filter_value(request: Request, field_name: str) -> str | None:
    match = re.search(rf"{field_name} eq '?([^']+)'?", request.query_params.get("$filter", ""))
    return match.group(1).replace("''", "'") if match else None


def idrac_app(sim: IdracSim) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def basic_auth(request: Request, call_next):
        denied = sim.authenticate(request)
        return denied if denied is not None else await call_next(request)

    @app.get(SYSTEM)
    async def system():
        return {
            "Id": "System.Embedded.1",
            "SKU": sim.service_tag,
            "Model": sim.model,
            "Manufacturer": "Dell Inc.",
            "BiosVersion": "1.6.6",
            "PowerState": sim.power,
            "HostName": sim.os_hostname,
        }

    @app.patch(SYSTEM)
    async def patch_system(request: Request):
        body = await request.json()
        if "HostName" in body:
            sim.os_hostname = str(body["HostName"])
        return {"Id": "System.Embedded.1", "HostName": sim.os_hostname}

    @app.get(ROOT_ACCOUNT)
    async def root_account():
        return {"Id": "2", "UserName": "root", "RoleId": "Administrator"}

    @app.patch(ROOT_ACCOUNT)
    async def patch_root_account(request: Request):
        body = await request.json()
        if "Password" in body:
            sim.root_password = str(body["Password"])
            sim.password_writes.append(sim.root_password)
        return {"Id": "2", "UserName": "root"}

    @app.get(MANAGER)
    async def manager():
        return {"Id": "iDRAC.Embedded.1", "FirmwareVersion": sim.firmware}

    @app.get(f"{SYSTEM}/Storage")
    async def storage():
        return {"Members": [{"@odata.id": f"{SYSTEM}/Storage/{c}"} for c in (BOSS, PERC, NVME)]}

    @app.get(f"{SYSTEM}/Storage/{{controller}}")
    async def controller(controller: str):
        names = {BOSS: "BOSS-N1 Monolithic", PERC: "PERC H965i Front", NVME: "CPU.1"}
        raid_types = {BOSS: sim.boss_raid_types, PERC: ["RAID0", "RAID1", "RAID5", "RAID6", "RAID10"], NVME: []}
        return {
            "Id": controller,
            "Name": names[controller],
            "StorageControllers": [{"Name": names[controller], "SupportedRAIDTypes": raid_types[controller]}],
            "Drives": [{"@odata.id": d.odata_id} for d in sim.drives if d.controller == controller],
            "Volumes": {"@odata.id": f"{SYSTEM}/Storage/{controller}/Volumes"},
        }

    @app.get(f"{SYSTEM}/Storage/{{controller}}/Drives/{{fqdd}}")
    async def drive(controller: str, fqdd: str):
        found = next(d for d in sim.drives if d.fqdd == fqdd)
        oem = {found.oem_key: {"RaidStatus": found.status}} if found.status else {found.oem_key: {}}
        return {"Id": fqdd, "Oem": {"Dell": oem}}

    @app.get(f"{SYSTEM}/Storage/{{controller}}/Volumes")
    async def volumes(controller: str):
        if controller == NVME:
            return Response(status_code=404)
        return {"Members": [{"@odata.id": v["@odata.id"]} for v in sim.volumes[controller]]}

    @app.get(f"{SYSTEM}/Storage/{{controller}}/Volumes/{{volume}}")
    async def volume(controller: str, volume: str):
        return next(v for v in sim.volumes[controller] if v["@odata.id"].endswith(volume))

    @app.post(f"{SYSTEM}/Storage/{{controller}}/Volumes")
    async def create_volume(controller: str, request: Request):
        body = await request.json()
        if sim.pending_on(controller):
            return _error(400, f"A configuration job already exists for {controller}.", "STOR023")
        if body.get("RAIDType") not in (sim.boss_raid_types if controller == BOSS else ["RAID1"]):
            return _error(400, f"RAIDType {body.get('RAIDType')} is not supported.", "STOR016")
        if controller == BOSS and body.get("@Redfish.OperationApplyTime") != "OnReset":
            return _error(400, "BOSS supports @Redfish.OperationApplyTime OnReset only.", "SYS427")
        drive_ids = [d["@odata.id"] for d in body.get("Drives", [])]
        mine = [d for d in sim.drives if d.odata_id in drive_ids and d.controller == controller]
        if len(mine) != len(drive_ids) or not mine:
            return _error(400, "Drives do not belong to the controller.", "STOR001")

        def apply() -> None:
            sim.volumes[controller].append(
                {
                    "@odata.id": f"{SYSTEM}/Storage/{controller}/Volumes/Disk.Virtual.0:{controller}",
                    "RAIDType": body["RAIDType"],
                    "Links": {"Drives": [{"@odata.id": d} for d in drive_ids]},
                }
            )
            for d in mine:
                d.status = "Online"

        job = sim.new_job(controller, "RAIDConfiguration", "Scheduled", apply)
        return Response(status_code=202, headers={"Location": f"{JOBS}/{job.id}"})

    @app.post(f"{SYSTEM}/Oem/Dell/DellRaidService/Actions/DellRaidService.ConvertToNonRAID")
    async def convert(request: Request):
        fqdds = (await request.json()).get("PDArray", [])
        targets = [d for d in sim.drives if d.fqdd in fqdds]
        if len(targets) != len(fqdds) or any(d.status != "Ready" for d in targets):
            return _error(400, "One or more physical disks are not in Ready state.", "STOR018")
        controller = targets[0].controller
        if sim.pending_on(controller):
            return _error(400, f"A configuration job already exists for {controller}.", "STOR023")

        def apply() -> None:
            for d in targets:
                d.status = "NonRAID"

        # A PERC applies this at once — Dell's RealTimeNoRebootConfiguration.
        job = sim.new_job(controller, "RealTimeNoRebootConfiguration", "Running", apply)
        return Response(status_code=202, headers={"Location": f"{JOBS}/{job.id}"})

    @app.get(JOBS)
    async def jobs():
        return {"Members": [j.as_json() for j in sim.jobs.values()]}

    @app.get(f"{JOBS}/{{job_id}}")
    async def job(job_id: str):
        found = sim.jobs.get(job_id)
        if found is None:
            return _error(404, "Job not found.")
        sim.read_job(found)
        return found.as_json()

    @app.post(f"{SYSTEM}/Actions/ComputerSystem.Reset")
    async def reset(request: Request):
        sim.reset((await request.json()).get("ResetType", ""))
        return Response(status_code=204)

    return app


def services_app(ome: OmeSim, scan: ScanSim, region_names: dict[str, str] | None = None) -> FastAPI:
    """OME under /api, the naming service under /namer, server-scan under /scan/api/v1."""
    app = FastAPI()
    idrac = ome.idrac

    def token_ok(request: Request) -> bool:
        return request.headers.get("x-auth-token") in ome.open_sessions

    @app.post("/api/SessionService/Sessions")
    async def login(request: Request):
        body = await request.json()
        if (body.get("UserName"), body.get("Password")) != (ome.username, ome.password) or body.get("SessionType") != "API":
            return Response(status_code=401)
        session_id = str(next(_ids))
        ome.open_sessions.add(f"tok-{session_id}")
        return JSONResponse({"Id": session_id}, status_code=201, headers={"X-Auth-Token": f"tok-{session_id}"})

    @app.delete("/api/SessionService/Sessions('{session_id}')")
    async def logout(session_id: str):
        ome.open_sessions.discard(f"tok-{session_id}")
        return Response(status_code=204)

    @app.middleware("http")
    async def session_required(request: Request, call_next):
        path = request.url.path
        if path.startswith("/api/") and not path.startswith("/api/SessionService") and not token_ok(request):
            return Response(status_code=401)
        return await call_next(request)

    @app.get("/api/DiscoveryConfigService/ProtocolToDeviceType")
    async def protocols():
        return {
            "value": [
                {"DeviceTypeId": 1000, "DeviceTypeName": "SERVER", "ProtocolName": "WSMAN"},
                {"DeviceTypeId": 1000, "DeviceTypeName": "SERVER", "ProtocolName": "REDFISH"},
                {"DeviceTypeId": 2000, "DeviceTypeName": "CHASSIS", "ProtocolName": "WSMAN"},
            ]
        }

    @app.get("/api/DiscoveryConfigService/DiscoveryConfigGroups")
    async def groups():
        return {"value": ome.groups}

    @app.post("/api/DiscoveryConfigService/DiscoveryConfigGroups")
    async def discover(request: Request):
        body = await request.json()
        ome.discovery_posts.append(body)
        model = body["DiscoveryConfigModels"][0]
        profile = json.loads(model["ConnectionProfile"])
        wsman = next(c for c in profile["credentials"] if c["type"] == "WSMAN")["credentials"]
        target = model["DiscoveryConfigTargets"][0]["NetworkAddressDetail"]
        if model.get("DeviceType") != [1000]:
            return _error(400, "Invalid device type.")

        def done():
            # OME discovers a machine only when the credential really logs in.
            if target == ome.idrac_ip and wsman["password"] == idrac.root_password:
                ome.device_id = ome.device_id or 10074
                ome.device_credential = wsman["password"]
                return (2060, "Completed")
            return (2070, "Failed")

        job_id = ome.new_job(2, done)
        group = {
            "DiscoveryConfigGroupId": next(_ids),
            "DiscoveryConfigGroupName": body["DiscoveryConfigGroupName"],
            "DiscoveryConfigTaskParam": [{"TaskId": job_id, "TaskTypeId": 0, "ExecutionSequence": 0}],
        }
        ome.groups.append(group)
        return JSONResponse(group, status_code=201)

    @app.get("/api/DeviceService/Devices")
    async def devices(request: Request):
        tag = _filter_value(request, "DeviceServiceTag")
        if ome.device_id is None or (tag and tag != idrac.service_tag):
            return {"@odata.count": 0, "value": []}
        return {
            "@odata.count": 1,
            "value": [
                {
                    "Id": ome.device_id,
                    "Type": 1000,
                    "DeviceServiceTag": idrac.service_tag,
                    "DeviceName": f"idrac-{idrac.service_tag}",
                    "ConnectionState": ome.device_credential == idrac.root_password,
                    "DeviceManagement": [{"NetworkAddress": ome.idrac_ip}],
                }
            ],
        }

    @app.get("/api/JobService/Jobs({job_id})")
    async def job(job_id: int):
        found = ome.read_job(job_id)
        return found if found is not None else _error(404, "Job not found.")

    @app.get("/api/TemplateService/Templates")
    async def templates(request: Request):
        name = _filter_value(request, "Name")
        items = [{"Id": ome.template_id, "Name": ome.template_name, "ViewTypeId": 2}]
        return {"value": [t for t in items if name is None or t["Name"] == name]}

    @app.post("/api/TemplateService/Actions/TemplateService.Deploy")
    async def deploy(request: Request):
        body = await request.json()
        ome.deploy_calls += 1
        if body.get("Id") != ome.template_id or body.get("TargetIds") != [ome.device_id]:
            return _error(400, "Invalid template or target.")
        profile = {
            "Id": next(_ids),
            "ProfileName": f"Profile {len(ome.profiles) + 1:05d}",
            "TemplateId": ome.template_id,
            "TemplateName": ome.template_name,
            "TargetId": ome.device_id,
            "ProfileState": 1,
            "DeploymentTaskId": 0,
        }
        ome.profiles.append(profile)

        def done():
            # OME can only push the template through a credential it holds.
            if ome.device_credential != idrac.root_password:
                return (2070, "Failed")
            idrac.root_password = ome.template_root_password
            profile["ProfileState"] = 4
            return (2060, "Completed")

        job_id = ome.new_job(3, done)
        profile["DeploymentTaskId"] = job_id
        return JSONResponse(job_id)

    @app.get("/api/ProfileService/Profiles")
    async def profiles(request: Request):
        target = _filter_value(request, "TargetId")
        return {"value": [p for p in ome.profiles if target is None or str(p["TargetId"]) == target]}

    @app.post("/namer/rename")
    async def rename(request: Request):
        body = await request.json()
        profile = next(p for p in ome.profiles if p["TargetId"] == body["ome_device_id"] and p["ProfileState"] > 0)
        # The service reads cores/memory/disks from OME and rounds them itself.
        profile["ProfileName"] = f"ocp-dell-r660-{body['region']}-128c-1024gb-10tb-{idrac.service_tag}"
        return {"renamed": profile["ProfileName"]}

    @app.get("/scan/api/v1/servers")
    async def servers(request: Request):
        doc = scan.current()
        search = (request.query_params.get("search") or "").upper()
        if doc and search and search in doc["identity"]["serial"].upper():
            return {"items": [{"id": doc["id"], "name": doc["name"]}]}
        return {"items": []}

    @app.get("/scan/api/v1/servers/{server_id}")
    async def server(server_id: str):
        doc = scan.current() if server_id == "srv_sim_1" else None
        return doc if doc else JSONResponse({"detail": "not found"}, status_code=404)

    return app
