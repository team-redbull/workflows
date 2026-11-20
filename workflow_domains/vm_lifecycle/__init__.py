"""VM-lifecycle domain: its workflows and their HTTP surface.

Everything belonging to the domain lives together here — the create-vm
workflow (create_vm.py), the per-zone lock it takes (kv_zone_lock.py), the fit
rule it decides with (placement.py) and the APIRouter that fronts create-vm
(router.py) — mirroring activities/vm_lifecycle/ on the limb side, which runs
inside each KV cluster. The domain's subject is the VirtualMachines that stand
in for physical servers under a KubeVirt InfraEnv: created here, then
installed by add-server-to-mce like any server server-scan lists.
"""
