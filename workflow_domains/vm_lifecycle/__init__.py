"""VM-lifecycle domain: its workflows and their HTTP surface.

Everything belonging to the domain lives together here — the create-vm
workflow (create_vm.py) and the APIRouter that fronts it (router.py) —
mirroring activities/vm_lifecycle/ on the limb side. The domain's subject is
the VirtualMachines that stand in for physical servers under a KubeVirt
InfraEnv: created here, then installed by add-server-to-mce like any server
server-scan lists.
"""
