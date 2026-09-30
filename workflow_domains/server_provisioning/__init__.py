"""Server-provisioning domain: its workflows and their HTTP surface.

Takes a machine from "the iDRAC has an IP" to "server-scan lists it, named and
ready to install". One workflow per vendor, because the path there shares
nothing but its end state: provision_dell_server.py drives iDRAC Redfish and
OpenManage Enterprise; Cisco, HPE and Intersight get their own later.

Beside the workflow sit the pure rules it applies — storage_plan.py (which
controller gets the RAID 1, which drives go Non-RAID) and server_name.py (what
a correctly named server looks like) — and regions.py, the router's mapping
from an iDRAC address to its region. None of them does I/O.
"""
