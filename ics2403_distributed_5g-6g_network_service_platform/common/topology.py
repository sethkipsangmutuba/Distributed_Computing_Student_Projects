"""
topology.py
-----------
Defines the distributed topology for the "Distributed 5G/6G Network
Service Platform" (Theme 1). This is the SAME topology reused across
every milestone (M1 through M12) so the system evolves rather than
being rebuilt each week, per the project's non-negotiable rule.

Layer hierarchy modeled (per the theme brief):
    Edge  <->  RAN (Radio/Access Network)  <->  Core  <->  Cloud

Resource capacities are deliberately staged so Edge is resource-poor
but low-latency, and Cloud is resource-rich but reached over more hops
-- this asymmetry is what makes later experiments (Milestone 3 edge
vs core trade-off, Milestone 5 edge-aware placement) meaningful.
"""

from dataclasses import dataclass


@dataclass
class NodeConfig:
    """
    Plain, picklable description of a node -- deliberately holds NO
    ResourceManager/ProcessManager/locks. On Linux, multiprocessing
    can hand a live object with locks to a new process (fork). On
    Windows, it cannot (spawn requires pickling the object first, and
    a threading.Lock can't be pickled). So the parent process only
    ever builds/passes these lightweight configs; each Node (with its
    locks) is constructed FRESH inside its own child process -- see
    common/node.py::_node_entry. This keeps the platform portable
    across Linux, macOS and Windows.
    """
    name: str
    node_type: str
    host: str
    port: int
    cpu_capacity: float
    memory_capacity: float
    bandwidth_capacity: float
    scheduling_policy: str = "priority"
    packet_loss_rate: float = 0.0
    election_id: int = 0       # [M4] rank used by the Bully leader-election algorithm
    services: tuple = ()        # [M3] microservices this node hosts (service decomposition)


# election_id: higher = more authoritative candidate for leader (mirrors capacity hierarchy:
# Cloud/Core are better positioned to coordinate the platform than resource-poor Edge nodes).
NODE_SPECS = [
    # name,        type,    host,        port,  cpu,  mem(MB), bw(Mbps), policy,    election_id
    ("edge-1",  "EDGE",  "127.0.0.1", 9101,   8,   3072,   150, "priority", 10),
    ("edge-2",  "EDGE",  "127.0.0.1", 9102,   8,   3072,   150, "priority", 11),
    ("ran-1",   "RAN",   "127.0.0.1", 9201,  12,   6144,   500, "priority", 20),
    ("core-1",  "CORE",  "127.0.0.1", 9301,  16,   8192,  1000, "priority", 30),
    ("cloud-1", "CLOUD", "127.0.0.1", 9401,  32,  16384,  5000, "priority", 40),
]

# [MILESTONE 3] Service decomposition: which microservices each node TYPE hosts.
# This is what turns the plain node topology into a service-oriented / microservice
# architecture (Section 4.3 of the brief) rather than a monolith replicated per node.
SERVICES_BY_TYPE = {
    "EDGE":  ("SessionHandling", "LocalQoSScheduling"),
    "RAN":   ("RadioResourceControl", "LocalQoSScheduling"),
    "CORE":  ("ResourceOrchestration", "MobilityManagement", "Coordination"),
    "CLOUD": ("Analytics", "LongTermStorage", "PolicyManagement"),
}


def build_nodes(packet_loss_rate: float = 0.02):
    """Returns a fresh list of NodeConfig objects for the standard topology."""
    configs = []
    for name, ntype, host, port, cpu, mem, bw, policy, election_id in NODE_SPECS:
        configs.append(NodeConfig(name, ntype, host, port, cpu, mem, bw,
                                   scheduling_policy=policy, packet_loss_rate=packet_loss_rate,
                                   election_id=election_id, services=SERVICES_BY_TYPE.get(ntype, ())))
    return configs
