# Node-Loss Lab Setup

Status: support note for the active `Node-Loss Survival MVP` plan.

The D3/D4 gates need three Kubernetes nodes. They do not strictly need three
physical machines for the MVP, but the bundle must disclose the physical-host
shape and must not claim full physical-host loss when Kubernetes nodes share a
machine.

## Minimum MVP Lab

Acceptable:

```text
m02 control-plane
m01 worker
one additional worker VM or containerized worker on m01 or m02
```

Required from `kubectl get nodes -o wide`:

```text
3 Ready schedulable Kubernetes nodes
each with a non-loopback InternalIP
```

Current lab boundary:

```text
m02  192.168.1.184  RoCE-capable host, used here through LAN TCP/iSCSI
m01  192.168.1.181  RoCE-capable host, used here through LAN TCP/iSCSI
tp01 192.168.1.188  non-RoCE host, used only through LAN TCP/iSCSI
```

Do not use `10.0.0.x` RDMA/RoCE addresses in this node-loss gate. The D3/D4
claim is Kubernetes node-loss recovery over TCP/iSCSI on `192.168.1.x`, not
RoCE, NVMe/RDMA, or performance validation.

Recommended node labels:

```bash
kubectl label node <node-on-m02> sw-block.seaweedfs.com/physical-host=m02 --overwrite
kubectl label node <node-on-m01> sw-block.seaweedfs.com/physical-host=m01 --overwrite
kubectl label node <extra-worker> sw-block.seaweedfs.com/physical-host=<m01-or-m02> --overwrite
```

The D3 preflight uses this label first. If it is absent, it falls back to
`topology.kubernetes.io/zone`, then `kubernetes.io/hostname`, then node name.

## Fast Readiness Check

Run on the machine with kubeconfig access:

```bash
cd /tmp/seaweed_block
bash scripts/preflight-node-loss-lab.sh \
  --min-k8s-nodes 3 \
  --env-out /tmp/node-loss.env \
  --placement-out /tmp/node-placement.before.txt \
  --topology-out /tmp/node-loss-nodes.txt
```

Pass means:

```text
node_loss_lab_eligible=true
selected_node_count=3
physical_domain_count=<n>
physical_domain_shape=full-physical-host|shared-physical-host
kubernetes_node_loss_claimed=true
physical_host_loss_claimed=false
```

If this fails, do not ask QA to rerun D3. The full D3 scenario will fail closed
for the same reason.

Failure is intentionally machine-readable. An ineligible but reachable lab exits
with code `3`, writes a negative placement artifact when `--placement-out` is
supplied, and does not write the env file:

```text
node_loss_topology_eligible=false
node_loss_lab_eligible=false
reason=requires_3_ready_schedulable_nodes_with_non_loopback_internal_ip
found=<n>
kubernetes_node_loss_claimed=false
physical_host_loss_claimed=false
```

Treat any non-zero exit from this preflight as a lab blocker, not a product
validation result.

## Claim Boundary

Three Kubernetes nodes on two physical machines proves only:

```text
Kubernetes-node placement and recovery mechanics
```

It does not prove:

```text
full physical-machine loss
```

Full physical-host-loss requires distinct physical fault domains for the failed
primary and surviving promotion candidates, and should be a later stricter gate.

## Practical k3s Notes

Typical shape:

```text
m02: k3s server / control-plane
m01: k3s agent
extra worker: k3s agent in a VM or container with a routable InternalIP
```

The workers must be able to reach:

```text
blockvolume iSCSI ports: 3260+
blockvolume status ports: 23260+
replication data/control ports: 19101+
k3s API/control-plane ports required by the cluster
```

Avoid `127.0.0.1`, `localhost`, `0.0.0.0`, or unspecified addresses in the node
InternalIP used by the gate.
