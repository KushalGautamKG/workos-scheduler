# Kubernetes Production Policies

Day 125 separates local and production Kubernetes configuration with Kustomize overlays and adds availability, security, and resource policies for production.

**Interview sound bite:** *“Base stays portable; production overlays add replicas, limits, security contexts, soft spread, and PDBs—without hard anti-affinity that breaks single-node clusters.”*

---

## Before Day 125

```
Deployment → Pods run successfully (local overlay behavior)
```

## After Day 125

```
Deployment
  ├── Resource boundaries (requests / limits)
  ├── Security boundaries (non-root, no privilege escalation, RO rootfs, drop ALL)
  ├── Availability policies (rolling update, PDBs)
  └── Scheduling policies (topology spread, preferred anti-affinity)
```

---

## Layout

```
deploy/kubernetes/
├── base/                 # environment-independent
└── overlays/
    ├── local/            # Day 124 smoke path (replicas: 1)
    └── production/       # patches + PDBs (replicas: 2)
```

## Policy notes

| Topic | Intent |
|-------|--------|
| **Requests vs limits** | Scheduler reservation vs hard ceiling (OOM if memory exceeded) |
| **Non-root + seccomp** | Reduce host blast radius |
| **drop ALL + no privilege escalation** | Block capability / setuid escalation |
| **Read-only rootfs + `/tmp` emptyDir** | Immutable root; writable scratch only where needed |
| **RollingUpdate maxUnavailable: 0** | Do not shrink healthy capacity during rollout |
| **Topology spread / preferred anti-affinity** | Soft preference — still schedules on one node |
| **PDB minAvailable: 1** | Guard **voluntary** drains only — not crashes or node death |

Initial CPU/memory values are **operational defaults**, not load-tested capacity.

---

## Related

- [local-kubernetes.md](local-kubernetes.md)
- [containerization.md](containerization.md)
- [day125-kubernetes-policies.md](../benchmarks/day125-kubernetes-policies.md)
- `./worker/scripts/smoke_k8s_policies.sh`
- `./worker/scripts/smoke_k8s.sh` (local overlay)
