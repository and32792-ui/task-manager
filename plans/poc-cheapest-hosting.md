# POC Hosting — Cheapest Options with Self-Managed K8s

## Goal
Run Phase 1 + 2 as a POC on the cheapest possible infrastructure with self-managed Kubernetes. No high-availability needed.

---

## Minimum POC Hardware Requirements

For a POC, we can consolidate everything onto fewer machines with reduced replication:

| Component | CPU | RAM | Disk |
|-----------|-----|-----|------|
| Node.js API | 0.5 | 512 MB | 1 GB |
| PostgreSQL | 1 | 1 GB | 10 GB |
| Kafka (single broker) | 1 | 2 GB | 20 GB |
| Debezium | 0.5 | 512 MB | 1 GB |
| Ozone SCM | 1 | 1 GB | 10 GB |
| Ozone OM | 1 | 1 GB | 10 GB |
| Ozone DataNode x1 | 1 | 1 GB | 50 GB |
| Ozone S3 Gateway | 0.5 | 512 MB | 1 GB |
| Spark Driver | 1 | 1 GB | — |
| Spark Executor x1 | 1 | 1 GB | — |
| Hive Metastore | 0.5 | 512 MB | 1 GB |
| K8s control plane (k3s) | 1 | 1 GB | 10 GB |
| **Total** | **~10 vCPU** | **~12 GB RAM** | **~115 GB** |

**Realistic minimum: 1 server with 8-12 vCPU, 16-32 GB RAM, 200 GB SSD**

---

## Cheapest Hosting Providers (Ranked by Price)

### Tier 1: Ultra-Budget — $20-50/mo

| Provider | Plan | CPU | RAM | Disk | Price/mo | Notes |
|----------|------|-----|-----|------|----------|-------|
| **Hetzner Cloud** | CPX41 | 8 vCPU (AMD) | 16 GB | 240 GB SSD | **~$16/mo** | Best value. EU/US DCs |
| **Hetzner Cloud** | CCX33 | 8 dedicated vCPU (AMD) | 32 GB | 240 GB SSD | **~$36/mo** | Dedicated cores, better for Spark |
| **Netcup** | RS 4000 G11 | 12 vCPU (AMD EPYC) | 32 GB | 512 GB SSD | **~$22/mo** | Incredible value, EU only |
| **OVHcloud** | B2-30 | 8 vCPU | 30 GB | 200 GB SSD | **~$26/mo** | EU/US/APAC |

### Tier 2: Budget — $50-100/mo

| Provider | Plan | CPU | RAM | Disk | Price/mo | Notes |
|----------|------|-----|-----|------|----------|-------|
| **Hetzner Dedicated** | AX42 | 8-core Ryzen 5600G | 64 GB | 2x512 GB NVMe | **~$44/mo** | Bare metal, best perf/$ |
| **DigitalOcean** | Premium 8vCPU | 8 vCPU (AMD) | 16 GB | 320 GB SSD | **~$68/mo** | Simple, good DX |
| **Vultr** | High Frequency 8vCPU | 8 vCPU | 32 GB | 384 GB SSD | **~$96/mo** | NVMe, global DCs |
| **Linode/Akamai** | Dedicated 8GB | 4 dedicated | 8 GB | 160 GB | **~$72/mo** | Need 16GB+ plan |

### Tier 3: Bare Metal Budget — $40-80/mo

| Provider | Plan | CPU | RAM | Disk | Price/mo | Notes |
|----------|------|-----|-----|------|----------|-------|
| **Hetzner Auction** | Various | 8-16 cores | 32-64 GB | 2x HDD or SSD | **$25-50/mo** | Refurbished servers, amazing value |
| **OVH Kimsufi/So You Start** | Various | 4-8 cores | 16-32 GB | 2x HDD | **$20-40/mo** | Budget bare metal |
| **Scaleway Dedibox** | Start-2-M | 8 cores | 32 GB | 2x1TB HDD | **~$30/mo** | EU only |

---

## Recommended POC Setup

### Best Value: Hetzner Cloud CPX41 or CCX33

**Option A — Single Node ($16/mo)**
```
Hetzner CPX41: 8 vCPU AMD, 16 GB RAM, 240 GB SSD
+ k3s (lightweight K8s)
+ All services on one node
```

**Option B — Single Node with More Headroom ($36/mo)**
```
Hetzner CCX33: 8 dedicated vCPU AMD, 32 GB RAM, 240 GB SSD
+ k3s
+ Comfortable for all Phase 1+2 services
```

**Option C — Bare Metal Beast ($44/mo)**
```
Hetzner AX42: Ryzen 5600G, 64 GB RAM, 2x512 GB NVMe
+ k3s
+ Room to grow, real CPU cores (not shared)
```

---

## K8s Distribution for POC: k3s

For a single-node POC, **k3s** is the best choice:

- Lightweight (single binary, ~70MB)
- Built-in: containerd, CoreDNS, Traefik ingress, local-path storage
- Full K8s API compatibility
- Runs on a single node easily

### Install k3s
```bash
# On the remote server
curl -sfL https://get.k3s.io | sh -

# Get kubeconfig
cat /etc/rancher/k3s/k3s.yaml
# Copy to local machine, update server address
```

### Alternative: k0s
Similar to k3s but from Mirantis. Also single-binary, lightweight.

---

## POC Deployment Adjustments

For a single-node POC, reduce resource requests in K8s manifests:

| Change | From | To |
|--------|------|-----|
| Ozone DataNode replicas | 3 | 1 |
| Ozone replication factor | 3 | 1 |
| Ozone S3 Gateway replicas | 2 | 1 |
| API replicas | 2 | 1 |
| Spark executor instances | 2 | 1 |
| All resource requests | Various | Remove or set minimal |
| All resource limits | Various | Remove or set generous |
| PVC sizes | Large | Minimal (10-20 GB each) |

---

## Setup Steps for Hetzner POC

### 1. Provision Server
```bash
# Via Hetzner Cloud Console or CLI
hcloud server create \
  --name taskmanager-poc \
  --type cpx41 \
  --image ubuntu-24.04 \
  --location fsn1 \
  --ssh-key your-key
```

### 2. Install k3s
```bash
ssh root@<server-ip>
curl -sfL https://get.k3s.io | sh -
```

### 3. Configure kubectl locally
```bash
scp root@<server-ip>:/etc/rancher/k3s/k3s.yaml ~/.kube/config
# Edit server address to point to <server-ip>:6443
```

### 4. Install Operators
```bash
# Spark Operator
kubectl create namespace spark
helm install spark-operator spark-operator/spark-operator -n spark --set webhook.enable=true

# No Strimzi needed for POC — use plain Kafka deployment from docker-compose adapted to K8s
```

### 5. Deploy Everything
```bash
kubectl apply -f infra/k8s/namespace.yaml
kubectl apply -f infra/k8s/ozone-namespace.yaml
kubectl apply -f infra/k8s/spark-namespace.yaml
kubectl apply -f infra/k8s/postgres-statefulset.yaml
# ... (follow deployment order from remote-deployment-requirements.md)
```

---

## Alternative: Skip K8s for POC — Just Use Docker Compose

For the absolute simplest POC, you could skip K8s entirely and run everything via Docker Compose on a single server:

```bash
ssh root@<server-ip>
git clone <your-repo>
cd task-manager
docker compose up -d
npm run migrate
./infra/ozone/init-buckets.sh
```

**Pros:** Simplest setup, no K8s overhead, works on $16/mo server
**Cons:** No K8s experience, no Spark Operator, manual Spark submit

---

## Summary

| Option | Server | Cost/mo | K8s | Complexity |
|--------|--------|---------|-----|------------|
| Docker Compose only | Hetzner CPX41 | **$16** | None | Lowest |
| k3s single node | Hetzner CPX41 | **$16** | k3s | Low |
| k3s with headroom | Hetzner CCX33 | **$36** | k3s | Low |
| k3s bare metal | Hetzner AX42 | **$44** | k3s | Low |

**Recommendation for POC: Hetzner CCX33 ($36/mo) + k3s.** Dedicated cores handle Spark better than shared vCPUs, and 32 GB RAM gives comfortable headroom for all services.
