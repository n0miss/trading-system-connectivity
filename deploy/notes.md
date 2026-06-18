# AWS Deployment Notes — Crypto CEX Market Data Connector

This document covers EC2 instance selection, placement group configuration, time
synchronisation, and NIC/CPU tuning for production connector deployments.

---

## 1. EC2 Instance Selection

### Recommended types

| Instance | vCPUs | Memory | Network | Notes |
|----------|-------|--------|---------|-------|
| `c6in.4xlarge` | 16 | 32 GiB | 25 Gbps | Best for latency-sensitive workloads; ENA Express |
| `c5n.4xlarge`  | 16 | 42 GiB | 25 Gbps | Older but proven; enhanced networking |
| `m6i.2xlarge`  |  8 | 32 GiB | 12.5 Gbps | Good cost/performance balance |

### Sizing guidance

Each logical shard runs one async Tokio runtime thread plus a background I/O
thread.  A 4-shard deployment fits comfortably on a `c6in.2xlarge` (8 vCPUs)
with 2 cores reserved for the OS and NIC interrupts.

For high symbol counts (>100 symbols per shard), prefer 16+ vCPUs and ≥32 GiB
to accommodate the larger order-book heap.

### Storage

- Root volume: 20 GiB gp3 (binary + config; no data written by the connector).
- Aeron Archive (if enabled): a separate 100 GiB io2 volume mounted at
  `/var/connector/archive` for low-jitter sequential writes.

---

## 2. Placement Groups

Use a **cluster placement group** to minimise network latency between the
connector and downstream consumers (e.g. execution gateways or analytics
instances) that are co-located in the same AZ.

```bash
# Create the placement group (one-time)
aws ec2 create-placement-group \
    --group-name connector-cluster \
    --strategy cluster \
    --region eu-west-1

# Launch the connector instance into the group
aws ec2 run-instances \
    --image-id ami-xxxxxxxx \
    --instance-type c6in.4xlarge \
    --placement "GroupName=connector-cluster" \
    --network-interfaces "DeviceIndex=0,Groups=sg-xxxxxxxx,DeleteOnTermination=true" \
    ...
```

Cluster placement groups guarantee <25 µs p99 intra-group latency on supported
instance types and provide higher per-flow bandwidth.  All instances in a group
must be in the same AZ; plan for AZ-level failure by running a standby
deployment in a second group in a different AZ.

---

## 3. Time Synchronisation

Accurate timestamps are critical for latency measurement (exchange event time →
local receive time).  AWS provides two synchronisation options:

### 3a. Amazon Time Sync Service (NTP/chrony) — default

Available at `169.254.169.123` from every EC2 instance.  The service is
disciplined by GPS receivers and PTP hardware on the hypervisor.

Typical accuracy: **±10–100 µs** on a lightly-loaded instance.

```bash
# /etc/chrony.conf — prepend this server above the defaults
server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4

# Force immediate sync after launch
chronyc makestep

# Check offset
chronyc tracking
```

`aws-tuning.sh` performs this configuration automatically.

### 3b. EC2 PTP Hardware Clock — best accuracy

Supported on Nitro-based instances.  Adds a `/dev/ptp0` device (or `/dev/phc0`)
that `chronyd` or `linuxptp` can use as a hardware reference.

Typical accuracy: **<1 µs** (PHC-disciplined).

```bash
# Install linuxptp
dnf install -y linuxptp

# /etc/ptp4l.conf
[global]
time_stamping   hardware
slaveOnly       1
tx_timestamp_timeout 10

[eth0]

# Start PTP
systemctl enable --now ptp4l
systemctl enable --now phc2sys   # synchronise system clock to PHC
```

The connector logs `local_recv_ts` as nanoseconds since Unix epoch; a PTP-
disciplined clock ensures these timestamps are comparable across hosts.

---

## 4. NIC Tuning

### Enhanced Networking (ENA)

Ensure ENA is installed and enabled.  All Nitro-based instances ship with it by
default on Amazon Linux 2023 and Ubuntu ≥ 20.04.

```bash
ethtool -i eth0 | grep driver   # should show "ena"
```

### Ring buffers

Increase RX and TX ring buffers to absorb bursts without packet drops.

```bash
ethtool -G eth0 rx 4096 tx 4096
# Verify
ethtool -g eth0
```

### Interrupt coalescing

The default coalescing interval (200 µs on ENA) batches interrupts to reduce
CPU load at the cost of latency.  For the connector, 50 µs is a good starting
point; measure with `ping` or a latency probe and tune down if needed.

```bash
ethtool -C eth0 rx-usecs 50
```

Setting `rx-usecs 0` (adaptive disabled) minimises latency further but
significantly increases CPU usage from interrupt storms; only use on isolated
cores.

### Generic Receive Offload (GRO)

GRO batches small packets before they reach the kernel TCP stack.  It reduces
CPU overhead for bulk transfers but adds latency for market-data streams.

```bash
ethtool -K eth0 gro off
```

### ENA Express (c6in only)

`c6in` instances support ENA Express, which routes traffic over a dedicated
low-latency fabric bypassing the standard ENA path.

```bash
# Enable ENA Express on the ENI (requires instance stop/start)
aws ec2 modify-network-interface-attribute \
    --network-interface-id eni-xxxxxxxx \
    --ena-srd-specification "EnaSrdEnabled=true"
```

---

## 5. CPU Tuning

### Core allocation

Reserve cores for specific roles to avoid OS jitter:

| Core(s) | Role |
|---------|------|
| 0 | OS scheduler, kernel threads |
| 1 | NIC interrupts (`/proc/irq/*/smp_affinity_list`) |
| 2–N | Connector shards (one shard per core) |

### CPU isolation (`isolcpus`)

Add to the kernel boot parameters (e.g. via `/etc/default/grub`) to prevent the
OS scheduler from placing other tasks on connector cores:

```
isolcpus=2,3,4,5 nohz_full=2,3,4,5 rcu_nocbs=2,3,4,5
```

Apply with `update-grub && reboot` (Ubuntu) or `grub2-mkconfig` (Amazon Linux).

### Frequency governor

Set to `performance` to prevent the CPU from dropping frequency under load:

```bash
cpupower frequency-set -g performance
# Or directly
echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

### C-states

Deep C-states (C2+) cause wake-up latencies of 50–200 µs.  Disable them:

```bash
# At runtime
for f in /sys/devices/system/cpu/cpu*/cpuidle/state[2-9]/disable; do
    echo 1 > "$f"
done

# At boot (persist via kernel params)
# Add to GRUB_CMDLINE_LINUX: intel_idle.max_cstate=1 processor.max_cstate=1
```

### Pinning connector shards to cores

Use the `CPUAffinity` drop-in per shard (see `connector@.service`):

```bash
# Pin shard 0 to core 2, shard 1 to core 3, etc.
for i in 0 1 2 3; do
    mkdir -p "/etc/systemd/system/connector@${i}.service.d"
    cat > "/etc/systemd/system/connector@${i}.service.d/affinity.conf" <<EOF
[Service]
CPUAffinity=$((i + 2))
EOF
done
systemctl daemon-reload
```

---

## 6. Aeron Configuration

The Aeron C media driver must be started before any connector shard.  In the
current Phase 1 implementation the connector uses a null publication (no real
Aeron), so this section applies once Aeron is wired in.

### /dev/shm sizing

The media driver allocates its term buffers in `/dev/shm`.  The default Linux
tmpfs limit is 50% of RAM; explicitly set it to avoid surprises:

```bash
mount -o remount,size=4g /dev/shm
# Persist in /etc/fstab:
# tmpfs /dev/shm tmpfs defaults,size=4g 0 0
```

### Huge pages

Allocate huge pages to reduce TLB pressure in the media driver hot path:

```bash
echo 512 > /proc/sys/vm/nr_hugepages   # 512 × 2 MiB = 1 GiB
# Persist:
echo "vm.nr_hugepages = 512" >> /etc/sysctl.d/99-connector.conf
```

### Aeron properties (aeron.properties)

```properties
aeron.dir=/dev/shm/aeron
aeron.term.buffer.length=67108864        # 64 MiB
aeron.ipc.mtu.length=1408               # match NIC MTU minus headers
aeron.socket.so_rcvbuf=4194304          # 4 MiB
aeron.socket.so_sndbuf=4194304
aeron.driver.timeout=10000              # ms; how long before a crashed driver is detected
```

---

## 7. Security Group Rules

The connector makes outbound-only connections to Binance WebSocket and REST
endpoints.  No inbound rules are required for the connector itself; the Prometheus
metrics endpoint (port 9090) should be restricted to monitoring VPC CIDRs.

```
Outbound:
  443/TCP  → 0.0.0.0/0   (Binance HTTPS + WSS)
Inbound:
  9090/TCP → 10.0.0.0/8  (Prometheus scrape from monitoring host)
```

---

## 8. Deployment Checklist

- [ ] Instance in a cluster placement group with downstream consumers
- [ ] ENA or ENA Express enabled; driver version checked (`ethtool -i eth0`)
- [ ] `aws-tuning.sh` run and rebooted to activate boot-param changes
- [ ] `chronyc tracking` shows offset < 100 µs; PTP configured if < 10 µs required
- [ ] CPU governor = performance; C-states 2+ disabled
- [ ] `/dev/shm` mounted at ≥ 4 GiB; hugepages allocated
- [ ] `connector` system user created; `/etc/connector/` owned correctly
- [ ] `connector@.service` installed and enabled for each shard
- [ ] `RUST_LOG=info` in `/etc/connector/env`
- [ ] `config/default.toml` deployed to `/etc/connector/default.toml` with correct
      `[instance]` and `[aeron]` sections for this host
- [ ] Prometheus alerting on `connector_reconnect_count` and `connector_gap_count`
- [ ] Runbook for failover in place (see `deploy/runbook.md`, Stage 12.45)
