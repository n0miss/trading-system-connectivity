#!/usr/bin/env bash
# aws-tuning.sh — one-shot EC2 instance tuning for the market-data connector.
#
# Run as root on a fresh Amazon Linux 2023 or Ubuntu 22.04 instance.
# Re-running is idempotent for most steps.
#
# Usage:
#   sudo bash deploy/aws-tuning.sh [--nic eth0] [--shm-gb 4] [--hugepages 512]
#
# What this script does:
#   1. Install chrony and configure Amazon Time Sync Service (PTP-disciplined).
#   2. Set CPU frequency governor to performance.
#   3. Disable CPU C-states (keep cores at full frequency).
#   4. Tune NIC ring buffers, interrupt coalescing, and IRQ affinity.
#   5. Increase socket and netdev receive buffers.
#   6. Allocate huge pages for Aeron's media driver.
#   7. Mount /dev/shm at the requested size.
#   8. Create the connector system user and directories.

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults (override with flags)
# ---------------------------------------------------------------------------
NIC="eth0"
SHM_GB=4
HUGEPAGES=512      # 2 MiB each → 1 GiB; Aeron typically needs 256–512

while [[ $# -gt 0 ]]; do
    case $1 in
        --nic)         NIC="$2";       shift 2 ;;
        --shm-gb)      SHM_GB="$2";   shift 2 ;;
        --hugepages)   HUGEPAGES="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

echo "=== Connector tuning: NIC=$NIC SHM=${SHM_GB}g HUGEPAGES=$HUGEPAGES ==="

# ---------------------------------------------------------------------------
# 1. Time synchronisation — Amazon Time Sync Service
# ---------------------------------------------------------------------------
echo "--- [1/8] Time synchronisation"

if command -v dnf &>/dev/null; then
    dnf install -y chrony
elif command -v apt-get &>/dev/null; then
    apt-get install -y --no-install-recommends chrony
fi

# Amazon Time Sync Service is reachable at 169.254.169.123 from every EC2
# instance and is disciplined by PTP hardware clocks on the hypervisor.
CHRONY_CONF=/etc/chrony.conf
if ! grep -q "169.254.169.123" "$CHRONY_CONF" 2>/dev/null; then
    # Prepend the AWS time source so it takes precedence.
    sed -i '1s|^|server 169.254.169.123 prefer iburst minpoll 4 maxpoll 4\n|' "$CHRONY_CONF"
fi
systemctl enable --now chronyd
chronyc makestep   # force immediate sync

echo "    chrony tracking: $(chronyc tracking | grep 'Reference ID')"

# ---------------------------------------------------------------------------
# 2. CPU frequency governor → performance
# ---------------------------------------------------------------------------
echo "--- [2/8] CPU governor"

if ! command -v cpupower &>/dev/null; then
    if command -v dnf &>/dev/null; then
        dnf install -y kernel-tools
    elif command -v apt-get &>/dev/null; then
        apt-get install -y --no-install-recommends linux-tools-generic
    fi
fi

cpupower frequency-set -g performance || \
    for cpu in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor; do
        echo performance > "$cpu" 2>/dev/null || true
    done
echo "    governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unavailable)"

# ---------------------------------------------------------------------------
# 3. Disable CPU C-states (latency spikes from deep sleep → wake)
# ---------------------------------------------------------------------------
echo "--- [3/8] CPU C-states"

# Write via cpuidle; some instance types (c5, c6i) support this.
for cpu in /sys/devices/system/cpu/cpu[0-9]*/cpuidle/state[2-9]/disable; do
    echo 1 > "$cpu" 2>/dev/null || true
done

# Also set the kernel boot parameter for persistence across reboots.
# (Requires grubby or similar; skip if not available.)
if command -v grubby &>/dev/null; then
    grubby --update-kernel=ALL --args="intel_idle.max_cstate=1 processor.max_cstate=1" 2>/dev/null || true
fi

echo "    C-state 0 latency: $(cat /sys/devices/system/cpu/cpu0/cpuidle/state0/latency 2>/dev/null || echo n/a) us"

# ---------------------------------------------------------------------------
# 4. NIC tuning
# ---------------------------------------------------------------------------
echo "--- [4/8] NIC ($NIC) tuning"

if ! ip link show "$NIC" &>/dev/null; then
    echo "    WARNING: $NIC not found — skipping NIC tuning"
else
    # Ring buffer: maximise to reduce drop rate on burst reception.
    ethtool -G "$NIC" rx 4096 tx 4096 2>/dev/null || true

    # Interrupt coalescing: 50 µs is a good starting point.
    # Lower values reduce latency but increase CPU %; tune empirically.
    ethtool -C "$NIC" rx-usecs 50 2>/dev/null || true

    # Disable GRO (Generic Receive Offload): it batches packets which adds
    # latency; disable when latency matters more than throughput.
    ethtool -K "$NIC" gro off 2>/dev/null || true

    # IRQ affinity: steer NIC interrupts to CPU 1 (isolated from shard CPUs).
    # The NIC's IRQ numbers can be found with: cat /proc/interrupts | grep $NIC
    # This script pins the first IRQ we find; adjust for multi-queue NICs.
    IRQ=$(grep -m1 "$NIC" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ' || true)
    if [[ -n "$IRQ" && -f "/proc/irq/$IRQ/smp_affinity_list" ]]; then
        echo 1 > "/proc/irq/$IRQ/smp_affinity_list"
        echo "    IRQ $IRQ pinned to CPU 1"
    else
        echo "    IRQ affinity: could not determine NIC IRQ — set manually"
    fi

    echo "    ring buffer: $(ethtool -g "$NIC" 2>/dev/null | grep -A2 'Current hardware settings' | tail -2 || echo see ethtool -g $NIC)"
fi

# ---------------------------------------------------------------------------
# 5. Socket and netdev receive buffers
# ---------------------------------------------------------------------------
echo "--- [5/8] Socket buffers"

cat >> /etc/sysctl.d/99-connector.conf <<'EOF'
# Connector — network tuning
net.core.rmem_max           = 134217728   # 128 MiB
net.core.wmem_max           = 134217728
net.core.rmem_default       = 25165824    # 24 MiB
net.core.wmem_default       = 25165824
net.core.netdev_max_backlog = 250000
net.ipv4.tcp_low_latency    = 1
net.ipv4.tcp_timestamps     = 1
net.ipv4.tcp_sack           = 1
EOF
sysctl --system --quiet

echo "    rmem_max: $(sysctl -n net.core.rmem_max)"

# ---------------------------------------------------------------------------
# 6. Huge pages for Aeron media driver
# ---------------------------------------------------------------------------
echo "--- [6/8] Huge pages ($HUGEPAGES × 2 MiB)"

echo "$HUGEPAGES" > /proc/sys/vm/nr_hugepages

# Persist across reboots.
grep -q "vm.nr_hugepages" /etc/sysctl.d/99-connector.conf 2>/dev/null || \
    echo "vm.nr_hugepages = $HUGEPAGES" >> /etc/sysctl.d/99-connector.conf

ALLOCATED=$(cat /proc/sys/vm/nr_hugepages)
echo "    allocated: $ALLOCATED pages ($(( ALLOCATED * 2 )) MiB)"

# Mount hugetlbfs if not already mounted.
if ! mountpoint -q /mnt/huge 2>/dev/null; then
    mkdir -p /mnt/huge
    mount -t hugetlbfs nodev /mnt/huge
    echo "nodev /mnt/huge hugetlbfs defaults 0 0" >> /etc/fstab
fi

# ---------------------------------------------------------------------------
# 7. /dev/shm for Aeron IPC
# ---------------------------------------------------------------------------
echo "--- [7/8] /dev/shm (${SHM_GB} GiB)"

# Remount with explicit size; default is half of RAM.
mount -o remount,size="${SHM_GB}g" /dev/shm 2>/dev/null || \
    echo "    WARNING: could not remount /dev/shm — add 'tmpfs /dev/shm tmpfs defaults,size=${SHM_GB}g 0 0' to /etc/fstab"

mkdir -p /dev/shm/aeron

echo "    /dev/shm size: $(df -h /dev/shm | awk 'NR==2{print $2}')"

# ---------------------------------------------------------------------------
# 8. System user and directories
# ---------------------------------------------------------------------------
echo "--- [8/8] System user and directories"

id connector &>/dev/null || useradd -r -g daemon -s /sbin/nologin -c "Market data connector" connector
chown connector:daemon /dev/shm/aeron

mkdir -p /etc/connector /var/log/connector
chown connector:daemon /var/log/connector

echo "    user: $(id connector)"
echo ""
echo "=== Tuning complete. Reboot to persist C-state and governor changes. ==="
