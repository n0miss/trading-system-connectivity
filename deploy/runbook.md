# Deployment-Generation Migration Runbook

A **deployment generation** is a single commit of (binary, config, `total_logical_shards`)
that runs without disruption until the next migration.  Changing `total_logical_shards`
or making a breaking binary change (schema version bump, new Aeron stream layout) requires
a generation migration — standing up the new generation in **shadow mode** alongside the
current one, comparing their outputs, and then performing an atomic switchover.

---

## 1. Concepts

### Generation identity

Each generation is identified by its `total_logical_shards` count and the git SHA of the
binary.  The generation is stored in `/etc/connector/env`:

```
GENERATION=g4-abc1234   # 4 shards, git SHA abc1234
TOTAL_SHARDS=4
```

### Stream ID convention

The active generation publishes to Aeron stream IDs `1..=N` (one per shard).  The shadow
generation publishes to `SHADOW_STREAM_ID_OFFSET + 1..=SHADOW_STREAM_ID_OFFSET + N`,
where `SHADOW_STREAM_ID_OFFSET = 1000` (defined in `shadow-compare`).

| Generation | Shard 0 | Shard 1 | Shard 2 | Shard 3 |
|------------|---------|---------|---------|---------|
| Active     | stream 1 | stream 2 | stream 3 | stream 4 |
| Shadow     | stream 1001 | stream 1002 | stream 1003 | stream 1004 |

### Migration triggers

| Trigger | Required action |
|---------|-----------------|
| Change `total_logical_shards` | Full generation migration (symbols rehash) |
| Binary upgrade with schema version bump | Full generation migration |
| Binary upgrade, same schema | Shadow + compare + in-place binary swap |
| Config-only change (REST timeouts, log level) | Restart, no shadow needed |
| Instance type change | Replace host, start fresh generation |

---

## 2. Pre-migration Checklist

Before starting a migration, verify the current generation is healthy:

```bash
# All shards reporting Live
journalctl -u 'connector@*' --since '10m ago' | grep FeedStatus | grep -v Live
# Expected: no output (all shards Live)

# No recent reconnects
journalctl -u 'connector@*' --since '30m ago' | grep -i reconnect
# Expected: 0 or very few lines

# Prometheus metrics
curl -s localhost:9090/metrics | grep connector_reconnect_count
curl -s localhost:9090/metrics | grep connector_gap_count
# Expected: both counts should be low and stable

# Disk space for shadow (Aeron Archive, if enabled)
df -h /dev/shm /var/connector/archive

# Review the diff between current and new binary/config
git log --oneline active-generation..new-generation
git diff active-generation..new-generation config/
```

---

## 3. Build the New Generation

```bash
# On the build host (or in CI)
git checkout new-generation-branch
cargo build --release --bin connector

# Verify schema version matches expectation
strings target/release/connector | grep "schema_version"

# Package the binary and config
tar czf connector-new-gen.tar.gz \
    target/release/connector \
    config/default.toml
scp connector-new-gen.tar.gz deploy-host:/tmp/
```

---

## 4. Deploy Shadow Generation

### 4a. Install the new binary alongside the current one

```bash
# On the target host (as root)
tar xzf /tmp/connector-new-gen.tar.gz -C /tmp/new-gen/
install -m 755 /tmp/new-gen/target/release/connector /usr/local/bin/connector-shadow
install -m 644 /tmp/new-gen/config/default.toml       /etc/connector/shadow.toml
```

### 4b. Install shadow systemd units

Shadow units use the same `connector@.service` template but override the binary,
config, and stream ID offset:

```bash
# Create drop-in for each shard that overrides the ExecStart
TOTAL_SHARDS=4    # adjust to match the new generation
for i in $(seq 0 $(( TOTAL_SHARDS - 1 ))); do
    mkdir -p "/etc/systemd/system/connector-shadow@${i}.service.d"
    cat > "/etc/systemd/system/connector-shadow@${i}.service.d/shadow.conf" <<EOF
[Unit]
Description=Crypto CEX Market Data Connector — shadow shard %i

[Service]
ExecStart=
ExecStart=/usr/local/bin/connector-shadow \
    --config /etc/connector/shadow.toml \
    --shard-id %i \
    --total-shards ${TOTAL_SHARDS}
Environment=RUST_LOG=info
Environment=SHADOW_STREAM_ID_OFFSET=1000
EOF
done

# Install the base template (reuse the active one)
cp /etc/systemd/system/connector@.service /etc/systemd/system/connector-shadow@.service
systemctl daemon-reload
```

### 4c. Start shadow shards

```bash
TOTAL_SHARDS=4
SHARDS=$(seq -s, 0 $(( TOTAL_SHARDS - 1 )))
systemctl start $(eval echo "connector-shadow@{0..$((TOTAL_SHARDS-1))}.service")

# Verify all shadow shards are running
systemctl status 'connector-shadow@*'
journalctl -u 'connector-shadow@*' -f &   # watch logs in background
```

Wait ~30 seconds for shadow shards to connect and reach `Live` state before proceeding.

---

## 5. Shadow Comparison

Run `shadow-compare` for a stability window of at least **5 minutes** (300 ticks at 1 s
interval).  The tool exits with code 0 (stable) or 1 (diverging).

```bash
# Start shadow-compare — reads from active (streams 1..N) and shadow (streams 1001..N)
# Note: pass Aeron dir when real Aeron is wired; --demo flag for dry-run testing
shadow-compare \
    --tolerance-bps 1 \
    --min-samples 300 \
    --max-divergence-pct 0.0 \
    --aeron-dir /dev/shm/aeron \
    --total-shards 4

# Exit codes
# 0 — STABLE: all symbols match within tolerance for the full window
# 1 — DIVERGING: at least one symbol exceeded tolerance
# 2 — TIMEOUT: window elapsed without reaching min-samples (shadow too slow)
```

**Minimum stability window**: 5 minutes  
**Recommended window before switchover**: 15 minutes during market hours

If `shadow-compare` exits with code 1 or 2, investigate before proceeding:

```bash
# Check which symbols diverged
shadow-compare --report-file /tmp/shadow-report.json ...
cat /tmp/shadow-report.json | jq '.by_symbol | to_entries | sort_by(.value.max_bid_diff_bps) | reverse | .[0:5]'

# Common causes:
# - Shadow started mid-gap recovery → wait and retry
# - New binary has a normalisation bug → rollback
# - New shard count causes different symbol assignment → expected difference, verify mapping
```

---

## 6. Switchover Procedure

Switchover is atomic from the clients' perspective: they switch their Aeron subscription
from active stream IDs to shadow stream IDs at the moment the active shards stop
publishing.

### 6a. Notify downstream consumers

If consumers support a "maintenance mode" API, signal them now so they buffer output
rather than reading from a dead stream.  Otherwise, they will naturally detect the
`BookStale` / `Heartbeat` timeout and handle reconnect.

```bash
# Example: update the consumer config to add shadow stream IDs as fallback
# (implementation is consumer-specific)
```

### 6b. Stop active shards

```bash
# Stop active shards (clients now read from shadow)
TOTAL_SHARDS=4
systemctl stop $(eval echo "connector@{0..$((TOTAL_SHARDS-1))}.service")

# Wait for clients to detect the switch (Aeron image close timeout ~10 s by default)
sleep 15

# Confirm all active shards are stopped
systemctl is-active 'connector@*' && echo "SOME STILL RUNNING — STOP MANUALLY" || echo "all stopped"
```

### 6c. Promote shadow to active

```bash
# Move the new binary into place
install -m 755 /usr/local/bin/connector-shadow /usr/local/bin/connector
install -m 644 /etc/connector/shadow.toml      /etc/connector/default.toml

# Remove shadow-specific drop-ins so stream IDs reset to 1..N
for i in $(seq 0 $(( TOTAL_SHARDS - 1 ))); do
    rm -f "/etc/systemd/system/connector-shadow@${i}.service.d/shadow.conf"
done
systemctl daemon-reload

# Update generation marker
sed -i "s/^GENERATION=.*/GENERATION=new-gen-$(git rev-parse --short HEAD)/" /etc/connector/env
sed -i "s/^TOTAL_SHARDS=.*/TOTAL_SHARDS=${TOTAL_SHARDS}/"                   /etc/connector/env

# Stop shadow, start active (shadow stream IDs → active stream IDs)
systemctl stop  $(eval echo "connector-shadow@{0..$((TOTAL_SHARDS-1))}.service")
systemctl start $(eval echo "connector@{0..$((TOTAL_SHARDS-1))}.service")
```

### 6d. Verify post-switchover

```bash
# All shards should reach Live within 30 s
journalctl -u 'connector@*' -f --since now &
sleep 30
journalctl -u 'connector@*' --since '30s ago' | grep Live | wc -l
# Expected: TOTAL_SHARDS lines

# Metrics green
curl -s localhost:9090/metrics | grep connector_reconnect_count
curl -s localhost:9090/metrics | grep connector_gap_count
```

---

## 7. Post-Migration Cleanup

```bash
# Remove shadow binaries and configs
rm -f /usr/local/bin/connector-shadow /etc/connector/shadow.toml

# Remove shadow systemd units
systemctl disable --now 'connector-shadow@*' 2>/dev/null || true
rm -f /etc/systemd/system/connector-shadow@.service
rm -rf /etc/systemd/system/connector-shadow@*.service.d/
systemctl daemon-reload

# Tag the migration in git
git tag "gen-$(date +%Y%m%d)-shards${TOTAL_SHARDS}" -m "deployed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
git push origin --tags
```

---

## 8. Rollback Procedure

If post-switchover checks fail within the **10-minute observation window**, roll back:

```bash
# The old binary is still on disk (we only moved connector-shadow → connector)
# Find the previous generation's binary:
git stash                          # or: git checkout active-generation -- .
cargo build --release --bin connector   # rebuild old gen; or keep a copy in /usr/local/bin/connector.prev

# Update config back to previous shard count
cp /etc/connector/default.toml.bak /etc/connector/default.toml   # if you took a backup
sed -i "s/^TOTAL_SHARDS=.*/TOTAL_SHARDS=PREV_SHARD_COUNT/" /etc/connector/env

# Restart
systemctl restart 'connector@*'

# Verify recovery
journalctl -u 'connector@*' -f
```

**Always keep the previous generation's binary as `/usr/local/bin/connector.prev`** before
overwriting, so rollback is a single `cp` + `systemctl restart`.

---

## 9. Scripted / Unattended Switchover

`shadow-compare` exits with machine-readable exit codes for use in CI/CD:

| Exit code | Meaning |
|-----------|---------|
| 0 | Stable — safe to switchover |
| 1 | Diverging — abort switchover |
| 2 | Timeout — shadow too slow, abort |
| 3 | Error — internal error |

Example pipeline integration:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Start shadow, wait for warm-up
bash deploy/shadow-start.sh

# Compare for 15 minutes
if ! shadow-compare \
        --tolerance-bps 1 --min-samples 900 --max-divergence-pct 0.0 \
        --aeron-dir /dev/shm/aeron --total-shards "${TOTAL_SHARDS}"; then
    echo "Shadow comparison failed (exit $?); aborting" >&2
    bash deploy/shadow-teardown.sh
    exit 1
fi

# Execute switchover
bash deploy/switchover.sh
```

---

## 10. Checklist Summary

**Before migration:**
- [ ] Current generation: all shards Live, no recent gaps or reconnects
- [ ] New binary built and checksummed; config diff reviewed
- [ ] Aeron Archive has sufficient disk space (if enabled)
- [ ] Downstream consumers notified (or maintenance mode enabled)
- [ ] `/usr/local/bin/connector.prev` created

**Shadow deployment:**
- [ ] Shadow shards started; all reach Live within 60 s
- [ ] `shadow-compare` exits 0 after ≥5 minutes

**Switchover:**
- [ ] Active shards stopped; clients detect image close
- [ ] New binary installed; stream IDs reset to 1..N
- [ ] Active shards started from new binary
- [ ] All shards reach Live within 30 s; metrics green

**Post-migration:**
- [ ] Shadow units removed; old binary removed
- [ ] Migration tagged in git
- [ ] On-call engineer monitors for 1 hour
