# trackerstream — restore / reinstall drill

The control plane is reproducible from the deploy artifact + a backup. The data
plane (block store) is re-pinnable from the archive, so the durable state to back
up is the **catalog DB** and the **pinset list** (`deploy/backup.sh`), plus a
**DigitalOcean volume snapshot** for fast block-store recovery.

## Clean reinstall (fresh droplet)

1. Provision a Debian 13 droplet + 250 GB volume; run `deploy/provision.sh`
   (base toolchain, swap, volume in fstab, `trackerstream` user, `/srv/trackerstream`).
2. Build + ship the artifact: `bash deploy/build-artifact.sh`, scp it, extract to
   `/opt/trackerstream`, run `sudo bash deploy/install.sh` (Node + kubo + coturn +
   systemd + ufw + master config).
3. Restore data (below) **or** sync the corpus (`deploy/sync-archive.sh`) and run
   `systemctl start trackerstream-ingest` to rebuild catalog + pinset from scratch.

## Restore from backup

```
# stop services
systemctl stop trackerstream-api trackerstream-ipfs

# 1) catalog
cp /srv/trackerstream/backups/<ts>/catalog.db /srv/trackerstream/catalog/catalog.db
chown trackerstream:trackerstream /srv/trackerstream/catalog/catalog.db

# 2a) FAST: restore the block volume from a DO snapshot (whole datastore), OR
# 2b) RE-PIN from the pinset list (blocks re-fetched/rebuilt from the archive):
xargs -a /srv/trackerstream/backups/<ts>/pinset.txt -n1 \
  env IPFS_PATH=/srv/trackerstream/ipfs ipfs pin add

systemctl start trackerstream-ipfs trackerstream-api
```

## Verify after restore

```
bash deploy/verify-pinset.sh     # catalog roots == pinset (no missing/orphan)
curl localhost:8080/healthz      # API up, module count matches
```

A successful drill = services up, `verify-pinset.sh` reports 0 missing, and the
search/fetch/play path works against the restored instance.

## Scheduled ops (backups, metrics, log retention) — D3

`deploy/install.sh` installs and enables these automatically. To enable/refresh
them by hand on an existing droplet:

```
# (re)install units + journald drop-in from the artifact root (/opt/trackerstream)
install -m644 deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
mkdir -p /etc/systemd/journald.conf.d
install -m644 deploy/journald-trackerstream.conf /etc/systemd/journald.conf.d/trackerstream.conf
systemctl restart systemd-journald
systemctl daemon-reload

# daily catalog+pinset backup (+ DO volume snapshot if doctl is authed)
systemctl enable --now trackerstream-backup.timer
# ~1-min /healthz -> Prometheus textfile export
systemctl enable --now trackerstream-metrics.timer

# check
systemctl list-timers 'trackerstream-*'
systemctl start trackerstream-backup.service   # run a backup now
journalctl -u trackerstream-backup.service -n 50
```

### Backups (`trackerstream-backup.timer` → `deploy/backup.sh`)
Daily, `Persistent=true` (catches up after downtime). Writes
`/srv/trackerstream/backups/<ts>/` (catalog.db + pinset.txt + server.env),
rotates to the last `KEEP_LOCAL` (default 14). If `doctl` is installed **and**
authenticated it also triggers a DigitalOcean Block Storage volume snapshot and
prunes to the last `KEEP_SNAPSHOTS` (default 7) `trackerstream-*` snapshots.
Tunables via env / `/etc/trackerstream/server.env`: `KEEP_LOCAL`,
`KEEP_SNAPSHOTS`, `DO_VOLUME_ID`, `DO_VOLUME_NAME` (default `trackerstream`).

**Operator action — enable volume snapshots:** install + auth doctl on the droplet:
```
# install doctl (see DO docs), then:
doctl auth init        # paste a DO API token with read/write on volumes+snapshots
doctl account get      # confirm auth
```
Without doctl the script still succeeds — it just backs up catalog+pinset locally
and prints a hint. Restore from a volume snapshot via the DO control panel /
`doctl compute volume create --snapshot <id>` and reattach (see "Restore from
backup" 2a above).

### Metrics (`trackerstream-metrics.timer` → `deploy/metrics-export.sh`)
Every ~1 min, curls `http://127.0.0.1:8080/healthz` and atomically writes
`/var/lib/node_exporter/textfile_collector/trackerstream.prom` with gauges:
`trackerstream_up`, `trackerstream_modules`, `trackerstream_playlists`,
`trackerstream_users`, `trackerstream_uptime_seconds` (`_up 0` when the API is
down, so outages are observable, not just absent).

**Operator action — surface the metrics** (optional):
```
apt-get install -y prometheus-node-exporter
# ensure node_exporter runs with the textfile collector pointed at the dir:
#   ARGS="--collector.textfile.directory=/var/lib/node_exporter/textfile_collector"
# in /etc/default/prometheus-node-exporter, then restart it.
```
Or skip Prometheus and point an external uptime check straight at
`http(s)://<host>/healthz` (HTTP 200 = up). Tunables: `HEALTHZ_URL`,
`TEXTFILE_DIR`, `TEXTFILE`.

### Log retention (`deploy/journald-trackerstream.conf`)
All services (Caddy, API, kubo, coturn) log to journald. The drop-in
(`/etc/systemd/journald.conf.d/trackerstream.conf`) bounds the journal:
`SystemMaxUse=1G`, `MaxRetentionSec=30day`, persistent storage. Edit + reapply
with `systemctl restart systemd-journald` to change the window.
