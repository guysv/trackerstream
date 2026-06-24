# trackerstream — DigitalOcean → Hetzner migration

Move the single master node from the DO fra1 droplet to Hetzner Cloud. Same stack
(Debian + kubo master + coturn + Node API + Caddy), ~4× cheaper compute and ~10×
cheaper bandwidth (cost appendix at the end).

**Flow (agreed):** you launch the Hetzner VM, give me its SSH alias, and point
`trackerstream.xyz` at it. DO stays alive until the migration is verified. So this
is a plain copy-over, not a hot cutover — no TTL dance, no rollback ceremony.

---

## What must survive the move

Shipped desktop builds reach the master by **two pinned values** in
`packages/config/index.js`, both baked into every binary:

1. **`MASTER_PEER_ID`** (`12D3KooW…XvzL`) — the libp2p identity, stored in the kubo
   keypair at `$IPFS_PATH/config`. **Preserve it**: copy the IPFS data dir verbatim
   (step 2) and do **not** `ipfs init` on the new box. A fresh init changes the
   PeerID and breaks every client even on the right IP.
2. **`MASTER_IPV4` / `MASTER_IPV6`** — the dialed address. This **changes** (a
   Hetzner IP can't equal the DO one). Clients pin the literal IP, not the `/dns4`
   hostname, because the embedded `rust-ipfs 0.15` node won't dial `/dns4` bootstrap
   (`19b8bdd`). So clients need a rebuild with the new IP (step 5) — unless you
   first land the client-side resolve-then-dial fix, which makes the IP follow DNS
   and removes step 5 forever. Pre-launch, just rebuild.

The HTTP/API plane follows DNS automatically (WKWebView resolves the hostname), so
only the libp2p/streaming plane needs the new IP.

---

## Spec mapping (DO → Hetzner)

| | DigitalOcean (now) | Hetzner (target) |
|---|---|---|
| Compute | 4 vCPU / 8 GB (`s-4vcpu-8gb`) | **CX32** (Intel) or **CPX31** (AMD, faster) — both 4 vCPU / 8 GB |
| Region | `fra1` | `fsn1` / `nbg1` (closest to Frankfurt) |
| OS | Debian 13 | Debian 13 (`hcloud image list`; else Debian 12 + dist-upgrade) |
| Volume | 250 GB, `/mnt/volume_fra1_…`, `/dev/sda` | 250 GB, `/mnt/HC_Volume_<id>`, `/dev/disk/by-id/scsi-0HC_Volume_<id>` |
| Public IP | direct on iface (no NAT) | direct on iface (no NAT) — `coturn external-ip` works unchanged |
| CLI | `doctl` | `hcloud` |
| Firewall | `ufw` (host) | `ufw` (host) — unchanged |

---

## 1. You provision (Hetzner console or `hcloud`)

- CX32 (or CPX31), `debian-13`, `fsn1`, your existing SSH key.
- A 250 GB volume, auto-mounted + ext4, attached to the server.
- Reuse the existing keypair so it's reachable.

Then add it to `~/.ssh/config` mirroring the current `trackerstream-server` entry
(same `User root`, `IdentityFile`, and bifrost `ProxyCommand`) and **tell me the
alias** + the new public **IPv4 and IPv6**. Point `trackerstream.xyz` A/AAAA at it
whenever you're ready — the install re-issues the cert once DNS resolves.

Find the volume's mount + device on the box (needed below):

```bash
ssh <alias> 'lsblk -o NAME,SIZE,MOUNTPOINT; ls -l /dev/disk/by-id/ | grep HC_Volume'
# -> mount /mnt/HC_Volume_<id> ; dev /dev/disk/by-id/scsi-0HC_Volume_<id>
```

Base-provision (the script is already parameterized — no edit):

```bash
ssh <alias> "TRACKERSTREAM_VOLUME=/mnt/HC_Volume_<id> \
             TRACKERSTREAM_VOLUME_DEV=/dev/disk/by-id/scsi-0HC_Volume_<id> \
             bash -s" < deploy/provision.sh
```

---

## 2. Copy the data (preserves PeerID, pins, catalog, corpus)

Quiesce writers on DO, rsync `/srv/trackerstream` + `/etc/trackerstream` to
Hetzner, restart DO (it keeps serving until cutover):

```bash
ssh trackerstream-server 'systemctl stop trackerstream-ingest.timer trackerstream-api trackerstream-ipfs'

# data: ipfs datastore + catalog + ~52 GB archive corpus
ssh trackerstream-server "rsync -aHX --info=progress2 /srv/trackerstream/ \
  -e ssh root@<NEW_IPV4>:/srv/trackerstream/"
# secrets/config: server.env, turn.secret
ssh trackerstream-server "rsync -aHX /etc/trackerstream/ \
  -e ssh root@<NEW_IPV4>:/etc/trackerstream/"

ssh <alias> 'chown -R trackerstream:trackerstream /srv/trackerstream'
ssh trackerstream-server 'systemctl start trackerstream-ipfs trackerstream-api trackerstream-ingest.timer'
```

If box-to-box SSH is blocked by the proxy, relay through the workstation, or
re-sync only the corpus with `deploy/sync-archive.sh` (`HOST=<alias>`) and rebuild
catalog+pins per `RESTORE.md` — but copying `ipfs/` directly is faster and keeps
the PeerID with no re-ingest.

---

## 3. Install the control plane (with the new IPs)

```bash
bash deploy/build-artifact.sh
scp deploy/dist/trackerstream-*.tar.* <alias>:/tmp/
ssh <alias> 'mkdir -p /opt/trackerstream && tar -xf /tmp/trackerstream-*.tar.* -C /opt/trackerstream --strip-components=1'

ssh <alias> "cd /opt/trackerstream && \
  PUBLIC_IP=<NEW_IPV4> PUBLIC_IP6=<NEW_IPV6> sudo bash deploy/install.sh"
```

The printed peer id **must equal** `12D3KooW…XvzL`. If not, the keypair didn't copy
— redo step 2 before continuing. (`install.sh` skips `ipfs init` when
`$IPFS_PATH/config` exists, so a correct copy keeps the PeerID automatically.)

> Hetzner IPv6 is a `/64`; pass one stable address from it as `PUBLIC_IP6` and
> confirm it's on the interface (`ip -6 addr`).

---

## 4. TLS (after DNS points at Hetzner)

```bash
ssh <alias> 'cd /opt/trackerstream && sudo TS_DOMAIN=trackerstream.xyz bash deploy/setup-tls.sh'
curl -fsS https://trackerstream.xyz/healthz
```

---

## 5. Update + rebuild clients (new IP)

```js
// packages/config/index.js  — PEER_ID and HOST stay the same
export const MASTER_IPV4 = "<NEW_IPV4>";
export const MASTER_IPV6 = "<NEW_IPV6>";
```

Also bump the `PUBLIC_IP`/`PUBLIC_IP6` defaults in `deploy/install.sh` for
consistency, then rebuild/release the desktop app (`docs/RELEASE.md`). (Skip this
entire step if the client-side DNS-dial fix is in — clients then pick up the new IP
from DNS on next launch.)

### Verify

```bash
ssh <alias> 'IPFS_PATH=/srv/trackerstream/ipfs ipfs id -f "<id>\n"'   # == 12D3KooW…XvzL
ssh <alias> 'cd /opt/trackerstream && bash deploy/verify-pinset.sh'    # 0 missing/orphan
ipfs swarm connect /ip4/<NEW_IPV4>/udp/4001/quic-v1/p2p/12D3KooW…XvzL   # from a workstation
curl -fsS https://trackerstream.xyz/healthz
```

Pass = a client (rebuilt, or DNS-fix build) searches → fetches → plays a module
against the Hetzner master.

---

## 6. Decommission DO

After a few stable days:

```bash
ssh trackerstream-server 'cd /opt/trackerstream && bash deploy/backup.sh'   # final source backup
doctl compute droplet delete 579550404
doctl compute volume delete <do-volume-id>
doctl compute reserved-ip delete <reserved-ip>     # if assigned
```

Rename the Hetzner alias → `trackerstream-server` so existing scripts
(`sync-archive.sh` default `HOST`, etc.) keep working.

---

## Repo cleanup to commit afterward (not required to migrate)

- **`deploy/backup.sh`** — the DO volume-snapshot block won't run on Hetzner; port
  to `hcloud volume create-snapshot` (same graceful-skip pattern) or drop it. The
  catalog+pinset backup already degrades cleanly without `doctl`, so backups don't
  break meanwhile.
- **`deploy/provision.sh`** — defaults assume DO's `/dev/sda`; switch to the
  `scsi-0HC_Volume_*` device once DO is gone (env overrides cover it until then).
- **`deploy/RESERVED-IP.md`** — DO-specific; supersede with Hetzner Primary IP, or
  delete if the client DNS-dial fix lands (no static IP needed).
- **`deploy/turnserver.conf`** — no change; `external-ip` is correct (Hetzner
  assigns the public IP directly, no NAT).

---

## Cost appendix (monthly, ex-VAT; reclaimable as a business)

| Component | DigitalOcean | Hetzner |
|---|---|---|
| 4 vCPU / 8 GB | $48 | €6.80 (CX32) / €13.10 (CPX31) |
| 250 GB volume | $25 | ~€11 |
| IPv4 | included | €0.50 |
| **Box total** | **~$73** | **~$18 (CX32) / ~$25 (CPX31)** |
| Egress incl. / overage | 5 TB / $10/TB | 20 TB / €1/TB |

~4× on compute, ~10× on bandwidth overage (the latter matters most for a TURN relay
+ IPFS block re-server). Figures from my Jan 2026 knowledge — confirm cents on the
pricing pages before committing; the gap is structural, not a promo.
