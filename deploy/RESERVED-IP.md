# Reserved IP + DNS durability (MVP-FOLLOWUP D1)

The client side of D1 is **done in code**: `packages/config` addresses everything
by hostname — the HTTP API as `https://trackerstream.xyz` and the libp2p
bootstrap/STUN as `/dns4|/dns6/trackerstream.xyz/...`. So both planes survive an
IP change once DNS is updated; the literal IPs in config are fallback-only.

The remaining step pins the IP itself so a droplet **rebuild** doesn't change it.
It needs your DigitalOcean account (a `doctl` token or the control panel) and
registrar/DNS access — it is **not** done automatically. `doctl` is not installed
on the droplet; run these from a workstation with `doctl auth init`.

Current droplet id: **579550404** (region fra1).

## 1. Create + assign a Reserved IP

```bash
doctl compute reserved-ip create --region fra1
#   -> prints the new reserved IP, e.g. 203.0.113.50
doctl compute reserved-ip-action assign 203.0.113.50 579550404
doctl compute reserved-ip list      # confirm it's bound to the droplet
```

## 2. Point DNS at the Reserved IP

At the `trackerstream.xyz` DNS host (Cloudflare/registrar), update the **A** record
to the reserved IPv4. (IPv6/AAAA: DO reserved IPs are v4-only; keep the AAAA on the
droplet's stable v6 `2a03:b0c0:3:f0:0:2:959c:b000`, or drop AAAA.) Let Caddy renew
the cert as normal — it follows the hostname.

## 3. Re-announce the reserved IP on the master node

The kubo master announces its public address for libp2p. Update it so peers dial
the durable IP:

```bash
# in deploy/install.sh (and the live node), set PUBLIC_IP to the reserved IP:
ssh trackerstream-server
export IPFS_PATH=/srv/trackerstream/ipfs
RIP=203.0.113.50
sudo -u trackerstream env IPFS_PATH=$IPFS_PATH ipfs config --json Addresses.Announce \
  "[\"/ip4/$RIP/tcp/4001\",\"/ip6/2a03:b0c0:3:f0:0:2:959c:b000/tcp/4001\",\"/ip4/$RIP/udp/4001/quic-v1\",\"/ip6/2a03:b0c0:3:f0:0:2:959c:b000/udp/4001/quic-v1\"]"
systemctl restart trackerstream-ipfs
```

Also bump `PUBLIC_IP=` in `deploy/install.sh` and `MASTER_IPV4` in
`packages/config/index.js` (the fallback) to the reserved IP for consistency.
Clients prefer the `/dns4/...` bootstrap, so they keep working through DNS even
before this — this just makes the announced literal match.

## 4. Rebuild without changing the IP

Because the data volume + reserved IP are detachable, a droplet rebuild is now:

```bash
# detach the block-storage volume + reassign the reserved IP after rebuild
doctl compute volume-action detach <volume-id> 579550404
# ... rebuild/recreate the droplet ...
doctl compute volume-action attach <volume-id> <new-droplet-id>
doctl compute reserved-ip-action assign 203.0.113.50 <new-droplet-id>
# re-run deploy/install.sh; DNS + the cert + libp2p addresses are unchanged.
```

DNS never changes, so the cert, the API URL, and the libp2p bootstrap all keep
resolving across the rebuild — the durability goal of D1.
