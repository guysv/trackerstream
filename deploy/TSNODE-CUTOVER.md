# Master cutover: kubo → tsnode

The trackerstream master moves from stock **kubo** to the custom **tsnode** node
(go-libp2p + boxo, one binary, custom DHT `/trackerstream/kad/1.0.0` + IPNS-over-gossipsub).
kubo is **kept installed but stopped** as a warm-standby rollback — its repo
(`/srv/trackerstream/ipfs`, incl. `keystore/`) is the rollback identity and is never touched.

tsnode runs from its **own** repo (`/srv/trackerstream/tsnode`) with a re-ingested blockstore
(CID parity), so the two masters never share a datastore. Both bind `:4001` (swarm) and
`127.0.0.1:5001` (kubo-compatible RPC), so they are mutually exclusive (the systemd units
`Conflicts=` each other).

## What changed in the box
- `apps/server` no longer runs an HTTP **tracker** (presence/roster/peers + sweep deleted).
  It keeps only the **/ipns** record cache (publish ← ingest, resolve → client) and `/healthz`.
  Membership/holder-discovery/IPNS-distribution moved into libp2p on tsnode.
- Ingest (`apps/server/bin/ingest.ts`) is **unchanged** — it drives tsnode over `KUBO_API`
  (`block/put`, `pin/add`, `add`, `key/gen`, `name/publish`, `routing/get`), all of which
  tsnode serves. Its in-process IPNS sign also pushes the record on the catalog gossipsub topic.

## One-time prep (build + ship)
```sh
# on the workstation: cross-compile the static linux binary into deploy/dist/
bash deploy/build-tsnode.sh                  # produces deploy/dist/tsnode-linux-amd64 (+ desktop triples)
# ship the artifact as usual (build-artifact.sh / rsync), then on the box:
sudo bash deploy/install.sh                  # installs /usr/local/bin/tsnode + the node unit (kubo still live)
```

## Cutover (reversible)
```sh
# 1) stage tsnode's repo + import the kubo identities (PeerId + catalog IPNS name preserved)
sudo bash deploy/cutover-tsnode.sh prepare

# 2) flip: stop+disable kubo, start tsnode, re-ingest the corpus, verify the pinset
sudo bash deploy/cutover-tsnode.sh cutover
#    aborts + auto-rolls-back if tsnode's PeerId != MASTER_PEER_ID.

# 3) smoke from a client: resolve the catalog IPNS name + stream a module.
curl -fsS -X POST 127.0.0.1:5001/api/v0/node/status | jq .
```

## Rollback (instant)
```sh
sudo bash deploy/cutover-tsnode.sh rollback   # stop tsnode, re-enable kubo warm standby
```
tsnode's repo is left intact for a retry; kubo resumes on its original identity + pinset.

## Verify / backup / restore
- `deploy/verify-pinset.sh` — catalog roots ⊆ live pinset (via RPC `pin/ls`, either master).
- `deploy/backup.sh` — catalog `.backup` + pinset (RPC `pin/ls`) + `server.env`.
- Restore re-pins from `pinset.txt` against whichever master is live (`pin/add` per root) and
  re-ingests from the archive for any missing blocks (boxo blockstore, GC-disabled = durable).

## Identities (must be preserved — baked into shipped clients)
- swarm `MASTER_PEER_ID = 12D3KooWGb7eHYgZnMFfADEDeS5xDEwEVQKPTGozsKanpDf9XvzL`
  — imported from kubo `config.Identity.PrivKey` → tsnode `identity.key`.
- catalog `CATALOG_IPNS_KEY = 12D3KooWDb53qFZvANj5kDCr3riMhT2HJG32i5xqFKhvBtzh7wPC`
  — imported from `ipfs key export catalog` → tsnode `keystore/catalog`.
  (If the box's kubo has no `catalog` key yet — it's born on first ingest — tsnode mints one;
  if its name differs, update `packages/config` + ship clients.)
