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
