# Installing and running NebulaDB

NebulaDB is two static binaries (pure Go, no C dependencies, no runtime):

* `nebuladb`: the server (Redis protocol on `:6380`, admin/Inspector API on `:7380`).
* `nebula`: the maintenance CLI (`stats`, `verify`, `dump-wal`, `dump-sstable`, `dump-manifest`, `repair`, `compact`).

Linux and macOS are the supported platforms. The Windows binaries compile, but the
data-directory lock and `-supervise` are Unix-only, so treat Windows as untested.

## 1. From a release archive

```sh
sha256sum -c SHA256SUMS --ignore-missing          # verify the download
tar xzf nebuladb-<version>-linux-amd64.tar.gz
cd nebuladb-<version>-linux-amd64
./nebuladb -version
./nebuladb -dir ./data                            # RESP on 127.0.0.1:6380, admin on 127.0.0.1:7380
redis-cli -p 6380 SET hello world
```

Archives are built by `scripts/release.sh [VERSION]` (or `make release VERSION=x.y.z`) for
linux, darwin and windows on amd64 and arm64. The script only writes files into `dist/`; it
does not upload or tag anything.

## 2. From source

Needs Go 1.27 or newer.

```sh
go build -o bin/nebuladb ./cmd/nebuladb
go build -o bin/nebula   ./cmd/nebula
bin/nebuladb -dir ./data
```

## 3. Docker

```sh
docker build -t nebuladb:local --build-arg VERSION=0.1.0 .   # ~16 MB image: static binary on an empty base
docker run -d --name nebula -p 127.0.0.1:6380:6380 -p 127.0.0.1:7380:7380 -v nebula-data:/data nebuladb:local
redis-cli -p 6380 PING
```

The image runs as a non-root user (65532), keeps data in the `/data` volume and has a
health check (`nebula stats -addr 127.0.0.1:6380`). Inside a container the server must listen on
`0.0.0.0`, so **publish the ports to `127.0.0.1` only**: the admin port has no
authentication and exposes a command proxy.

### Docker Compose (database + Inspector)

```sh
docker compose up --build
# database:  redis-cli -p 6380 ...        Inspector: http://localhost:8080
```

If a host port is taken (6380 is a common Redis port), override it:
`NEBULA_PORT=16380 NEBULA_ADMIN_PORT=17380 INSPECTOR_PORT=18080 docker compose up`, then open
`http://localhost:18080/?admin=http://127.0.0.1:17380`.

The compose file starts the server with `-dev` (flush and compact buttons work). The Inspector's
"crash it" button needs `-supervise`, which restarts the process after `kill -9`; inside a
container a restart policy does the same job, but the button asks the server to kill itself, so for
that demo run the server natively (`bin/nebuladb -dir ./data -supervise`) and the Inspector with
`cd inspector && npm install && npm run dev`.

## 4. Configuration that matters

| flag | default | meaning |
|---|---|---|
| `-dir` | `./data` | data directory (a lock file stops two servers sharing it) |
| `-addr` | `127.0.0.1:6380` | RESP listen address |
| `-admin-addr` | `127.0.0.1:7380` | admin HTTP (empty disables). Non-loopback means NO authentication on a command proxy: don't expose it |
| `-fsync` | `always` | `always` (no acknowledged write lost, even on power loss), `every` (up to `-fsync-interval` lost on power loss), `never` |
| `-memtable-size` | 4 MiB | when the active memtable is sealed and flushed |
| `-cache-size` | 64 MiB | block cache (0 disables) |
| `-compaction` | `leveled` | or `size-tiered` |
| `-repl-listen` / `-replicaof` | off | replication, see below |

`nebuladb -h` lists everything. Process crashes (kill -9, OOM kill, panic) never lose an
acknowledged write in any `-fsync` mode; the modes differ only for power loss
(see `docs/FAILURES.md`).

## 5. Running as a service (systemd example)

```ini
# /etc/systemd/system/nebuladb.service
[Unit]
Description=NebulaDB
After=network.target

[Service]
User=nebula
ExecStart=/usr/local/bin/nebuladb -dir /var/lib/nebuladb -addr 0.0.0.0:6380
Restart=on-failure
LimitNOFILE=65536       # every live SSTable keeps a file descriptor open

[Install]
WantedBy=multi-user.target
```

Bind `-addr` to a private interface or put it behind a firewall: the Redis protocol port has no
authentication either.

## 6. Replication (leader and read-only replica)

```sh
# leader: wait for one replica to confirm every write before replying OK (omit -repl-min-acks for async)
nebuladb -dir ./leader  -addr 127.0.0.1:6380 -admin-addr "" -repl-listen 127.0.0.1:7381 -repl-min-acks 1
# replica: read-only, follows the leader
nebuladb -dir ./replica -addr 127.0.0.1:6381 -admin-addr "" -replicaof 127.0.0.1:7381
```

`INFO` shows role, link state, applied sequence and lag. Failover is manual: stop the replica and
start it again without `-replicaof`. Exact guarantees and limits: `NOTES.md`, section "Replication".

## 7. Backup, verify, upgrade

* **Verify** a data directory (read-only; stop the server first so the files are not changing under it): `nebula verify ./data`.
* **Backup**: stop the server (or take it from a replica) and copy the directory; the on-disk
  format is the same on every platform (little-endian, checksummed).
* **Salvage** a damaged directory into a new one: `nebula repair ./data ./recovered`.
* SSTables and the MANIFEST carry magic numbers and checksums; the server refuses to open a directory
  it finds corrupt rather than guessing (see `NOTES.md` for the formats and `docs/FAILURES.md` for what happens).
