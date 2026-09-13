# pi-mix

A two-node home-lab sensor pipeline. The Pi reads Arduino DHT11/LDR over UART at 9600 baud, receives IR and BLE events, and serves live MQTT/SSE data. The Vaio stores history on its HDD.

| Node | Services | Storage |
| --- | --- | --- |
| Pi (`the-bear`) | UART bridge, DHT11/LDR/IR/BLE agents, MQTT, API, dashboard | Live state and bounded queues in RAM; no sensor history volumes |
| Vaio (`vaio-server`) | PostgreSQL, Redis, db-ingestor, VictoriaMetrics, vmagent | Local-path PVCs on HDD |

PostgreSQL is shared infrastructure, independent of this sensor application. Sensors use the dedicated `pi_mix` database and restricted `pi_mix_writer` role. Other projects should have their own databases and roles. Redis is an expendable shared cache: each project gets an ACL user and key prefix. The API caches successful history queries for 20 seconds under `pi-mix:history:*`; cache failure falls back to VictoriaMetrics.

VictoriaMetrics retains detailed sensor history for one year. PostgreSQL keeps 15-minute and hourly aggregates. The writer checkpoints immutable batches to the Vaio every 10 seconds and writes PostgreSQL every minute. MQTT acknowledgements follow checkpoint fsync; transactional event and batch identities prevent duplicate replay. Hourly averages are weighted by sample count, and late arrivals update closed buckets.

## Availability limits

Live collection remains usable while the Vaio is offline. Pi publishers and MQTT use bounded RAM queues, intentionally without SD persistence. A Pi/broker restart loses data that has not reached the Vaio; a sufficiently long storage outage fills queues and causes gaps. This architecture does not promise lossless history during those failures. Persistent Vaio batches survive writer/database restarts. Events older than seven days are rejected; event deduplication records are retained for nine days, while batch replay identities remain permanent.

The dashboard distinguishes stale sensor values from an open SSE connection and reports history-storage failures. IR and scale are event-driven, so lack of a recent event does not itself mean hardware failure.

## Repository and deployment

- `agents/`, `web/`, `firmware/`: application source.
- `k8s/`: sensor application manifests and encrypted credentials.
- `infrastructure/database/`: shared PostgreSQL/Redis, access policies and daily HDD backups. Manage separately from the sensor Argo application; shared resources have prune/delete protection.
- `agents/db-ingestor/migrations/`: explicitly applied database schema and upgrades. Runtime writers do not have DDL privileges.
- `ops/`: recovery, credential provisioning and manual firmware job. Firmware is flashed explicitly, never as an automatic sync hook.

GitHub Actions tests all Go services, Python publishers and the typed dashboard build before publishing images. Vaio workloads have amd64 and arm64 images. One manifest commit updates all images together only after every build succeeds. Hardware services use Recreate to avoid simultaneous UART/GPIO/MQTT ownership.

Credentials are cluster-scoped SealedSecrets. To provision or reseal, run `ops/provision-credentials.py --cert <public-cert.pem> --kubeseal <binary>` locally. It reuses existing secrets and writes only encrypted manifests to Git. Configure the PostgreSQL role/schema before switching the writer. Do not apply the Vaio history manifests over an existing Pi installation until its data has been copied to the new PVCs.

Shared database access is allowed from `database`, `iot`, and namespaces labeled `homelab/database-access=true`; authentication still applies. MQTT permits only authenticated traffic: `pi/#` (read/write) and `cudy/#` (read for the ingestor, write-only for the router collector). UART mutation RPCs require `bridge-control`; read RPCs are restricted by a NetworkPolicy to sensor agents and the manual flasher.

## Cudy router telemetry

The Cudy LT18 4G router is read-only scraped by the `cudy-collector` (separate repo), which publishes one scalar series per message on `cudy/telemetry/<metric>` with payload `{ts, value, unit}`. The ingestor subscribes `cudy/telemetry/+` and stores each series as `sensor_cudy_<metric>` in the same Redis/PG rolling layout (raw, 15m, 1h), deduplicated deterministically per `(metric, ts)`. History queries work via the normal API, e.g. `{__name__="sensor_cudy_rsrp"}`.

- Series follow the collector's contract: `rssi, rsrp, rsrq, sinr, band, ul_bandwidth, dl_bandwidth, connected`, traffic counters per interface and `clients_count`.
- The broker ACL grants `cudy` a write-only `cudy/#` (collector) and `pi-mix` read access to `cudy/#` (ingestor). The `cudy` MQTT user's password lives in the `mosquitto-auth` SealedSecret — reseal it after adding the user.
- Counters (byte/packet totals) are stored and rolled up as-is; the `last`/`last_ts` aggregation fields carry the latest counter value. Rate conversion is a future dashboard-side concern.
- Live SSE/dashboard panels for the router are not part of the ingestor; the API history path serves the series. Dashboard UI is a follow-up.

## Verification and backups

Run `go test ./...` in each Go agent directory, `python3 -m unittest discover -s tests`, and `npm ci && npm run build` in `web`. Database replay/late-arrival integration tests additionally require `TEST_PG_DSN` pointing to a disposable database with both SQL migrations applied. Never use a production database for test setup.

The PostgreSQL CronJob writes compressed cluster dumps at 03:15 UTC to the Vaio, retaining 14 days. Role passwords are excluded: retain encrypted credentials and the SealedSecrets recovery key separately. Same-disk backups protect against logical errors, not HDD failure; copy verified backups to another machine for disk-loss protection.

For a post-deployment audit, check node and pod readiness, fresh `/api/state` timestamps and `/api/status`, successful history queries, writer `/status` pending batches, PostgreSQL aggregates, Redis cache hits and rejected anonymous access. Confirm PVC node affinity selects the Vaio and inspect failed backup jobs.

Operational references: [Mosquitto configuration](https://mosquitto.org/man/mosquitto-conf-5.html), [Redis ACLs](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/), [VictoriaMetrics migration](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#data-migration).
