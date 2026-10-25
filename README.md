# PlantOps Edge Scale

Offline-first unmanned two-pass truck weighbridge controller.

## Production shape

```text
Windows
└── plantops-edge-scale.exe
    ├── native Windows Service host
    ├── embedded HTTP/API + animated operator UI
    ├── two-way physical pass state machine
    ├── authoritative two-pass business-cycle coordinator
    ├── scale / Modbus / RFID / LPR / camera adapters
    ├── embedded pure-Go SQLite
    ├── raw-weight + operational hash-chained audit
    └── offline Central sync + heartbeat worker
```

**One Go EXE is the production runtime.** No IIS, Docker Desktop, WSL2, Node, .NET runtime, PostgreSQL service, CGO, SQLite DLL, CDN, or separate frontend server.

Persistent state:

```text
data/edge.db             cycles + passes + tickets + overrides + sync queue
data/raw-weight.jsonl    every scale controller frame, exact bytes, hash chain
data/events.jsonl        decisions + evidence + faults + output + sync audit chain
```

## Authoritative business cycle

```text
PASS #1  A -> B
  identity + safety + position
  audited stable W1 = GROSS
  -> durable QUEUED cycle
  -> NO final ticket
  -> NO completed Central sync
  -> physical lane returns IDLE

CALL exact queued cycle

PASS #2  B -> A
  identity must match CALLED cycle
  audited stable W2 = TARE
  validate pair
  NET = W1 - W2 > 0
  -> atomic COMPLETE + final ticket + clear queue + sync_queue
```

A physical pass may complete while the business cycle remains queued. An orphan, wrong truck, uncalled return, invalid time pair, invalid weight pair, or missing raw audit evidence never becomes a completed transaction.

See `docs/TWO-PASS-CYCLE.md`.

## Scale input

| `-scale-driver` | Instrument | Line |
|---|---|---|
| `ascii` (default) | TCP text stream `WT=…;ST=…` | `-scale-addr host:port` |
| `w100` | LAUMAS W100/W200, Modbus RTU, gross weight | `-scale-port COM5`, `/dev/ttyUSB0` or `tcp:host:port` |

Bench guide with the W100 and a load cell simulator: `docs/W100-BENCH.md`.

## Safety and audit invariants

```text
Scale weight/stable/fault authority: NEVER software-overridden.
Every scale frame: exact bytes + Edge UTC durably journaled before business use.
Accepted pass weight: exact raw audit seq/hash.
Raw audit failure: reading cannot become business truth.
Entry: identity + physical safety + clear deck + audited stable near-zero scale.
Auxiliary override: transaction-scoped evidence only.
Critical/unsafe failure: FAULT_LOCKOUT.
B->A automatic entry: exact durable CALLED cycle required.
Final ticket: two valid opposite-direction passes required.
Permissive physical output: durable audit intent before Modbus execution.
Audit failure: permissive action blocked; safe/off action remains allowed.
Central/WAN: never part of local completion or truck release.
Physical barrier anti-collision/safety: independent of the application process.
Restart: SafeState first; never replay stale permissive commands.
UI: visualization/control request only; browser state never authorizes hardware.
```

## Cameras

Default installed topology:

```text
C1A   A-side LPR · A -> B
C1B   B-side LPR · B -> A
C3    weighbridge / load overview
```

Optional C2/C4 or future cameras are configured, not hard-coded into the domain.

```text
-camera-ids C1A,C1B,C3
```

LPR and overview image references are attached to the physical pass, persisted into `WeighPass`, and aggregated into the final paired ticket. Camera evidence is also written to the operational audit chain.

See `docs/CAMERA-EVIDENCE.md`.

## Operator UI

Open `/` or `/operator.html`.

The embedded console shows:

- animated A->B and B->A truck movement
- side-A / side-B barriers and physical GREEN feedback
- C1A / C1B / C3 camera activity
- RFID/LPR identity
- front/rear/approach sensors
- authoritative live/stable weight
- all-frame raw weight curve
- physical-pass state
- business cycle status `QUEUED / CALLED / COMPLETE`
- active queue with exact `CALL` action
- gross / tare / net
- SQLite and Central pending-sync state
- operational audit timeline

`VISUAL DEMO` performs no hardware writes and no ticket commits.

```text
/operator.html?demo=1
/operator.html?demo=WEIGHING
```

## Native Windows Service

```text
plantops-edge-scale.exe -service install [runtime flags...]
plantops-edge-scale.exe -service start
plantops-edge-scale.exe -service status
plantops-edge-scale.exe -service stop
plantops-edge-scale.exe -service uninstall
```

Service configuration:

```text
Automatic + Delayed Auto Start
restart after failure: 5s / 15s / 60s
SCM Stop / Windows shutdown -> SafeState -> HTTP shutdown -> SQLite checkpoint
```

See `docs/WINDOWS-SERVICE.md`.

## Central sync

Central is optional and asynchronous.

```text
-central-ticket-url https://central.example/.../tickets
-central-heartbeat-url https://central.example/.../heartbeat
-central-token <secret>
-central-sync-interval 5s
-central-heartbeat-interval 30s
-central-timeout 5s
```

A completed ticket is already locally durable before Central delivery is attempted. Network failure leaves it in `sync_queue` with bounded exponential retry. Bearer secrets are never returned by status API or audit.

See `docs/CENTRAL-SYNC.md`.

## Main APIs

```text
GET  /healthz
GET  /api/workflow
GET  /api/queue
POST /api/queue/{cycleID}/call
GET  /api/tickets/latest
GET  /api/scale/status
GET  /api/io/status
GET  /api/central/status
GET  /api/storage/status
GET  /api/identity
GET  /api/audit/weights?limit=200
GET  /api/audit/weights/verify
GET  /api/audit/events?limit=200
GET  /api/audit/events/verify
POST /io/rfid
POST /io/lpr
POST /io/camera/{cameraID}
```

`/sim/*` is present only with explicit `-simulation`.

## Evidence path

```text
scale controller bytes
-> raw-weight fsync + seq/hash
-> AuditedScaleReading
-> physical pass stable acceptance
-> WeighPass

PASS1 A->B -> QUEUED
PASS2 B->A + exact CALLED cycle
-> pair validation
-> SQLite atomic COMPLETE
   ├── first + second raw refs
   ├── gross / tare / net
   ├── RFID/LPR
   ├── C1A/C1B/C3 evidence refs
   ├── overrides
   └── pending Central sync item
-> local physical release
-> asynchronous Central ACK
```

## Code map

```text
cmd/edge/main.go
internal/domain/          workflow / pair / audit / evidence contracts
internal/engine/          proven physical-pass state machine
internal/twopass/         bidirectional normalization + commit bridge
internal/cycle/           durable queue/call/pair coordinator
internal/workflowaudit/   synchronous operational audit recorder
internal/runtimeio/       safe-start Modbus reconciler + output audit gate
internal/centralsync/     offline retry + ACK + heartbeat
internal/httpapi/         embedded API + operator UI
internal/winservice/      native SCM host

internal/adapters/
  scaleascii/             persistent scale TCP transport/parser boundary
  rawjournal/             all-frame raw-weight SHA-256 chain
  auditjournal/           operational SHA-256 chain
  sqlitestore/            edge.db cycles/passes/tickets/sync queue
  modbustcp/              dependency-free Modbus TCP DI/coil adapter
  ingress/                RFID/LPR normalized ingress
  registry/               bootstrap RFID -> plate map
```

## Windows CI acceptance

GitHub-hosted Windows runners verify:

```text
go test ./...
go vet ./...
single Windows EXE build
A->B 28,460 kg -> QUEUED, ticket count still 0
exact CALL
B->A 11,820 kg -> net 16,640 kg -> COMPLETE
SQLite restart/recovery
raw + operational hash-chain integrity
bidirectional Modbus simulator
barrier request -> OPEN feedback -> GREEN
permissive outputs off after completion
Chromium executes embedded operator UI
native Windows SCM install/start/health/stop/restart/uninstall
Central HTTP ACK / offline retry tests
camera C1A/C1B/C3 evidence aggregation tests
artifact SHA-256
```

## Phase status

Software phases that can be completed without site-specific hardware are now implemented:

```text
S0  C# behavioral reference / migration baseline             DONE
S1  Go single-binary architecture + hardware ports           DONE
S2  all-frame raw weight + operational audit                 DONE
S3  deterministic safety workflow + Modbus reconciliation    DONE
S4  pure-Go SQLite durability / recovery                     DONE
S5  embedded animated operator UI                            DONE
S6  authoritative A->B / B->A two-pass queue/call/net        DONE
S7  native Windows Service lifecycle                         DONE IN CODE + CI GATE
S8  C1A/C1B/C3 generic camera evidence contract              DONE IN CODE + CI GATE
S9  offline Central sync + heartbeat generic adapter         DONE IN CODE + CI GATE
```

The following are **external commissioning gates**, not unfinished generic software:

```text
H1  exact real scale-controller protocol parser              BLOCKED: controller manual/raw frames
H2  real remote-I/O / barriers / sensors bench               BLOCKED: physical hardware
H3  real RFID + C1A/C1B/C3 vendor integration                BLOCKED: device/API details
H4  camera/NVR/media object retention + integrity policy     BLOCKED: deployment storage choice
SITE shadow mode with live traffic                            SITE ACCEPTANCE
SITE supervised output enablement                             SITE ACCEPTANCE
SITE Go primary / retire legacy runtime                       SITE CUTOVER
```

Those gates must not be reported as completed from simulators. The repository is intended to reach them without another architectural rewrite.
