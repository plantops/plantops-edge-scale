# Per-component hardware configuration

Every component of the site is configured on its own, and any of them can be real
or simulated. The usual bench setup is **a real Laumas indicator with everything
else simulated**: `config/bench-real-scale.json`.

```text
edge.exe -config config/bench-real-scale.json
edge.exe -config config/bench-real-scale.json -scale-serial COM5     # a flag overrides the file
```

A JSON file is read with `-config`; any flag given explicitly on the command line
overrides the same setting in the file. The file is parsed strictly: an unknown or
misspelt key is an error, not a silent default. Inconsistent hardware settings
(for example `laumas-rtu` without a port, or a Modbus I/O without an address) are
refused at start-up, not at the first truck.

## Components and adapters

| Component | `adapter` | Meaning |
|---|---|---|
| `scale` | `laumas-rtu` | real Laumas W100/W200, Modbus RTU: `serial` (COM port) **or** `addr` (serial gateway). See `LAUMAS-RTU.md` |
| | `ascii` | TCP line stream (placeholder protocol) |
| | `fake` | simulated indicator inside the edge; weight set from the bench panel |
| | `none` | no scale |
| `io` | `modbustcp` | real remote I/O module: presence sensors, lamps, buzzer, barriers |
| | `fake` | simulated I/O inside the edge; the real runtime I/O controller still runs on top of it |
| | `none` | no I/O |
| `rfid`, `lpr`, `cameras` | `webhook` | the real device POSTs to `/io/rfid`, `/io/lpr`, `/io/camera/{id}` |
| | `fake` | simulated; the bench panel posts the same requests |

Defaults when `adapter` is omitted: `scale` is `ascii` if an address is given,
otherwise `none`; `io` is `modbustcp` if an address is given, otherwise `none`;
`rfid`, `lpr` and `cameras` are `webhook`. So every existing command line keeps
working unchanged.

## Why "fake" is loud

A simulated component is declared everywhere, so a bench run can never be mistaken
for production:

* `GET /api/hardware` returns `mode` (`LIVE`, `MIXED` or `SIMULATED`), the list of
  simulated components and each component's adapter.
* A `HARDWARE_PROFILE` event is the first thing written to the hash-chained audit
  journal at every start.
* The log prints a warning naming the simulated and the real components.
* **Tickets are not delivered to Central while any component is simulated.** They
  stay pending and `/api/central/status` shows `ticket_sync_blocked` with the
  reason. Heartbeats still go out and carry the hardware profile. Override with
  `-central-allow-simulated` (or `central.allow_simulated`), which is what the
  fleet simulator does when it talks to its mock Central.
* Simulated scale frames go through the raw-weight journal like real ones, with
  source `FAKE-SCALE`, so the audit chain stays intact and simulated frames are
  recognisable in it.

## Bench API (only for components set to `fake`)

```text
GET  /api/fake/io       {inputs, outputs, entry_barrier, exit_barrier}
POST /api/fake/io       {"entry_present":true, "front_present":..., "rear_present":..., "exit_present":..., "safety_clear":...}
GET  /api/fake/scale    {weight_kg, stable, moving, overload, fault, frames}
POST /api/fake/scale    {"weight_kg":24000, "moving":false, "overload":false, "fault":""}
```

Only the fields present are changed; unknown fields are rejected. A weight change
makes the simulated indicator read *not stable* until `settle_ms` has passed, like
the real one. The simulated barriers take `barrier_delay_ms` to travel and report
neither OPEN nor CLOSED meanwhile. RFID and plate reads for a simulated reader or
camera are the ordinary `POST /io/rfid` and `POST /io/lpr`.

## Presets

| File | Scale | I/O, RFID, plate camera, cameras |
|---|---|---|
| `config.example.json` | real Laumas (COM3) | real (Modbus TCP I/O, webhooks) |
| `config/bench-real-scale.json` | **real Laumas (COM3)** | **simulated** |
| `config/all-fake.json` | simulated | simulated |

Run the simulated fleet against an all-fake edge with `fleetsim -bench`.
