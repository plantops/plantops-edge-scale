# Fleet simulator

`cmd/fleetsim` runs a whole simulated weighbridge site against the **real edge
binary**, with no scale, RFID reader, camera, remote I/O or barrier hardware. It
exists to test the workflow and the adapters under realistic traffic, and to
produce efficiency figures in the plant's own terms.

> Simulator results are **not** commissioning evidence. Gates H1-H4 stay open
> until they are closed on site (see the repository README).

## What is simulated

```text
                  +-----------------------------+
 fleetsim         | Plant: trucks, queue, dispatcher
                  |   Laumas RTU indicator  ---- TCP ----> edge (-scale-protocol laumas-rtu)
                  |   Modbus TCP I/O slave  ---- TCP ----> edge (sensors, lamps, barriers)
                  |   RFID / LPR            ---- HTTP ---> edge POST /io/rfid, /io/lpr
                  |   Dispatcher            ---- HTTP ---> edge GET /api/queue, POST /api/queue/{id}/call
                  |   Mock Central          <--- HTTP ---- edge ticket + heartbeat sync
                  +-----------------------------+
```

* **Trucks** each have a plate, an RFID tag and a tare. Arrivals are a Poisson
  process sized from `-turns-per-day` over `-work-hours` (450 turns over 06:00-22:00
  by default). A turn is either `MATERIAL_IN` (loaded on A->B, empty back) or
  `MATERIAL_OUT` (empty on A->B, loaded back, a *tare-first* turn); `-inbound-share`
  sets the mix. Some "empty" weighings carry a small remainder.
* **One lane, one physical transaction at a time**, as in the edge. Trucks queue on
  the A side; a truck that has finished at the store waits to be CALLed. The
  dispatcher serves whichever side has waited longest, and CALLs a returning truck
  only when the scale is free, because the B-side approach sensor is also the
  B-side exit sensor.
* Each pass follows the real sensor sequence: approach sensor, identification,
  wait for green lamp + barrier request, drive on (front, rear sensors, weight
  ramp, settle), wait for the release outputs, drive off (exit sensor), wait for
  the edge to return to idle. The sim only moves when the lamp and barrier
  outputs it reads from the Modbus coils allow it.
* **Fault injection** (on by default, `-faults=false` to disable): RFID not read on
  the first try, plate misread (the edge must reach `IDENTITY_MISMATCH`, then
  recover when the camera re-reads), and dropped indicator polls.

## Running

```text
go build -o edge.exe ./cmd/edge
go build -o fleetsim.exe ./cmd/fleetsim
fleetsim.exe -edge edge.exe -turns 40 -speed 12 -fleet 12
```

`-speed N` plays N simulated seconds per real second. Sensor holds are floored at
three poll intervals so the edge can always see them; at high speed this makes
passes proportionally longer than real, so **scale utilisation and throughput
read pessimistically above roughly x10**. Use `-speed 1` for faithful timing.
`-seed` makes a run reproducible. `-data-dir` keeps the edge database and both
audit journals for inspection. `-edge-log` captures the edge output.

Exit status: `0` all checks passed, `1` a check failed, `2` the run could not start.

## What it checks

Every completed turn is compared with the simulator's ground truth:

* the ticket's net, gross, tare and `flow` equal the true values;
* the second weighing is after the first and both carry raw-weight audit references;
* each completed turn produced exactly one ticket at Central (no loss, no duplicate);
* the two lamps were never green together and the two barriers were never
  requested open together;
* both hash-chained audit journals verify.

The report prints tons per simulated hour, first-to-second-weighing cycle time,
queue waits on both sides and scale-zone occupancy.

## Findings so far

The simulator found two real defects in the existing workflow that unit tests and
the scripted CI simulator did not reach. Both are fixed on this branch with
regression tests:

1. **Entry re-authorised after release** (`internal/engine`). The edge calls
   `ClearFault(SCALE)` on every healthy scale frame, which re-evaluated identity
   and re-ran entry authorisation with no state check. As soon as the truck left
   the deck and the scale read empty, a transaction in `EXIT_AUTHORIZED` /
   `EXITING` went back to `ENTRY_AUTHORIZED` and the entry lamp + barrier were
   requested open while the exit side was still open (both lamps green, both
   barriers requested open).
2. **Second-pass release withdrawn after commit** (`internal/twopass`). After the
   B->A commit the cycle is `COMPLETE`, no longer `CALLED`; the next position
   update re-resolved the binding, found nothing and unbound it. The snapshot
   became `QUEUE_MISMATCH` / `UNPAIRED_RETURN` and the release outputs were
   withdrawn: the final ticket existed but the truck was never let out.

## Limits

* It simulates the *site*, not hardware timing or electrical behaviour. Real
  RS485 turn-around, relay and barrier delays, sensor chatter, camera latency and
  RFID collisions are not modelled.
* One physical lane and one scale, matching the edge.
* The trucks obey the lamps. A driver who ignores a red lamp is not simulated.
* The remote I/O address map is the documented default plus `safety_clear=8`.
* A run takes real time: about 5-6 minutes for 30 turns at `-speed 12`.
