# W100 bench: real indicator, simulated load cell

Scale input from a LAUMAS W100 over Modbus RTU, before the IND760 or the real deck.
Barriers, lights and loops stay simulated (`-simulation`, `/sim/position`) until an ADAM-6050 is on the bench.

## Wiring

| From | To W100 terminal |
|---|---|
| 24 VDC + / − | 1 / 2 |
| USB-RS485 A / B | 18 / 17 (if no answer, swap A and B) |
| USB-RS485 GND | 2 |
| Unipulse 513B | as already wired for the SP01 test (W100: EX+ 20, EX− 19, REF+ 21, REF− 22, SIG+ 24, SIG− 23) |

Terminal numbers are the ones read off the SP01 W100 label.

## W100 settings

| Menu item | Value | Why |
|---|---|---|
| Communication | MODBUS RTU | default is **disabled** |
| Address | 1 | `-scale-unit-id` |
| Baud | 9600 | `-scale-baud` |
| Parity / stop | none / 1 | `-scale-parity N -scale-stop-bits 1` |
| Delay | 0 ms | the driver waits 300 ms for an answer |
| Unit | kg | t and g also work; other units are refused |
| Display | GROSS | tickets always use gross; a keypad tare is ignored |

For a deck-like range, calibrate the W100 to the bench's 513B mV/V output with a deck full scale (e.g. 60 000 kg, division 20 kg).
Then 513B steps give truck weights.

## 1. Probe (wiring check, nothing recorded)

```
plantops-edge-scale.exe -probe -scale-driver w100 -scale-port COM5
```

Expect one line every 0.5 s: `W100 status=0x0800 gross_kg=… stable=true …`.
`no answer` → COM port, A/B swapped, baud/parity, address, or Modbus not enabled.

## 2. Run the lane

```
plantops-edge-scale.exe -scale-driver w100 -scale-port COM5 -simulation ^
  -vehicle-map RFID-DEMO-001=15C-123.45 -empty-scale-max-kg 200 -pair-min-net-kg 1000
```

Open `http://127.0.0.1:8080/`. Drive a pass with `/sim/position`, `/io/rfid`, `/io/lpr` (same calls as the CI acceptance step).

## 3. Bench tests

| Do on the 513B / line | Expect |
|---|---|
| 0 mV/V | stable zero; entry allowed |
| Step to ~28 t, let it settle | pass 1 gross accepted, cycle QUEUED |
| Back to 0, CALL, step to ~12 t | pass 2 tare, NET, ticket COMPLETE |
| Change value during weighing | not stable → no acceptance |
| Over full scale | `overload=true` → no acceptance |
| Pull the RS485 cable | after 3 missed answers: TRANSPORT_ERROR journaled, scale fault, lane blocked; reconnects by itself |
| Pull the 513B cable | W100 cell error → `fault=CELL_ERROR` |
| Tare on the W100 keypad | ticket weight unchanged (gross) |
| `/api/audit/weights/verify` | `verified: true` |

## Raw journal

Every answer the driver uses is journaled as exact bytes (`raw_base64`) plus a readable line, before the workflow sees it.
An answer identical to the last one is journaled at most once per `-scale-repeat-every` (default 1 s) and is not delivered in between.
Any change is journaled and delivered at once. `-scale-repeat-every 0` journals every poll (5 per second by default).

Without hardware: `go run ./.github/testdata/w100-sim.go` and `-scale-port tcp:127.0.0.1:19003`.
