# Two-Pass Weighbridge Business Cycle

This document is the authoritative business-cycle model for PlantOps Edge Scale.

## Core invariant

One completed business transaction requires **two valid, paired weigh passes in opposite directions**.

```text
PASS #1  A -> B
  identify same vehicle
  capture audited stable first weight W1
  persist first pass
  open cycle
  add vehicle/cycle to queue
  release scale for other vehicles

QUEUE
  cycle remains OPEN/QUEUED while the physical weighbridge returns to IDLE
  many cycles may be queued at the same time

CALL
  explicitly select/call the correct queued vehicle/cycle

PASS #2  B -> A
  identify vehicle again
  must match the called open cycle
  capture audited stable second weight W2
  validate direction + identity + pair time + weights
  calculate NET
  atomically complete cycle
  remove/clear queue item
```

A single pass is **never** a completed business transaction.

## Separate long-lived cycle from short-lived physical pass

The old one-state-machine-one-transaction model is not sufficient. A truck can spend minutes or hours inside the site after its first weigh while the scale must continue processing other vehicles.

```text
Business layer

WeighCycle #101     QUEUED       truck 15C-111.11
WeighCycle #102     CALLED       truck 15C-222.22
WeighCycle #103     QUEUED       truck 15C-333.33

                       |
                       v

Physical scale layer

PassSession
IDLE -> IDENTIFY -> POSITION -> WEIGH -> RELEASE -> CLEAR -> IDLE
```

`PassSession` is ephemeral workflow state. `WeighCycle` is durable business state.

## Direction

Direction is part of every pass and is not inferred from weight.

```text
A_TO_B = mandatory first-pass direction
B_TO_A = mandatory second-pass direction
```

The two C1 cameras form a direction-aware identity group:

```text
C1-A  observes A side / A -> B approach
C1-B  observes B side / B -> A approach
C3    overview evidence across the weighbridge
C2    optional cab/driver evidence
C4    optional additional rear/overview evidence
```

Direction should be proven by configured physical approach evidence (sensor/camera trigger sequence). Weight must never be used as direction evidence.

## First pass

A valid first pass requires the normal physical/safety rules plus:

```text
direction == A_TO_B
identity accepted
position accepted
authoritative scale healthy
stable audited weight accepted
raw weight seq/hash available
```

On durable first-pass commit:

```text
create WeighCycle
status = QUEUED
first_pass = durable PassRecord
queue_entered_at = now
```

The physical pass can then release the truck and return the scale to IDLE. The business cycle remains open.

## Queue and call-right-truck rule

The queue contains durable open cycles, not loose vehicle names.

```text
queue item
  cycle_id
  vehicle identity / plate / RFID
  first weight + timestamp + raw audit ref
  queued_at
  status = QUEUED | CALLED
```

A B -> A return does not automatically become the second pass of whichever record looks similar. The vehicle must resolve to the **called open cycle**.

Recommended hard rule:

```text
second-pass identity
AND direction == B_TO_A
AND cycle.status == CALLED
AND called cycle identity matches
    -> candidate pair
else
    -> UNPAIRED_RETURN / WRONG_TRUCK
       business completion forbidden
```

Calling one truck does not clear other queued cycles.

## Pair validity

A pair is valid only when all configured constraints pass.

```text
same durable cycle
same vehicle identity
first direction  = A_TO_B
second direction = B_TO_A
second time > first time
pair elapsed time within configured valid window
both accepted weights come from authoritative audited stable frames
weight relationship valid for the configured operation
```

### Gross and tare follow the weights, not the pass order

The physical directions never change: pass 1 is always `A_TO_B` and pass 2 is always `B_TO_A`. What the two weights *mean* depends on the goods:

```text
MATERIAL_IN   W1 > W2   loaded on A -> B, emptied at the store, empty on B -> A
                        GROSS = W1, TARE = W2
MATERIAL_OUT  W1 < W2   empty on A -> B, loaded at the store, loaded on B -> A
                        GROSS = W2, TARE = W1

NET = GROSS - TARE = |W1 - W2|
```

A valid completion requires a positive net (`W1 != W2`). The result is recorded as `flow` on the cycle and on the final ticket (`MATERIAL_IN` / `MATERIAL_OUT`). Direction is never inferred from weight and the flow is never inferred from direction.

`gross` / `tare` limits in the pair policy apply to the heavier / lighter weight respectively, whichever pass produced it. A small remainder of goods left on a truck at its "empty" weighing is simply part of that weight; the net is the difference actually weighed.

The legacy inbound-only rule (the second pass must be lighter) is available as `-pair-require-gross-first`.

Minimum/maximum elapsed time and weight/net limits are configuration/business-master data; they must not be hidden constants in the state machine.

## Orphan rule

An orphan is evidence, not a completed transaction.

Examples:

```text
first pass exists but no valid second pass before pair window expires
B -> A vehicle arrives without a matching CALLED open cycle
identity mismatch between passes
invalid pair elapsed time
W1 == W2 (no net), or W2 > W1 when -pair-require-gross-first is set
weight outside configured validity limits
raw audit reference missing/invalid
```

The system must retain the pass and audit evidence, but:

```text
NO completed ticket
NO net quantity
NO Central completed-transaction sync
NO automatic queue clear
```

Use explicit statuses such as:

```text
ORPHANED_FIRST_PASS
UNPAIRED_RETURN
PAIR_TIME_INVALID
PAIR_WEIGHT_INVALID
WRONG_TRUCK
```

Supervisor handling may physically release a vehicle if site safety/procedure requires it, but such a release does **not** manufacture a completed business transaction.

## Completion durability boundary

Completion is one atomic local durability operation:

```text
persist second PassRecord
+ validate/record pair
+ calculate net
+ mark WeighCycle COMPLETE
+ create final completed Ticket
+ remove/close queue item
+ enqueue Central sync item
-------------------------------
ONE SQLite transaction
```

Only after that local commit may the second-pass release path become business-authorized.

Central/WAN is not part of local completion or truck release.

## Final ticket evidence

The final ticket references both accepted raw controller frames.

```text
Ticket
  cycle_id
  vehicle identity
  first_pass
    direction = A_TO_B
    weight_kg           (gross for MATERIAL_IN, tare for MATERIAL_OUT)
    observed_at
    raw_seq
    raw_hash
    camera evidence
  second_pass
    direction = B_TO_A
    weight_kg           (tare for MATERIAL_IN, gross for MATERIAL_OUT)
    observed_at
    raw_seq
    raw_hash
    camera evidence
  pair_elapsed
  gross_kg, tare_kg   (by role)
  flow = MATERIAL_IN | MATERIAL_OUT
  net_kg = gross_kg - tare_kg
  pair validation result
  overrides
  completed_at
```

Audit navigation must support:

```text
final ticket
  -> both pass records
  -> exact two accepted raw-weight frames
  -> full raw weight curves around both passes
  -> RFID/LPR/direction/camera evidence
  -> queue + call history
  -> overrides/faults/output commands
```
