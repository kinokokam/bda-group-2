# AML BQ2: Laundering Exposure

Where money laundering concentrates in the IBM AML HI-Small transfers, and how each laundering scheme moves money. Also covers the live monitor that replays those transfers slice by slice.

## Language

### Laundering

**Scheme**:
The shape a laundering attempt takes as money moves between accounts: fan-in, fan-out, gather-scatter, scatter-gather, stack, cycle, bipartite or random.
_Avoid_: pattern (only the column name), typology

**Laundering attempt**:
One run of a scheme: the group of transfers that the patterns file lists together.
_Avoid_: case, incident

**Unstructured laundering**:
A laundering transfer that belongs to no listed laundering attempt, so it has no scheme.
_Avoid_: unknown, other

**Tail**:
The 11–18 Sep transfers. They are simulator leftovers and mostly laundering, so every analysis excludes them. The live monitor uses them only as the quarantine example.

### Scoring

**Risk score**:
The model's estimate, from 0 to 1, that a transfer is laundering.

**Live score**:
A risk score built only from what was known when the transfer happened (Experiment A). The live monitor uses it.
_Avoid_: real-time model

**Analysis score**:
A risk score that also uses relay features, so it becomes available only 24–72 hours after the transfer (Experiment B). It answers BQ2.
_Avoid_: delayed model, network model

**Relay feature**:
A feature that looks ahead in time to see whether the receiver passed the money on, or sent it back to the sender.

**Out-of-day features**:
Account features for a transfer that are built from other days only, never from the transfer's own day.

### Evaluation

**Alert budget**:
The share of transfers that may be flagged: the top 0.5% by risk score (6,728 alerts on the test days).
_Avoid_: threshold (the score at the budget's edge), cut-off

**Detection rate**:
The share of a scheme's laundering transfers that fall inside the alert budget.
_Avoid_: recall per scheme, catch rate

**Risk lift**:
A group's share of the summed risk score divided by its share of transfers. Above 1 means the group carries more risk than its volume suggests.

**Slice**:
One simulated hour of transfers. The live monitor replays them one at a time.
