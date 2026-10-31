# AML BQ2: Laundering Exposure

Where money laundering concentrates in the IBM AML HI-Small transfers, and how each laundering scheme moves money. Also covers the monitor that replays those transfers slice by slice, and the Helical dashboard that shows both.

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
_Avoid_: quota (the budget only places the alert threshold; it doesn't cap alerts per slice)

**Alert threshold**:
The fixed risk score above which a transfer is flagged, placed so that roughly the alert budget is flagged on a normal day. An unusual slice can flag far more or fewer than the budget.
_Avoid_: cut-off, line

**Detection rate**:
The share of a scheme's laundering transfers that fall inside the alert budget.
_Avoid_: recall per scheme, catch rate

**Risk lift**:
A group's share of the summed risk score divided by its share of transfers. Above 1 means the group carries more risk than its volume suggests.

### Monitoring

**Slice**:
One simulated day of transfers. The monitor replays them one at a time, 1–10 Sep plus 11 Sep from the tail.
_Avoid_: batch, hour (slices were hourly until 26 Oct)

**Check**:
One of four tests a slice must pass: recon, quality, drift or anomaly. Each result is pass or fail; there is no warning level.

**Normal day**:
A day the monitor compares slices against: the weekdays 2 and 5–9 Sep, and the weekend days 3–4 Sep. A slice is compared only with the other normal days of its kind, never with its own day.
_Avoid_: baseline day, reference day

**Gate**:
The rule that accepts a slice only if every check passes. It is code, never the agent.

**Quarantine**:
Where a slice that failed the gate goes, so it never reaches the dashboard.
_Avoid_: reject, drop

**Triage note**:
The agent's short explanation of a failed slice: severity, verdict (expected, data issue or investigate), summary and suggested action. A slice that passes every check gets no note.
_Avoid_: AI summary, report

**Snapshot**:
The local copy of the S3 data, result tables, checks and triage notes that the dashboard reads. S3 stays the source of truth.
_Avoid_: cache, extract
