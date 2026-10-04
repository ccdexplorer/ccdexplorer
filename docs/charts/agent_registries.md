# Agent registries

Agents registered on CIS-8004 registry contracts, per day.

## Source

| | |
|---|---|
| Collection | `statistics`, type `statistics_agent_registry` |
| Field | `agents_registered` |

Counts registration events observed on contracts implementing CIS-8004. Each
document also lists the contracts involved that day.

Data begins on 27 May 2026, when the first such contract appeared. Ranges
before that are empty rather than zero.

## Aggregation

Registrations are events occurring during the period, so grouping by week or
month **sums** the daily counts.

A period covering fewer days than the rest holds a smaller total; such periods
are drawn faded and identified on hover.

## Distinctions

Counts registration events, not agents currently registered. An agent
registered and later removed is counted once, on the day it registered.

## Provenance

| | |
|---|---|
| Producer | Dagster `agent_registry` asset |
| Frequency | one document per day |
