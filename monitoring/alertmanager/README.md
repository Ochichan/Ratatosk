# Ratatosk Alertmanager (single-node starter)

`alertmanager.yml` is a minimal, single-instance Alertmanager configuration for
Ratatosk. It routes the alert rules in
[`../prometheus/ratatosk-alerts.yml`](../prometheus/ratatosk-alerts.yml) to a
pager path or a ticketing path based on each alert's `severity` label.

This is a **starter config for one Alertmanager process — no clustering / HA.**
The receiver webhook URLs are **placeholders**; replace them with your real pager
and ticketing endpoints before relying on it.

## How it wires to the Prometheus rules

The rules in `../prometheus/ratatosk-alerts.yml` attach two labels that this
config keys off of:

| Label | Values | Effect here |
|---|---|---|
| `service`  | `ratatosk`        | scopes the route subtree to this service |
| `severity` | `page` / `ticket` | selects the `pager` vs `tickets` receiver |

Routing summary:

- Alerts are grouped by `['alertname', 'service']`.
- `severity = "page"` (e.g. `RatatoskAofWritesLatched`, `RatatoskAofWriteErrors`)
  -> `pager` receiver, short `group_wait`, frequent `repeat_interval`.
- `severity = "ticket"` (e.g. `RatatoskRdbSaveErrors`, `RatatoskFdUtilizationHigh`)
  -> `tickets` receiver, longer waits and repeat cadence.
- An **inhibit rule** lets a firing `page` suppress a `ticket` for the same
  `alertname` + `service`, so an actively-paged incident does not also file noise.

Point Prometheus at this Alertmanager in `prometheus.yml`:

```yaml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['127.0.0.1:9093']
rule_files:
  - ratatosk-alerts.yml
```

## Validate

Use Alertmanager's bundled `amtool` to check the config before loading it:

```bash
amtool check-config monitoring/alertmanager/alertmanager.yml
```

You can also dry-run routing for a given label set to confirm an alert lands on
the receiver you expect:

```bash
amtool config routes test \
  --config.file=monitoring/alertmanager/alertmanager.yml \
  service=ratatosk severity=page
```

Quick YAML sanity check without amtool installed:

```bash
python3 -c 'import yaml,sys; yaml.safe_load(open(sys.argv[1]))' \
  monitoring/alertmanager/alertmanager.yml
```

## See also

- Alert rules: [`../prometheus/ratatosk-alerts.yml`](../prometheus/ratatosk-alerts.yml)
- Objectives behind these alerts: [`../../docs/SLO.md`](../../docs/SLO.md)
