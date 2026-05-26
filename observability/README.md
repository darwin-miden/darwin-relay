# darwin-relay observability

The `darwin_relay_v2` binary serves Prometheus-format metrics on
`GET /metrics`, on the same axum listener as the REST API.

Series exposed:

- `darwin_relay_up` — 1 while the service is alive
- `darwin_relay_intents_total{stage=...}` — gauge, intent rows by stage
- `darwin_relay_redemptions_total{stage=...}` — gauge, redemption rows by stage
- `darwin_relay_positions` — distinct (user, basket) positions tracked

Counts are pulled live from sqlite, so a restart does not zero them.

## Local stack

```bash
docker run -d --name darwin-prom \
  -p 9090:9090 \
  -v $(pwd)/prometheus.scrape.yml:/etc/prometheus/prometheus.yml:ro \
  prom/prometheus:latest

docker run -d --name darwin-grafana \
  -p 3000:3000 \
  --link darwin-prom \
  grafana/grafana:latest
```

In Grafana (admin/admin), add a Prometheus datasource at
`http://darwin-prom:9090`, then import `grafana-dashboard.json`.

## Why /metrics straight off the relay (no exporter)

The relay already has authoritative state in sqlite. A dedicated
exporter would have to either (a) re-implement the same queries, or
(b) read the same db file out-of-process and race with writes. Hand-
rolled text from the relay itself is the smallest reasonable surface
and exactly matches the data shape Grafana queries.
