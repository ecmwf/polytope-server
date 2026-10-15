<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Polytope Grafana dashboards

Dashboards for `polytope-server`, the broker/dispatcher, workers and BOBS, as
JSON in `dashboards/`. They are written to work against any Prometheus-compatible
datasource that scrapes Polytope; nothing in them is tied to a particular
Grafana instance, org or datasource UID.

## Using them

- **Grafana UI**: Dashboards → New → Import, upload a file, then pick your
  Prometheus datasource in the **Datasource** dropdown at the top of the dashboard.
- **File provisioning**: point a dashboard provider at `dashboards/`
  (see `dashboards.yaml` for the docker-compose dev stack).

The saved default of the **Datasource** variable is a site-specific name; if it
doesn't exist in your Grafana, just select yours (it is remembered per user).

## What the dashboards expect from your metrics

- The metric names exported by Polytope: `polytope_broker_*`,
  `polytope_worker_*` and `bobs_*`.
- Standard Prometheus scrape labels: `namespace`, `pod`, `job`.
  Per-instance panels group by `pod`.
- `cluster` is **optional**. If your series carry a `cluster` label (e.g. via
  Prometheus external labels in a multi-cluster setup) the **Cluster** variable
  lets you filter and split by it; if not, leave it on *All* — the filter is a
  no-op and the variable is otherwise inert.
- The **Pod Health** row on *Polytope Overview* (restarts, memory/CPU vs
  limits) additionally needs kube-state-metrics and cadvisor series with the
  kube-prometheus-stack label conventions (`job="kubelet"`,
  `metrics_path="/metrics/cadvisor"`, `job="kube-state-metrics"`). Without them
  those panels are simply empty.
