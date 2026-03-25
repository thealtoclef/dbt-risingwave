# dbt-risingwave

A [RisingWave](https://github.com/risingwavelabs/risingwave) adapter plugin for [dbt](https://www.getdbt.com/).

RisingWave is a cloud-native streaming database that uses SQL as the interface language. It is designed to reduce the complexity and cost of building real-time applications. See <https://www.risingwave.com>.

dbt enables data analysts and engineers to transform data using software engineering workflows. For the broader RisingWave integration guide, see <https://docs.risingwave.com/integrations/other/dbt>.

## Getting Started

1. Install `dbt-risingwave`.

```shell
python3 -m pip install dbt-risingwave
```

2. Get RisingWave running by following the official guide: <https://www.risingwave.dev/docs/current/get-started/>.

3. Configure `~/.dbt/profiles.yml`.

```yaml
default:
  outputs:
    dev:
      type: risingwave
      host: 127.0.0.1
      user: root
      pass: ""
      dbname: dev
      port: 4566
      schema: public
  target: dev
```

4. Run `dbt debug` to verify the connection.

## Common Features

Detailed reference: [`docs/`](docs/README.md).

### Schema Authorization

Use `schema_authorization` when dbt should create schemas with a specific owner:

```sql
{{ config(materialized='table', schema_authorization='my_role') }}
```

See [docs/configuration.md](docs/configuration.md) for model-level and `dbt_project.yml` examples.

### Streaming Parallelism

The adapter supports RisingWave session settings such as `streaming_parallelism`, `streaming_parallelism_for_backfill`, and `streaming_max_parallelism` in both profiles and model configs.

See [docs/configuration.md](docs/configuration.md) for the full configuration matrix.

### Serverless Backfill

Use `enable_serverless_backfill=true` in a model config or profile to enable serverless backfills for streaming queries.

See [docs/configuration.md](docs/configuration.md) for examples.

### Background DDL

`background_ddl=true` lets supported materializations submit background DDL while still preserving dbt semantics by issuing RisingWave `WAIT` before dbt continues.

See [docs/configuration.md](docs/configuration.md) for supported materializations, examples, and the cluster-wide `WAIT` caveat.

### Zero-Downtime Rebuilds

`materialized_view` and `view` support swap-based zero-downtime rebuilds through `zero_downtime={'enabled': true}` plus the runtime flag `--vars 'zero_downtime: true'`.

See [docs/zero-downtime-rebuilds.md](docs/zero-downtime-rebuilds.md) for requirements, cleanup behavior, and helper commands.

### Indexes

RisingWave indexes support `INCLUDE` and `DISTRIBUTED BY` clauses beyond what the Postgres adapter exposes. Configure them in the model config:

```sql
{{ config(
    materialized='materialized_view',
    indexes=[
        {'columns': ['user_id'], 'include': ['name', 'email'], 'distributed_by': ['user_id']}
    ]
) }}
```

This generates:

```sql
CREATE INDEX IF NOT EXISTS "__dbt_index_mv_user_id"
  ON mv (user_id)
  INCLUDE (name, email)
  DISTRIBUTED BY (user_id);
```

| Option | Description |
| --- | --- |
| `columns` | Key columns for the index (required). |
| `include` | Additional columns stored in the index but not part of the key (optional). |
| `distributed_by` | Columns used to distribute the index across nodes (optional). |

Note: RisingWave does not support `unique` or `type` (index method) options from the Postgres adapter. These options are silently ignored.

### Iceberg Documentation Sync (Embedded Sink)

When an embedded sink targets an Iceberg connector, the adapter can sync model and column descriptions to the corresponding Iceberg table via [PyIceberg](https://py.iceberg.apache.org/).

Install the optional dependency for your storage backend:

```shell
pip install 'dbt-risingwave[iceberg-gcp]'   # Google Cloud / BigLake
pip install 'dbt-risingwave[iceberg-aws]'   # AWS / Glue / S3
pip install 'dbt-risingwave[iceberg-azure]' # Azure / ADLS
pip install 'dbt-risingwave[iceberg]'       # generic (REST, Hive, SQL catalogs)
```

**Important**: Iceberg doc sync requires `iceberg_persist_docs: true` **and at least one of** `persist_docs.relation: true` **or** `persist_docs.columns: true`. The `iceberg_persist_docs` flag enables the Iceberg sync, while `persist_docs` controls which descriptions are synced:
- `persist_docs.relation: true` — syncs the model description to the Iceberg table comment
- `persist_docs.columns: true` — syncs column descriptions to Iceberg field-level docs
- Set either one or both — only the enabled sync types will be applied

Enable per model by setting `iceberg_persist_docs: true` in the `embedded_sink` config with a `connection_ref` pointing to a `connection` model. The catalog connection properties are automatically derived from the connection model's `connector_properties` — no separate catalog config needed:

```yaml
models:
  - name: my_iceberg_model
    description: "Sales fact table"
    config:
      persist_docs:
        relation: true
        columns: true
      embedded_sink:
        enabled: true
        iceberg_persist_docs: true
        connection_ref: conn_iceberg
        with_properties:
          database.name: prod_iceberg
          table.name: sales_facts
    columns:
      - name: order_id
        description: "Unique order identifier"
```

Sync behaves the same as standard dbt `persist_docs` — if any operation fails, the model run fails with an error.

See [docs/configuration.md](docs/configuration.md) for the full reference.

## Materializations

The adapter follows standard dbt model workflows, with RisingWave-specific materializations and behaviors.

Typical usage:

```sql
{{ config(materialized='materialized_view') }}

select *
from {{ ref('events') }}
```

| Materialization | Notes |
| --- | --- |
| `materialized_view` | Creates a materialized view. This is the main streaming materialization for RisingWave. |
| `materializedview` | Deprecated. Kept only for backward compatibility. Use `materialized_view` instead. |
| `ephemeral` | Uses common table expressions under the hood. |
| `table` | Creates a table from the model query. |
| `view` | Creates a view from the model query. |
| `incremental` | Batch-style incremental updates for tables. Prefer `materialized_view` when a streaming MV fits the workload. |
| `connection` | Runs a full `CREATE CONNECTION` statement supplied by the model SQL. |
| `source` | Runs a full `CREATE SOURCE` statement supplied by the model SQL. |
| `table_with_connector` | Runs a full `CREATE TABLE ... WITH (...)` statement supplied by the model SQL. |
| `sink` | Creates a sink, either from adapter configs or from a full SQL statement. |

See [docs/configuration.md](docs/configuration.md) for adapter-specific configuration examples, including streaming session settings and background DDL.

## Documentation

- [docs/README.md](docs/README.md): documentation index
- [docs/configuration.md](docs/configuration.md): profile options, model configs, sink settings, and background DDL usage
- [docs/zero-downtime-rebuilds.md](docs/zero-downtime-rebuilds.md): zero-downtime rebuild behavior for materialized views and views

## dbt Run Behavior

- `dbt run`: creates models that do not already exist.
- `dbt run --full-refresh`: drops and recreates models so the deployed objects match the current dbt definitions.

## Graph Operators

[Graph operators](https://docs.getdbt.com/reference/node-selection/graph-operators) are useful when you want to rebuild only part of a project.

## Data Tests

`dbt-risingwave` extends dbt data-test failure storage to support `materialized_view` in addition to the upstream `table` and `view` options.

Example:

```yaml
models:
  - name: my_model
    columns:
      - name: id
        tests:
          - not_null:
              config:
                store_failures: true
                store_failures_as: materialized_view
```

This is useful for realtime monitoring workflows where test failures should remain continuously queryable as a RisingWave materialized view.

```sh
dbt run --select "my_model+"   # select my_model and all children
dbt run --select "+my_model"   # select my_model and all parents
dbt run --select "+my_model+"  # select my_model, and all of its parents and children
```

## Examples

- Official dbt example: [jaffle_shop](https://github.com/dbt-labs/jaffle_shop)
- RisingWave example: [dbt_rw_nexmark](https://github.com/risingwavelabs/dbt_rw_nexmark)
