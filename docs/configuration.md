# Configuration

This page documents the adapter-specific settings supported by `dbt-risingwave`.

## Profile Configuration

The basic dbt profile looks like this:

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

The adapter also supports several RisingWave session settings directly in the profile. When these are present, `dbt-risingwave` issues the corresponding `SET` statements as soon as the connection opens.

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
      streaming_parallelism: 2
      streaming_parallelism_for_backfill: 2
      streaming_max_parallelism: 8
      enable_serverless_backfill: true
  target: dev
```

Supported adapter-specific profile keys:

| Key | Description |
| --- | --- |
| `streaming_parallelism` | Sets `SET streaming_parallelism = ...` for the session. |
| `streaming_parallelism_for_backfill` | Sets `SET streaming_parallelism_for_backfill = ...` for the session. |
| `streaming_max_parallelism` | Sets `SET streaming_max_parallelism = ...` for the session. |
| `enable_serverless_backfill` | Sets `SET enable_serverless_backfill = true/false` for the session. |

## Model Configuration

The adapter also supports RisingWave-specific model configs. These can be set in `config(...)` blocks or in `dbt_project.yml`.

### Schema Authorization

Use `schema_authorization` to set the owner of schemas created by dbt:

```sql
{{ config(materialized='table', schema_authorization='my_role') }}

select *
from ...
```

Or globally:

```yaml
models:
  my_project:
    +schema_authorization: my_role
```

Generated SQL:

```sql
create schema if not exists <schema_name> authorization "my_role"
```

### SQL Header

Use `sql_header` to prepend custom SQL before the main statement:

```sql
{{ config(
    materialized='table',
    sql_header='set query_mode = local;'
) }}

select *
from ...
```

The adapter appends its own RisingWave session settings after the custom header when those configs are present.

### Streaming Parallelism Per Model

You can override the session-level streaming settings for an individual model:

```sql
{{ config(
    materialized='materialized_view',
    streaming_parallelism=2,
    streaming_parallelism_for_backfill=2,
    streaming_max_parallelism=8
) }}

select *
from {{ ref('events') }}
```

These values are emitted in the SQL header before the model DDL runs.

### Serverless Backfill

Use `enable_serverless_backfill` to enable serverless backfills for streaming queries on a per-model basis:

```sql
{{ config(
    materialized='materialized_view',
    enable_serverless_backfill=true
) }}

select *
from {{ ref('events') }}
```

Or set it globally in `dbt_project.yml`:

```yaml
models:
  my_project:
    +enable_serverless_backfill: true
```

This emits `set enable_serverless_backfill = true;` before the model DDL runs.

### Background DDL

`dbt-risingwave` supports opting into RisingWave background DDL for these paths:

- `materialized_view`
- `table`
- `sink`
- index creation triggered by model `indexes` config

Enable it per model:

```sql
{{ config(
    materialized='materialized_view',
    background_ddl=true
) }}

select *
from {{ ref('events') }}
```

Or set it in `dbt_project.yml`:

```yaml
models:
  my_project:
    +background_ddl: true
```

How it works:

- The adapter sets `background_ddl = true` before running supported DDL.
- After submitting the DDL, the adapter issues RisingWave `WAIT`.
- dbt does not continue to downstream models, hooks, or tests until `WAIT` returns.

Caveat:

- RisingWave `WAIT` waits for all background creating jobs, not only the job started by the current dbt model. If other background DDL is running in the same cluster, the dbt node may wait on that work too.

### Index Configuration Changes

`materialized_view`, `table`, and `table_with_connector` support dbt's `on_configuration_change` behavior for index changes.

```sql
{{ config(
    materialized='materialized_view',
    indexes=[{'columns': ['user_id']}],
    on_configuration_change='apply'
) }}

select *
from {{ ref('events') }}
```

Supported values:

| Value | Behavior |
| --- | --- |
| `apply` | Apply index configuration changes. |
| `continue` | Keep going and emit a warning. |
| `fail` | Stop the run with an error. |

### Zero-Downtime Rebuilds

`materialized_view` and `view` support swap-based zero-downtime rebuilds.

```sql
{{ config(
    materialized='materialized_view',
    zero_downtime={'enabled': true}
) }}

select *
from {{ ref('events') }}
```

At runtime, enable the behavior with:

```bash
dbt run --vars 'zero_downtime: true'
```

For full details, see [zero-downtime-rebuilds.md](zero-downtime-rebuilds.md).

## Sink Configuration

The `sink` materialization supports two usage patterns.

### Adapter-Managed Sink DDL

Provide connector settings in model config and let the adapter build the `CREATE SINK` statement:

```sql
{{ config(
    materialized='sink',
    connector='kafka',
    connector_parameters={
      'topic': 'orders',
      'properties.bootstrap.server': '127.0.0.1:9092'
    },
    data_format='plain',
    data_encode='json',
    format_parameters={}
) }}

select *
from {{ ref('orders_mv') }}
```

Supported sink-specific configs:

| Key | Required | Description |
| --- | --- | --- |
| `connector` | Yes | Connector name placed in `WITH (...)`. |
| `connector_parameters` | Yes | Connector properties emitted into `WITH (...)`. |
| `data_format` | No | Sink format used in `FORMAT ...`. |
| `data_encode` | No | Sink encoding used in `ENCODE ...`. |
| `format_parameters` | No | Extra format/encode options emitted inside `FORMAT ... ENCODE ... (...)`. |

### Raw SQL Sink DDL

If `connector` is omitted, the adapter runs the SQL in the model as-is. This is useful when you want full control over the sink statement:

```sql
{{ config(materialized='sink') }}

create sink my_sink
from my_mv
with (
  connector = 'blackhole'
)
```

## Iceberg Documentation Sync (Embedded Sink)

When an embedded sink targets an Iceberg connector and `iceberg_persist_docs` is enabled, `dbt-risingwave` can sync model and column descriptions to the corresponding Iceberg table metadata via [PyIceberg](https://py.iceberg.apache.org/).

This is separate from dbt's normal `persist_docs`, which writes `COMMENT ON` to RisingWave. Iceberg doc sync writes to the Iceberg catalog that the embedded sink writes to.

### Installation

PyIceberg is an optional dependency. Install it with the appropriate extra for your catalog's storage backend:

| Extra | Use case |
| --- | --- |
| `iceberg` | Base install — REST, Hive, SQL catalogs with local or generic storage |
| `iceberg-gcp` | Google Cloud (BigLake REST catalog, GCS storage, `google-auth`) |
| `iceberg-aws` | AWS (Glue, S3 storage via s3fs) |
| `iceberg-azure` | Azure (ADLS storage via adlfs) |

```shell
# Google Cloud / BigLake
pip install 'dbt-risingwave[iceberg-gcp]'

# AWS / Glue
pip install 'dbt-risingwave[iceberg-aws]'

# Azure
pip install 'dbt-risingwave[iceberg-azure]'

# Generic (REST, Hive, SQL catalogs without cloud-specific storage)
pip install 'dbt-risingwave[iceberg]'
```

### Catalog Configuration (Zero Extra Config)

The adapter automatically derives PyIceberg catalog connection properties from the `connection_ref` model's `connector_properties`. No separate catalog config is needed — the same properties you already define for the RisingWave connection are reused for PyIceberg.

The following RisingWave connector properties are automatically mapped to PyIceberg equivalents:

| RisingWave `connector_properties` | PyIceberg catalog property | Notes |
| --- | --- | --- |
| **Catalog core** | | |
| `catalog.type` | `type` | |
| `catalog.name` | `name` | |
| `catalog.uri` | `uri` | |
| `warehouse.path` | `warehouse` | |
| **REST catalog – auth** | | |
| `catalog.credential` | `credential` | Client credentials (`client_id:secret`) |
| `catalog.token` | `token` | Bearer token |
| `catalog.oauth2_server_uri` | `oauth2-server-uri` | OAuth2 token endpoint |
| `catalog.scope` | `scope` | OAuth2 scope |
| **REST catalog – SigV4** | | |
| `catalog.rest.signing_region` | `rest.signing-region` | AWS SigV4 signing region |
| `catalog.rest.signing_name` | `rest.signing-name` | AWS SigV4 service name |
| **REST catalog – headers** | | |
| `catalog.header` | `header.*` (expanded) | Semicolon-delimited `key=val` pairs expanded into individual `header.key` properties |
| **AWS S3** | | |
| `s3.region` | `s3.region` | |
| `s3.endpoint` | `s3.endpoint` | |
| `s3.access.key` | `s3.access-key-id` | |
| `s3.secret.key` | `s3.secret-access-key` | |
| `s3.path.style.access` | `s3.force-virtual-addressing` | Value inverted: `true` → `False` |
| **Azure Blob / ADLS** | | |
| `azblob.account_name` | `adls.account-name` | |
| `azblob.account_key` | `adls.account-key` | |
| `azblob.endpoint_url` | `adls.account-host` | |
| **GCS** | | |
| `gcs.credential` | `gcs.oauth2.token` | |
| **Authentication** | | |
| `catalog.security` | `auth.type` | e.g. `'google'` → `'google'` |

The following RisingWave properties have no PyIceberg equivalent and are **skipped**: `catalog.io_impl`, `namespace.properties`, `table.properties`, `enable_config_load`, `catalog.jdbc.user`, `catalog.jdbc.password`.

### Connection Model

Define your Iceberg connection as a `connection` materialization model using `connector` and `connector_properties` in the config block:

```sql
-- models/conn_iceberg.sql
{{ config(
    materialized='connection',
    connector='iceberg',
    connector_properties={
        'catalog.type': 'rest',
        'catalog.uri': 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
        'catalog.security': 'google',
        'catalog.io_impl': 'org.apache.iceberg.gcp.gcs.GCSFileIO',
        'warehouse.path': 'bq://projects/my-project/locations/asia-southeast1',
    }
) }}

SELECT 1
```

The `connector` maps to the RisingWave connection `type`, and `connector_properties` become the `WITH (...)` properties. This follows the same pattern as the `sink` materialization.

### Enabling Iceberg Doc Sync

Set `iceberg_persist_docs: true` in the `embedded_sink` config with `connection_ref` pointing to the connection model name. The `connection_ref` is resolved via `ref()` to get the RisingWave connection relation, and its `connector_properties` are reused for PyIceberg:

**Important**: Iceberg doc sync requires `iceberg_persist_docs: true` **and at least one of** `persist_docs.relation: true` **or** `persist_docs.columns: true`. The `iceberg_persist_docs` flag enables the Iceberg sync, while `persist_docs` controls which descriptions are synced:
- `persist_docs.relation: true` — syncs the model description to the Iceberg table comment
- `persist_docs.columns: true` — syncs column descriptions to Iceberg field-level docs
- Set either one or both — only the enabled sync types will be applied

```yaml
# models/schema.yml
models:
  - name: my_iceberg_model
    description: "Sales fact table backed by Iceberg"
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

Or inline in the model file:

```sql
{{ config(
    materialized='materialized_view',
    persist_docs={'relation': true, 'columns': true},
    embedded_sink={
        'enabled': true,
        'iceberg_persist_docs': true,
        'connection_ref': 'conn_iceberg',
        'with_properties': {
            'database.name': 'prod_iceberg',
            'table.name': 'sales_facts',
        }
    }
) }}

select ...
```

| `embedded_sink` Key | Required | Description |
| --- | --- | --- |
| `iceberg_persist_docs` | Yes | Set to `true` to enable Iceberg doc sync for this model. |
| `connection_ref` | Yes | Name of a `connection` materialization model. Resolved via `ref()` for SQL; its `connector_properties` are reused for PyIceberg. |

The Iceberg table identity is derived from `with_properties`:

| `with_properties` Key | Required | Description |
| --- | --- | --- |
| `database.name` | Yes | The Iceberg namespace (database) containing the table. |
| `table.name` | Yes | The Iceberg table name. |

### How It Works

When an embedded sink is created for a model that has `iceberg_persist_docs: true` and `connection_ref` points to an iceberg connection:

1. The standard RisingWave `COMMENT ON TABLE` / `COMMENT ON COLUMN` SQL is applied via `persist_docs` (existing behavior, independent of this feature).
2. The adapter connects to the Iceberg catalog via PyIceberg.
3. The model description is written to the Iceberg table's `comment` property.
4. Column descriptions are written as field-level `doc` on the Iceberg schema.
5. Both changes are committed in a single atomic transaction.

The `persist_docs.relation` and `persist_docs.columns` flags are respected — setting `relation: false` skips the table-level comment, and `columns: false` skips column docs.

## Related dbt Configs

The adapter also works with standard dbt configs such as `indexes`, `contract`, `grants`, `unique_key`, and `on_schema_change`. Refer to the dbt docs for the generic semantics; this page focuses on RisingWave-specific behavior.
