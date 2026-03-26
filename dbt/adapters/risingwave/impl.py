import time
from typing import Any, Dict

from dbt.adapters.base.meta import available
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.postgres.impl import PostgresAdapter

from dbt.adapters.risingwave.connections import RisingWaveConnectionManager
from dbt.adapters.risingwave.relation import RisingWaveRelation


logger = AdapterLogger("RisingWave")


def _deep_merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dicts: override takes precedence, nested dicts are merged recursively."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(value, dict) and isinstance(result[key], dict):
            result[key] = _deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


# RisingWave-specific config keys that should be deep-merged rather than clobbered
# when users define them in the models: block of dbt_project.yml.
_RISINGWAVE_DEEP_MERGE_KEYS = frozenset(["embedded_sink", "embedded_sub"])


def _install_risingwave_config_deep_merge() -> None:
    """Patch dbt-core's config merge to deep-merge RisingWave-specific config keys.

    dbt-core's default behavior for unknown config keys (like embedded_sink, embedded_sub)
    is a shallow clobber: a model-level config() call replaces the entire project-level
    value, so nested dicts like with_properties are lost rather than merged.

    This patch intercepts BaseConfig.update_from to apply _deep_merge_dicts for
    embedded_sink and embedded_sub, enabling users to define shared defaults in the
    models: block of dbt_project.yml and override only specific keys per model:

        # dbt_project.yml
        models:
          my_project:
            +embedded_sink:
              enabled: true
              with_properties:
                connector: iceberg
                warehouse.path: s3://my-bucket/

        # model config (merges with_properties rather than replacing)
        {{ config(embedded_sink={'with_properties': {'table.name': 'my_table'}}) }}
    """
    try:
        from dbt_common.contracts.config import base as _config_base
    except ImportError:
        return

    _orig_update_from = _config_base.BaseConfig.update_from

    def _patched_update_from(self, data: dict, config_cls, validate: bool = True):
        # Stash RW keys before the original processes them (which would shallow-clobber them).
        rw_stash = {}
        for key in list(data):
            if key in _RISINGWAVE_DEEP_MERGE_KEYS:
                rw_stash[key] = data.pop(key)

        # Let dbt handle all other keys normally.
        result = _orig_update_from(self, data, config_cls, validate=validate)

        # Restore stashed keys so we don't permanently mutate the caller's dict.
        data.update(rw_stash)

        if not rw_stash:
            return result

        # Deep-merge the RW keys on top of what the original produced.
        result_dct = result.to_dict(omit_none=False)
        for key, new_val in rw_stash.items():
            existing = result_dct.get(key)
            if isinstance(existing, dict) and isinstance(new_val, dict):
                result_dct[key] = _deep_merge_dicts(existing, new_val)
            else:
                result_dct[key] = new_val
        return result.from_dict(result_dct)

    _config_base.BaseConfig.update_from = _patched_update_from


_install_risingwave_config_deep_merge()

# 1:1 mapping from RisingWave iceberg connection connector_properties to PyIceberg
# catalog properties.  Properties not in this map are handled by special-case
# logic in _rw_props_to_pyiceberg() or silently skipped (RisingWave-only props
# like catalog.io_impl, namespace.properties, table.properties, enable_config_load).
_RW_TO_PYICEBERG: Dict[str, str] = {
    # ── Catalog core ──────────────────────────────────────────────────────
    "catalog.type": "type",
    "catalog.name": "name",
    "catalog.uri": "uri",
    "warehouse.path": "warehouse",
    # ── REST catalog – auth / OAuth2 ──────────────────────────────────────
    "catalog.credential": "credential",
    "catalog.token": "token",
    "catalog.oauth2_server_uri": "oauth2-server-uri",
    "catalog.scope": "scope",
    # ── REST catalog – SigV4 signing ──────────────────────────────────────
    "catalog.rest.signing_region": "rest.signing-region",
    "catalog.rest.signing_name": "rest.signing-name",
    # ── AWS S3 ────────────────────────────────────────────────────────────
    "s3.region": "s3.region",
    "s3.endpoint": "s3.endpoint",
    "s3.access.key": "s3.access-key-id",
    "s3.secret.key": "s3.secret-access-key",
    # ── Azure Blob / ADLS (RW: azblob.* → PyIceberg: adls.*) ─────────────
    "azblob.account_name": "adls.account-name",
    "azblob.account_key": "adls.account-key",
    "azblob.endpoint_url": "adls.account-host",
    # ── GCS ────────────────────────────────────────────────────────────────
    "gcs.credential": "gcs.oauth2.token",
}

# Mapping from RW catalog.security values to PyIceberg auth.type values.
_RW_SECURITY_TO_PYICEBERG_AUTH: Dict[str, str] = {
    "google": "google",
}


def _rw_props_to_pyiceberg(connector_properties: Dict[str, Any]) -> Dict[str, Any]:
    """Convert RisingWave connection connector_properties to PyIceberg catalog properties.

    Handles three kinds of properties:
    1. Direct 1:1 key renames via _RW_TO_PYICEBERG.
    2. Special-case properties that need value transformation
       (catalog.security, catalog.header, s3.path.style.access).
    3. RisingWave-only properties that have no PyIceberg equivalent are skipped
       (catalog.io_impl, namespace.properties, table.properties, enable_config_load,
       catalog.jdbc.*).
    """
    result: Dict[str, Any] = {}

    # 1) Direct 1:1 mappings
    for rw_key, pyiceberg_key in _RW_TO_PYICEBERG.items():
        value = connector_properties.get(rw_key)
        if value is not None:
            result[pyiceberg_key] = value

    # 2) catalog.security = 'VALUE' → auth = {'type': 'VALUE'}
    security = connector_properties.get("catalog.security")
    if security:
        auth_type = _RW_SECURITY_TO_PYICEBERG_AUTH.get(str(security).lower())
        if auth_type:
            result["auth"] = {"type": auth_type}

    # 3) catalog.header → individual header.* properties
    #    RW format: "key1=val1;key2=val2"
    #    PyIceberg format: header.key1 = val1, header.key2 = val2
    header_str = connector_properties.get("catalog.header")
    if header_str:
        for pair in str(header_str).split(";"):
            pair = pair.strip()
            if "=" in pair:
                hdr_key, hdr_val = pair.split("=", 1)
                result[f"header.{hdr_key.strip()}"] = hdr_val.strip()

    # 4) s3.path.style.access → s3.force-virtual-addressing (inverted)
    #    RW: true = use path-style; PyIceberg: False = use path-style
    path_style = connector_properties.get("s3.path.style.access")
    if path_style is not None:
        result["s3.force-virtual-addressing"] = str(path_style).lower() != "true"

    return result


class RisingWaveAdapter(PostgresAdapter):
    ConnectionManager = RisingWaveConnectionManager
    Relation = RisingWaveRelation

    def _link_cached_relations(self, manifest):
        # lack of `pg_depend`, `pg_rewrite`
        pass

    @available
    @classmethod
    def sleep(cls, seconds):
        time.sleep(seconds)

    @available
    def iceberg_persist_docs(
        self,
        relation,
        model: Dict[str, Any],
        connector_properties: Dict[str, Any],
        resolved_props: Dict[str, Any],
    ) -> None:
        """Sync model/column descriptions to the Iceberg table that an embedded sink writes to.

        Called from the Jinja macro only when embedded_sink.iceberg_persist_docs
        is true and the connection_ref points to an iceberg connection.

        Args:
            connector_properties: from the connection model's config (catalog connection).
            resolved_props: with_properties after auto_table_name/auto_database_name
                are applied (table identity).
        """
        # Catalog props: derived solely from the connection model's connector_properties.
        catalog_props: Dict[str, Any] = _rw_props_to_pyiceberg(connector_properties)

        # Table identity from resolved with_properties (includes auto_* values).
        namespace: str = resolved_props.get("database.name", "")
        table_name: str = resolved_props.get("table.name", "")
        if not namespace or not table_name:
            missing = [
                k
                for k, v in [
                    ("database.name", namespace),
                    ("table.name", table_name),
                ]
                if not v
            ]
            raise ValueError(
                f"iceberg_persist_docs is enabled on model '{relation.identifier}' but "
                f"required with_properties key(s) are missing: {missing}"
            )

        try:
            from pyiceberg.catalog import load_catalog
        except ImportError:
            raise ImportError(
                "PyIceberg is required for iceberg_persist_docs. "
                "Install with: pip install 'dbt-risingwave[iceberg]' or "
                "pip install 'dbt-risingwave[iceberg-gcp]' or "
                "pip install 'dbt-risingwave[iceberg-aws]' or "
                "pip install 'dbt-risingwave[iceberg-azure]'"
            )

        # Connect to Iceberg catalog (no retry - same as standard persist_docs)
        try:
            catalog = load_catalog(**catalog_props)
        except Exception as e:
            raise RuntimeError(
                f"Failed to connect to Iceberg catalog: {e}"
            ) from e

        # Load the Iceberg table (no retry - same as standard persist_docs)
        try:
            table = catalog.load_table((namespace, table_name))
        except Exception as e:
            raise RuntimeError(
                f"Failed to load Iceberg table '{namespace}.{table_name}': {e}"
            ) from e

        description: str = model.get("description", "")
        columns: Dict[str, Any] = model.get("columns", {})

        persist_docs: Dict[str, Any] = model.get("config", {}).get("persist_docs", {})
        for_relation: bool = persist_docs.get("relation", False)
        for_columns: bool = persist_docs.get("columns", False)

        iceberg_cols = {field.name for field in table.schema().fields}
        cols_in_both: Dict[str, str] = (
            {
                col_name: col_info.get("description", "")
                for col_name, col_info in columns.items()
                if col_name in iceberg_cols
            }
            if for_columns and columns
            else {}
        )

        def _commit():
            with table.transaction() as txn:
                if for_relation:
                    if description:
                        txn.set_properties({"comment": description})
                    else:
                        txn.remove_properties("comment")
                if cols_in_both:
                    with txn.update_schema() as schema_update:
                        for col_name, col_doc in cols_in_both.items():
                            schema_update.update_column(col_name, doc=col_doc)

        # Commit docs to Iceberg
        try:
            _commit()
        except Exception as e:
            raise RuntimeError(
                f"Failed to sync docs to Iceberg table '{namespace}.{table_name}': {e}"
            ) from e
