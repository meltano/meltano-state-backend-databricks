"""StateStoreManager for Databricks state backend.

State is stored in Delta tables in Unity Catalog, accessed through a Databricks SQL
warehouse with the ``databricks-sql-connector``.

Delta Lake does not enforce primary keys, so the lock table cannot rely on a unique
constraint violation the way the Postgres/Snowflake backends do. Instead the lock is
taken with ``MERGE ... WHEN NOT MATCHED THEN INSERT`` on a table configured with
``delta.isolationLevel = Serializable``, which makes Delta fail one of two concurrent
transactions that would both insert a lock for the same state ID. The winner is then
verified by reading the lock row back.
"""

from __future__ import annotations

import logging
import sys
import uuid
from contextlib import contextmanager
from time import sleep
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, unquote, urlparse

from databricks import sql as dbsql
from meltano.core.error import MeltanoError
from meltano.core.setting_definition import SettingDefinition, SettingKind
from meltano.core.state_store.base import (
    MeltanoState,
    MissingStateBackendSettingsError,
    StateIDLockedError,
    StateStoreManager,
)

if sys.version_info >= (3, 12):  # pragma: no cover
    from typing import override
else:  # pragma: no cover
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from databricks.sdk.core import CredentialsProvider
    from databricks.sql.client import Connection

logger = logging.getLogger(__name__)

AUTH_TYPE_PAT = "pat"
AUTH_TYPE_OAUTH_M2M = "oauth_m2m"
AUTH_TYPES = (AUTH_TYPE_PAT, AUTH_TYPE_OAUTH_M2M)

DEFAULT_SCHEMA_NAME = "default"
DEFAULT_TABLE_NAME = "meltano_state"
DEFAULT_LOCK_TABLE_NAME = "meltano_state_locks"
USER_AGENT_ENTRY = "meltano-state-backend-databricks"
LOCK_TIMEOUT_SECONDS = 30
STALE_LOCK_MINUTES = 5


class DatabricksStateBackendError(MeltanoError):
    """Base error for Databricks state backend."""


DATABRICKS_SERVER_HOSTNAME = SettingDefinition(
    name="state_backend.databricks.server_hostname",
    label="Databricks Server Hostname",
    description="Databricks workspace hostname, e.g. dbc-1234.cloud.databricks.com",
    kind=SettingKind.STRING,
    env_specific=True,
)

DATABRICKS_HTTP_PATH = SettingDefinition(
    name="state_backend.databricks.http_path",
    label="Databricks HTTP Path",
    description="HTTP path of the SQL warehouse, e.g. /sql/1.0/warehouses/abc123",
    kind=SettingKind.STRING,
    env_specific=True,
)

DATABRICKS_AUTH_TYPE = SettingDefinition(
    name="state_backend.databricks.auth_type",
    label="Databricks Authentication Type",
    description=(
        "`pat` for a personal access token (default), `oauth_m2m` for a service "
        "principal client ID and secret"
    ),
    kind=SettingKind.OPTIONS,
    options=[
        {"label": "Personal access token", "value": AUTH_TYPE_PAT},
        {"label": "OAuth M2M (service principal)", "value": AUTH_TYPE_OAUTH_M2M},
    ],
    env_specific=True,
)

DATABRICKS_ACCESS_TOKEN = SettingDefinition(
    name="state_backend.databricks.access_token",
    label="Databricks Access Token",
    description="Databricks personal access token (when auth_type is `pat`)",
    kind=SettingKind.STRING,
    sensitive=True,
    env_specific=True,
)

DATABRICKS_CLIENT_ID = SettingDefinition(
    name="state_backend.databricks.client_id",
    label="Databricks Client ID",
    description="Service principal client ID (when auth_type is `oauth_m2m`)",
    kind=SettingKind.STRING,
    env_specific=True,
)

DATABRICKS_CLIENT_SECRET = SettingDefinition(
    name="state_backend.databricks.client_secret",
    label="Databricks Client Secret",
    description="Service principal OAuth secret (when auth_type is `oauth_m2m`)",
    kind=SettingKind.STRING,
    sensitive=True,
    env_specific=True,
)

DATABRICKS_CATALOG = SettingDefinition(
    name="state_backend.databricks.catalog",
    label="Databricks Catalog",
    description="Unity Catalog name (default: the warehouse default catalog)",
    kind=SettingKind.STRING,
    env_specific=True,
)

DATABRICKS_SCHEMA = SettingDefinition(
    name="state_backend.databricks.schema",
    label="Databricks Schema",
    description="Schema where the state tables are stored (default: default)",
    kind=SettingKind.STRING,
    env_specific=True,
)


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _glob_to_like(pattern: str) -> str:
    r"""Convert a glob pattern to a SQL ``LIKE`` pattern (default ``\`` escape)."""
    escaped = pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped.replace("*", "%").replace("?", "_")


class DatabricksStateStoreManager(StateStoreManager):
    """State backend for Databricks."""

    table_name: str = DEFAULT_TABLE_NAME
    lock_table_name: str = DEFAULT_LOCK_TABLE_NAME

    @property
    @override
    def label(self) -> str:
        """Get the label for this state store manager."""
        return "Databricks"  # pragma: no cover

    def __init__(
        self,
        uri: str,
        *,
        server_hostname: str | None = None,
        http_path: str | None = None,
        auth_type: str | None = None,
        access_token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the DatabricksStateStoreManager.

        Args:
            uri: The state backend URI, e.g.
                ``databricks://token:<pat>@<hostname>/<catalog>/<schema>?http_path=<path>``
            server_hostname: Databricks workspace hostname
            http_path: SQL warehouse HTTP path
            auth_type: ``pat`` (default) or ``oauth_m2m``
            access_token: Personal access token
            client_id: Service principal client ID
            client_secret: Service principal OAuth secret
            catalog: Unity Catalog name
            schema: Schema name (default: ``default``)
            kwargs: Additional keyword args to pass to parent

        Raises:
            MissingStateBackendSettingsError: If a required setting is missing.
        """
        super().__init__(**kwargs)
        self.uri = uri
        parsed = urlparse(uri)
        query_params = parse_qs(parsed.query)

        def from_query(key: str) -> str | None:
            return query_params.get(key, [None])[0]

        self.server_hostname = server_hostname or (
            unquote(parsed.hostname) if parsed.hostname else None
        )
        if not self.server_hostname:
            msg = "Databricks server hostname is required"
            raise MissingStateBackendSettingsError(msg)

        self.http_path = http_path or from_query("http_path")
        if not self.http_path:
            msg = "Databricks HTTP path is required"
            raise MissingStateBackendSettingsError(msg)

        self.auth_type = auth_type or from_query("auth_type") or AUTH_TYPE_PAT
        if self.auth_type not in AUTH_TYPES:
            msg = f"Databricks auth_type must be one of {', '.join(AUTH_TYPES)}"
            raise MissingStateBackendSettingsError(msg)

        uri_user = unquote(parsed.username) if parsed.username else None
        uri_password = unquote(parsed.password) if parsed.password else None

        self.access_token: str | None = None
        self.client_id: str | None = None
        self.client_secret: str | None = None
        if self.auth_type == AUTH_TYPE_OAUTH_M2M:
            self.client_id = client_id or uri_user
            self.client_secret = client_secret or uri_password
            if not self.client_id or not self.client_secret:
                msg = "Databricks client ID and client secret are required for oauth_m2m"
                raise MissingStateBackendSettingsError(msg)
        else:
            self.access_token = access_token or uri_password
            if not self.access_token:
                msg = "Databricks access token is required"
                raise MissingStateBackendSettingsError(msg)

        path_parts = parsed.path.strip("/").split("/") if parsed.path.strip("/") else []
        self.catalog = catalog or (path_parts[0] if path_parts else None)
        self.schema = schema or (path_parts[1] if len(path_parts) > 1 else DEFAULT_SCHEMA_NAME)

        prefix = f"{_quote(self.catalog)}." if self.catalog else ""
        self.state_table = f"{prefix}{_quote(self.schema)}.{_quote(self.table_name)}"
        self.lock_table = f"{prefix}{_quote(self.schema)}.{_quote(self.lock_table_name)}"
        self.schema_qualified = f"{prefix}{_quote(self.schema)}"

        self._connection: Connection | None = None
        self._ensure_tables()

    def _connect_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "user_agent_entry": USER_AGENT_ENTRY,
        }
        if self.catalog:
            kwargs["catalog"] = self.catalog

        if self.auth_type == AUTH_TYPE_OAUTH_M2M:
            kwargs["credentials_provider"] = self._service_principal_headers
        else:
            kwargs["access_token"] = self.access_token
        return kwargs

    def _service_principal_headers(self) -> CredentialsProvider:
        from databricks.sdk.core import Config, oauth_service_principal

        provider: CredentialsProvider = oauth_service_principal(
            Config(
                host=f"https://{self.server_hostname}",
                client_id=self.client_id,
                client_secret=self.client_secret,
            ),
        )
        return provider

    @property
    def connection(self) -> Connection:
        """Get a Databricks SQL connection.

        Returns:
            A Databricks SQL connection object.
        """
        if self._connection is None:
            self._connection = dbsql.connect(**self._connect_kwargs())
        return self._connection

    @connection.setter
    def connection(self, value: Connection | None) -> None:
        """Set the Databricks connection (for testing/mocking)."""
        self._connection = value

    def _ensure_tables(self) -> None:
        """Ensure the schema and the state and lock tables exist."""
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_qualified}")
            except dbsql.exc.Error as e:
                # The principal may lack `CREATE SCHEMA` while the schema already
                # exists. If it doesn't, creating the tables below fails clearly.
                logger.debug("Could not create schema %s: %s", self.schema_qualified, e)
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.state_table} (
                    state_id STRING NOT NULL,
                    state STRING
                ) USING DELTA
                """,
            )
            # Serializable isolation makes the MERGE used by `acquire_lock` conflict
            # with concurrent inserts for the same state ID.
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.lock_table} (
                    state_id STRING NOT NULL,
                    lock_id STRING NOT NULL,
                    locked_at TIMESTAMP DEFAULT current_timestamp()
                ) USING DELTA
                TBLPROPERTIES (
                    'delta.isolationLevel' = 'Serializable',
                    'delta.feature.allowColumnDefaults' = 'supported'
                )
                """,
            )

    @override
    def set(self, state: MeltanoState) -> None:
        """Set the job state for the given state_id.

        Args:
            state: the state to set.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                MERGE INTO {self.state_table} AS target
                USING (SELECT :state_id AS state_id, :state AS state) AS source
                ON target.state_id = source.state_id
                WHEN MATCHED THEN
                    UPDATE SET target.state = source.state
                WHEN NOT MATCHED THEN
                    INSERT (state_id, state) VALUES (source.state_id, source.state)
                """,  # noqa: S608
                {"state_id": state.state_id, "state": state.json()},
            )

    @override
    def get(self, state_id: str) -> MeltanoState | None:
        """Get the job state for the given state_id.

        Args:
            state_id: the name of the job to get state for.

        Returns:
            The current state for the given job, or None if not found.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT state FROM {self.state_table} WHERE state_id = :state_id",  # noqa: S608
                {"state_id": state_id},
            )
            row = cursor.fetchone()

        if not row:
            return None

        if row[0] is None:
            return MeltanoState(state_id=state_id)

        return MeltanoState.from_json(state_id, row[0])

    @override
    def delete(self, state_id: str) -> None:
        """Delete state for the given state_id.

        Args:
            state_id: the state_id to clear state for
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {self.state_table} WHERE state_id = :state_id",  # noqa: S608
                {"state_id": state_id},
            )

    @override
    def clear_all(self) -> int:
        """Clear all states.

        Returns:
            The number of states cleared from the store.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {self.state_table}")  # noqa: S608
            count = 0
            if row := cursor.fetchone():  # pragma: no branch
                count = int(row[0])

            cursor.execute(f"TRUNCATE TABLE {self.state_table}")
            return count

    @override
    def get_state_ids(self, pattern: str | None = None) -> Iterable[str]:
        """Get all state_ids available in this state store manager.

        Args:
            pattern: glob-style pattern to filter by

        Returns:
            An iterable of state_ids
        """
        with self.connection.cursor() as cursor:
            if pattern and pattern != "*":
                cursor.execute(
                    f"SELECT state_id FROM {self.state_table} WHERE state_id LIKE :pattern",  # noqa: S608
                    {"pattern": _glob_to_like(pattern)},
                )
            else:
                cursor.execute(f"SELECT state_id FROM {self.state_table}")  # noqa: S608
            return [row[0] for row in cursor.fetchall()]

    @override
    def close(self) -> None:
        """Close the state store manager and release any resources."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _try_acquire(self, state_id: str, lock_id: str) -> bool:
        """Try to take the lock for a state ID.

        Returns:
            Whether the lock was acquired.
        """
        with self.connection.cursor() as cursor:
            # Clear stale locks so a crashed run can't block the state forever.
            cursor.execute(
                f"""
                DELETE FROM {self.lock_table}
                WHERE state_id = :state_id
                AND locked_at < current_timestamp() - INTERVAL {STALE_LOCK_MINUTES} MINUTES
                """,  # noqa: S608
                {"state_id": state_id},
            )
            cursor.execute(
                f"""
                MERGE INTO {self.lock_table} AS target
                USING (SELECT :state_id AS state_id, :lock_id AS lock_id) AS source
                ON target.state_id = source.state_id
                WHEN NOT MATCHED THEN
                    INSERT (state_id, lock_id) VALUES (source.state_id, source.lock_id)
                """,  # noqa: S608
                {"state_id": state_id, "lock_id": lock_id},
            )
            cursor.execute(
                f"SELECT lock_id FROM {self.lock_table} WHERE state_id = :state_id",  # noqa: S608
                {"state_id": state_id},
            )
            holders = [row[0] for row in cursor.fetchall()]
        return holders == [lock_id]

    @override
    @contextmanager
    def acquire_lock(
        self,
        state_id: str,
        *,
        retry_seconds: float = 1,
    ) -> Generator[None, None, None]:
        """Acquire a lock for the given job's state.

        Args:
            state_id: the state_id to lock
            retry_seconds: the number of seconds to wait before retrying

        Yields:
            None

        Raises:
            StateIDLockedError: if the lock cannot be acquired
        """
        lock_id = str(uuid.uuid4())
        seconds_waited = 0.0

        while True:
            try:
                if self._try_acquire(state_id, lock_id):
                    break
            except dbsql.exc.Error as e:
                # Delta aborts one of two concurrent transactions on the same data
                if "CONCURRENT" not in str(e).upper():
                    raise
                logger.debug("Concurrent lock attempt for %s: %s", state_id, e)

            seconds_waited += retry_seconds
            if seconds_waited >= LOCK_TIMEOUT_SECONDS:
                msg = f"Could not acquire lock for state_id: {state_id}"
                raise StateIDLockedError(msg)
            sleep(retry_seconds)

        try:
            yield
        finally:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM {self.lock_table} WHERE state_id = :state_id AND lock_id = :lock_id",  # noqa: E501, S608
                    {"state_id": state_id, "lock_id": lock_id},
                )
