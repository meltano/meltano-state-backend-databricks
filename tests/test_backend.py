from __future__ import annotations

import shutil
from importlib.metadata import version
from typing import TYPE_CHECKING
from unittest import mock
from urllib.parse import urlparse

import pytest
from databricks.sql.exc import DatabaseError
from meltano.core.project import Project
from meltano.core.state_store import MeltanoState, state_store_manager_from_project_settings
from meltano.core.state_store.base import MissingStateBackendSettingsError, StateIDLockedError
from packaging.version import Version

from meltano_state_backend_databricks.backend import (
    DatabricksStateStoreManager,
    _glob_to_like,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

URI = "databricks://token:tok@host.example.com/cat/sch?http_path=/sql/1.0/warehouses/w"


@pytest.fixture
def project(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree("fixtures/explicit", path, ignore=shutil.ignore_patterns(".meltano"))
    return Project(path.resolve())


@pytest.fixture
def project_with_uri(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree("fixtures/only_uri", path, ignore=shutil.ignore_patterns(".meltano"))
    return Project(path.resolve())


@pytest.fixture
def mock_connection() -> Generator[tuple[mock.Mock, mock.Mock], None, None]:
    """Mock Databricks connection."""
    with mock.patch("databricks.sql.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor


@pytest.fixture
def subject(
    mock_connection: tuple[mock.Mock, mock.Mock],
) -> tuple[DatabricksStateStoreManager, mock.Mock]:
    _, mock_cursor = mock_connection
    manager = DatabricksStateStoreManager(URI)
    mock_cursor.reset_mock()
    return manager, mock_cursor


def sqls(cursor: mock.Mock) -> list[str]:
    return [" ".join(c.args[0].split()) for c in cursor.execute.call_args_list]


@pytest.mark.usefixtures("mock_connection")
def test_get_manager(project: Project) -> None:
    manager = state_store_manager_from_project_settings(project.settings)
    assert isinstance(manager, DatabricksStateStoreManager)
    assert urlparse(manager.uri).hostname == "dbc-1234.cloud.databricks.com"
    assert manager.server_hostname == "dbc-1234.cloud.databricks.com"
    assert manager.http_path == "/sql/1.0/warehouses/abc123"
    assert manager.access_token == "test_token"  # noqa: S105
    assert manager.state_table == "`test_catalog`.`test_schema`.`meltano_state`"


@pytest.mark.usefixtures("mock_connection")
def test_get_manager_from_uri(project_with_uri: Project) -> None:
    manager = state_store_manager_from_project_settings(project_with_uri.settings)
    assert isinstance(manager, DatabricksStateStoreManager)
    assert manager.server_hostname == "dbc-1234.cloud.databricks.com"
    assert manager.http_path == "/sql/1.0/warehouses/abc123"
    assert manager.access_token == "test_token"  # noqa: S105
    assert manager.catalog == "test_catalog"
    assert manager.schema == "test_schema"


@pytest.mark.parametrize(
    ("setting", "env_var"),
    (
        pytest.param("server_hostname", "SERVER_HOSTNAME"),
        pytest.param("http_path", "HTTP_PATH"),
        pytest.param("auth_type", "AUTH_TYPE"),
        pytest.param("access_token", "ACCESS_TOKEN"),
        pytest.param("client_id", "CLIENT_ID"),
        pytest.param("client_secret", "CLIENT_SECRET"),
        pytest.param("catalog", "CATALOG"),
        pytest.param("schema", "SCHEMA"),
    ),
)
def test_settings(project: Project, setting: str, env_var: str) -> None:
    found = project.settings.find_setting(f"state_backend.databricks.{setting}")
    assert found is not None
    assert (
        found.env_vars(prefixes=["meltano"])[0].key == f"MELTANO_STATE_BACKEND_DATABRICKS_{env_var}"
    )


@pytest.mark.usefixtures("mock_connection")
def test_defaults_and_connect_kwargs() -> None:
    manager = DatabricksStateStoreManager(
        "databricks://:p%40ss@host.example.com?http_path=/p",
    )
    assert manager.catalog is None
    assert manager.schema == "default"
    assert manager.state_table == "`default`.`meltano_state`"
    assert manager.access_token == "p@ss"  # noqa: S105

    with mock.patch("databricks.sql.connect") as mock_connect:
        manager.connection = None
        assert manager.connection is mock_connect.return_value
        mock_connect.assert_called_once_with(
            server_hostname="host.example.com",
            http_path="/p",
            user_agent_entry="meltano-state-backend-databricks",
            access_token="p@ss",  # noqa: S106
        )


def test_oauth_m2m() -> None:
    with mock.patch("databricks.sql.connect") as mock_connect:
        manager = DatabricksStateStoreManager(
            "databricks://id:secret@host.example.com/cat?http_path=/p&auth_type=oauth_m2m",
        )
        kwargs = mock_connect.call_args.kwargs
        assert kwargs["catalog"] == "cat"
        assert "access_token" not in kwargs
        with (
            mock.patch("databricks.sdk.core.Config") as config,
            mock.patch("databricks.sdk.core.oauth_service_principal") as sp,
        ):
            assert kwargs["credentials_provider"]() is sp.return_value
        config.assert_called_once_with(
            host="https://host.example.com",
            client_id="id",
            client_secret="secret",  # noqa: S106
        )
        assert manager.client_id == "id"
        assert manager.client_secret == "secret"  # noqa: S105


@pytest.mark.parametrize(
    ("uri", "kwargs", "match"),
    (
        pytest.param("databricks://", {}, "server hostname", id="hostname"),
        pytest.param("databricks://host", {}, "HTTP path", id="http_path"),
        pytest.param("databricks://host?http_path=/p", {}, "access token", id="token"),
        pytest.param(
            "databricks://host?http_path=/p&auth_type=nope",
            {},
            "auth_type",
            id="auth_type",
        ),
        pytest.param(
            "databricks://id@host?http_path=/p&auth_type=oauth_m2m",
            {},
            "client secret",
            id="oauth",
        ),
    ),
)
def test_missing_settings(uri: str, kwargs: dict[str, str], match: str) -> None:
    with pytest.raises(MissingStateBackendSettingsError, match=match):
        DatabricksStateStoreManager(uri, **kwargs)


def test_explicit_settings_override_uri() -> None:
    with mock.patch("databricks.sql.connect"):
        manager = DatabricksStateStoreManager(
            URI,
            server_hostname="other",
            http_path="/other",
            access_token="other_tok",  # noqa: S106
            catalog="c2",
            schema="s2",
        )
    assert (manager.server_hostname, manager.http_path) == ("other", "/other")
    assert (manager.access_token, manager.catalog, manager.schema) == ("other_tok", "c2", "s2")


def test_ensure_tables(mock_connection: tuple[mock.Mock, mock.Mock]) -> None:
    _, cursor = mock_connection
    DatabricksStateStoreManager(URI)
    statements = sqls(cursor)
    assert statements[0] == "CREATE SCHEMA IF NOT EXISTS `cat`.`sch`"
    assert "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`meltano_state`" in statements[1]
    assert "USING DELTA" in statements[1]
    assert "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`meltano_state_locks`" in statements[2]
    assert "'delta.isolationLevel' = 'Serializable'" in statements[2]


def test_ensure_tables_schema_creation_denied(
    mock_connection: tuple[mock.Mock, mock.Mock],
) -> None:
    _, cursor = mock_connection
    cursor.execute.side_effect = [DatabaseError("PERMISSION_DENIED"), None, None]
    DatabricksStateStoreManager(URI)
    assert len(cursor.execute.call_args_list) == 3


def test_set_state(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject
    state = MeltanoState(state_id="job", partial_state={"a": 1}, completed_state={"b": 2})
    manager.set(state)
    sql, params = cursor.execute.call_args.args
    assert "MERGE INTO `cat`.`sch`.`meltano_state`" in sql
    assert params == {"state_id": "job", "state": state.json()}


def test_get_state(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject
    expected = MeltanoState(state_id="job", partial_state={"a": 1}, completed_state={"b": 2})
    cursor.fetchone.return_value = (expected.json(),)
    assert manager.get("job") == expected
    assert cursor.execute.call_args.args[1] == {"state_id": "job"}


def test_get_state_null_value(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject
    cursor.fetchone.return_value = (None,)
    assert manager.get("job") == MeltanoState(state_id="job")


def test_get_state_not_found(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject
    cursor.fetchone.return_value = None
    assert manager.get("job") is None


def test_delete_state(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject
    manager.delete("job")
    sql, params = cursor.execute.call_args.args
    assert sql.startswith("DELETE FROM `cat`.`sch`.`meltano_state`")
    assert params == {"state_id": "job"}


def test_get_state_ids(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject
    cursor.fetchall.return_value = [("a",), ("b",)]
    assert list(manager.get_state_ids()) == ["a", "b"]
    assert list(manager.get_state_ids("*")) == ["a", "b"]
    assert "WHERE" not in cursor.execute.call_args.args[0]


def test_get_state_ids_with_pattern(
    subject: tuple[DatabricksStateStoreManager, mock.Mock],
) -> None:
    manager, cursor = subject
    cursor.fetchall.return_value = [("dev:tap-a",)]
    assert list(manager.get_state_ids("dev:*")) == ["dev:tap-a"]
    assert cursor.execute.call_args.args[1] == {"pattern": "dev:%"}


def test_glob_to_like() -> None:
    assert _glob_to_like("dev:tap_*-?") == "dev:tap\\_%-_"
    assert _glob_to_like("100%\\") == "100\\%\\\\"


def test_clear_all(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject
    cursor.fetchone.return_value = (3,)
    assert manager.clear_all() == 3
    assert sqls(cursor)[-1] == "TRUNCATE TABLE `cat`.`sch`.`meltano_state`"


def test_acquire_lock(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject

    def fetchall() -> list[tuple[str]]:
        merge = next(c for c in cursor.execute.call_args_list if "MERGE" in c.args[0])
        return [(merge.args[1]["lock_id"],)]

    cursor.fetchall.side_effect = fetchall
    with mock.patch("meltano_state_backend_databricks.backend.sleep") as sleep:
        with manager.acquire_lock("job"):
            statements = sqls(cursor)
            assert statements[0].startswith("DELETE FROM `cat`.`sch`.`meltano_state_locks`")
            assert "MERGE INTO `cat`.`sch`.`meltano_state_locks`" in statements[1]
        sleep.assert_not_called()
    assert "lock_id = :lock_id" in sqls(cursor)[-1]


def test_acquire_lock_retries_while_held(
    subject: tuple[DatabricksStateStoreManager, mock.Mock],
) -> None:
    manager, cursor = subject
    own: list[str] = []
    results = iter([[("someone-else",)], [("someone-else",)], None])

    def fetchall() -> list[tuple[str]]:
        result = next(results)
        return [(own[0],)] if result is None else result

    def execute(sql: str, params: dict[str, str] | None = None) -> None:
        if "MERGE" in sql and params:
            own[:] = [params["lock_id"]]

    cursor.execute.side_effect = execute
    cursor.fetchall.side_effect = fetchall
    with mock.patch("meltano_state_backend_databricks.backend.sleep") as sleep:
        with manager.acquire_lock("job", retry_seconds=2):
            pass
        assert sleep.call_count == 2
        sleep.assert_called_with(2)


def test_acquire_lock_concurrent_conflict_then_success(
    subject: tuple[DatabricksStateStoreManager, mock.Mock],
) -> None:
    manager, cursor = subject
    calls = {"n": 0}
    lock_ids: list[str] = []

    def execute(sql: str, params: dict[str, str] | None = None) -> None:
        if "MERGE" in sql:
            calls["n"] += 1
            if calls["n"] == 1:
                raise DatabaseError("[DELTA_CONCURRENT_APPEND] conflict")  # type: ignore[no-untyped-call]  # noqa: EM101
            assert params
            lock_ids[:] = [params["lock_id"]]

    cursor.execute.side_effect = execute
    cursor.fetchall.side_effect = lambda: [(lock_ids[0],)]
    with mock.patch("meltano_state_backend_databricks.backend.sleep") as sleep:
        with manager.acquire_lock("job", retry_seconds=1):
            pass
        sleep.assert_called_once_with(1)


def test_acquire_lock_timeout(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject
    cursor.fetchall.return_value = [("someone-else",)]
    with (
        mock.patch("meltano_state_backend_databricks.backend.sleep") as sleep,
        pytest.raises(StateIDLockedError, match="Could not acquire lock for state_id: job"),
        manager.acquire_lock("job", retry_seconds=10),
    ):
        pass  # pragma: no cover
    assert sleep.call_count == 2


def test_acquire_lock_other_error(subject: tuple[DatabricksStateStoreManager, mock.Mock]) -> None:
    manager, cursor = subject
    cursor.execute.side_effect = DatabaseError("permission denied")  # type: ignore[no-untyped-call]
    with (
        mock.patch("meltano_state_backend_databricks.backend.sleep") as sleep,
        pytest.raises(DatabaseError, match="permission denied"),
        manager.acquire_lock("job"),
    ):
        pass  # pragma: no cover
    sleep.assert_not_called()


@pytest.mark.xfail(
    condition=Version(version("meltano")) < Version("4.2.0"),
    reason="Requires Meltano 4.2+ for context manager support on StateStoreManager",
)
def test_context_manager(
    subject: tuple[DatabricksStateStoreManager, mock.Mock],
    mock_connection: tuple[mock.Mock, mock.Mock],
) -> None:
    manager, _ = subject
    mock_conn, _ = mock_connection

    with manager:
        assert manager._connection is not None

    mock_conn.close.assert_called_once()
    manager.close()
    mock_conn.close.assert_called_once()
    assert manager._connection is None
