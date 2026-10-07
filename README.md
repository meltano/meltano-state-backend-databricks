# `meltano-state-backend-databricks`

This is a [Meltano] extension that provides a [Databricks] [state backend][state-backend].

State is stored in [Delta] tables in Unity Catalog, through a Databricks SQL warehouse.

## Installation

This package needs to be installed in the same Python environment as Meltano.

### From GitHub

#### With [uv]

```bash
uv tool install --with git+https://github.com/meltano/meltano-state-backend-databricks.git meltano
```

#### With [pipx]

```bash
pipx install meltano
pipx inject meltano git+https://github.com/meltano/meltano-state-backend-databricks.git
```

## Configuration

To store state in Databricks, set the `state_backend.uri` setting to a URI of the form:

```
databricks://token:<access_token>@<server_hostname>/<catalog>/<schema>?http_path=<http_path>
```

State will be stored in two Delta tables that Meltano will create automatically (the catalog and schema must already exist):

- `meltano_state` - Stores the actual state data
- `meltano_state_locks` - Manages concurrency locks

All connection parameters can be provided in the URI, as individual Meltano settings, or a mix of both. Explicit settings take precedence over URI values.

Using a single URI:

```yaml
state_backend:
  uri: databricks://token:dapi123@dbc-1234.cloud.databricks.com/my_catalog/my_schema?http_path=/sql/1.0/warehouses/abc123
```

Using individual settings:

```yaml
state_backend:
  uri: databricks://dbc-1234.cloud.databricks.com
  databricks:
    http_path: /sql/1.0/warehouses/abc123
    access_token: dapi123
    catalog: my_catalog   # Optional: defaults to the warehouse default catalog
    schema: my_schema     # Optional: defaults to `default`
```

### Connection Parameters

- **server_hostname**: The workspace hostname (e.g. `dbc-1234.cloud.databricks.com`), the URI host
- **http_path**: The SQL warehouse HTTP path (required), the `http_path` query parameter
- **auth_type**: `pat` (default) or `oauth_m2m`, the `auth_type` query parameter
- **access_token**: A personal access token (`pat`), the URI password
- **client_id** / **client_secret**: Service principal credentials (`oauth_m2m`), the URI user and password
- **catalog**: The Unity Catalog name, the first URI path segment
- **schema**: The schema holding the state tables, the second URI path segment

### OAuth machine-to-machine (service principal)

```yaml
state_backend:
  uri: databricks://dbc-1234.cloud.databricks.com/my_catalog/my_schema?http_path=/sql/1.0/warehouses/abc123&auth_type=oauth_m2m
  databricks:
    client_id: 00000000-0000-0000-0000-000000000000
    client_secret: dose123
```

### Security considerations

- Use environment variables for secrets, e.g. `MELTANO_STATE_BACKEND_DATABRICKS_ACCESS_TOKEN`.
- Credentials with special characters (e.g. `@`, `%`) must be URL-encoded when included in the URI.
- The principal needs `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `SELECT` and `MODIFY` on the target schema.

### Locking

Delta Lake does not enforce primary keys, so locks are taken with a `MERGE ... WHEN NOT MATCHED THEN INSERT` into a table using `delta.isolationLevel = Serializable`, which makes Delta fail one of two concurrent transactions locking the same state ID. Locks older than 5 minutes are considered stale and are cleared.

## Development

### Setup

```bash
uv sync
```

### Run tests

Databricks has no local emulator or Docker image, so the unit tests mock the `databricks-sql-connector`.

```bash
uvx --with tox-uv --with tox-gh tox -e lint,types,3.14
```

### Bump the version

Using the [GitHub CLI][gh]:

```bash
gh release create v<new-version>
```

[databricks]: https://www.databricks.com/
[delta]: https://delta.io/
[gh]: https://cli.github.com/
[meltano]: https://meltano.com
[pipx]: https://github.com/pypa/pipx
[state-backend]: https://docs.meltano.com/concepts/state_backends
[uv]: https://docs.astral.sh/uv
