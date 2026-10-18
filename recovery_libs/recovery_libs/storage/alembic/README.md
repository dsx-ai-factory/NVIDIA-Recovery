# Alembic SQL Database Migration Tools

Alembic manages schema migrations for the shared NVIDIA Recovery SQL storage.

## Setup

Run commands from `recovery_libs/recovery_libs/storage/alembic` in an
environment that contains this repository's Alembic and storage packages.
Supply the database connection through `DATABASE_URL`:

```bash
export DATABASE_URL='mysql+pymysql://<username>:<password>@<hostname>:<port>/<database-name>'
```

Use an isolated local database when generating or testing migrations. Retrieve
non-local credentials through the operator's secret-management process; do not
place passwords, private database hosts, namespaces, or customer environment
names in this document or committed configuration.

## Common commands

Inspect the current revision:

```bash
alembic current -v
```

Generate a reviewed migration after the local database is at the current
revision:

```bash
alembic revision --autogenerate -m "descriptive revision message"
```

Apply or preview migrations:

```bash
alembic upgrade head
alembic upgrade head --sql
```

Downgrades and `alembic stamp` change migration state and require deliberate
operator review. Always inspect generated migration code before committing it.
