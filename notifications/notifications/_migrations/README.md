# Notifications Alembic migrations

Alembic is the only production DDL path for the Notifications-owned
`Notifications` and `UserPreferences` tables. It uses the separate
`alembic_versions__notifications` version table and ignores tables owned by
shared storage in the same physical database.

Run commands from `notifications/` in the locked workspace environment. Supply
an isolated MySQL 8 database through `DATABASE_URL`:

```bash
export DATABASE_URL='mysql+pymysql://<username>:<password>@<hostname>:<port>/<database-name>'
```

Import each new SQLModel table in `env.py` before `SQLModel` so it is registered
in the target metadata. After changing ORM metadata, bring the disposable
database to the current head and generate a revision:

```bash
uv run --locked --package notifications alembic \
  --config=notifications/_migrations/alembic.ini \
  upgrade head
uv run --locked --package notifications alembic \
  --config=notifications/_migrations/alembic.ini \
  revision --autogenerate -m "descriptive revision message"
```

Review generated imports and every table, column, type, index, constraint,
default, nullability rule, and downgrade before committing. Then verify the
result against disposable MySQL:

```bash
uv run --locked --package notifications alembic \
  --config=notifications/_migrations/alembic.ini check
uv run --locked --package notifications alembic \
  --config=notifications/_migrations/alembic.ini current --check-heads
```

Never stamp, drop, or test against an operator-managed database. Retrieve
non-local credentials through the operator's secret-management process and do
not commit them.
