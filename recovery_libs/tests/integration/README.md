# Integration Tests

This directory contains integration tests that require external services like MySQL databases.

## Setup

### MySQL Database

The integration tests require a MySQL database. You can set one up using Docker:

```bash
# Generate a disposable local password, then start MySQL
export MYSQL_PASSWORD="$(openssl rand -hex 16)"
docker run --name mysql-test \
  -e MYSQL_ROOT_PASSWORD="$MYSQL_PASSWORD" \
  -e MYSQL_DATABASE=recovery_test \
  -p 3306:3306 \
  -d mysql:8.0@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b

# Wait for MySQL to be ready
docker exec mysql-test mysqladmin ping -h localhost -u root -p"$MYSQL_PASSWORD"
```

### Environment Variables

Configure the database connection using environment variables:

- `MYSQL_HOST` - MySQL server hostname (default: localhost)
- `MYSQL_PORT` - MySQL server port (default: 3306)
- `MYSQL_USER` - MySQL username (default: root)
- `MYSQL_PASSWORD` - MySQL password (required)
- `MYSQL_DATABASE` - MySQL database name (default: recovery_test)

Example:
```bash
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD="<operator-supplied-password>"
export MYSQL_DATABASE=recovery_test
```

## Running Tests

### Run all integration tests

```bash
uv run --locked pytest tests/integration -v
```

### Run with coverage

```bash
uv run --locked pytest tests/integration --cov=recovery_libs --cov-report=term-missing
```

### Skip integration tests

Integration tests are automatically marked with `@pytest.mark.integration`. To skip them:

```bash
uv run --locked pytest -m "not integration"
```

## Cleanup

The tests automatically clean up after themselves using cleanup fixtures like `clean_advice_tables`. However, you may want to manually clean up the database or remove the container:

```bash
# Stop and remove MySQL container
docker stop mysql-test
docker rm mysql-test
```

## Troubleshooting

### Connection Refused
If you get "Connection refused" errors:
1. Ensure MySQL is running: `docker ps | grep mysql-test`
2. Check MySQL is ready: `docker exec mysql-test mysqladmin ping -h localhost -u root -ppassword`
3. Verify port mapping: `docker port mysql-test`

### Authentication Failed
If you get authentication errors:
1. Verify environment variables are set correctly
2. Check MySQL root password: should match `MYSQL_PASSWORD` env var
3. Ensure database exists: `docker exec mysql-test mysql -u root -ppassword -e "SHOW DATABASES;"`

### Schema Migration Issues
If you encounter schema migration problems:
1. The tests automatically run Alembic migrations
2. Check migration status: look at test output for Alembic logs
3. Manually reset: Drop and recreate the test database in the container

### Tests are Skipped
If integration tests are being skipped:
1. Check that MySQL is accessible
2. Verify pymysql is installed: `pip list | grep pymysql`
3. Review the skip message in test output for specific issues
