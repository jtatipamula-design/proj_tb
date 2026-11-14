# Brihas ERP

A metadata-driven ERP web app built on **Sanic** (async web server), **asyncpg** (PostgreSQL driver) and **Jinja2** (server-rendered templates).
Screens are dynamic CRUD views over database tables registered in `phc_screens_t`, so adding a table to that registry exposes it in the UI, subject to role-based screen assignments.

- Database: PostgreSQL (hosted on Neon in production)
- Deployment: Render, using the `Procfile` (`web: python server.py`)

## Environment variables

| Variable | Required | Notes |
|---|---|---|
| `DATABASE_URL` | yes | PostgreSQL connection string |
| `JWT_SECRET` | yes | Long random string. The server refuses to start without it. |
| `PORT` | no | Defaults to `10000` |
| `WORKERS` | no | Sanic worker count, defaults to `1` |

Put these in a `.env` file at the repo root for local development (it is git-ignored and must never be committed).

## Run locally

```bash
pip install -r requirements.txt
# create .env with DATABASE_URL and JWT_SECRET
python server.py
```

## Migrations

On startup the server applies any pending `migrations/*.sql` files in filename order and records each applied version in `phc_schema_migrations_t`. Name new files with a sortable prefix (for example `002_add_x.sql`).

## Tests

```bash
python -m pytest tests -q
```
