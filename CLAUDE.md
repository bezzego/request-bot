# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
poetry install

# Run the bot
poetry run python -m app.main

# Apply DB migrations
poetry run alembic upgrade head

# Create a new migration
poetry run alembic revision --autogenerate -m "description"

# Lint / format
poetry run ruff check .
poetry run black .
poetry run isort .
```

No test suite exists yet (`tests/` directory referenced in config but absent).

## Architecture

The bot is built with **aiogram 3** (async Telegram Bot API), **SQLAlchemy 2 async** (asyncpg driver), **Alembic** for migrations, and **pydantic-settings** for config. It runs as a single async process with polling.

### Entry point

`app/main.py` — creates the `Bot` + `Dispatcher`, registers all routers, starts the `ReminderScheduler`, then begins polling.

### Roles

The system has five user roles, each with its own handler module and keyboard module:

| Role | Handler | Keyboard |
|---|---|---|
| Specialist | `app/handlers/specialist/` | `app/keyboards/specialist_kb.py` |
| Engineer | `app/handlers/engineer/` | `app/keyboards/engineer_kb.py` |
| Master | `app/handlers/master/` | `app/keyboards/master_kb.py` |
| Manager/Leader | `app/handlers/manager.py` | `app/keyboards/manager_kb.py` |
| Client | `app/handlers/client.py` | `app/keyboards/client_kb.py` |

Admin actions (user/role management) are restricted to `SUPER_ADMIN_IDS` from `.env`.

### Handler structure

Routers are registered in `app/handlers/__init__.py`. Role-specific handlers that are complex (specialist, engineer, master) are split into sub-packages with further grouping by feature (e.g. `engineer/create/`, `engineer/filters/`, `master/materials/`).

The top-level `app/handlers/{role}.py` files typically contain only the router definition and import sub-routers; business logic lives in the sub-packages.

### Data model

Core models (all in `app/infrastructure/db/models/`):

- **`User`** — Telegram user with `UserRole` enum (specialist, engineer, master, manager, client). Each role has an optional profile table in `models/roles/`.
- **`Request`** — Central entity. Has `RequestStatus` lifecycle: `new → inspection_scheduled → inspected → assigned → in_progress → completed → ready_for_sign → closed` (or `cancelled`). References specialist, engineer, master, customer, object, contract, defect_type.
- **`WorkItem`** — Line items for work performed on a request.
- **`WorkSession`** — Master's work shift records (start/end time, geolocation).
- **`Act`** — Completion acts (`ActType`: work_act, material_act, letter_act).
- **`Photo`** — Photos attached to requests (`PhotoType`). Masters can attach photos by sending captioned images.
- **`RequestStageHistory`** — Audit log of status transitions.
- **`RequestReminder`** — Scheduled reminder records processed by `ReminderScheduler`.

All models inherit from `Base` (DeclarativeBase) exported from `app/infrastructure/db/models/__init__.py`.

### Database session

`app/infrastructure/db/session.py` provides `async_session()` — an async context manager yielding an `AsyncSession`. Always use this pattern; do not call `async_session_maker()` directly in handler code.

### Configuration

`app/config/settings.py` — `pydantic-settings` reads from `.env`. Required vars: `BOT_TOKEN`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`. Optional: `SUPER_ADMIN_IDS` (comma-separated integers).

### Services

`app/services/` contains stateless service modules:

- `request_service.py` — All DB operations on requests (create, update, status transitions, queries).
- `user_service.py` — User lookup/creation by telegram_id.
- `reminders.py` — `ReminderScheduler` background task that polls for due reminders and sends notifications.
- `export.py` — Excel export logic using openpyxl.
- `reporting.py` — Aggregated analytics queries.
- `material_catalog.py`, `work_catalog.py` — Catalog management for materials and work types (stored in `app/config/mat.json` and `bot_settings.json`).

### Utilities

- `app/utils/request_formatters.py` — `STATUS_TITLES` dict and formatting helpers (status labels, hours/minutes, budgets). All Russian status names live here — update this when adding statuses.
- `app/utils/pagination.py` — Generic paginator for inline keyboards.
- `app/utils/advanced_filters.py`, `request_filters.py` — Filter state helpers for list views.
- `app/utils/timezone.py` — `now_moscow()`, `to_moscow()`, `format_moscow()` — all timestamps use Moscow time (UTC+3).
- `app/utils/identifiers.py` — `generate_request_number()` for unique `RQ-XXXXXX` numbers.

### Conventions

- All timestamps stored timezone-aware; always use `now_moscow()` / `to_moscow()` from `app/utils/timezone`.
- HTML parse mode is set globally on the bot — use `<b>`, `<i>`, `<code>` in message text, not Markdown.
- Line length: 100 characters (black + isort + ruff configured in `pyproject.toml`).
- Migrations go in `migrations/versions/`; name files with date prefix (e.g. `20260104_description.py`).
