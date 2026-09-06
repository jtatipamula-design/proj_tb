# Project: project 2 dad

## Architecture
- Framework: Sanic web server with Jinja2 templating
- Database: PostgreSQL (Neon schema) via asyncpg
- Frontend: Tailwind CSS, HTML templates (dashboard.html, base.html, table_view.html, form_view.html) with canvas bioluminescent particles and frosted glassmorphism

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| 1 | Middleware & Sidebar Context | Sanic middleware auto-injecting `modules_tree`, `all_tables`, `table_modules` into Jinja template context for all routes | none | DONE |
| 2 | Dashboard Light Mode Colors | Tailwind/CSS contrast fix for dashboard module cards in light mode (`var(--color-glacier)`, `--color-pebble`, `--color-fog`) | none | DONE |
| 3 | Background Animation & Glassmorphism | Brighter canvas bubbles (alpha 0.25-0.35, lighter composite) & backdrop-blur 16px frosted glass UI in `base.html` & `dashboard.html` | none | DONE |
| 4 | Database Seed & Schema Cleanup | `seed_db.py` seeding 9 ERP modules in `phc_module_t`, mapping screens in `phc_screens_t`, dropping `phc_operating_orgs_t CASCADE` | none | DONE |
| 5 | Final Verification & Integration | E2E test suite execution, acceptance criteria validation, audit verification | M1, M2, M3, M4 | DONE |

## Interface Contracts
- Template Context Contract: `modules_tree`, `all_tables`, `table_modules` populated in Jinja context on every GET request rendering HTML templates.
- DB Schema Contract: `phc_module_t` populated with 9 ERP modules (IDs 1-9); `phc_screens_t.psn_module_id` linked to valid module IDs; `phc_operating_orgs_t` completely dropped.

## Code Layout
- Web server & middleware: `server.py`
- Database seeding: `seed_db.py`
- Templates: `templates/base.html`, `templates/dashboard.html`, `templates/table_view.html`, `templates/form_view.html`
