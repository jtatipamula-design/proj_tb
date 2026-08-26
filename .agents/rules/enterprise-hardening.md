# Enterprise Backend Hardening Rules

Always adhere to these strict enterprise constraints when developing backend architecture, especially for high-compliance or multi-worker systems:

1. **Advisory Locks for DDL & Seeding:** NEVER execute startup DDL (`CREATE TABLE`) or data seeding (`INSERT`) in an async multi-worker environment without first acquiring a database-level lock (e.g., PostgreSQL `pg_try_advisory_lock()`). This prevents Time-Of-Check to Time-Of-Use (TOCTOU) race conditions and constraint violations on boot.
2. **Never Swallow Audit Exceptions:** NEVER place audit logging execution inside a bare `try/except` block that suppresses errors. Audit trail insertions MUST be part of the atomic transaction of the action they record. If the audit insert fails, the transaction MUST abort and bubble up the error.
3. **Stream Large Data Exports:** NEVER buffer entire tables into memory (e.g., using `conn.fetch` and string concatenation) for CSV or JSON exports. ALWAYS use asynchronous database cursors and stream the response to the client in chunks to prevent server Out-Of-Memory (OOM) crashes.
4. **Enforce Ownership on Object Updates (IDOR Prevention):** NEVER update or delete a record using just its Primary Key if that record belongs to a specific user or role (e.g., notifications, private messages). ALWAYS include an ownership check in the `WHERE` clause (e.g., `WHERE id = $1 AND owner_id = $2`).
5. **Single Source of Truth for Validation:** NEVER duplicate validation logic across endpoints. If a validator function exists (e.g., `validate_password_strength()`), it MUST be invoked centrally rather than rewriting weaker checks inline.
