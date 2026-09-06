"""
================================================================================
RED-TEAM SECURITY & ADVERSARIAL FUZZING LOCUST SUITE
================================================================================
Simulates realistic adversarial tactics, threat actors, and edge-case attacks:
  1. High-Velocity Brute-Force & Credential Stuffing (Rate Limit / DoS probe)
  2. Broken Object-Level Authorization (BOLA / IDOR) & Privilege Escalation Probes
  3. Deep SQL Injection Fuzzing (Stacked queries, Tautologies, Time-based sleep)
  4. Advanced XSS & Polyglot Injection Probing
  5. Path Traversal & Sensitive File Exposure Probes (/..%2f.env, /.git/config)
  6. HTTP Verb Tampering & Payload Mutation Fuzzing (Oversized body, corrupt data)
  7. Multi-Tenant Parameter Pollution / Company ID Tampering
  8. Concurrent Legitimate Operator Activity (Evaluates resilience under attack)
"""

import os
import re
import random
import time
from urllib.parse import quote
from locust import HttpUser, task, between

# Legitimate multi-user pool
ADMIN_USERS = [f"admin{i}" for i in range(1, 16)]
ROLE_USERS = ["qa_lead", "prod_op", "qc_lab", "val_eng", "auditor"]
ALL_USERS_POOL = ADMIN_USERS + ROLE_USERS

# Attacker dictionaries
STUFFING_USERNAMES = [
    "root", "administrator", "guest", "test", "superadmin", "operator", 
    "sysadmin", "system", "postgres", "dbadmin", "user", "manager"
]
STUFFING_PASSWORDS = [
    "password", "123456", "admin", "admin123", "root", "toor", 
    "letmein", "secret", "welcome", "Pass@123", "password123"
]

# Advanced SQL Injection Fuzzing Payloads
SQLI_PAYLOADS = [
    "' OR '1'='1' --",
    "1; DROP TABLE fake_test_table_t; --",
    "' UNION SELECT NULL, version(), NULL, NULL --",
    "1' ORDER BY 100 --",
    "' AND 1=(SELECT CASE WHEN (1=1) THEN 1 ELSE 1/(SELECT 0) END) --",
    "1' OR 1=1 AND 'a'='a",
    "1%27%20OR%201=1",
    "'; SELECT pg_sleep(2); --"
]

# Advanced Cross-Site Scripting (XSS) Polyglot Payloads
XSS_PAYLOADS = [
    "<script>alert('SEC_TEST_1')</script>",
    "\"><img src=x onerror=alert('XSS_IMG')>",
    "<svg/onload=alert('XSS_SVG')>",
    "javascript:alert(document.cookie)",
    "';alert(String.fromCharCode(88,83,83))//",
    "<iframe src=\"javascript:alert('XSS')\"></iframe>"
]

# Sensitive Path Traversal Probes
TRAVERSAL_PATHS = [
    "/..%2f..%2f.env",
    "/.env",
    "/.git/config",
    "/.git/HEAD",
    "/static/..%2f..%2fserver.py",
    "/static/..%2f.env",
    "/server.py",
    "/wp-login.php",
    "/phpmyadmin",
    "/api/../../etc/passwd"
]

USER_ASSIGNMENT_INDEX = 0


# =============================================================================
# CLASS 1: ADVERSARY / RED-TEAM ATTACKER BOT
# =============================================================================

class AdversaryAttackerBot(HttpUser):
    """Simulates an external threat actor or malicious user actively hunting for vulnerabilities."""
    # Aggressive / rapid probing (0.2s - 0.8s think time)
    wait_time = between(0.2, 0.8)
    weight = 3  # 30% of spawned traffic

    # -------------------------------------------------------------------------
    # 1. High-Velocity Credential Stuffing & Rate-Limiting Probe
    # -------------------------------------------------------------------------
    @task(5)
    def probe_brute_force_and_rate_limiting(self):
        """Rapidly tests common credentials to verify authentication defense and rate-limiting."""
        uname = random.choice(STUFFING_USERNAMES)
        pwd = random.choice(STUFFING_PASSWORDS)

        with self.client.post(
            "/login",
            json={"username": uname, "password": pwd},
            catch_response=True,
            name="[Attack: Auth] Credential Stuffing Burst"
        ) as res:
            # 401 (Invalid creds) or 429 (Rate limited) is SECURE.
            # 500 (Unhandled crash) is a FAILURE.
            if res.status_code in (401, 403, 429):
                res.success()
            elif res.status_code == 500:
                res.failure(f"VULNERABILITY: 500 Crash on invalid login probe: {res.text[:100]}")
            else:
                # If an unauthorized username somehow logged in with 200 OK
                res.failure(f"SECURITY ALERT: Non-existent user '{uname}' logged in successfully!")

    # -------------------------------------------------------------------------
    # 2. Path Traversal & Secret Exposure Probe
    # -------------------------------------------------------------------------
    @task(4)
    def probe_path_traversal_and_secrets(self):
        """Probes for directory traversal, environment secrets, and git config leaks."""
        path = random.choice(TRAVERSAL_PATHS)
        
        with self.client.get(
            path,
            catch_response=True,
            name="[Attack: Traversal] Directory / Secret Probe"
        ) as res:
            # 404, 403, 400 are SECURE defenses.
            # 200 with sensitive contents (like SECRET_KEY, [core], or DB password) is CRITICAL LEAK.
            if res.status_code == 200:
                if any(secret in res.text for secret in ["SECRET_KEY", "DATABASE_URL", "[core]", "repositoryformatversion"]):
                    res.failure(f"CRITICAL VULNERABILITY: Sensitive system file exposed at '{path}'!")
                else:
                    res.success()
            elif res.status_code in (404, 403, 400, 302):
                res.success()
            else:
                res.failure(f"Unexpected status on traversal probe: HTTP {res.status_code}")

    # -------------------------------------------------------------------------
    # 3. Deep SQL Injection Fuzzing
    # -------------------------------------------------------------------------
    @task(4)
    def probe_deep_sql_injection(self):
        """Fuzzes search parameters, filters, and pagination with advanced SQLi payloads."""
        payload = random.choice(SQLI_PAYLOADS)
        target_endpoint = random.choice([
            f"/table/phc_users_t?q={quote(payload)}",
            f"/table/pcv_protocols_t?page={quote(payload)}",
            f"/table/phc_lookup_values_t?type_filter={quote(payload)}",
            f"/export/phc_users_t?q={quote(payload)}"
        ])

        with self.client.get(
            target_endpoint,
            catch_response=True,
            name="[Attack: SQLi] Advanced SQL Injection Fuzzing"
        ) as res:
            # 200 (sanitized search), 404, 403, 400 are SECURE.
            # 500 means SQL syntax crashed the server / leaked database query structure.
            if res.status_code in (200, 404, 403, 400, 302):
                if any(err in res.text.lower() for err in ["syntax error at or near", "pg_catalog", "pg_class", "asyncpg.exceptions"]):
                    res.failure("VULNERABILITY: Raw database SQL error/stack trace leaked in HTML!")
                else:
                    res.success()
            elif res.status_code == 500:
                res.failure(f"VULNERABILITY: SQL Injection vector crashed server with HTTP 500!")
            else:
                res.success()

    # -------------------------------------------------------------------------
    # 4. Advanced XSS & Script Reflection Fuzzing
    # -------------------------------------------------------------------------
    @task(4)
    def probe_advanced_xss(self):
        """Tests XSS reflection in search bars, query strings, and pagination parameters."""
        xss = random.choice(XSS_PAYLOADS)
        
        with self.client.get(
            f"/table/pcv_protocols_t?q={quote(xss)}",
            catch_response=True,
            name="[Attack: XSS] Polyglot XSS Reflection Probe"
        ) as res:
            if res.status_code == 200:
                # If unescaped script tag appears directly in the HTML body
                if "<script>alert(" in res.text or "<svg/onload=" in res.text or "<iframe src=" in res.text:
                    res.failure("CRITICAL: Unescaped XSS payload reflected into rendered DOM!")
                else:
                    res.success()
            else:
                res.success()

    # -------------------------------------------------------------------------
    # 5. Broken Object-Level Authorization (BOLA / Unauthenticated Access)
    # -------------------------------------------------------------------------
    @task(4)
    def probe_unauthenticated_bola_privilege(self):
        """Attempts unauthenticated mutations and exports of administrative tables."""
        action = random.choice([
            ("GET", "/export/phc_users_t", None),
            ("POST", "/api/phc_users_t", {"pus_user_name": "hacker_admin", "pus_user_type": "ADM"}),
            ("POST", "/api/phc_roles_t", {"prl_role_code": "HACK_ROLE", "prl_role_name": "Hacked Role"}),
            ("GET", "/table/phc_users_t", None)
        ])

        method, url, data = action
        if method == "GET":
            with self.client.get(url, allow_redirects=False, catch_response=True, name="[Attack: BOLA] Unauthenticated Admin Probe") as res:
                # Must be blocked by 401, 403, or redirected to /login (302)
                if res.status_code in (401, 403, 302, 404):
                    res.success()
                elif res.status_code == 200:
                    res.failure(f"CRITICAL PRIVILEGE ESCALATION: Unauthenticated access to '{url}' granted HTTP 200!")
        else:
            with self.client.post(url, data=data, catch_response=True, name="[Attack: BOLA] Unauthenticated Admin Mutation") as res:
                if res.status_code in (401, 403, 302, 404):
                    res.success()
                elif res.status_code in (200, 201):
                    res.failure(f"CRITICAL SECURITY FLAW: Unauthenticated user created administrative record at '{url}'!")

    # -------------------------------------------------------------------------
    # 6. HTTP Verb Tampering & Corrupted Data Fuzzing
    # -------------------------------------------------------------------------
    @task(3)
    def probe_verb_tampering_and_fuzzing(self):
        """Sends unexpected HTTP verbs and corrupted oversized data packets."""
        # 1. Verb tampering (PATCH, PUT on read routes)
        with self.client.request(
            "PATCH", 
            "/table/pcv_protocols_t", 
            data="A" * 5000, 
            catch_response=True, 
            name="[Attack: Fuzzing] Verb Tampering & Payload Buffer"
        ) as res:
            if res.status_code in (405, 404, 400, 403, 302):
                res.success()
            elif res.status_code == 500:
                res.failure("VULNERABILITY: Server 500 crash on unexpected HTTP method!")
            else:
                res.success()

        # 2. Corrupt Large Number in Pagination
        with self.client.get(
            "/table/pcv_protocols_t?page=99999999999999999999999999999",
            catch_response=True,
            name="[Attack: Fuzzing] Integer Overflow in Pagination"
        ) as res:
            if res.status_code in (200, 404, 400):
                res.success()
            elif res.status_code == 500:
                res.failure("VULNERABILITY: Integer overflow in pagination crashed server with 500!")


# =============================================================================
# CLASS 2: LEGITIMATE ENTERPRISE OPERATOR BOT
# =============================================================================

class LegitimateOperatorBot(HttpUser):
    """Simulates real business operators using the system concurrently to ensure attack resilience."""
    wait_time = between(1.5, 3.5)
    weight = 7  # 70% of spawned traffic

    def on_start(self):
        global USER_ASSIGNMENT_INDEX
        self.is_logged_in = False
        self.discovered_tables = set()
        self.known_record_pks = {}

        self.username = ALL_USERS_POOL[USER_ASSIGNMENT_INDEX % len(ALL_USERS_POOL)]
        USER_ASSIGNMENT_INDEX += 1
        self.password = "admin123"

        self.perform_login()
        if self.is_logged_in:
            self.discover_tables()

    def perform_login(self, max_retries=3):
        for attempt in range(1, max_retries + 1):
            with self.client.post(
                "/login",
                json={"username": self.username, "password": self.password},
                catch_response=True,
                name="[Legit: Auth] POST /login"
            ) as res:
                if res.status_code == 200:
                    self.is_logged_in = True
                    res.success()
                    return
                elif res.status_code in (502, 503, 504):
                    if attempt < max_retries:
                        time.sleep(4)
                        continue
                    res.failure(f"Cold-start timeout: {res.status_code}")
                else:
                    res.failure(f"Login failed for {self.username}: {res.status_code}")
                    return

    def discover_tables(self):
        res = self.client.get("/", name="[Legit: Discovery] GET / (Command Center)")
        if res.status_code == 200:
            found_tables = re.findall(r'/table/([a-zA-Z0-9_]+)', res.text)
            if found_tables:
                self.discovered_tables = set(found_tables)

    def get_random_table(self) -> str:
        if self.discovered_tables:
            return random.choice(list(self.discovered_tables))
        return "pcv_protocols_t"

    @task(6)
    def browse_tables_and_harvest(self):
        if not self.is_logged_in: return
        tbl = self.get_random_table()
        
        with self.client.get(
            f"/table/{tbl}", 
            catch_response=True, 
            name="[Legit: Ops] GET /table/:table"
        ) as res:
            if res.status_code in (200, 404, 403):
                res.success()
                pks = re.findall(rf'/(?:form|edit)/{re.escape(tbl)}/([a-zA-Z0-9\-_]+)', res.text)
                if pks:
                    self.known_record_pks[tbl] = list(set(self.known_record_pks.get(tbl, []) + pks))[:15]
            else:
                res.failure(f"Table browsing error on {tbl}: HTTP {res.status_code}")

    @task(3)
    def view_record_detail(self):
        if not self.is_logged_in: return
        valid_tables = [t for t, pks in self.known_record_pks.items() if pks]
        if not valid_tables: return
        tbl = random.choice(valid_tables)
        pk = random.choice(self.known_record_pks[tbl])

        with self.client.get(
            f"/form/{tbl}/{pk}", 
            catch_response=True, 
            name="[Legit: Ops] GET /form/:table/:pk"
        ) as res:
            if res.status_code in (200, 404, 403):
                res.success()
            else:
                res.failure(f"Form detail error on {tbl}/{pk}: HTTP {res.status_code}")

    @task(3)
    def stream_csv_export(self):
        if not self.is_logged_in: return
        tbl = self.get_random_table()

        with self.client.get(
            f"/export/{tbl}", 
            catch_response=True, 
            name="[Legit: Ops] GET /export/:table"
        ) as res:
            if res.status_code in (200, 404, 403):
                res.success()
            else:
                res.failure(f"Export error on {tbl}: HTTP {res.status_code}")
