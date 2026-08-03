"""
Locust Performance & Load Testing Suite
Tailored for Free Tier Deployments (Render Free Web Service + Neon PostgreSQL Free Tier).

Key Free-Tier Characteristics:
- Render: 0.1 CPU core, 512 MB RAM, spin-down after 15 min idle.
- Neon DB: Shared compute, auto-suspend when inactive.

Recommended Test Configurations:
  • Baseline Smoke Test:  Users: 5  | Spawn Rate: 1/s | Duration: 1 min
  • Moderate Load Test:   Users: 15 | Spawn Rate: 2/s | Duration: 3 min
  • Peak Capacity Test:   Users: 30 | Spawn Rate: 2/s | Duration: 5 min
"""

import os
import random
import time
from locust import HttpUser, task, between, events

# Configuration (can be overridden via environment variables)
TEST_USER = os.getenv("TEST_USER", "admin")
TEST_PASSWORD = os.getenv("TEST_PASSWORD", "admin123")

# Core Cleaning Validation & ERP Tables to Test
BENCHMARK_TABLES = [
    "pcv_protocols_t",
    "pcv_matrix_header_t",
    "pcv_sampling_plans_t",
    "pcv_equipment_master_t",
    "phc_users_t",
    "phc_lookup_values_t"
]

SEARCH_KEYWORDS = ["ACT", "Tablets", "Clean", "QA", "Batch", "Pass", "USP"]


class FreeTierERPUser(HttpUser):
    # Free-tier think time: 2 to 5 seconds between user actions (realistic human pacing)
    # This prevents artificial connection-burst exhaustion on Neon and Render
    wait_time = between(2.0, 5.0)

    def on_start(self):
        """Runs once per virtual user when spawned. Logs in and handles cold-starts."""
        self.is_logged_in = False
        self.perform_login()

    def perform_login(self, max_retries=3):
        """Attempts login with cold-start retry handling for Render/Neon spin-up."""
        for attempt in range(1, max_retries + 1):
            with self.client.post(
                "/login",
                json={"username": TEST_USER, "password": TEST_PASSWORD},
                catch_response=True,
                name="[Auth] POST /login"
            ) as response:
                if response.status_code == 200:
                    self.is_logged_in = True
                    response.success()
                    return
                elif response.status_code in (502, 503, 504):
                    # Cold start warning (Render or Neon is waking up)
                    if attempt < max_retries:
                        time.sleep(5)  # Wait for cold start
                        continue
                    response.failure(f"Cold start timeout (Render/Neon waking up): HTTP {response.status_code}")
                else:
                    response.failure(f"Login failed: HTTP {response.status_code} - {response.text[:100]}")
                    return

    @task(6)
    def view_dashboard(self):
        """Navigates to the main Command Center dashboard."""
        if not self.is_logged_in:
            return
        self.client.get("/dashboard", name="[Navigation] GET /dashboard")

    @task(10)
    def view_table_records(self):
        """Views a random cleaning validation table with pagination."""
        if not self.is_logged_in:
            return
        tbl = random.choice(BENCHMARK_TABLES)
        page = random.randint(1, 2)
        self.client.get(f"/table/{tbl}?page={page}", name=f"[Table View] GET /table/{tbl}")

    @task(6)
    def search_table(self):
        """Performs search filter query on protocols table."""
        if not self.is_logged_in:
            return
        q = random.choice(SEARCH_KEYWORDS)
        self.client.get(f"/table/pcv_protocols_t?q={q}", name="[Search Query] GET /table/pcv_protocols_t?q=...")

    @task(4)
    def view_form_record(self):
        """Opens a specific form record."""
        if not self.is_logged_in:
            return
        record_id = random.randint(1, 5)
        self.client.get(f"/form/pcv_protocols_t/{record_id}", name="[Form View] GET /form/pcv_protocols_t/:id")

    @task(2)
    def test_unauthorized_boundary(self):
        """Verifies 404/403 security boundary."""
        if not self.is_logged_in:
            return
        with self.client.get("/table/non_existent_screen_t", catch_response=True, name="[Security] 404 Guard") as res:
            if res.status_code in (404, 403):
                res.success()
            else:
                res.failure(f"Expected 404/403, received HTTP {res.status_code}")
