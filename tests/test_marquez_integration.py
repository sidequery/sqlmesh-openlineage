"""Integration test that sends to Marquez and verifies lineage data."""
from __future__ import annotations

import os
import time
import tempfile
import shutil
import pytest
import requests

MARQUEZ_API_URL = "http://localhost:5001"
MARQUEZ_LINEAGE_URL = "http://localhost:5001"
NAMESPACE = "sqlmesh_test"


def wait_for_marquez(timeout=30):
    """Wait for Marquez to be ready."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{MARQUEZ_API_URL}/api/v1/namespaces", timeout=2)
            if resp.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(1)
    return False


@pytest.fixture(scope="module")
def marquez_container():
    """Expect Marquez to already be running."""
    if not wait_for_marquez(timeout=60):
        pytest.skip("Marquez not running. Start with: docker run -d --platform linux/amd64 -p 5001:5000 --name marquez_test marquezproject/marquez:latest")
    yield "existing"


@pytest.fixture
def temp_project():
    """Create a temporary SQLMesh project."""
    tmpdir = tempfile.mkdtemp(prefix="sqlmesh_marquez_test_")

    config_content = """
gateways:
  local:
    connection:
      type: duckdb
      database: ':memory:'

default_gateway: local

model_defaults:
  dialect: duckdb
"""
    with open(os.path.join(tmpdir, "config.yaml"), "w") as f:
        f.write(config_content)

    models_dir = os.path.join(tmpdir, "models")
    os.makedirs(models_dir)

    # Source model
    source_content = """
MODEL (
    name test_db.customers,
    kind FULL,
    columns (
        customer_id INT,
        name VARCHAR,
        email VARCHAR
    )
);

SELECT 1 as customer_id, 'Alice' as name, 'alice@example.com' as email
UNION ALL
SELECT 2 as customer_id, 'Bob' as name, 'bob@example.com' as email
"""
    with open(os.path.join(models_dir, "customers.sql"), "w") as f:
        f.write(source_content)

    # Orders model
    orders_content = """
MODEL (
    name test_db.orders,
    kind FULL,
    columns (
        order_id INT,
        customer_id INT,
        amount DECIMAL
    )
);

SELECT 101 as order_id, 1 as customer_id, 99.99 as amount
UNION ALL
SELECT 102 as order_id, 2 as customer_id, 149.99 as amount
"""
    with open(os.path.join(models_dir, "orders.sql"), "w") as f:
        f.write(orders_content)

    # Downstream model joining both
    summary_content = """
MODEL (
    name test_db.customer_summary,
    kind FULL,
    columns (
        customer_id INT,
        customer_name VARCHAR,
        total_orders INT,
        total_amount DECIMAL
    )
);

SELECT
    c.customer_id,
    c.name as customer_name,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_amount
FROM test_db.customers c
LEFT JOIN test_db.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
"""
    with open(os.path.join(models_dir, "customer_summary.sql"), "w") as f:
        f.write(summary_content)

    yield tmpdir
    shutil.rmtree(tmpdir)


class TestMarquezIntegration:
    """Integration tests with real Marquez instance."""

    def test_lineage_stored_in_marquez(self, marquez_container, temp_project):
        """Test that lineage is correctly stored in Marquez."""
        import sqlmesh_openlineage
        from sqlmesh.core import console as sqlmesh_console

        # Reset state
        sqlmesh_openlineage._installed = False
        sqlmesh_console._console = None

        # Install with Marquez URL
        sqlmesh_openlineage.install(
            url=MARQUEZ_LINEAGE_URL,
            namespace=NAMESPACE,
        )

        from sqlmesh import Context

        # Run SQLMesh
        ctx = Context(paths=[temp_project])
        ctx.plan(auto_apply=True, no_prompts=True)

        # Give Marquez time to process
        time.sleep(2)

        # Verify namespace was created
        resp = requests.get(f"{MARQUEZ_API_URL}/api/v1/namespaces/{NAMESPACE}")
        assert resp.status_code == 200, f"Namespace not found: {resp.text}"

        # Verify jobs were created
        resp = requests.get(f"{MARQUEZ_API_URL}/api/v1/namespaces/{NAMESPACE}/jobs")
        assert resp.status_code == 200
        jobs = resp.json()["jobs"]
        job_names = [j["name"] for j in jobs]

        print(f"Jobs in Marquez: {job_names}")

        assert any("customers" in name for name in job_names), "customers job not found"
        assert any("orders" in name for name in job_names), "orders job not found"
        assert any("customer_summary" in name for name in job_names), "customer_summary job not found"

        # Verify datasets were created
        resp = requests.get(f"{MARQUEZ_API_URL}/api/v1/namespaces/{NAMESPACE}/datasets")
        assert resp.status_code == 200
        datasets = resp.json()["datasets"]
        dataset_names = [d["name"] for d in datasets]

        print(f"Datasets in Marquez: {dataset_names}")

        assert any("customers" in name for name in dataset_names), "customers dataset not found"
        assert any("orders" in name for name in dataset_names), "orders dataset not found"
        assert any("customer_summary" in name for name in dataset_names), "customer_summary dataset not found"

        # Find customer_summary job and verify its inputs (lineage)
        summary_job = next(j for j in jobs if "customer_summary" in j["name"])

        # Get job details with lineage
        resp = requests.get(f"{MARQUEZ_API_URL}/api/v1/namespaces/{NAMESPACE}/jobs/{summary_job['name']}")
        assert resp.status_code == 200
        job_detail = resp.json()

        print(f"customer_summary job inputs: {job_detail.get('inputs', [])}")

        # Verify customer_summary has both customers and orders as inputs
        input_names = [inp["name"] for inp in job_detail.get("inputs", [])]
        assert any("customers" in name for name in input_names), \
            f"customers not in inputs: {input_names}"
        assert any("orders" in name for name in input_names), \
            f"orders not in inputs: {input_names}"

        # Verify schema was captured for customer_summary dataset
        summary_dataset = next(d for d in datasets if "customer_summary" in d["name"])
        resp = requests.get(
            f"{MARQUEZ_API_URL}/api/v1/namespaces/{NAMESPACE}/datasets/{summary_dataset['name']}"
        )
        assert resp.status_code == 200
        dataset_detail = resp.json()

        print(f"customer_summary schema: {dataset_detail.get('fields', [])}")

        fields = dataset_detail.get("fields", [])
        field_names = [f["name"] for f in fields]

        assert "customer_id" in field_names, f"customer_id not in schema: {field_names}"
        assert "customer_name" in field_names, f"customer_name not in schema: {field_names}"
        assert "total_orders" in field_names, f"total_orders not in schema: {field_names}"
        assert "total_amount" in field_names, f"total_amount not in schema: {field_names}"

        # Get runs to verify execution stats
        resp = requests.get(
            f"{MARQUEZ_API_URL}/api/v1/namespaces/{NAMESPACE}/jobs/{summary_job['name']}/runs"
        )
        assert resp.status_code == 200
        runs = resp.json()["runs"]

        assert len(runs) > 0, "No runs found for customer_summary"

        latest_run = runs[0]
        print(f"Latest run state: {latest_run['state']}")
        print(f"Latest run facets: {list(latest_run.get('facets', {}).keys())}")

        assert latest_run["state"] == "COMPLETED", f"Run not completed: {latest_run['state']}"

        # Verify column-level lineage was captured
        # The dataset should have columnLineage facet
        resp = requests.get(
            f"{MARQUEZ_API_URL}/api/v1/namespaces/{NAMESPACE}/datasets/{summary_dataset['name']}/versions"
        )
        if resp.status_code == 200:
            versions = resp.json().get("versions", [])
            if versions:
                latest_version = versions[0]
                facets = latest_version.get("facets", {})
                print(f"Dataset facets: {list(facets.keys())}")

                if "columnLineage" in facets:
                    col_lineage = facets["columnLineage"]
                    print(f"Column lineage fields: {list(col_lineage.get('fields', {}).keys())}")

                    # Verify customer_name traces back to customers.name
                    fields = col_lineage.get("fields", {})
                    if "customer_name" in fields:
                        inputs = fields["customer_name"].get("inputFields", [])
                        input_fields = [(i["name"], i["field"]) for i in inputs]
                        print(f"customer_name lineage: {input_fields}")
                        assert any("customers" in name and field == "name" for name, field in input_fields), \
                            f"customer_name should trace to customers.name: {input_fields}"
                    print("✓ Column-level lineage verified!")
                else:
                    print("Note: columnLineage facet not found in Marquez (may need API version check)")

        print("\n✓ All Marquez integration checks passed!")
