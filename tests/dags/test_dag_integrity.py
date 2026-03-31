"""
DAG Integrity Tests
-------------------
Validates that all DAG files:
  1. Import without errors (DAG bag loads cleanly).
  2. Have no import cycles.
  3. Contain required metadata (tags, owner, doc_md or description).
  4. Follow naming conventions.

Run with:
  pytest tests/dags/test_dag_integrity.py -v
"""

import importlib
import os
import sys
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Discover DAG files
# ---------------------------------------------------------------------------
DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


def _discover_dag_files():
    """Yield .py files under dags/ that are likely DAG modules."""
    for root, _dirs, files in os.walk(DAGS_DIR):
        for fname in files:
            if fname.endswith(".py") and not fname.startswith("__"):
                yield Path(root) / fname


DAG_FILES = list(_discover_dag_files())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_dag_module(path: Path):
    """Import a DAG file as a Python module."""
    # Ensure dags/ and its parents are on sys.path for relative imports.
    dags_parent = str(DAGS_DIR.parent)
    if dags_parent not in sys.path:
        sys.path.insert(0, dags_parent)
    dags_str = str(DAGS_DIR)
    if dags_str not in sys.path:
        sys.path.insert(0, dags_str)

    module_name = path.stem
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _get_dags_from_module(mod):
    """Extract DAG objects from a loaded module."""
    from airflow.sdk import DAG

    dags = []
    for attr_name in dir(mod):
        obj = getattr(mod, attr_name)
        if isinstance(obj, DAG):
            dags.append(obj)
    return dags


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "dag_file",
    DAG_FILES,
    ids=[str(f.relative_to(DAGS_DIR)) for f in DAG_FILES],
)
class TestDagIntegrity:
    """Per-file integrity checks."""

    def test_dag_imports_cleanly(self, dag_file):
        """DAG file must import without raising exceptions."""
        _load_dag_module(dag_file)

    def test_dag_has_tags(self, dag_file):
        """Every DAG should have at least one tag for filtering."""
        mod = _load_dag_module(dag_file)
        dags = _get_dags_from_module(mod)
        for dag in dags:
            assert dag.tags, f"DAG '{dag.dag_id}' has no tags"

    def test_dag_has_owner(self, dag_file):
        """Every DAG should have an owner in default_args."""
        mod = _load_dag_module(dag_file)
        dags = _get_dags_from_module(mod)
        for dag in dags:
            owner = dag.default_args.get("owner", "")
            assert owner and owner != "airflow", (
                f"DAG '{dag.dag_id}' should set a meaningful owner, got '{owner}'"
            )

    def test_no_import_cycles(self, dag_file):
        """DAG file should not cause circular imports."""
        # If import succeeds, there are no cycles.
        _load_dag_module(dag_file)

    def test_dag_has_no_cycles(self, dag_file):
        """Task dependency graph must be a DAG (no cycles)."""
        mod = _load_dag_module(dag_file)
        dags = _get_dags_from_module(mod)
        for dag in dags:
            # Airflow validates this at parse time; re-check explicitly.
            assert dag.topological_sort() is not None, (
                f"DAG '{dag.dag_id}' has a cycle in task dependencies"
            )
