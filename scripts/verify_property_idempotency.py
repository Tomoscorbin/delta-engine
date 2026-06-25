"""
Verify whether adopted tables reach a true property no-op on a real cluster.

Run this against a live Databricks Unity Catalog session to settle the
"Unconfirmed" Priority 1 finding in ``docs/design-review.md`` ("Properties may
never reach a no-op on adopted tables").

The hypothesis under test
--------------------------
``DeltaTable`` injects ``delta.enableDeletionVectors=true`` and
``delta.columnMapping.mode=name`` into every desired state. ``_diff_properties``
emits a ``SetProperty`` whenever ``observed.get(name) != value`` for a declared
key, reading ``observed`` from the ``properties`` column of ``DESCRIBE DETAIL``.

The open question is whether, once the engine issues ``SET TBLPROPERTIES`` for a
managed key on a pre-existing (adopted) table, the next ``DESCRIBE DETAIL`` reads
that key back with the same value:

* If yes -> the second sync sees a match and reaches a true no-op. The table
  self-heals after one corrective sync; the finding is a non-issue.
* If no (the value is reported differently, or not at all, even after being set)
  -> the differ re-emits ``SetProperty`` on every run and the table never reaches
  a no-op. The finding is confirmed and the engine must reconcile against
  effective, DESCRIBE-reported properties rather than declared keys.

This cannot be reproduced on the local in-memory Delta catalog used by the test
suite, which is why it is a standalone script rather than an e2e test.

Usage
-----
On a cluster (or Databricks Connect session) where ``spark`` is available::

    python scripts/verify_property_idempotency.py --catalog <cat> --schema <schema>

It creates a *plain* Delta table directly via SQL -- with none of the managed
properties set -- to mimic a table created outside the engine, then registers
that table with the engine and syncs three times. The first sync applies the
managed properties; the second and third reveal whether the table has reached a
steady-state no-op. The temp table is dropped on exit.
"""

from __future__ import annotations

import argparse
import sys
from uuid import uuid4

from delta_engine import (
    Column,
    DeltaTable,
    Integer,
    Registry,
    String,
    build_databricks_engine,
)
from delta_engine.schema.properties import Property


def _get_spark():  # type: ignore[no-untyped-def]
    """Return the ambient SparkSession, or exit with guidance if there is none."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        sys.exit("pyspark is not installed in this environment.")

    spark = SparkSession.getActiveSession()
    if spark is None:
        sys.exit(
            "No active SparkSession. Run this on a Databricks cluster or in a "
            "Databricks Connect session where `spark` is available."
        )
    return spark


def _describe_managed_properties(spark, fully_qualified_name: str) -> dict[str, str]:  # type: ignore[no-untyped-def]
    """Return the engine-managed property keys as reported by DESCRIBE DETAIL."""
    described = spark.sql(f"DESCRIBE DETAIL {fully_qualified_name}").first()
    properties = dict(described["properties"]) if described else {}
    managed = {Property.ENABLE_DELETION_VECTORS.value, Property.COLUMN_MAPPING_MODE.value}
    return {key: properties[key] for key in managed if key in properties}


def _actions_executed(report) -> int:  # type: ignore[no-untyped-def]
    """Total number of execution results across all tables in a SyncReport."""
    return sum(len(table.execution.results) for table in report.table_reports)


def main() -> int:
    """Run the idempotency probe and return a process exit code (0 = no-op reached)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", required=True, help="Schema to create the temp table in")
    args = parser.parse_args()

    spark = _get_spark()
    engine = build_databricks_engine(spark)

    table_name = f"verify_prop_idem_{uuid4().hex[:8]}"
    fully_qualified_name = f"{args.catalog}.{args.schema}.{table_name}"

    desired = DeltaTable(
        args.catalog,
        args.schema,
        table_name,
        columns=(Column("id", Integer(), nullable=False), Column("name", String())),
        comment="property idempotency probe",
    )
    registry = Registry()
    registry.register(desired)

    try:
        # Create a plain Delta table with NO managed properties: this is the
        # adopted-table condition the finding is about (a table created outside
        # the engine, so the managed keys live only in catalog defaults, if at all).
        spark.sql(
            f"CREATE TABLE {fully_qualified_name} (id INT NOT NULL, name STRING) USING DELTA"
        )
        print("Managed properties before any sync (adopted table):")
        print(f"  {_describe_managed_properties(spark, fully_qualified_name) or '<none reported>'}")

        first = engine.sync(registry)
        print(f"\nSync 1 executed {_actions_executed(first)} action(s) (expected > 0: sets props).")
        print("Managed properties after sync 1:")
        print(f"  {_describe_managed_properties(spark, fully_qualified_name) or '<none reported>'}")

        second = engine.sync(registry)
        third = engine.sync(registry)
        second_actions = _actions_executed(second)
        third_actions = _actions_executed(third)
        print(f"\nSync 2 executed {second_actions} action(s).")
        print(f"Sync 3 executed {third_actions} action(s).")

        if second_actions == 0 and third_actions == 0:
            print("\nRESULT: steady-state no-op reached. Finding NOT reproduced.")
            print("Adopted tables self-heal after one sync; the declared-subset diff is safe.")
            return 0

        print("\nRESULT: the table never reaches a no-op. Finding CONFIRMED.")
        print("SET TBLPROPERTIES does not round-trip through DESCRIBE DETAIL for a managed key,")
        print("so the engine re-emits SetProperty every run. Fix: reconcile against effective,")
        print("DESCRIBE-reported properties rather than declared keys.")
        return 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {fully_qualified_name}")


if __name__ == "__main__":
    raise SystemExit(main())
