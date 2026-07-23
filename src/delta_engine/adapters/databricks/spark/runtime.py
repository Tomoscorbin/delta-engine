"""Databricks Runtime compatibility policy for the Spark backend."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_MINIMUM_DBR = (16, 4)
_MAXIMUM_DBR_MAJOR_EXCLUSIVE = 19

DATABRICKS_RUNTIME_REQUIREMENT = (
    f">={_MINIMUM_DBR[0]}.{_MINIMUM_DBR[1]},"
    f"<{_MAXIMUM_DBR_MAJOR_EXCLUSIVE}"
)

_VERSION_QUERY = "SELECT current_version().dbr_version AS dbr_version"
_VERSION_PREFIX = re.compile(
    r"^\s*(?P<major>\d+)(?:\.(?P<minor>\d+|x)(?=$|[.\s-])|(?=$|[\s-]))",
    re.IGNORECASE,
)


def require_supported_databricks_runtime(spark: SparkSession) -> str:
    """
    Return the running DBR version after enforcing this release's support range.

    Databricks serverless compute reports values such as
    ``18.x-aarch64-photon-scala2.13``. The major family is sufficient for the
    upper bound; the minor is required only when checking the 16.4 floor.
    """
    try:
        row = spark.sql(_VERSION_QUERY).first()
    except Exception as error:
        raise RuntimeError(
            "delta-engine could not determine the Databricks Runtime version; "
            f"the Spark backend requires DBR {DATABRICKS_RUNTIME_REQUIREMENT} "
            "and refuses to run when compatibility cannot be verified"
        ) from error

    raw_version: object = None if row is None else row["dbr_version"]
    if not isinstance(raw_version, str):
        raise RuntimeError(
            "delta-engine could not determine the Databricks Runtime version: "
            f"current_version().dbr_version returned {raw_version!r}; "
            f"the Spark backend requires DBR {DATABRICKS_RUNTIME_REQUIREMENT}"
        )

    match = _VERSION_PREFIX.match(raw_version)
    if match is None:
        raise RuntimeError(
            f"delta-engine could not parse Databricks Runtime {raw_version!r}; "
            f"the Spark backend requires DBR {DATABRICKS_RUNTIME_REQUIREMENT}"
        )

    major = int(match.group("major"))
    minor_text = match.group("minor")
    minor = int(minor_text) if minor_text is not None and minor_text.casefold() != "x" else None
    if not _is_supported(major, minor):
        raise RuntimeError(
            f"Databricks Runtime {raw_version!r} is not supported by this "
            f"delta-engine release; the Spark backend requires DBR "
            f"{DATABRICKS_RUNTIME_REQUIREMENT}"
        )

    return raw_version


def release_compatibility_markdown() -> str:
    """Render the compatibility section appended to GitHub release notes."""
    return (
        "### Databricks compatibility\n\n"
        f"- Spark backend: Databricks Runtime `{DATABRICKS_RUNTIME_REQUIREMENT}`\n"
        "- SQL warehouse backend: not governed by a Databricks Runtime version"
    )


def _is_supported(major: int, minor: int | None) -> bool:
    if major >= _MAXIMUM_DBR_MAJOR_EXCLUSIVE or major < _MINIMUM_DBR[0]:
        return False
    if major > _MINIMUM_DBR[0]:
        return True
    return minor is not None and minor >= _MINIMUM_DBR[1]
