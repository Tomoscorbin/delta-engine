# Real `DESCRIBE TABLE EXTENDED … AS JSON` outputs (fixture source)

Companion to `2026-07-15-databricks-reader-efficiency-design.md`. Three verified outputs from a
live Databricks workspace, to seed `tests/adapters/databricks/sql/fixtures/`.

**Note:** these are rendered in Python-literal form (single-quoted inner strings, trailing
commas). Normalize to strict JSON when creating the actual fixtures — the real command returns
a JSON string. Key things to preserve: structured `type` objects, `comment: ""` vs omitted,
top-level `clustering_columns`, the synthesized `table_properties` blob, and the
`table_constraints` string format.

## demo_table (clustered, single-col PK, empty table comment, column mapping on)

```
{
    "table_name": "demo_table",
    "catalog_name": "dev",
    "namespace": ["silver"],
    "schema_name": "silver",
    "columns": [
        {"name": "id", "type": {"name": "int"}, "nullable": false, "comment": "pk"},
        {"name": "name", "type": {"name": "string", "collation": "UTF8_BINARY"}, "nullable": true, "comment": "name"}
    ],
    "clustering_columns": ["id"],
    "type": "MANAGED",
    "comment": "",
    "collation": "UTF8_BINARY",
    "location": "",
    "provider": "delta",
    "owner": "tom_corbin1@hotmail.com",
    "is_managed_location": true,
    "predictive_optimization": "ENABLE (inherited from METASTORE metastore_aws_eu_west_1)",
    "table_properties": {
        "clusteringColumns": "[[\"id\"]]",
        "delta.checkpointPolicy": "v2",
        "delta.columnMapping.maxColumnId": "3",
        "delta.columnMapping.mode": "name",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true",
        "delta.feature.appendOnly": "supported",
        "delta.feature.clustering": "supported",
        "delta.feature.columnMapping": "supported",
        "delta.feature.deletionVectors": "supported",
        "delta.feature.domainMetadata": "supported",
        "delta.feature.invariants": "supported",
        "delta.feature.rowTracking": "supported",
        "delta.feature.v2Checkpoint": "supported",
        "delta.minReaderVersion": "3",
        "delta.minWriterVersion": "7",
        "delta.rowTracking.materializedRowCommitVersionColumnName": "_row-commit-version-col-ef8b3562-3244-49a6-ab82-fa8f48388e93",
        "delta.rowTracking.materializedRowIdColumnName": "_row-id-col-158d3c6a-19b4-45dd-b7fc-335204f00579"
    },
    "created_time": "2025-08-15T16:35:10Z",
    "last_access": "UNKNOWN",
    "created_by": "Spark ",
    "partition_provider": "Catalog",
    "table_constraints": "[(pk_dev_silver_demo_table__id,PRIMARY KEY (`id`))]"
}
```

## users_data (14 cols, columns omit comment, default data-skipping NOT in table_properties)

```
{
    "table_name": "users_data",
    "catalog_name": "source",
    "namespace": ["raw"],
    "schema_name": "raw",
    "columns": [
        {"name": "id", "type": {"name": "bigint"}, "nullable": true},
        {"name": "current_age", "type": {"name": "bigint"}, "nullable": true},
        {"name": "retirement_age", "type": {"name": "bigint"}, "nullable": true},
        {"name": "birth_year", "type": {"name": "bigint"}, "nullable": true},
        {"name": "birth_month", "type": {"name": "bigint"}, "nullable": true},
        {"name": "gender", "type": {"name": "string", "collation": "UTF8_BINARY"}, "nullable": true},
        {"name": "address", "type": {"name": "string", "collation": "UTF8_BINARY"}, "nullable": true},
        {"name": "latitude", "type": {"name": "double"}, "nullable": true},
        {"name": "longitude", "type": {"name": "double"}, "nullable": true},
        {"name": "per_capita_income", "type": {"name": "string", "collation": "UTF8_BINARY"}, "nullable": true},
        {"name": "yearly_income", "type": {"name": "string", "collation": "UTF8_BINARY"}, "nullable": true},
        {"name": "total_debt", "type": {"name": "string", "collation": "UTF8_BINARY"}, "nullable": true},
        {"name": "credit_score", "type": {"name": "bigint"}, "nullable": true},
        {"name": "num_credit_cards", "type": {"name": "bigint"}, "nullable": true}
    ],
    "type": "MANAGED",
    "comment": "Created by the file upload UI",
    "collation": "UTF8_BINARY",
    "location": "",
    "provider": "delta",
    "owner": "tom_corbin1@hotmail.com",
    "is_managed_location": true,
    "predictive_optimization": "ENABLE (inherited from METASTORE metastore_aws_eu_west_1)",
    "table_properties": {
        "delta.checkpoint.writeStatsAsJson": "false",
        "delta.checkpoint.writeStatsAsStruct": "true",
        "delta.enableDeletionVectors": "true",
        "delta.feature.appendOnly": "supported",
        "delta.feature.deletionVectors": "supported",
        "delta.feature.invariants": "supported",
        "delta.minReaderVersion": "3",
        "delta.minWriterVersion": "7"
    },
    "created_time": "2025-07-18T20:41:44Z",
    "last_access": "UNKNOWN",
    "created_by": "Spark ",
    "statistics": {
        "size_in_bytes": 91792,
        "num_rows": 2000,
        "data_skipping_columns": ["num_credit_cards", "latitude", "birth_month", "longitude", "current_age", "yearly_income", "credit_score", "id", "address", "total_debt", "per_capita_income", "retirement_age", "birth_year", "gender"],
        "column_selection_method": "first-32"
    },
    "partition_provider": "Catalog"
}
```

## order_fact (column comments, mixed nullability, PK + FK in table_constraints)

```
{
    "table_name": "order_fact",
    "catalog_name": "dev",
    "namespace": ["gold"],
    "schema_name": "gold",
    "columns": [
        {"name": "order_id", "type": {"name": "int"}, "nullable": false, "comment": "Unique identifier for the order"},
        {"name": "user_id", "type": {"name": "int"}, "nullable": false, "comment": "Identifier for the user who placed the order"},
        {"name": "order_number", "type": {"name": "int"}, "nullable": false, "comment": "Sequential number of the order for the user"},
        {"name": "order_day_of_week", "type": {"name": "int"}, "nullable": false, "comment": "Day of the week when the order was placed"},
        {"name": "order_hour", "type": {"name": "int"}, "nullable": false, "comment": "Hour of the day the order was placed"},
        {"name": "days_since_prior_order", "type": {"name": "int"}, "nullable": true, "comment": "Days elapsed since the previous order"},
        {"name": "product_id", "type": {"name": "int"}, "nullable": true, "comment": "Unique identifier for a product"}
    ],
    "type": "MANAGED",
    "comment": "Fact table capturing core metrics for each order",
    "collation": "UTF8_BINARY",
    "location": "",
    "provider": "delta",
    "owner": "tom_corbin1@hotmail.com",
    "is_managed_location": true,
    "predictive_optimization": "ENABLE (inherited from METASTORE metastore_aws_eu_west_1)",
    "table_properties": {
        "delta.columnMapping.maxColumnId": "7",
        "delta.columnMapping.mode": "name",
        "delta.enableDeletionVectors": "true",
        "delta.feature.appendOnly": "supported",
        "delta.feature.columnMapping": "supported",
        "delta.feature.deletionVectors": "supported",
        "delta.feature.invariants": "supported",
        "delta.minReaderVersion": "3",
        "delta.minWriterVersion": "7"
    },
    "created_time": "2025-08-02T21:49:32Z",
    "last_access": "UNKNOWN",
    "created_by": "Spark ",
    "statistics": {
        "size_in_bytes": 24031181,
        "num_rows": 3421083,
        "data_skipping_columns": ["order_id", "days_since_prior_order", "user_id", "order_hour", "col-b76d3d9e-a915-403c-aaf1-23a0346f55c7", "order_day_of_week", "order_number"],
        "column_selection_method": "first-32"
    },
    "partition_provider": "Catalog",
    "table_constraints": "[(pk_dev_gold_order_fact,PRIMARY KEY (`order_id`)), (fk_dev_gold_order_fact_product_id_to_product_dimension_product_id,FOREIGN KEY (`product_id`) REFERENCES `dev`.`gold`.`product_dimension` (`product_id`))]",
    "predictive_optimization_evaluations": {
        "VACUUM": {"last_run_status": "SUCCESSFUL", "last_run_time": "2026-07-14T10:32:19Z"}
    }
}
```
