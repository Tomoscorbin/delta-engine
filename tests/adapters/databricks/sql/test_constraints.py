import pytest

from delta_engine.adapters.databricks.sql.constraints import (
    ConstraintParseError,
    ParsedForeignKey,
    ParsedPrimaryKey,
    parse_table_constraints,
)


def test_none_and_empty_mean_no_constraints():
    assert parse_table_constraints(None).primary_key is None
    assert parse_table_constraints(None).foreign_keys == ()
    assert parse_table_constraints("").foreign_keys == ()
    assert parse_table_constraints("[]").primary_key is None


def test_single_column_primary_key():
    parsed = parse_table_constraints("[(pk_dev_silver_demo_table__id,PRIMARY KEY (`id`))]")
    assert parsed.primary_key == ParsedPrimaryKey("pk_dev_silver_demo_table__id", ("id",))
    assert parsed.foreign_keys == ()


def test_composite_primary_key_preserves_order():
    parsed = parse_table_constraints("[(pk_t,PRIMARY KEY (`a`, `b`, `c`))]")
    assert parsed.primary_key == ParsedPrimaryKey("pk_t", ("a", "b", "c"))


def test_primary_key_and_foreign_key_from_real_output():
    value = (
        "[(pk_dev_gold_order_fact,PRIMARY KEY (`order_id`)), "
        "(fk_dev_gold_order_fact_product_id_to_product_dimension_product_id,"
        "FOREIGN KEY (`product_id`) REFERENCES `dev`.`gold`.`product_dimension` (`product_id`))]"
    )
    parsed = parse_table_constraints(value)
    assert parsed.primary_key == ParsedPrimaryKey("pk_dev_gold_order_fact", ("order_id",))
    assert parsed.foreign_keys == (
        ParsedForeignKey(
            constraint_name="fk_dev_gold_order_fact_product_id_to_product_dimension_product_id",
            local_columns=("product_id",),
            referenced_table=("dev", "gold", "product_dimension"),
            referenced_columns=("product_id",),
        ),
    )


def test_composite_foreign_key_pairs_positionally():
    value = "[(fk_x,FOREIGN KEY (`a`, `b`) REFERENCES `c`.`s`.`t` (`x`, `y`))]"
    [fk] = parse_table_constraints(value).foreign_keys
    assert fk.local_columns == ("a", "b")
    assert fk.referenced_columns == ("x", "y")


def test_identifiers_are_casefolded():
    parsed = parse_table_constraints("[(PK_T,PRIMARY KEY (`ID`))]")
    assert parsed.primary_key == ParsedPrimaryKey("pk_t", ("id",))


def test_doubled_backtick_is_a_literal_backtick():
    parsed = parse_table_constraints("[(pk,PRIMARY KEY (`we``ird`))]")
    assert parsed.primary_key.columns == ("we`ird",)


def test_malformed_raises():
    with pytest.raises(ConstraintParseError):
        parse_table_constraints("not a bracketed list")
    with pytest.raises(ConstraintParseError):
        parse_table_constraints("[(only_a_name)]")


def test_primary_key_without_column_list_raises():
    with pytest.raises(ConstraintParseError):
        parse_table_constraints("[(pk_x,PRIMARY KEY)]")


def test_foreign_key_without_references_raises():
    with pytest.raises(ConstraintParseError):
        parse_table_constraints("[(fk_x,FOREIGN KEY (`a`) REFERENCES `c`.`s`.`t`)]")


def test_foreign_key_cardinality_mismatch_raises():
    with pytest.raises(ConstraintParseError):
        parse_table_constraints("[(fk_x,FOREIGN KEY (`a`, `b`) REFERENCES `c`.`s`.`t` (`x`))]")


def test_empty_primary_key_columns_raises():
    with pytest.raises(ConstraintParseError):
        parse_table_constraints("[(pk_x,PRIMARY KEY ())]")


def test_unrecognized_constraint_type_is_ignored():
    parsed = parse_table_constraints("[(pk_t,PRIMARY KEY (`id`)), (chk_amt,CHECK (`amount` > 0))]")
    assert parsed.primary_key.columns == ("id",)
    assert parsed.foreign_keys == ()
