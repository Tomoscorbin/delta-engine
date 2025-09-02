# def test_delta_table_rejects_duplicate_column_names_case_insensitive() -> None:
#     cols = [UserColumn("ID", Integer(), is_nullable=False), UserColumn("id", Integer())]
#     with pytest.raises(ValueError, match="Duplicate column name"):
#         DeltaTable(catalog="dev", schema="silver", name="people", columns=cols)
