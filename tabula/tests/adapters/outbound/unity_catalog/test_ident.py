from tabula.adapters.outbound.unity_catalog.ident import quote_ident, table_ref
from tabula.domain.model.qualified_name import QualifiedName


def test_quote_ident_escapes_backticks_and_wraps():
    assert quote_ident("plain") == "`plain`"
    assert quote_ident("we`ird") == "`we``ird`"


def test_table_ref_backticks_and_escapes_all_parts():
    qn = QualifiedName("Cat`alog", "Sch`ema", "Na`me")
    # QualifiedName should case-fold to lowercase
    assert table_ref(qn) == "`cat``alog`.`sch``ema`.`na``me`"
