from hypothesis import strategies as st

from delta_engine.domain.model.data_type import DataType, Integer, StructField

_NON_DATA_TYPE_LEAVES = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(),
    st.text(),
    st.binary(),
    st.sampled_from((DataType, Integer, StructField)),
)

NON_DATA_TYPES = st.recursive(
    _NON_DATA_TYPE_LEAVES,
    lambda children: st.one_of(
        st.lists(children, max_size=3),
        st.dictionaries(st.text(max_size=5), children, max_size=3),
        st.tuples(children, children),
    ),
    max_leaves=6,
)
