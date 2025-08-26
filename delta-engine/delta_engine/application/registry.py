from delta_engine.domain.model import DesiredTable


class Registry:
    def __init__(self) -> None:
        self._tables_by_name: dict[str, DesiredTable] = {}

    def register(self, *tables: DesiredTable) -> None:
        for t in tables:
            key = str(t.qualified_name)
            if key in self._tables_by_name:
                raise ValueError(f"Duplicate table registration: {key}")
            self._tables_by_name[key] = t

    def __iter__(self):
        for key in sorted(self._tables_by_name):
            yield self._tables_by_name[key]
