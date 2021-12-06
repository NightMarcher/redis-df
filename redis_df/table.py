from redis import Redis
from typing import Iterable, List, Optional, Set, Tuple, Union

from .column import Column
from .exceptions import UndefinedColumnsError


class Table:
    def __init__(
        self,
        name: str,
        *columns: Iterable[Column],
        client: Optional[Redis] = None,
    ) -> None:
        self.name = name
        self.columns = columns
        self.client = client
        self.column_names = frozenset(col.name for col in columns)

    def read(
        self,
        id: Union[int, str],
        fields: Union[List[str], Set[str], Tuple[str], None],
    ):
        if fields:
            undefined_columns = self.column_names - set(fields)
            if undefined_columns:
                raise UndefinedColumnsError(undefined_columns)
        else:
            fields = self.column_names

        for col in self.columns:
            if col.name not in fields:
                continue
