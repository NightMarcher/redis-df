from pandas import DataFrame
from redis import Redis
from typing import FrozenSet, Iterable, List, Optional, Set, Tuple, Union

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
        self.df = DataFrame(columns=self.column_names)
        self.update_time = None

    def get(
        self,
        uid: Union[int, str] = None,
        fields: Union[List[str], Set[str], Tuple[str]] = None,
    ):
        fields = self._check_fields(fields)
        self._read_by_columns(uid, fields)

    def _check_fields(
        self,
        fields: Union[List[str], Set[str], Tuple[str]] = None,
    ) -> frozenset:
        if not fields:
            return self.column_names
        fields = frozenset(fields)
        undefined_columns = self.column_names - fields
        if undefined_columns:
            raise UndefinedColumnsError(undefined_columns)
        return fields

    def _read_by_columns(
        self,
        uid: Union[int, str],
        fields: FrozenSet[str],
    ):
        for col in self.columns:
            if col.name not in fields:
                continue

            # TODO check client
            client = col.client or self.client
            if not client:
                # TODO log
                print(f"No client provided for {col}")
                continue
            key = (col.tmpl.format(uid)
                   if uid and col.tmpl.count('{}') else col.tmpl)

            data = col.dtype.read(client, key)
            print(data)
