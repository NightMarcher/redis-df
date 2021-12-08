from pandas import DataFrame, concat
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
        self.converters = {col.name: col.converter for col in columns}
        self.column_names = frozenset(self.converters)

    def get(
        self,
        uid: Union[int, str] = None,
        fields: Union[List[str], Set[str], Tuple[str]] = None,
    ):
        fields = self._check_fields(fields)
        raw_df = self._read_by_columns(uid, fields)
        print(raw_df.to_markdown())

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
    ) -> DataFrame:
        objs = []
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

            objs.append(col.dtype.read(client, key, col.name))
        return concat(objs, axis="columns")
