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
        self.to_process_columns = None

    def get(
        self,
        uid: Union[int, str] = None,
        fields: Union[List[str], Set[str], Tuple[str]] = None,
    ):
        fields = self._check_fields(fields)
        raw_df = self._read_by_columns(uid, fields)
        if raw_df is None:
            return None
        final_df = self._process_df(raw_df)
        return final_df

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
    ) -> Optional[DataFrame]:
        objs = []
        self.to_process_columns = []
        for col in self.columns:
            if col.name not in fields:
                continue

            self.to_process_columns.append(col)
            # TODO check client
            client = col.client or self.client
            if not client:
                # TODO log
                print(f"No client provided for {col}")
                continue
            key = (col.tmpl.format(uid)
                   if uid and col.tmpl.count('{}') else col.tmpl)

            objs.append(col.dtype.read(client, key, col.name))
        if not objs:
            return None
        return concat(objs, axis="columns")

    def _process_df(self, df: DataFrame) -> DataFrame:
        defaults = {}
        for col in self.to_process_columns:
            if col.converter is not None:
                df[col.name] = df[col.name].map(col.converter)
            if col.default is not None:
                defaults[col.name] = col.default

        if defaults:
            df.fillna(defaults, inplace=True)
        return df
