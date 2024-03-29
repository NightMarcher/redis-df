from typing import FrozenSet, Iterable, List, Optional, Set, Tuple, Union

from pandas import DataFrame, concat
from redis import Redis

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
        self.to_format_columns = None

    def get(
        self,
        uid: Union[int, str] = None,
        fields: Union[List[str], Set[str], Tuple[str]] = None,
    ) -> DataFrame:
        fields = self._check_fields(fields)
        raw_df = self._read_by_columns(uid, fields)
        if raw_df.empty:
            return raw_df
        final_df = self._format_df(raw_df)
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
    ) -> DataFrame:
        objs = []
        self.to_format_columns = []

        for col in self.columns:
            if col.name not in fields:
                continue
            self.to_format_columns.append(col)

            # TODO check client
            client = col.client or self.client
            if not client:
                # TODO log
                print(f"No client provided for {col}")
                continue
            key = col.tmpl.format(uid)

            data = col.dtype.read(client, key, col.name)
            if data.empty:
                continue
            # print(data)
            objs.append(data)
        if not objs:
            return DataFrame()
        return concat(objs, axis="columns")

    def _format_df(self, df: DataFrame) -> DataFrame:
        defaults = {}
        for col in self.to_format_columns:
            if col.converter is not None:
                df[col.name] = df[col.name].apply(
                    col.converter, **col.apply_kwargs
                )
            if col.default is not None:
                defaults[col.name] = col.default

        if defaults:
            df.fillna(defaults, inplace=True)
        return df
