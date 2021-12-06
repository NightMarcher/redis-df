from typing import Union

from .dtype import Hash, List, Set


class Column:
    def __init__(
        self,
        name: str,
        dtype: Union[Hash, List, Set],
        tmpl: str,
    ) -> None:
        self.name = name
        self.dtype = dtype
        self.tmpl = tmpl
