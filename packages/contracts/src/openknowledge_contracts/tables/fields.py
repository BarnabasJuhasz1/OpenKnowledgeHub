"""Short spellings for the Arrow fields contracts are written in.

Every field says whether it may be null. List items are required unless stated, because a null
inside a list is rare enough that it should be declared where it occurs.
"""

from __future__ import annotations

from typing import cast

import pyarrow as pa

# pyarrow's stubs make a field generic in its type. Contracts mix every type, so they use this.
type ArrowField = pa.Field[pa.DataType]


def required(name: str, type_: pa.DataType) -> ArrowField:
    """A field that is always present and never null."""
    return pa.field(name, type_, nullable=False)


def optional(name: str, type_: pa.DataType) -> ArrowField:
    """A field that may be null or missing."""
    return pa.field(name, type_, nullable=True)


def list_of(item: pa.DataType, *, nullable_items: bool = False) -> pa.ListType[pa.DataType]:
    """A list of ``item`` values."""
    return pa.list_(pa.field("item", item, nullable=nullable_items))


def struct_of(*fields: ArrowField) -> pa.StructType:
    """An object with exactly these keys."""
    return pa.struct(fields)


def string_map(*, nullable_values: bool) -> pa.MapType[pa.DataType, pa.DataType]:
    """An object with arbitrary string keys and string values, kept with their original keys."""
    value = pa.field("value", pa.string(), nullable=nullable_values)
    # pyarrow takes a field here, which carries the values' nullability; the stubs list only a type.
    return cast(
        "pa.MapType[pa.DataType, pa.DataType]",
        pa.map_(pa.string(), value),  # pyright: ignore[reportArgumentType]
    )


def fields_of(type_: pa.Schema | pa.StructType) -> list[ArrowField]:
    """The fields of a schema or struct, in order.

    The stubs leave the type of these fields open, so code that walks a schema reads them here.
    """
    return list(type_)
