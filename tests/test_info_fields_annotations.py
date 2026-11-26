"""get_info_fields with a TInfoDict whose annotations cannot be evaluated (PEP 649)."""

from typing import TYPE_CHECKING, Any, TypedDict

from upath import UPath

from upathtools.filesystems.base import BaseFileSystem


if TYPE_CHECKING:
    from decimal import Decimal  # only imported for type checkers


class _Info(TypedDict):
    name: str
    price: Decimal


class _FS(BaseFileSystem[UPath, _Info]):
    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        return []


def test_info_fields_with_type_checking_only_annotation() -> None:
    assert _FS.get_info_fields() == ["name", "price"]
