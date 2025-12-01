from __future__ import annotations

import dataclasses
from typing import Literal


TreeStyleStr = Literal[
    "regular",
    "ansi",
    "ascii",
    "const",
    "const_bold",
    "rounded",
    "double",
    "spaces",
]


@dataclasses.dataclass(frozen=True)
class TreeStyle:
    identifier: TreeStyleStr
    filename_middle: str
    filename_last: str
    parent_middle: str
    parent_last: str


regular_style = TreeStyle(
    identifier="regular",
    filename_middle="├──",
    filename_last="└──",
    parent_middle="    ",
    parent_last="│   ",
)
ansi_style = TreeStyle(
    identifier="ansi",
    filename_middle="|-- ",
    filename_last="`-- ",
    parent_middle="    ",
    parent_last="|   ",
)
ascii_style = TreeStyle(
    identifier="ascii",
    filename_middle="|-- ",
    filename_last="+-- ",
    parent_middle="    ",
    parent_last="|   ",
)
const_style = TreeStyle(
    identifier="const",
    filename_middle="\u251c\u2500\u2500 ",
    filename_last="\u2514\u2500\u2500 ",
    parent_middle="    ",
    parent_last="\u2502   ",
)
const_bold_style = TreeStyle(
    identifier="const_bold",
    filename_middle="\u2523\u2501\u2501 ",
    filename_last="\u2517\u2501\u2501 ",
    parent_middle="    ",
    parent_last="\u2503   ",
)
rounded_style = TreeStyle(
    identifier="rounded",
    filename_middle="\u251c\u2500\u2500 ",
    filename_last="\u2570\u2500\u2500 ",
    parent_middle="    ",
    parent_last="\u2502   ",
)
double_style = TreeStyle(
    identifier="double",
    filename_middle="\u2560\u2550\u2550 ",
    filename_last="\u255a\u2550\u2550 ",
    parent_middle="    ",
    parent_last="\u2551   ",
)
spaces_style = TreeStyle(
    identifier="spaces",
    filename_middle="    ",
    filename_last="    ",
    parent_middle="    ",
    parent_last="    ",
)

STYLES: dict[TreeStyleStr, TreeStyle] = {
    style.identifier: style
    for style in [
        regular_style,
        ansi_style,
        ascii_style,
        const_style,
        const_bold_style,
        rounded_style,
        double_style,
        spaces_style,
    ]
}
