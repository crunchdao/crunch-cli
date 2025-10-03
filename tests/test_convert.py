import textwrap
from typing import Any, Dict, List, Literal

from crunch.convert import extract_cells


def _cell(
    id: str,
    type: Literal["markdown", "code"],
    source: List[str],
) -> Dict[str, Any]:
    return {
        "metadata": {
            "id": id,
        },
        "cell_type": type,
        "source": source,
    }


def test_normal():
    (
        source_code,
        _,
        _,
    ) = extract_cells([
        _cell("a", "code", [
            "# Hello World",
        ]),
        _cell("b", "code", [
            "a = 42",
            "def hello(x):",
            "    return x + 1",
        ]),
        _cell("c", "code", [
            "a += 1",
            "",
            "class Model:",
            "    pass",
        ])
    ])

    content = textwrap.dedent("""
        # Hello World
        
        
        #a = 42
        def hello(x):
            return x + 1
        
        
        #a += 1
        
        class Model:
            pass
    """).lstrip()

    assert content == source_code
