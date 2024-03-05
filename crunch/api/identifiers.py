import typing

import typing_extensions

CompetitionIdentifierType = typing.Union[
    int,
    str
]

RoundIdentifierType = typing.Union[
    int,
    typing_extensions.Literal[
        "@current",
        "@last",
    ]
]

PhaseIdentifierType = typing.Union[
    str,
    typing_extensions.Literal[
        "submission",
        "out-of-sample",
    ],
    typing_extensions.Literal[
        "@current",
    ]
]

CrunchIdentifierType = typing.Union[
    int,
    typing_extensions.Literal[
        "@current",
        "@next",
        "@published",
    ]
]
