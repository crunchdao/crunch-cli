import typing

CompetitionIdentifierType = typing.Union[
    int,
    str
]

RoundIdentifierType = typing.Union[
    int,
    typing.Literal[
        "@current",
        "@last",
    ]
]

PhaseIdentifierType = typing.Union[
    str,
    typing.Literal[
        "submission",
        "out-of-sample",
    ],
    typing.Literal[
        "@current",
    ]
]

CrunchIdentifierType = typing.Union[
    int,
    typing.Literal[
        "@current",
        "@next",
        "@published",
    ]
]
