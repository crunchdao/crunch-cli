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

LeaderboardIdentifierType = typing.Union[
    str,
    typing_extensions.Literal[
        "@default",
        "@mine",
    ]
]

UserIdentifierType = typing.Union[
    int,
    str,
]

ProjectIdentifierType = typing.Union[
    int,
    str,
]
