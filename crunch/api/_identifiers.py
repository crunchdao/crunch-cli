from typing import Union

from typing_extensions import Literal

CompetitionIdentifierType = Union[
    int,
    str
]

RoundIdentifierType = Union[
    int,
    Literal[
        "@current",
        "@last",
    ]
]

PhaseIdentifierType = Union[
    str,
    Literal[
        "submission",
        "out-of-sample",
    ],
    Literal[
        "@current",
    ]
]

CrunchIdentifierType = Union[
    int,
    Literal[
        "@current",
        "@next",
        "@published",
    ]
]

LeaderboardIdentifierType = Union[
    str,
    Literal[
        "@default",
        "@mine",
    ]
]

UserIdentifierType = Union[
    int,
    str,
]

ProjectIdentifierType = Union[
    int,
    str,
]
