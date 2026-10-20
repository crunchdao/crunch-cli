from typing import TYPE_CHECKING, Literal, TypeAlias

if TYPE_CHECKING:
    from crunch.api._domain.phase import PhaseType

CompetitionIdentifierType: TypeAlias = int | str

RoundIdentifierType: TypeAlias = int | Literal["@current", "@last"]

PhaseIdentifierType: TypeAlias = str | Literal["submission", "out-of-sample"] | "PhaseType" | Literal["@current"]

CrunchIdentifierType: TypeAlias = int | Literal["@current", "@next", "@last", "@published"]

LeaderboardIdentifierType: TypeAlias = str | Literal["@default", "@mine"]

UserIdentifierType: TypeAlias = int | str

ProjectIdentifierType: TypeAlias = int | str | Literal["@first"]

SubmissionIdentifierType: TypeAlias = int | Literal["@last"]
