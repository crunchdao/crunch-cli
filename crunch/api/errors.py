import sys
import typing

import inflection

from .. import utils
from .domain import *


class ApiException(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class CrunchNotFoundException(ApiException):

    def __init__(
        self,
        message: str,
        phase_type: typing.Optional[typing.Union[str, ProjectTokenType]],
        round_number: int,
        competition_name: str
    ):
        super().__init__(message)

        if isinstance(phase_type, str):
            phase_type = PhaseType[phase_type]

        self.phase_type = phase_type
        self.round_number = round_number
        self.competition_name = competition_name


class InvalidProjectTokenException(ApiException):

    def __init__(
        self,
        message: str,
        token_type: ProjectTokenType
    ):
        super().__init__(message)

        self.token_type = token_type


class NeverSubmittedException(ApiException):
    pass


class ProjectNotFoundException(ApiException):

    def __init__(
        self,
        message: str,
        competition_id: int,
        user_id: int,
    ):
        super().__init__(message)

        self.competition_id = competition_id
        self.user_id = user_id


class RoundNotFoundException(ApiException):

    def __init__(
        self,
        message: str,
        round_id: typing.Optional[int],
    ):
        super().__init__(message)

        self.round_id = round_id


# TODO Only use one class like crunch
CurrentRoundNotFoundException = RoundNotFoundException
LatestRoundNotFoundException = RoundNotFoundException
NextRoundNotFoundException = RoundNotFoundException


def convert_error(
    response: dict
):
    code = response.pop("code", "")

    props = {
        inflection.underscore(key): value
        for key, value in response.items()
    }

    error_class = find_error_class(code)
    return utils.smart_call(error_class, props)


def find_error_class(
    code: str
):
    module = sys.modules[__name__]

    if code:
        base_class_name = inflection.camelize(code.lower())

        for suffix in ["Exception", "Error"]:
            class_name = base_class_name + suffix

            clazz = getattr(module, class_name, None)
            if clazz is not None:
                return clazz

    return ApiException
