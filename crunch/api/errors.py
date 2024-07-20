import json
import sys
import typing

import inflection

from .. import utils
from .domain import *


def _print_contact(
    and_: typing.Optional[str] = None
):
    message = "If you think that is an error"
    if and_:
        message += " and " + and_
    message += ", please contact an administrator."

    print("")
    print(message)


class ApiException(Exception):

    def __init__(self, message: str):
        self.message = message  # because python does not keep a reference himself...
        super().__init__(message)

    def print_helper(self, **kwargs):
        print(f"A problem occured: {self.message}")

        _print_contact()


class InternalServerException(ApiException):

    def __init__(self, message: str):
        super().__init__(message)

    def print_helper(self, **kwargs):
        print(f"An internal error occured: {self.message}")

        print(f"\nPlease contact an administrator.")


# microservice-related: failed to establish communication between services
RetryableException = InternalServerException
AnnotatedConnectException = InternalServerException

# java related
NullPointerException = InternalServerException


class ValidationFailedException(ApiException):

    def __init__(
        self,
        message: str,
        field_errors: list,
    ):
        super().__init__(message)

        self.field_errors = field_errors

    def print_helper(self, **kwargs):
        print(f"A validation error occured: {self.message}")

        print(json.dumps(self.field_errors, indent=4))

        _print_contact()


##
# domain
##

class CheckException(ApiException):

    def __init__(self, message: str):
        super().__init__(message)

    def print_helper(self, **kwargs):
        print(f"Checks failed with error: {self.message}")


class CompetitionNameNotFoundException(ApiException):

    def __init__(
        self,
        message: str,
        competition_name: str
    ):
        super().__init__(message)

        self.competition_name = competition_name

    def print_helper(self, **kwargs):
        print(f"Competition `{self.competition_name}` not found.")
        _print_contact()


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

    def print_helper(self, **kwargs):
        print("Crunch not found.")
        print("")
        print("The competition may be over or the server is not correctly configured.")

        _print_contact()


class CurrentPhaseNotFoundException(ApiException):

    def __init__(
        self,
        message: str
    ):
        super().__init__(message)

    def print_helper(self, **kwargs):
        print("Current phase not found.")
        print("")
        print("The competition may be over or the server is not correctly configured.")

        _print_contact()


class DailySubmissionLimitExceededException(ApiException):

    def __init__(
        self,
        message: str,
        limit: int
    ):
        super().__init__(message)

        self.limit = limit

    def print_helper(self, **kwargs):
        print("Daily submission limit exceeded.")

        print(f"\nCurrent limit: {self.limit}")

        _print_contact("you should get more")


class ForbiddenLibraryException(ApiException):

    def __init__(
        self,
        message: str,
        packages: typing.List[str]
    ):
        super().__init__(message)

        self.packages = packages

    def print_helper(self, **kwargs):
        print("Forbidden packages has been found and the server is unable to accept your work.")

        print("\nProblematic packages:")
        for package in self.packages:
            print(f"- {package}")

        _print_contact("the package should be allowed")


class InvalidProjectTokenException(ApiException):

    def __init__(
        self,
        message: str,
        token_type: ProjectTokenType
    ):
        super().__init__(message)

        self.token_type = token_type

    def print_helper(self, competition_name: str = None, **kwargs):
        from .client import Client

        print("Your token seems to have expired or is invalid.")

        if competition_name is None:
            competition_name = utils.try_get_competition_name()

        client = Client.from_env()

        if competition_name is not None:
            print("\nPlease follow this link to copy and paste your new setup command:")
            print(client.format_web_url(f'/competitions/{competition_name}/submit'))
        else:
            print("\nPlease go on the competition page and get a new setup command:")
            print(client.format_web_url(f''))

        _print_contact()


class NeverSubmittedException(ApiException):

    def __init__(self, message: str):
        super().__init__(message)

    def print_helper(self, **kwargs):
        raise NotImplementedError()


class PredictionSubmissionNotAllowedException(ApiException):

    def __init__(
        self,
        message: str,
    ):
        super().__init__(message)

    def print_helper(self, **kwargs):
        print("Prediction submission are not allowed for this competition.")


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

    def print_helper(self, **kwargs):
        from .client import Client

        print("Project not found.")

        client, project = Client.from_project()

        print("\nPlease follow this link to copy and paste your new setup command:")
        print(client.format_web_url(f'/competitions/{project.competition.name}/submit'))

        _print_contact()


class RunNotFoundException(ApiException):

    def __init__(
        self,
        message: str,
        run_id: typing.Optional[int],
    ):
        super().__init__(message)

        self.run_id = run_id

    def print_helper(self, **kwargs):
        print("Run not found.")
        print("")
        print("The run may have been removed or the project is not the owner.")

        _print_contact()


class RestrictedPhaseActionException(ApiException):

    def __init__(
        self,
        message: str,
        phase_type: str,
    ):
        super().__init__(message)

        self.phase_type = PhaseType[phase_type]

    def print_helper(self, **kwargs):
        print(f"This action cannot be done during the {self.phase_type.pretty()} phase.")

        _print_contact()


class RoundNotFoundException(ApiException):

    def __init__(
        self,
        message: str,
        round_number: typing.Optional[int],
    ):
        super().__init__(message)

        self.round_number = round_number

    def print_helper(self, **kwargs):
        print("Round not found.")
        print("")
        print("The competition may be over or the server is not correctly configured.")

        _print_contact()


# TODO Only use one class like crunch
CurrentRoundNotFoundException = RoundNotFoundException
LatestRoundNotFoundException = RoundNotFoundException
NextRoundNotFoundException = RoundNotFoundException


def convert_error(
    response: dict
):
    code = response.pop("code", "")
    message = response.pop("message", "")

    props = {
        inflection.underscore(key): value
        for key, value in response.items()
    }

    error_class = find_error_class(code)
    if error_class == ApiException:
        message = f"{code}: {message}"

    return utils.smart_call(
        error_class,
        props,
        {
            "message": message
        },
        log=False
    )


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
