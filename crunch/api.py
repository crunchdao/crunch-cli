class ApiException(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class NeverSubmittedException(ApiException):
    pass


class CurrentCrunchNotFoundException(ApiException):
    pass


class InvalidProjectTokenException(ApiException):
    pass
