class CrunchApiException(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class NeverSubmittedException(CrunchApiException):
    pass


class CurrentCrunchNotFoundException(CrunchApiException):
    pass
