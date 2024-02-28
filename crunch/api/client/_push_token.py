import typing

from ._base import Client, ClientConfiguration


class PushTokenClient(Client):

    def __init__(
        self,
        push_token: str,
        configuration: typing.Optional[ClientConfiguration]=None,
    ):
        super().__init__(configuration)

        self._session.params.update({
            "pushToken": push_token
        })

    def orthogonalization():
        pass
