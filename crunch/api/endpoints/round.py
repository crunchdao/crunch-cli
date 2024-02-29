class RoundEndpointMixin:

    def list_rounds(
        self,
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/rounds"
            ),
            json=True
        )

    def get_round(
        self,
        competition_identifier,
        identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/rounds/{identifier}"
            ),
            json=True
        )
