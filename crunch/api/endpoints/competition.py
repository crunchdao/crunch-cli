class CompetitionEndpointMixin:

    def list_competitions(
        self
    ):
        return self._result(
            self.get(
                "/v1/competitions"
            ),
            json=True
        )

    def get_competition(
        self,
        identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{identifier}"
            ),
            json=True
        )
