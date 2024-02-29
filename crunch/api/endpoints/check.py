class CheckEndpointMixin:

    def list_checks(
        self,
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/checks"
            ),
            json=True
        )
