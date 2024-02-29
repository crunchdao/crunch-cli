class CrunchEndpointMixin:

    def list_crunches(
        self,
        competition_identifier,
        round_identifier,
        phase_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/{phase_identifier}/crunches"
            ),
            json=True
        )

    def get_crunch(
        self,
        competition_identifier,
        round_identifier,
        phase_identifier,
        crunch_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/{phase_identifier}/crunches/{crunch_identifier}"
            ),
            json=True
        )
