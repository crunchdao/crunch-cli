class PhaseEndpointMixin:

    def list_phases(
        self,
        competition_identifier,
        round_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases"
            ),
            json=True
        )

    def get_phase(
        self,
        competition_identifier,
        round_identifier,
        phase_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/{phase_identifier}"
            ),
            json=True
        )
