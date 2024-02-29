class ScoreEndpointMixin:

    def list_scores(
        self,
        competition_identifier,
        user_identifier,
        prediction_id
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/predictions/{prediction_id}/scores"
            ),
            json=True
        )

    def get_score_summary(
        self,
        competition_identifier,
        user_identifier,
        prediction_id
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/predictions/{prediction_id}/scores/summary"
            ),
            json=True
        )
