class PredictionEndpointMixin:

    def list_predictions(
        self,
        competition_identifier,
        user_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/predictions"
            ),
            json=True
        )

    def get_prediction(
        self,
        competition_identifier,
        user_identifier,
        prediction_id
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/predictions/{prediction_id}"
            ),
            json=True
        )
