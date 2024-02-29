class DataReleaseEndpointMixin:

    def list_data_releases(
        self,
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/data-releases"
            ),
            json=True
        )

    def get_data_release(
        self,
        competition_identifier,
        number,
        include_splits: bool = False
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/data-releases/{number}",
                params={
                    "includeSplits": include_splits
                }
            ),
            json=True
        )
