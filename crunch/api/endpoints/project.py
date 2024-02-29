class ProjectEndpointMixin:

    def get_project(
        self,
        competition_identifier,
        user_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}"
            ),
            json=True
        )
