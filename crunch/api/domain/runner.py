import typing

from ..resource import Model


class RunnerRun(Model):

    def __init__(
        self,
        run_id: int,
        client=None
    ):
        super().__init__({}, client, None)

        self._run_id = run_id

    @property
    def code(self) -> typing.Dict[str, str]:
        return self._client.api.get_runner_run_code(
            self._run_id
        )

    @property
    def model(self) -> typing.Dict[str, str]:
        return self._client.api.get_runner_run_model(
            self._run_id
        )

    @property
    def data(self):
        from .data_release import DataRelease, DataReleaseCollection

        data_release_attrs = self._client.api.get_runner_run_data(
            self._run_id
        )

        data_release = DataReleaseCollection(None).prepare_model(data_release_attrs)
        return typing.cast(DataRelease, data_release)

    def report_current(
        self,
        work: str,
        moon: typing.Optional[int]
    ):
        self._client.api.report_runner_run_current(
            self._run_id,
            work,
            moon
        )

    def report_trace(
        self,
        content: str,
        moon: typing.Optional[int]
    ):
        self._client.api.report_runner_run_trace(
            self._run_id,
            content,
            moon
        )

    def submit_result(
        self,
        use_initial_model: bool,
        deterministic: typing.Optional[bool],
        files: typing.List[typing.Tuple]
    ):
        self._client.api.submit_runner_run_result(
            self._run_id,
            use_initial_model,
            deterministic,
            files
        )


class RunnerRunEndpointMixin:

    def get_runner_run_code(
        self,
        run_id,
    ):
        return self._result(
            self.get(
                f"/v1/runner/runs/{run_id}/code"
            ),
            json=True
        )

    def get_runner_run_data(
        self,
        run_id,
    ):
        return self._result(
            self.get(
                f"/v1/runner/runs/{run_id}/data"
            ),
            json=True
        )

    def get_runner_run_model(
        self,
        run_id,
    ):
        return self._result(
            self.get(
                f"/v1/runner/runs/{run_id}/model"
            ),
            json=True
        )

    def report_runner_run_current(
        self,
        run_id,
        work,
        moon,
    ):
        return self._result(
            self.post(
                f"/v1/runner/runs/{run_id}/current",
                json={
                    "work": work,
                    "moon": moon,
                }
            )
        )

    def report_runner_run_trace(
        self,
        run_id,
        content,
        moon,
    ):
        return self._result(
            self.post(
                f"/v1/runner/runs/{run_id}/trace",
                json={
                    "content": content,
                    "moon": moon,
                }
            )
        )

    def submit_runner_run_result(
        self,
        run_id,
        use_initial_model,
        deterministic,
        files
    ):
        return self._result(
            self.post(
                f"/v1/runner/runs/{run_id}/result",
                files=tuple(files),
                data={
                    "useInitialModel": use_initial_model,
                    "deterministic": deterministic,
                },
            )
        )
