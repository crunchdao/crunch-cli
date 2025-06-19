import dataclasses
import enum
import typing

import dataclasses_json
import requests

from ..resource import Collection, Model


class UploadStatus(enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


class UploadProvider(enum.Enum):
    AWS_S3 = "AWS_S3"


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE,
)
@dataclasses.dataclass(frozen=True)
class PresignedUploadRequest:

    method: str
    url: str
    headers: dict


class Upload(Model):

    resource_identifier_attribute = "id"

    @property
    def chunked(self):
        return self._attrs["chunked"]

    @property
    def status(self):
        return UploadStatus[self._attrs["status"]]

    @property
    def status_message(self):
        return self._attrs["statusMessage"]

    @property
    def provider(self):
        return UploadProvider[self._attrs["provider"]]

    @property
    def chunks(self) -> typing.List["UploadChunk"]:
        return [
            UploadChunk(self, chunk_attrs, self._client)
            for chunk_attrs in self._attrs["chunks"]
        ]

    def complete(self):
        self._attrs.update(
            self._client.api.complete_upload(
                self.id,
            )
        )

    def abort(self):
        self._attrs.update(
            self._client.api.abort_upload(
                self.id,
            )
        )


class UploadChunk(Model):

    id_attribute = None
    resource_identifier_attribute = "number"

    def __init__(
        self,
        upload: Upload,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._upload = upload

    @property
    def upload(self):
        return self._upload

    @property
    def number(self) -> int:
        return self._attrs["number"]

    @property
    def offset(self) -> int:
        return self._attrs["offset"]

    @property
    def size(self) -> int:
        return self._attrs["size"]

    @property
    def last(self) -> bool:
        return self._attrs["last"]

    @property
    def completed(self) -> bool:
        return self._attrs["completed"]

    @property
    def request(self):
        return PresignedUploadRequest.from_dict(
            self._client.api.get_upload_chunk_request(
                self.upload.id,
                self.number,
            )
        )

    def confirm(self, hash: str):
        self._attrs.update(
            self._client.api.confirm_upload_chunk(
                self.upload.id,
                self.number,
                hash,
            )
        )

    def send(self, fd: typing.IO[bytes]):
        from ...utils import LimitedSizeIO

        fd.seek(self.offset)

        request = self.request
        response = requests.request(
            request.method,
            request.url,
            headers=request.headers,
            data=LimitedSizeIO(fd, self.size),
        )

        response.raise_for_status()

        provider = self.upload.provider
        if provider == UploadProvider.AWS_S3:
            hash = response.headers["ETag"]
        else:
            raise ValueError(f"unknown provider: {provider}")

        self.confirm(hash)


class UploadCollection(Collection):

    model = Upload

    def __iter__(self) -> typing.Iterator[Upload]:
        return super().__iter__()

    def create(
        self,
        name: str,
        size: int,
        preferred_chunk_size: typing.Optional[int] = None
    ) -> Upload:
        return self.prepare_model(
            self._client.api.create_upload(
                name,
                size,
                preferred_chunk_size
            )
        )

    def send_from_file(
        self,
        path: str,
        name: str,
        size: int,
        preferred_chunk_size: typing.Optional[int] = None
    ) -> Upload:
        upload = self.create(name, size, preferred_chunk_size)

        try:
            # print(upload._attrs)
            with open(path, "rb") as fd:
                for chunk in upload.chunks:
                    # print(chunk)
                    chunk.send(fd)
        except Exception as error:
            try:
                upload.abort()
            except Exception as error2:
                raise error from error2

            raise error

        upload.complete()

        return upload

    def get(
        self,
        id: str
    ) -> Upload:
        return self.prepare_model(
            self._client.api.get_upload(
                id
            )
        )


class UploadEndpointMixin:

    def create_upload(
        self,
        name,
        size,
        preferred_chunk_size
    ):
        return self._result(
            self.post(
                "/v1/uploads",
                json={
                    "name": name,
                    "size": size,
                    "preferredChunkSize": preferred_chunk_size
                }
            ),
            json=True
        )

    def get_upload(
        self,
        id
    ):
        return self._result(
            self.get(
                f"/v1/uploads/{id}"
            ),
            json=True
        )

    def get_upload_chunk_request(
        self,
        id,
        chunk_number
    ):
        return self._result(
            self.get(
                f"/v1/uploads/{id}/chunks/{chunk_number}/request",
            ),
            json=True
        )

    def confirm_upload_chunk(
        self,
        id,
        chunk_number,
        hash
    ):
        return self._result(
            self.post(
                f"/v1/uploads/{id}/chunks/{chunk_number}/confirm",
                json={
                    "hash": hash,
                },
            ),
            json=True
        )

    def abort_upload(
        self,
        id
    ):
        return self._result(
            self.post(
                f"/v1/uploads/{id}/abort",
                json={},
            ),
            json=True
        )

    def complete_upload(
        self,
        id
    ):
        return self._result(
            self.post(
                f"/v1/uploads/{id}/complete",
                json={},
            ),
            json=True
        )
