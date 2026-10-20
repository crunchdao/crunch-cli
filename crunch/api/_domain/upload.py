import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING, Any, BinaryIO, Callable

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent
from crunch.api._serialization import JsonDataClassMixin
from crunch.utils import split_into_chunks

if TYPE_CHECKING:
    from crunch.api._client import Client

_MAXIMUM_BATCH_SIZE = 100


class UploadStatus(ApiEnum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


class UploadProvider(ApiEnum):
    AWS_S3 = "AWS_S3"


class DeleteUploadResult(ApiEnum):
    DELETED = "DELETED"
    NOT_FOUND = "NOT_FOUND"


@dataclass(frozen=True)
class PresignedUploadRequest(JsonDataClassMixin):
    method: str
    url: str = field(repr=False)
    headers: dict[str, str] = field(repr=False)


@dataclass
class UploadChunk(ApiModel):
    number: int
    offset: int
    size: int
    last: bool
    completed: bool
    completed_at: datetime | None = None

    upload = Parent["Upload"]()

    @property
    def request(self) -> PresignedUploadRequest:
        return PresignedUploadRequest.from_dict(self.upload.client.api.get_json([
            "v1", "uploads", self.upload.id, "chunks", self.number, "request"
        ]))

    def confirm(self, hash: str) -> "UploadChunk":
        return self._update_from(UploadChunk.from_dict(self.upload.client.api.post_json(
            ["v1", "uploads", self.upload.id, "chunks", self.number, "confirm"],
            json={
                "hash": hash
            },
        )))

    def send(
        self,
        fd: BinaryIO,
        *,
        max_retry: int = 10,
        byte_callback: Callable[[int], None] | None = None,
        retry_callback: Callable[[], None] | None = None,
    ):
        import requests

        from crunch.utils import LimitedSizeIO

        seek_back_to = self.offset
        if not fd.seekable():
            buffer = bytearray()
            while len(buffer) < self.size:
                buffer.extend(fd.read(self.size - len(buffer)))

            fd = BytesIO(buffer)
            seek_back_to = 0

        response: requests.Response | None = None
        for retry in range(max_retry + 1):
            last = retry == max_retry

            try:
                fd.seek(seek_back_to)

                request = self.request
                response = requests.request(
                    request.method,
                    request.url,
                    headers=request.headers,
                    data=LimitedSizeIO(fd, self.size, callback=byte_callback),
                )

                response.raise_for_status()

                break
            except requests.exceptions.ConnectionError as error:
                if last:
                    raise

                print(f"retrying {retry + 1}/{max_retry} because of {error.__class__.__name__}: {str(error) or '(no message)'}")
                time.sleep(1)

                if retry_callback is not None:
                    retry_callback()

        # the last attempt either succeeds or raises
        assert response is not None, "no upload attempt was made"

        provider = self.upload.provider
        if provider == UploadProvider.AWS_S3:
            hash = response.headers["ETag"]
        else:
            raise ValueError(f"unknown provider: {provider}")

        self.confirm(hash)


@dataclass
class Upload(ApiModel):
    id: str
    name: str
    size: int
    encrypted: bool
    media_type: str
    chunked: bool
    status: UploadStatus
    provider: UploadProvider
    expires_at: datetime
    created_at: datetime
    status_message: str | None = None
    chunks: list[UploadChunk] = field(default_factory=list[UploadChunk])

    client = Parent["Client"]()

    def _update_from(self, other: ApiModel) -> "Upload":
        super()._update_from(other)

        # the new chunks still point to the other copy
        for chunk in self.chunks:
            chunk.upload = self

        return self

    def reload(self) -> "Upload":
        return self._update_from(self.client.uploads.get(self.id))

    def complete(self) -> "Upload":
        return self._update_from(self.client.uploads.from_dict(self.client.api.post_json([
            "v1", "uploads", self.id, "complete"],
            json={}
        )))

    def abort(self) -> "Upload":
        return self._update_from(self.client.uploads.from_dict(self.client.api.post_json([
            "v1", "uploads", self.id, "abort"],
            json={}
        )))

    def delete(self) -> None:
        self.client.api.call(
            "DELETE",
            ["v1", "uploads", self.id]
        )


class UploadCollection(ApiCollection[Upload]):

    model = Upload

    def _attach(self, upload: Upload) -> Upload:
        upload.client = self.client

        for chunk in upload.chunks:
            chunk.upload = upload

        return upload

    def get(
        self,
        id: str,
    ) -> Upload:
        return self.from_dict(self.client.api.get_json(["v1", "uploads", id]))

    def create(
        self,
        *,
        name: str,
        size: int,
        encrypted: bool = False,
        preferred_chunk_size: int | None = None,
    ) -> Upload:
        return self.from_dict(self.client.api.post_json(
            ["v1", "uploads"],
            json={
                "name": name,
                "size": size,
                "encrypted": encrypted,
                "preferredChunkSize": preferred_chunk_size,
            },
        ))

    def send_from_file(
        self,
        *,
        path: str,
        name: str,
        size: int | None = None,
        preferred_chunk_size: int | None = None,
        progress_bar: bool = False,
        max_retry: int = 10,
    ) -> Upload:
        if size is None:
            size = os.path.getsize(path)

        with open(path, "rb") as file:
            return self.send_from_io(
                io=file,
                name=name,
                size=size,
                preferred_chunk_size=preferred_chunk_size,
                progress_bar=progress_bar,
                max_retry=max_retry,
            )

    def send_from_io(
        self,
        *,
        io: BinaryIO,
        name: str,
        size: int,
        preferred_chunk_size: int | None = None,
        progress_bar: bool = False,
        max_retry: int = 10,
    ) -> Upload:
        from tqdm.auto import tqdm

        upload = self.create(
            name=name,
            size=size,
            encrypted=False,
            preferred_chunk_size=preferred_chunk_size,
        )

        progress = tqdm(
            total=size,
            desc=f"uploading `{name}`",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            miniters=1,
            disable=not progress_bar,
            leave=False,
        )

        try:
            for chunk in upload.chunks:
                def retry_callback():
                    progress.last_print_n = progress.n = chunk.offset
                    progress.refresh()

                if progress_bar:
                    if upload.chunked:
                        progress.desc = f"uploading `{name}` (chunk {chunk.number}/{len(upload.chunks)})"
                    else:
                        progress.desc = f"uploading `{name}` (direct)"

                chunk.send(
                    io,
                    max_retry=max_retry,
                    byte_callback=progress.update,  # type: ignore
                    retry_callback=retry_callback,
                )
        except (Exception, KeyboardInterrupt) as error:
            try:
                upload.abort()
            except Exception as error2:
                raise error from error2

            raise error
        finally:
            progress.close()

        upload.complete()

        return upload

    def batch_list(
        self,
        upload_ids: list[str],
    ) -> dict[str, Upload | None]:
        results: dict[str, Upload | None] = {}

        for upload_ids_chunk in split_into_chunks(upload_ids, _MAXIMUM_BATCH_SIZE):
            uploads: dict[str, dict[str, Any] | None] = self.client.api.post_json(
                ["v1", "uploads", "~", "list"],
                json={"uploadIds": upload_ids_chunk}
            )["uploads"]

            for upload_id in upload_ids_chunk:
                payload = uploads.get(upload_id)
                results[upload_id] = self.from_dict(payload) if payload is not None else None

        return results

    def batch_delete(
        self,
        upload_ids: list[str],
    ) -> dict[str, DeleteUploadResult]:
        results: dict[str, DeleteUploadResult] = {}

        for upload_ids_chunk in split_into_chunks(upload_ids, _MAXIMUM_BATCH_SIZE):
            uploads: dict[str, str] = self.client.api.post_json(
                ["v1", "uploads", "~", "delete"],
                json={"uploadIds": upload_ids_chunk}
            )["uploads"]

            for upload_id in upload_ids_chunk:
                results[upload_id] = DeleteUploadResult(uploads[upload_id])

        return results
