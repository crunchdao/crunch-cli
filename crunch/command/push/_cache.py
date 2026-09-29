import hashlib
import json
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, List, NewType, Optional, Tuple, cast

from mashumaro import field_options
from mashumaro.config import BaseConfig
from mashumaro.mixins.dict import DataClassDictMixin

from crunch.api import ApiException, Client, Upload, UploadStatus
from crunch.constants import DOT_CRUNCH_DIRECTORY, UPLOAD_CACHE_FILE, UPLOAD_CACHE_VERSION

_INITIAL_TIME_TO_LIVE = 3
_EARLY_DELETE_THRESHOLD = timedelta(hours=3)


@dataclass
class KnownLocation(DataClassDictMixin):
    path: str
    size: int
    modification_time: datetime

    def fast_check(self, size: int, modification_time: datetime) -> bool:
        return self.size == size and self.modification_time == modification_time

    class Config(BaseConfig):
        lazy_compilation = True
        serialize_by_alias = True
        aliases = {
            "modification_time": "modificationTime"
        }


Checksum = NewType("Checksum", str)


@dataclass
class UploadCacheEntry(DataClassDictMixin):
    checksum: Checksum
    upload_id: str
    expires_at: datetime
    time_to_live: int
    known_locations: list[KnownLocation] = field(default_factory=cast(Callable[[], List[KnownLocation]], list),)

    upload: Upload | None = field(
        default=None,
        init=False,
        metadata=field_options(serialize="omit"),
    )

    def find_known_location(self, relative_path: str) -> Optional[KnownLocation]:
        for location in self.known_locations:
            if location.path == relative_path:
                return location

    class Config(BaseConfig):
        lazy_compilation = True
        serialize_by_alias = True
        aliases = {
            "upload_id": "uploadId",
            "expires_at": "expiresAt",
            "time_to_live": "timeToLive",
            "known_locations": "knownLocations",
        }


class UploadCache(ABC):

    @abstractmethod
    def try_reuse_file(self, *, relative_path: str, absolute_path: str) -> Tuple[Checksum, Optional[Upload]]:
        ...

    @abstractmethod
    def try_reuse_bytes(self, *, data: bytes) -> Tuple[Checksum, Optional[Upload]]:
        ...

    @abstractmethod
    def register_file(self, *, checksum: Checksum, upload: Upload, relative_path: str, size: int, modification_time: datetime) -> None:
        ...

    @abstractmethod
    def register_bytes(self, *, checksum: Checksum, upload: Upload) -> None:
        ...

    @abstractmethod
    def persist(self) -> None:
        ...


class NoUploadCache(UploadCache):

    def try_reuse_file(self, *, relative_path: str, absolute_path: str) -> Tuple[Checksum, Optional[Upload]]:
        return (Checksum("none"), None)

    def try_reuse_bytes(self, *, data: bytes) -> Tuple[Checksum, Optional[Upload]]:
        return (Checksum("none"), None)

    def register_file(self, *, checksum: Checksum, upload: Upload, relative_path: str, size: int, modification_time: datetime) -> None:
        pass

    def register_bytes(self, *, checksum: Checksum, upload: Upload) -> None:
        pass

    def persist(self) -> None:
        pass


class FileUploadCache(UploadCache):

    def __init__(
        self,
        directory: str,
        entries: List[UploadCacheEntry],
    ):
        self._directory = directory
        self._entries = entries

    def try_reuse_file(self, *, relative_path: str, absolute_path: str) -> Tuple[Checksum, Optional[Upload]]:
        checksum, stat = self._compute_checksum(relative_path, absolute_path)

        entry = self._find_entry_by_checksum(checksum)
        if entry is None:
            return (checksum, None)

        assert entry.upload is not None, "a cache entry must always carry a validated upload"
        entry.time_to_live = _INITIAL_TIME_TO_LIVE

        self._touch(entry, relative_path, stat)

        return (checksum, entry.upload)

    def try_reuse_bytes(self, *, data: bytes) -> Tuple[Checksum, Optional[Upload]]:
        checksum = sha256_bytes(data)

        entry = self._find_entry_by_checksum(checksum)
        if entry is None:
            return (checksum, None)

        assert entry.upload is not None, "a cache entry must always carry a validated upload"
        entry.time_to_live = _INITIAL_TIME_TO_LIVE

        return (checksum, entry.upload)

    def register_file(self, *, checksum: Checksum, upload: Upload, relative_path: str, size: int, modification_time: datetime) -> None:
        entry = self._register_checksum(checksum, upload)
        entry.known_locations.append(KnownLocation(
            path=relative_path,
            size=size,
            modification_time=modification_time,
        ))

    def register_bytes(self, *, checksum: Checksum, upload: Upload) -> None:
        self._register_checksum(checksum, upload)

    def persist(self) -> None:
        try:
            path = _cache_file_path(self._directory)
            directory_path = os.path.dirname(path)
            os.makedirs(directory_path, exist_ok=True)

            tmpfd, temporary_path = tempfile.mkstemp(
                prefix=f".{UPLOAD_CACHE_FILE}.",
                dir=directory_path,
            )

            try:
                with os.fdopen(tmpfd, "w") as fd:
                    content: Any = {
                        "version": UPLOAD_CACHE_VERSION,
                        "entries": [
                            entry.to_dict()
                            for entry in self._entries
                        ],
                    }

                    json.dump(content, fd)

                os.replace(temporary_path, path)
            except BaseException:
                os.unlink(temporary_path)
                raise
        except Exception as exception:
            print(f"upload cache: could not be saved: {exception}")

    def _find_entry_by_checksum(self, checksum: str) -> Optional[UploadCacheEntry]:
        return next(
            (entry for entry in self._entries if entry.checksum == checksum),
            None,
        )

    def _compute_checksum(self, relative_path: str, absolute_path: str) -> Tuple[Checksum, os.stat_result]:
        stat = os.stat(absolute_path)
        modification_time = to_modification_time(stat)

        for entry in self._entries:
            existing = entry.find_known_location(relative_path)
            if existing is None:
                continue

            if existing.fast_check(stat.st_size, modification_time):
                return entry.checksum, stat

            entry.known_locations.remove(existing)  # will be added back by _touch later

        return sha256_file(absolute_path), stat

    def _register_checksum(self, checksum: Checksum, upload: Upload) -> UploadCacheEntry:
        entry = self._find_entry_by_checksum(checksum)
        if entry is None:
            entry = UploadCacheEntry(
                checksum=checksum,
                upload_id=upload.id,
                expires_at=upload.expires_at,
                time_to_live=_INITIAL_TIME_TO_LIVE,
            )

            self._entries.append(entry)
        else:
            entry.upload_id = upload.id
            entry.expires_at = upload.expires_at
            entry.time_to_live = _INITIAL_TIME_TO_LIVE

        entry.upload = upload

        return entry

    def _touch(self, entry: UploadCacheEntry, relative_path: str, stat: os.stat_result) -> None:
        existing = entry.find_known_location(relative_path)
        if existing is not None:
            existing.size = stat.st_size
            existing.modification_time = to_modification_time(stat)

        else:
            entry.known_locations.append(KnownLocation(
                path=relative_path,
                size=stat.st_size,
                modification_time=to_modification_time(stat),
            ))

    @staticmethod
    def load(directory: str, client: Client) -> "FileUploadCache":
        entries = _load_upload_cache(directory)
        entries = _reconcile_upload_cache_entries(entries, client)
        entries = _age_upload_cache_entries(entries, client)

        return FileUploadCache(directory, entries)


def _cache_file_path(directory: str) -> str:
    return os.path.join(directory, DOT_CRUNCH_DIRECTORY, UPLOAD_CACHE_FILE)


def _load_upload_cache(directory: str) -> List[UploadCacheEntry]:
    path = _cache_file_path(directory)
    if not os.path.exists(path):
        return []

    try:
        with open(path) as fd:
            content = json.load(fd)

        version = content.get("version")
        if version != UPLOAD_CACHE_VERSION:
            print(f"{path}: unsupported version {version}, starting with an empty cache")
            return []

        return [
            UploadCacheEntry.from_dict(raw_entry)  # pyright: ignore[reportUnknownMemberType]
            for raw_entry in content.get("entries", [])
        ]
    except Exception as error:
        print(f"{path}: could not be read, starting with an empty cache ({error.__class__.__name__}: {error})")
        return []


def _reconcile_upload_cache_entries(
    entries: List[UploadCacheEntry],
    client: Client,
    *,
    now: Optional[datetime] = None,
) -> List[UploadCacheEntry]:
    if now is None:
        now = datetime.now(tz=timezone.utc).replace(tzinfo=None)

    # delete already expired
    entries = _delete_matching_upload(client, entries, lambda entry: entry.expires_at <= now)

    # delete expiring soon
    entries = _delete_matching_upload(client, entries, lambda entry: entry.expires_at - now < _EARLY_DELETE_THRESHOLD)

    # delete invalid ones
    def should_delete(entry: UploadCacheEntry) -> bool:
        upload = results.get(entry.upload_id)
        if upload is None or upload.status != UploadStatus.SUCCEEDED:
            return True

        entry.upload = upload
        return False

    results = client.uploads.batch_list([entry.upload_id for entry in entries])
    entries = _delete_matching_upload(client, entries, should_delete)

    return entries


def _age_upload_cache_entries(
    entries: List[UploadCacheEntry],
    client: Client,
) -> List[UploadCacheEntry]:
    def should_delete(entry: UploadCacheEntry) -> bool:
        entry.time_to_live -= 1

        return entry.time_to_live <= 0

    return _delete_matching_upload(client, entries, should_delete)


def _delete_matching_upload(
    client: Client,
    entries: List[UploadCacheEntry],
    predicate: Callable[[UploadCacheEntry], bool],
) -> List[UploadCacheEntry]:
    matched_ids = {
        entry.upload_id
        for entry in entries
        if predicate(entry)
    }

    if matched_ids:
        entries = [
            entry
            for entry in entries
            if entry.upload_id not in matched_ids
        ]

    try:
        client.uploads.batch_delete(list(matched_ids))
    except ApiException as exception:
        print(f"upload cache: cleanup error {exception}")

    return entries


def to_modification_time(stat: os.stat_result) -> datetime:
    return datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).replace(tzinfo=None)


def sha256_file(path: str, buffer_size: int = 4 * 1024 * 1024) -> Checksum:
    hasher = hashlib.sha256()
    buffer = bytearray(buffer_size)
    buffer_view = memoryview(buffer)

    with open(path, "rb") as fd:
        bytes_read = fd.readinto(buffer)
        while bytes_read > 0:
            hasher.update(buffer_view[:bytes_read])
            bytes_read = fd.readinto(buffer)

    return Checksum(f"sha256:{hasher.hexdigest()}")


@staticmethod
def sha256_bytes(data: bytes) -> Checksum:
    return Checksum(f"sha256:{hashlib.sha256(data).hexdigest()}")
