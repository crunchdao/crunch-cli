import secrets
from enum import Enum
from io import BytesIO, UnsupportedOperation
from typing import Any, BinaryIO, List, Optional, Self, Tuple, Union

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.ciphers import (AEADDecryptionContext,
                                                    Cipher, algorithms, modes)


def _derive_key(
    *,
    backend: Any,
    shared_secret: bytes,
    key_length: int = 32,
) -> bytes:
    """
    Derive encryption/decryption key from shared secret using SHA256.

    Args:
        shared_secret: The ECDH shared secret
        key_length: Length of derived key in bytes (default: 32 for AES-256)

    Returns:
        Derived key bytes
    """

    # For lengths > 32, we need to use multiple rounds or different approach
    if key_length <= 32:
        digest = hashes.Hash(hashes.SHA256(), backend=backend)
        digest.update(shared_secret)
        derived = digest.finalize()
        return derived[:key_length]

    else:
        # For longer keys, use multiple SHA256 rounds
        result = b""
        counter = 0

        while len(result) < key_length:
            digest = hashes.Hash(hashes.SHA256(), backend=backend)
            digest.update(shared_secret)
            digest.update(counter.to_bytes(4, "big"))
            result += digest.finalize()
            counter += 1

        return result[:key_length]


RecipientPublicKeyPem = str
EphemeralPublicKeyPem = str
MasterPublicKeyPem = str
MasterPrivateKeyPem = str


class _EncryptionStep(Enum):
    INITIALIZATION_VECTOR = "INITIALIZATION_VECTOR"
    FILE = "FILE"
    AUTH_TAG = "AUTH_TAG"
    DONE = "DONE"


class ECIESReadableStream(BinaryIO):

    def __init__(
        self,
        file_io: BinaryIO,
    ):
        super().__init__()

        if hasattr(file_io, "mode"):
            assert file_io.mode == "rb", "file_io must be opened in binary read mode ('rb')"

        self._file_io = file_io
        self._closed = False

        self._step = _EncryptionStep.INITIALIZATION_VECTOR

    @property
    def mode(self) -> str:
        return "rb"

    def close(self) -> None:
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def fileno(self) -> int:
        return self._file_io.fileno()

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return self._file_io.isatty()

    def readable(self) -> bool:
        return self._step != _EncryptionStep.DONE

    def readline(self, limit: int = -1) -> bytes:
        raise UnsupportedOperation()

    def readlines(self, hint: int = -1) -> List[bytes]:
        raise UnsupportedOperation()

    def seek(self, offset: int, whence: int = 0) -> int:
        raise UnsupportedOperation()

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return -1

    def truncate(self, size: Optional[int] = None) -> int:
        raise UnsupportedOperation()

    def writable(self) -> bool:
        return False

    def write(self, s: Union[bytes, bytearray]) -> int:  # type: ignore
        raise UnsupportedOperation()

    def writelines(self, lines: List[bytes]) -> None:  # type: ignore
        raise UnsupportedOperation()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type, value, traceback) -> None:  # type: ignore
        pass


class ECIESEncryptStream(ECIESReadableStream):
    """
    ECIES encryption using secp256k1 curve and AES-GCM.
    """

    def __init__(
        self,
        file_io: BinaryIO,
        *,
        public_key_pem: RecipientPublicKeyPem,
        initialization_vector: Optional[bytes] = None,
        backend: Any = None,
    ):
        super().__init__(
            file_io=file_io,
        )

        self._current_position = 0

        self._backend = backend or default_backend()

        # Parse recipient's public key
        recipient_public_key = serialization.load_pem_public_key(
            public_key_pem.encode("utf-8"),
            backend=self._backend,
        )
        assert isinstance(recipient_public_key, ec.EllipticCurvePublicKey), "Recipient public key must be an EllipticCurvePublicKey"

        # Generate ephemeral keypair using the same curve as the recipient
        recipient_curve = recipient_public_key.curve
        ephemeral_private_key = ec.generate_private_key(
            curve=recipient_curve,
            backend=self._backend
        )

        # Get ephemeral public key in PEM format
        ephemeral_public_key = ephemeral_private_key.public_key()
        self._ephemeral_public_key_pem = ephemeral_public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ).decode("utf-8")

        # Perform ECDH to get shared secret
        shared_secret = ephemeral_private_key.exchange(
            algorithm=ec.ECDH(),
            peer_public_key=recipient_public_key
        )

        # Derive encryption key
        encryption_key = _derive_key(
            backend=backend,
            shared_secret=shared_secret,
        )

        # Generate random IV for AES-GCM (12 bytes - 96-bit IV for GCM)
        if initialization_vector is None:
            initialization_vector = secrets.token_bytes(12)
        self._initialization_vector = initialization_vector

        # Encrypt data using AES-GCM
        self._cipher = Cipher(
            algorithms.AES(encryption_key),
            modes.GCM(initialization_vector),
            backend=self._backend,
        )

        self._encryptor = self._cipher.encryptor()

    @property
    def name(self) -> str:
        return f"ecies:encrypt:{self._file_io.name}"

    def read(self, n: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        if n == -1:
            n = 4096

        if self._step == _EncryptionStep.INITIALIZATION_VECTOR:
            read_length = min(
                len(self._initialization_vector) - self._current_position,
                n
            )

            data = self._initialization_vector[self._current_position:self._current_position + read_length]
            self._current_position += read_length

            if self._current_position >= len(self._initialization_vector):
                self._step = _EncryptionStep.FILE
                self._current_position = 0

            return data

        elif self._step == _EncryptionStep.FILE:
            data = self._file_io.read(n)

            if not data:
                final_data = self._encryptor.finalize()
                assert len(final_data) == 0, "finalization with data is not supported"

                self._step = _EncryptionStep.AUTH_TAG
                self._auth_tag = self._encryptor.tag

                return self.read(n)

            data = self._encryptor.update(data)

            return data

        elif self._step == _EncryptionStep.AUTH_TAG:
            assert getattr(self, "_auth_tag", None) is not None, "auth tag not set"

            read_length = min(
                len(self._auth_tag) - self._current_position,
                n
            )

            data = self._auth_tag[self._current_position:self._current_position + read_length]
            self._current_position += read_length

            if self._current_position >= len(self._auth_tag):
                self._step = _EncryptionStep.DONE
                self._current_position = 0

            return data

        elif self._step == _EncryptionStep.DONE:
            return b""

        else:
            raise UnsupportedOperation("invalid step")

    def ephemeral_public_key_pem(self) -> EphemeralPublicKeyPem:
        return self._ephemeral_public_key_pem


class ECIESDecryptStream(ECIESReadableStream):
    """
    ECIES decryption using secp256k1 curve and AES-GCM.
    """

    def __init__(
        self,
        file_io: BinaryIO,
        file_length: int,
        *,
        private_key_pem: MasterPrivateKeyPem,
        ephemeral_public_key_pem: EphemeralPublicKeyPem,
        backend: Any = None,
    ):
        super().__init__(
            file_io=file_io,
        )

        assert file_length >= 12 + 16, "file_length must be greater than 12 + 16 bytes (initialization vector + tag)"

        self._content_length = file_length - 12 - 16
        self._read_length = 0

        self._backend = backend or default_backend()

        # Convert hex private key to private key object
        private_key_bytes = bytes.fromhex(private_key_pem)
        private_key = ec.derive_private_key(
            private_value=int.from_bytes(private_key_bytes, "big"),
            curve=ec.SECP256K1(),
            backend=self._backend
        )
        assert isinstance(private_key, ec.EllipticCurvePrivateKey), "private key must be an EllipticCurvePrivateKey"

        # Parse ephemeral public key
        ephemeral_public_key = serialization.load_pem_public_key(
            data=ephemeral_public_key_pem.encode("utf-8"),
            backend=self._backend
        )
        assert isinstance(ephemeral_public_key, ec.EllipticCurvePublicKey), "ephemeral public key must be an EllipticCurvePublicKey"

        # Perform ECDH to get shared secret
        shared_secret = private_key.exchange(
            algorithm=ec.ECDH(),
            peer_public_key=ephemeral_public_key,
        )

        self._decryption_key = _derive_key(
            backend=self._backend,
            shared_secret=shared_secret
        )

        self._cipher: Cipher[modes.GCM] = None  # type: ignore
        self._decryptor: AEADDecryptionContext = None  # type: ignore

    @property
    def name(self) -> str:
        return f"ecies:decrypt:{self._file_io.name}"

    def read(self, n: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        if n == -1:
            n = 4096

        if self._step == _EncryptionStep.INITIALIZATION_VECTOR:
            read_length = 12
            initialization_vector = self._file_io.read(read_length)
            if not initialization_vector:
                raise EOFError("unexpected end of file while reading initialization vector")

            if len(initialization_vector) != read_length:
                raise ValueError(f"invalid initialization vector size: expected {read_length} bytes, got {len(initialization_vector)} bytes")

            self._step = _EncryptionStep.FILE
            self._create_cipher(initialization_vector)

            return self.read(n)

        elif self._step == _EncryptionStep.FILE:
            read_length = min(
                self._content_length - self._read_length,
                n,
            )

            if read_length <= 0:
                self._step = _EncryptionStep.AUTH_TAG
                return self.read(n)

            data = self._file_io.read(read_length)
            self._read_length += len(data)

            data = self._decryptor.update(data)

            return data

        elif self._step == _EncryptionStep.AUTH_TAG:
            read_length = 16

            auth_tag = self._file_io.read(read_length)
            if not auth_tag:
                raise EOFError("unexpected end of file while reading authentication tag")

            if len(auth_tag) != read_length:
                raise ValueError(f"invalid authentication tag size: expected {read_length} bytes, got {len(auth_tag)} bytes")

            self._step = _EncryptionStep.DONE

            final_data = self._decryptor.finalize_with_tag(auth_tag)
            assert len(final_data) == 0, "finalization with tag should not return data"

            return b""

        elif self._step == _EncryptionStep.DONE:
            return b""

        else:
            raise UnsupportedOperation("invalid step")

    def _create_cipher(self, initialization_vector: bytes) -> None:
        assert self._decryptor is None, "cipher already created"

        self._cipher = Cipher(
            algorithm=algorithms.AES(self._decryption_key),
            mode=modes.GCM(initialization_vector),
            backend=self._backend
        )

        self._decryptor = self._cipher.decryptor()


def encrypt(
    data: bytes,
    *,
    public_key_pem: str,
) -> Tuple[bytes, EphemeralPublicKeyPem]:
    input_io = ECIESEncryptStream(
        file_io=BytesIO(data),
        public_key_pem=public_key_pem,
    )

    chunks: List[bytes] = []

    with input_io:
        while input_io.readable():
            chunk = input_io.read()
            chunks.append(chunk)

    return (
        b''.join(chunks),
        input_io.ephemeral_public_key_pem()
    )


def decrypt(
    data: bytes,
    *,
    private_key_pem: MasterPrivateKeyPem,
    ephemeral_public_key_pem: EphemeralPublicKeyPem,
) -> bytes:
    input_io = ECIESDecryptStream(
        file_io=BytesIO(data),
        file_length=len(data),
        private_key_pem=private_key_pem,
        ephemeral_public_key_pem=ephemeral_public_key_pem,
    )

    chunks: List[bytes] = []

    with input_io:
        while input_io.readable():
            chunk = input_io.read()
            chunks.append(chunk)

    return b''.join(chunks)


def generate_keypair_pem(
    backend: Any = None
) -> Tuple[MasterPrivateKeyPem, MasterPublicKeyPem]:
    backend = backend or default_backend()

    private_key = ec.generate_private_key(ec.SECP256K1(), backend)
    public_key = private_key.public_key()

    private_key_bytes = private_key.private_numbers().private_value.to_bytes(32, "big")
    private_key_hex = private_key_bytes.hex()  # TODO Format as PEM

    public_key_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")

    return private_key_hex, public_key_pem
