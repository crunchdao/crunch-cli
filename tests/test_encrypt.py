from crunch._encrypt import decrypt, encrypt, generate_keypair_pem


def test_encrypt():
    original = b"Hello, World!"

    private_key_pem, public_key_pem = generate_keypair_pem()

    (
        encrypted,
        ephemeral_public_key_pem,
    ) = encrypt(original, public_key_pem=public_key_pem)

    decrypted = decrypt(
        encrypted,
        private_key_pem=private_key_pem,
        ephemeral_public_key_pem=ephemeral_public_key_pem,
    )

    assert original == decrypted
