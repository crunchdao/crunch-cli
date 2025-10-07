from os import makedirs
from os.path import exists, join


def run_fake_server(
    *,
    port: int,
    storage_directory_path: str,
):
    from crunch_encrypt.ecies import generate_keypair_pem
    from flask import Flask, jsonify

    app = Flask(__name__)

    @app.route('/')
    def index():  # type: ignore
        return jsonify({
            "routes": [
                "/keypair/{id}",
            ]
        })

    @app.route('/keypair/<path:id>')
    def get_key_pair(id: str):  # type: ignore
        keys_directory_path = join(storage_directory_path, id)
        makedirs(keys_directory_path, exist_ok=True)

        public_key_path = join(keys_directory_path, 'public_key.pem')
        private_key_path = join(keys_directory_path, 'private_key.pem')

        if not exists(public_key_path) or not exists(private_key_path):
            (
                private_key_pem,
                public_key_pem,
            ) = generate_keypair_pem()

            with open(private_key_path, 'w') as fd:
                fd.write(private_key_pem)

            with open(public_key_path, 'w') as fd:
                fd.write(public_key_pem)

        else:
            with open(private_key_path, 'r') as fd:
                private_key_pem = fd.read()

            with open(public_key_path, 'r') as fd:
                public_key_pem = fd.read()

        return jsonify({
            "submission_id": id,
            "private_key": private_key_pem,
            "public_key": public_key_pem,
            "certificate_chain": "--unused--"
        })

    app.run(
        port=port,
    )
