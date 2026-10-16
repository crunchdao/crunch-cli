import unittest

import crunch.api as api


class AuthTest(unittest.TestCase):

    def test_bearer(self):
        auth = api.Auth.bearer("hello")

        self.assertEqual(auth.scheme, api.AuthorizationScheme.BEARER)
        self.assertEqual(auth.header_value, "Bearer hello")

    def test_api_key(self):
        auth = api.Auth.api_key("hello")

        self.assertEqual(auth.scheme, api.AuthorizationScheme.API_KEY)
        self.assertEqual(auth.header_value, "API-Key hello")

    def test_project_token(self):
        auth = api.Auth.project_token("hello")

        self.assertEqual(auth.scheme, api.AuthorizationScheme.PROJECT_TOKEN)
        self.assertEqual(auth.header_value, "Project-Token hello")

    def test_run_token(self):
        auth = api.Auth.run_token("hello")

        self.assertEqual(auth.scheme, api.AuthorizationScheme.RUN_TOKEN)
        self.assertEqual(auth.header_value, "Run-Token hello")

    def test_repr_hides_token(self):
        auth = api.Auth.api_key("hello")

        self.assertNotIn("hello", repr(auth))

    def test_client_sets_header(self):
        client = api.Client("http://api/", "http://web/", api.Auth.api_key("hello"))

        self.assertEqual(client.api.headers["Authorization"], "API-Key hello")

    def test_client_without_auth(self):
        client = api.Client("http://api/", "http://web/", None)

        self.assertNotIn("Authorization", client.api.headers)
