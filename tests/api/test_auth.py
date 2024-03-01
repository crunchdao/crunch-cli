import unittest

import crunch.api as api


class AuthTest(unittest.TestCase):

    def test_none(self):
        auth = api.auth.NoneAuth()

        headers = {}
        params = {}
        data = {}

        auth.apply(headers, params, data)

        self.assertDictEqual(headers, {})
        self.assertDictEqual(params, {})
        self.assertDictEqual(data, {})

    def test_api_key(self):
        key = "hello"
        auth = api.auth.ApiKeyAuth(key)

        headers = {}
        params = {}
        data = {}

        auth.apply(headers, params, data)

        self.assertDictEqual(headers, {
            "Authorization": f"API-Key {key}"
        })
        self.assertDictEqual(params, {})
        self.assertDictEqual(data, {})

    def test_push_token_data(self):
        token = "hello"
        auth = api.auth.PushTokenAuth(token)

        headers = {}
        params = {}
        data = {}

        auth.apply(headers, params, data)

        self.assertDictEqual(headers, {})
        self.assertDictEqual(params, {})
        self.assertDictEqual(data, {
            "pushToken": token
        })

    def test_push_token_params(self):
        token = "hello"
        auth = api.auth.PushTokenAuth(token)

        headers = {}
        params = {}

        auth.apply(headers, params, None)

        self.assertDictEqual(headers, {})
        self.assertDictEqual(params, {
            "pushToken": token
        })
