import datetime

import dataclasses_json
import marshmallow

date_config = dataclasses_json.config(
    encoder=datetime.date.isoformat,
    decoder=datetime.date.fromisoformat,
    mm_field=marshmallow.fields.DateTime(format='iso')
)

datetime_config = dataclasses_json.config(
    encoder=datetime.datetime.isoformat,
    decoder=datetime.datetime.fromisoformat,
    mm_field=marshmallow.fields.Date(format='iso')
)


class ApiException(Exception):

    def __init__(self, message: str):
        super().__init__(message)
