# Copyright (c) 2016-present, CloudZero, Inc. All rights reserved.
# Licensed under the BSD-style license. See LICENSE file in the project root for full license information.
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID
import simplejson as json


class ExtendedEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, Enum):
            return o.value
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, UUID):
            return str(o)
        return super(ExtendedEncoder, self).default(o)


def dumps(obj, **kwargs):
    return json.dumps(obj, **{**dict(
        cls=ExtendedEncoder,
        use_decimal=False,
        iterable_as_array=True
    ), **kwargs})


def loads(fp, **kwargs):
    return json.loads(fp, **kwargs)


def serializable(blob: Any) -> Any:
    return loads(dumps(blob))
