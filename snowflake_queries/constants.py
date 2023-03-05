# Copyright (c) 2016-present, CloudZero, Inc. All rights reserved.
# Licensed under the BSD-style license. See LICENSE file in the project root for full license information.
from datetime import timedelta
from typing import NamedTuple


class Table(NamedTuple):
    database: str
    schema: str
    name: str

    def __str__(self):
        return f'"{self.database}"."{self.schema}"."{self.name}"'

MAX_RECORDS_PER_CALL = 3000
TELEMETRY_URL = 'https://api.cloudzero.com/unit-cost/v1/telemetry'
TELEMETRY_SECRETS_ID = 'cloudzero_telemetry_secrets'
STREAM_NAME = 'query-execution-time'
QUERY_EXECUTION_TIME_TELEMETRY_VIEW = Table(
    database='OPERATIONS',
    schema='CLOUDZERO_TELEMETRY',
    name='QUERY_EXECUTION_TIME'
)

DEFAULT_WAREHOUSE = 'OPERATIONS_WAREHOUSE'
SNOWFLAKE_SECRETS_ID = 'snowflake_secrets'
DATA_LATENCY = timedelta(hours=1)
