# Copyright (c) 2016-present, CloudZero, Inc. All rights reserved.
# Licensed under the BSD-style license. See LICENSE file in the project root for full license information.
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import NamedTuple, Dict, List

import requests
from dateutil.tz import tzutc
from snowflake.connector import SnowflakeConnection
from toolz import partition_all, keymap

from constants import TELEMETRY_SECRETS_ID, TELEMETRY_URL, Table, MAX_RECORDS_PER_CALL, SNOWFLAKE_SECRETS_ID, \
    DEFAULT_WAREHOUSE, DATA_LATENCY, STREAM_NAME, QUERY_EXECUTION_TIME_TELEMETRY_VIEW
from util import aws, snowflake, json
from util.json import serializable

logger = logging.getLogger('snowflake-queries-telemetry')


class UnitCostGranularity(Enum):
    hourly = 'hourly'
    daily = 'daily'


class TelemetryApiConnection(NamedTuple):
    url: str
    api_key: str


class DateRange(NamedTuple):
    start: datetime
    end: datetime


class TelemetryRecord(NamedTuple):
    granularity: UnitCostGranularity
    element_name: str
    filter: Dict[str, List[str]]
    telemetry_stream: str
    value: float
    timestamp: datetime


def _connect_api() -> TelemetryApiConnection:
    external_api_key = aws.get_secrets(TELEMETRY_SECRETS_ID)['external_api_key']
    return TelemetryApiConnection(url=TELEMETRY_URL, api_key=external_api_key)


def _send_telemetry_records(conn: TelemetryApiConnection, records: List[TelemetryRecord]):
    logger.debug(f'Sending telemetry to {conn.url}')
    response = requests.post(
        conn.url,
        headers={
            'Authorization': conn.api_key
        },
        json={
            'records': [keymap(lambda k: k.replace('_', '-'), x) for x in serializable(records)]
        })
    if not response.ok:
        logger.error(f'Got {response.status_code} sending telemetry to {conn.url}')
        logger.error(response.text)
        response.raise_for_status()


def _collect_records_from_view(conn: SnowflakeConnection, date_range: DateRange,
                               stream_name: str, view: Table) -> List[TelemetryRecord]:
    sql = f"""
        SELECT element_name, timestamp, filter, value
        FROM {view}
        WHERE timestamp >= '{date_range.start.isoformat()}' AND
              timestamp < '{date_range.end.isoformat()}'
        """
    result = snowflake.execute(conn, sql)

    return [TelemetryRecord(
        granularity=UnitCostGranularity.hourly,
        element_name=x['element_name'],
        timestamp=x['timestamp'],
        filter=json.loads(x['filter']),
        telemetry_stream=stream_name,
        value=x['value']
    ) for x in result]


def send_data_from_view(date_range: DateRange, stream: str, view: Table):
    api_conn = _connect_api()
    snow_conn = snowflake.connect(SNOWFLAKE_SECRETS_ID, DEFAULT_WAREHOUSE)

    records = _collect_records_from_view(snow_conn, date_range, stream, view)
    for record_group in partition_all(MAX_RECORDS_PER_CALL, records):
        if record_group:
            _send_telemetry_records(api_conn, record_group)


if __name__ == '__main__':
    current_hour = datetime.now(tz=tzutc()).replace(minute=0, second=0, microsecond=0)
    start = current_hour - DATA_LATENCY - timedelta(hours=1)
    range = DateRange(
        start=start,
        end=start + timedelta(hours=1)
    )

    send_data_from_view(range, STREAM_NAME, QUERY_EXECUTION_TIME_TELEMETRY_VIEW)