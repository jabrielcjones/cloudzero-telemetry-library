# Copyright (c) 2016-present, CloudZero, Inc. All rights reserved.
# Licensed under the BSD-style license. See LICENSE file in the project root for full license information.
import logging
from typing import Mapping, Any, List, cast

import snowflake
from snowflake.connector import SnowflakeConnection

from util import aws

DictRow = Mapping[str, Any]
QueryResult = List[DictRow]


logger = logging.getLogger('snowflake-queries-telemetry')


def connect(secrets_id: str, default_warehouse: str) -> SnowflakeConnection:
    credentials = aws.get_secrets(secrets_id)
    return snowflake.connector.connect(
        warehouse=default_warehouse,
        user=credentials['user'],
        account=credentials['account'],
        password=credentials['password'])


def execute(conn: SnowflakeConnection, sql: str, timeout: int = None) -> QueryResult:
    logger.debug(f"Execute Query: [{sql}]")

    with conn.cursor(snowflake.connector.DictCursor) as curs:
        curs.execute(sql, timeout=timeout)
        result = cast(List[dict], curs.fetchall())
        return [{k.lower(): v for k, v in x.items()} for x in result]
