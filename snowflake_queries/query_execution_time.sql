-- Copyright (c) 2016-present, CloudZero, Inc. All rights reserved.
-- Licensed under the BSD-style license. See LICENSE file in the project root for full license information.

CREATE OR REPLACE VIEW OPERATIONS.CLOUDZERO_TELEMETRY.QUERY_EXECUTION_TIME (
	ELEMENT_NAME,
	TIMESTAMP,
	FILTER,
	VALUE
) AS
    WITH queries AS (
        SELECT 
               USER_NAME as query_prop1,
               ROLE_NAME as query_prop2,
               QUERY_TAG as query_prop3,
               DATABASE_NAME as query_prop4,
               SCHEMA_NAME as query_prop5,
               NVL(query_prop1, 'none') || '||' || NVL(query_prop2, 'none') || '||' || NVL(query_prop3, 'none') || '||' || NVL(query_prop4, 'none' || '||' || NVL(query_prop5, 'none') as element_name, -- Use a delimiter not present in any of the property values
               CONVERT_TIMEZONE('UTC', end_time)::TIMESTAMP_NTZ AS adj_end_time,
               DATEADD('ms', -1 * execution_time, adj_end_time) AS adj_start_time,  -- Uses the execution time, not queuing, compilation, etc.
               LOWER(warehouse_name) AS warehouse
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE adj_start_time > '2022-06-01T00:00:00+00:00'::TIMESTAMP_NTZ -- A reasonable starting point
        AND cluster_number IS NOT NULL                                    -- Excludes cache hits
    ), hours AS (
        SELECT DATEADD('hour', row_number() OVER (ORDER BY NULL),
                        DATE_TRUNC('hour', '2022-06-01T00:00:00+00:00'::TIMESTAMP_NTZ)) AS query_hour
        FROM TABLE (generator(ROWCOUNT => 10*365*24))
        QUALIFY query_hour < SYSDATE()
    ), time_per_query_in_hour AS (
        SELECT adj_start_time as start_time,
               adj_end_time as end_time,
               element_name,
               query_hour,
               warehouse,
               DATEDIFF('ms', GREATEST(start_time, query_hour),
                              LEAST(end_time, query_hour + INTERVAL '1 hour')) AS query_time_ms
        FROM queries
        -- Split queries into hour aligned chunks.  If a query spans an hour boundary, only consider the portion of the execution time
        -- that falls into each hour the query covers.  This will properly handle even multi-hour queries.
            LEFT JOIN hours
        ON query_hour >= DATE_TRUNC('hour', adj_start_time) AND query_hour < adj_end_time
    ), result AS (
        SELECT element_name,
               query_hour,
               warehouse,
               SUM(query_time_ms) as total_time
        FROM time_per_query_in_hour
        GROUP BY element_name, query_hour, warehouse
    )
    SELECT
        element_name,
        query_hour as timestamp,
        OBJECT_CONSTRUCT(
            'account', LOWER(CURRENT_ACCOUNT() || '.' || CURRENT_REGION()),
            'custom:Snowflake Warehouse', ARRAY_CONSTRUCT(warehouse)) as filter,  -- Assumes we have a custom dimension named "Snowflake Warehouse"
        total_time as value
    FROM result
    WHERE element_name IS NOT NULL;