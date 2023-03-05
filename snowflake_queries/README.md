# Snowflake Query Telemetry
This code allows you to get query level visibility into your Snowflake cost data.  It does this by letting you "tag" each query with metadata and then sending that data to CloudZero's [telemetry API](https://docs.cloudzero.com/reference/telemetry#telemetry).  The code consists of two parts:
  - **query_execution_time.sql** -- SQL to create a Snowflake View called: `OPERATIONS.CLOUDZERO_TELEMETRY.QUERY_EXECUTION_TIME`.  The View uses `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` to find the total execution time of every query and the warehouse on which it ran.  This is shaped into records to be sent to the telemetry API.
  - **handler.py** (and supporting Python modules) -- Sample Python code that reads from the `QUERY_EXECUTION_TIME` view and sends records to the API.  Expected to run once an hour.
## Overview
  To use this code you will need to go through the following steps:
1. Add metadata to your queries with information about the query's purpose or any other meaningful information.
2. Create a database view in Snowflake to extract this metadata and shape it into telemetry records.
3. Create a CloudZero Custom Dimension called "Snowflake Warehouse" to show your per-warehouse cost.
4. Deploy the Python code so that it executes once an hour.  It will query the database view and send records to the CloudZero telemetry API.

The following sections describe these steps in detail.
## Query Metadata
  To use the `QUERY_EXECUTION_TIME` view, you must first "tag" your queries by embedding a special block comment in each one with attributes that identify the query's purpose.  These attributes can be anything related to your business and associated with the query.  Examples include:
  - Customer ID
  - ETL Job Name
  - Feature Name
  - Customer Segment

This data is added as JSON in a block comment surrounded by special tokens: `/*QUERYDATA>...<QUERYDATA*/`.  This allows it to be parsed out of the query text and turned into telemetry records.  For example:
```sql
SELECT /*QUERYDATA>{"customer_id": "aaa-bbb-cc", "etl_job_id": "xxx-yyy-zzz"}<QUERYDATA*/
    id, foo, bar
FROM
    DATA_PROCESSING.RESULTS;
``` 
_Note that the comment must be placed after the `SELECT` or it [will be removed.](https://docs.snowflake.com/en/release-notes/2017-04.html#queries-leading-comments-removed-during-execution)_

## Query Execution Time
### Metadata Properties
The next step is to review/modify `query_execution_time.sql` to use your specific query metadata.  Modify lines 12-16 and change the `query_prop*` values to your actual "QUERYDATA" parameters.  For example, in the sample above those would be `customer_id` ans `etl_job_id`.  On line 16 these are concatenated into a single element name which will be split later using CloudZero Dimensions.  Ensure that the delimiter used ("`||`" in the sample code) does not appear in any metadata values.
### View Name
The sample code create a view named `OPERATIONS.CLOUDZERO_TELEMETRY.QUERY_EXECUTION_TIME`.  This is arbitrary and can be changed to anything you want.  Just be sure to also change this in `constants.py`.
### Snowflake Warehouse Dimension
The telemetry records associate query metadata with a CloudZero custom dimension called `Snowflake Warehouse`.  The CloudZero Customer Success team can assist with creating this dimension, the values of which should be your Snowflake warehouse names in all lowercase.

## Send Telemetry Records
The `handler.py` file includes sample code which queries the `QUERY_EXECUTION_TIME` view and sends records to the CloudZero Telemetry API.  This code is not intended to be run as-is, but rather serve as a reference.  Some important things to note:
 - The sample code assumes that secrets (CloudZero API Key, Snowflake user/password) are stored in the AWS Secrets Manager.  This should be updated to whatever mechanism your organization uses for managing secrets.  Additional information on using the [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-to-snowflake) can be found in the Snowflake documentation.  Be sure to use a Snowflake user/role with read access to the [QUERY_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/query_history.html#query-history-view) View.
 - The sample code is designed to be run once an hour.  Each time it is run it collects data for the most recent complete hour in `QUERY_HISTORY` and sends it to the telemetry API.  It's best to schedule this run in the middle of an hour rather than the top of the hour.  This helps avoid issues of duplicate or missed hours due to small time skews.

