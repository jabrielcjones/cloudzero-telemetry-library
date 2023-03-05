# Copyright (c) 2016-present, CloudZero, Inc. All rights reserved.
# Licensed under the BSD-style license. See LICENSE file in the project root for full license information.
import json
import boto3

sm = boto3.client('secretsmanager')


def get_secrets(secret_id):
    secret = sm.get_secret_value(SecretId=secret_id)['SecretString']
    return json.loads(secret)
