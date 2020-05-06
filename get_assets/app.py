import json
import re

import boto3

assets_prefix = 'financial_data/'


def key_to_asset(key):
    name = key.replace(assets_prefix, '').replace('.csv', '')
    return {
        'id': re.sub('_.+', '', name),
        'asset_name': re.sub('.+_', '', name),
        'path': key
    }


def lambda_handler(event, context):
    payload = {
        'prefix': assets_prefix
    }
    lambda_client = boto3.client('lambda')
    list_keys_response = lambda_client.invoke(FunctionName='ListS3ObjectKeysWithPrefixFunction',
                                              Payload=bytes(json.dumps(payload), encoding='utf8'))
    financial_data_keys = json.loads(list_keys_response['Payload'].read().decode())
    assets = [key_to_asset(key) for key in financial_data_keys]
    return {
        "body": json.dumps(assets)
    }
