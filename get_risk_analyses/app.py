import json
import re

import boto3

risk_analyses_prefix = 'risk_analyses/'


def key_to_risk_analysis(key):
    name = key.replace(risk_analyses_prefix, '').replace('.csv', '')
    return {
        'id': re.sub('_.+', '', name),
        'asset_name': re.sub('.+_', '', name),
        'path': key
    }


def lambda_handler(event, context):
    payload = {
        'prefix': risk_analyses_prefix
    }
    lambda_client = boto3.client('lambda')
    list_keys_response = lambda_client.invoke(FunctionName='ListS3ObjectKeysWithPrefixFunction',
                                              Payload=bytes(json.dumps(payload), encoding='utf8'))
    risk_analyses_keys = json.loads(list_keys_response['Payload'].read().decode())
    risk_analyses = [key_to_risk_analysis(key) for key in risk_analyses_keys]
    risk_analyses.sort(key=lambda analysis: int(analysis['id']))
    return {
        "body": json.dumps(risk_analyses)
    }
