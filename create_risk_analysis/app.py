import json

import boto3

assets_prefix = 'financial_data/'


def lambda_handler(event, context):
    body = json.loads(event.get('body'))
    payload = {
        'id': body.get('id'),
        'ma_period': body.get('ma_period')
    }
    lambda_client = boto3.client('lambda')
    new_analysis_uri_response = lambda_client.invoke(FunctionName='AnalyseRiskFunction',
                                                     Payload=bytes(json.dumps(payload), encoding='utf8'))
    new_analysis_id = json.loads(new_analysis_uri_response['Payload'].read().decode())
    return {
        "statusCode": 201,
        "headers": {
            "location": new_analysis_id
        }
    }
