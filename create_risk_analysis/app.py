import json

import boto3

assets_prefix = 'financial_data/'


def lambda_handler(event, context):
    # body = json.loads(event.get('body'))
    body = event.get('body')
    lambda_client = boto3.client('lambda')
    asset_id = body.get('id')
    analyse_risk_payload = {
        'id': asset_id,
        'ma_period': body.get('ma_period')
    }
    new_analysis_uri_response = lambda_client.invoke(FunctionName='AnalyseRiskFunction',
                                                     Payload=bytes(json.dumps(analyse_risk_payload), encoding='utf8'))
    new_analysis_id = json.loads(new_analysis_uri_response['Payload'].read().decode())
    assess_risk_payload = {
        "id": new_analysis_id,
        "var_window": body.get('var_window'),
        "mc_samples": body.get('mc_samples'),
        "scalable_services": body.get('scalable_services'),
        "parallel_resources": body.get('parallel_resources')
    }
    assess_risk_response = lambda_client.invoke(FunctionName='AssessRiskFunction',
                                                     Payload=bytes(json.dumps(assess_risk_payload), encoding='utf8'))
    new_analysis_id = json.loads(assess_risk_response['Payload'].read().decode())
    return {
        "statusCode": 201,
        "headers": {
            "location": new_analysis_id
        }
    }
