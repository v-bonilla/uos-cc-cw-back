import os

import boto3
import pandas as pd
import s3fs

s3_client = boto3.client('s3')
risk_analyses_prefix = 'risk_analyses/'
bucket_name = os.environ.get('S3_BUCKET_NAME')


def get_object_name(asset_prefix):
    asset_key = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=asset_prefix).get('Contents', [])[0].get('Key')
    return asset_key.replace(asset_prefix, '').replace('.csv', '')


def lambda_handler(event, context):
    id = event.get('pathParameters').get('id')
    report_prefix = risk_analyses_prefix + id + '_'
    report_name = get_object_name(report_prefix)
    s3_report_path = 's3://' + bucket_name + '/' + report_prefix + report_name + '.csv'
    risk_analysis_metadata_json = '{\"id\": \"' + id + '\", \"report_name\": \"' + report_name + '\", \"data\": '
    report_data_json = pd.read_csv(s3_report_path).to_json()
    # TODO: Add total_p_l and average_var fields in the response
    risk_analysis_json = risk_analysis_metadata_json + report_data_json + '}'
    return {
        "body": risk_analysis_json
    }
