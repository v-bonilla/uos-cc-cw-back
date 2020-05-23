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
    ra = pd.read_csv(s3_report_path)
    if 'var_95' in ra.columns:
        ra_var_filtered = ra.loc[ra['var_95'].notna()]
        total_p_l = ra_var_filtered['p_l'].sum()
        average_var_95 = ra_var_filtered['var_95'].mean()
        average_var_99 = ra_var_filtered['var_99'].mean()
        risk_analysis_metadata_json = '{\"id\": \"' + id + '\", \"report_name\": \"' + report_name + \
                                      '\", \"total_p_l\": \"' + str(total_p_l) + '\", \"average_var_95\": \"' + \
                                      str(average_var_95) + '\", \"average_var_99\": \"' + str(average_var_99) + \
                                      '\", \"data\": '
    else:
        risk_analysis_metadata_json = '{\"id\": \"' + id + '\", \"report_name\": \"' + report_name + '\", \"data\": '
    report_data_json = ra.to_json()
    risk_analysis_json = risk_analysis_metadata_json + report_data_json + '}'
    return {
        "body": risk_analysis_json
    }
