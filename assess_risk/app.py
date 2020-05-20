import json
import os

import numpy as np
import pandas as pd
import s3fs
import boto3

s3_client = boto3.client('s3')
bucket_name = os.environ.get('S3_BUCKET_NAME')


def calculate_var(df, var_window, mc_samples):
    var_win_scaled = var_window - 1
    for row in df.iterrows():
        index = row[0]
        if index >= var_win_scaled:  # To ensure window
            series = row[1]
            if series['sig'] != 0:
                price_window = df.loc[index - var_win_scaled:index + 1, 'Adj Close']
                change = price_window.rolling(window=2).apply(lambda x: (x.iloc[1] - x.iloc[0])/x.iloc[0]).dropna()
                mc_series = np.random.normal(change.mean(), change.std(), mc_samples)
                mc_series.sort()
                if series['sig'] == 1:
                    var_quantiles = np.quantile(mc_series, [0.95, 0.99])
                else:
                    var_quantiles = np.quantile(mc_series, [0.05, 0.01])
                df.loc[index, ['var_95', 'var_99']] = [(1 + x) * series['Adj Close'] for x in var_quantiles]


def lambda_handler(event, context):
    ra_id = event.get('id')
    var_window = int(event.get('var_window'))
    mc_samples = int(event.get('mc_samples'))
    scalable_services = int(event.get('scalable_services'))
    if not scalable_services:
        # Get risk analysis
        payload = {
            'pathParameters': {
                'id': ra_id
            }
        }
        lambda_client = boto3.client('lambda')
        ra_response = lambda_client.invoke(FunctionName='BrowseRiskAnalysisFunction',
                                           Payload=bytes(json.dumps(payload), encoding='utf8'))
        ra = json.loads(json.loads(ra_response['Payload'].read().decode()).get('body'))
        df = pd.read_json(json.dumps(ra.get('data')))
        # Calculate VAR
        df['var_95'] = np.nan
        df['var_99'] = np.nan
        calculate_var(df, var_window, mc_samples)
        # Store results
        report_s3_url = 's3://' + bucket_name + '/' + 'risk_analyses/' + ra.get('id') + '_' + ra.get('report_name') + '.csv'
        df.to_csv(report_s3_url, index=False)
        return {
            "status": 200
        }
    else:
        parallel_resources = int(event.get('parallel_resources'))
        # launch EMR. code file stored in s3?
        pass

