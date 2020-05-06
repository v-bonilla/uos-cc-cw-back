import json
import os

import pandas as pd
import s3fs
import boto3

s3_client = boto3.client('s3')
bucket_name = os.environ.get('S3_BUCKET_NAME')


def get_object_name(asset_prefix):
    asset_key = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=asset_prefix).get('Contents', [])[0].get('Key')
    return asset_key.replace(asset_prefix, '').replace('.csv', '')


def get_new_ra_id():
    lambda_client = boto3.client('lambda')
    risk_analyses_response = lambda_client.invoke(FunctionName='BrowseRiskAnalysesFunction')
    risk_analyses_payload = json.loads(risk_analyses_response['Payload'].read().decode())
    risk_analyses = json.loads(risk_analyses_payload.get('body'))
    if risk_analyses:
        return int(risk_analyses[-1].get('id')) + 1
    else:
        return 1


def get_asset_df(s3_path):
    return pd.read_csv(s3_path, parse_dates=['Date'], usecols=['Date', 'Adj Close'], index_col='Date').sort_index().convert_dtypes()


def determine_signal(first, second):
    if first < 0 and second > 0:
        return 1
    elif first > 0 and second < 0:
        return -1
    else:
        return 0


def calculate_profit_lose(signal_type, price_open, price_close):
    stake = 1000
    if signal_type == 1:
        return (price_close - price_open) * stake
    elif signal_type == -1:
        return (price_open - price_close) * stake
    else:
        return 0


def settle_positions(df):
    signals_df = df.loc[df['sig'].notna() & (df['sig'] != 0), ['Adj Close', 'sig']]
    price_open = 0
    signal_open = 0
    for row in signals_df.iterrows():
        if price_open != 0 and signal_open != 0:
            df.loc[row[0], 'p_l'] = calculate_profit_lose(signal_open, price_open, row[1].loc['Adj Close'])
        price_open = row[1].loc['Adj Close']
        signal_open = row[1].loc['sig']


def analyse(df, ma_period):
    df['ma'] = df['Adj Close'].rolling(window=ma_period).mean()
    df['diff'] = df['Adj Close'] - df['ma']
    df['sig'] = df['diff'].rolling(window=2).apply(lambda x: determine_signal(x.iloc[0], x.iloc[1]))
    df['p_l'] = 0
    settle_positions(df)
    return df.copy()


def lambda_handler(event, context):
    id = str(event.get('id'))
    asset_prefix = 'financial_data/' + id + '_'
    asset_name = get_object_name(asset_prefix)
    asset_df = get_asset_df('s3://' + bucket_name + '/' + asset_prefix + asset_name + '.csv')
    report = analyse(asset_df, int(event.get('ma_period')))
    new_report_id = str(get_new_ra_id())
    report_s3_url = 's3://' + bucket_name + '/' + 'risk_analyses/' + new_report_id + '_' + asset_name + '.csv'
    report.to_csv(report_s3_url)
    return 'riskAnalysis/' + new_report_id
