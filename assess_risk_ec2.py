import sys

import boto3
import numpy as np
import pandas as pd
import s3fs

bucket_name = 'uos-cc-cw'
var_results_url = 's3://' + bucket_name + '/var_results/'


def calculate_var(df, mc_samples_ec2):
    print('Calculating VAR...')
    v95 = 'var_95_' + str(id)
    v99 = 'var_99_' + str(id)
    df[v95] = np.nan
    df[v99] = np.nan
    for row in df.iterrows():
        index = row[0]
        series = row[1]
        if series['vw_mean'] != np.nan and series['vw_std'] != np.nan:
            mc_series = np.random.normal(series['vw_mean'], series['vw_std'], mc_samples_ec2)
            mc_series.sort()
            if series['sig'] == 1:
                var_quantiles = np.quantile(mc_series, [0.95, 0.99])
            else:
                var_quantiles = np.quantile(mc_series, [0.05, 0.01])
            df.loc[index, [v95, v99]] = [(1 + x) * series['Adj Close'] for x in var_quantiles]
    df.drop(columns=['vw_mean', 'vw_std'], inplace=True)


def get_s3_objects(s3_client, bucket_name, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(Bucket=bucket_name, MaxKeys=10000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3_client.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):
            break
        continuation_token = response.get('NextContinuationToken')


def list_s3_objects(s3_client, prefix):
    return [s3_object.get('Key') for s3_object in get_s3_objects(s3_client, bucket_name, Prefix=prefix) if
            not s3_object['Key'] == prefix]


def delete_s3_objects(s3_client, key_list):
    objects = [{'Key': k} for k in key_list]
    delete_params = {'Objects': objects}
    s3_client.delete_objects(Bucket=bucket_name, Delete=delete_params)


def merge_var_results(df, parallel_resources):
    print('Merging results from other ec2 instances...')
    df_res = df.copy()
    df_res.rename(columns={'var_95_0': 'var_95', 'var_99_0': 'var_99'}, inplace=True)
    s3_client = boto3.client('s3')
    # Wait for results in S3
    var_results_ready = False
    while not var_results_ready:
        var_results = list_s3_objects(s3_client, 'var_results/')
        if len(var_results) == parallel_resources - 1:
            var_results_ready = True
    # Merge results
    for id in range(1, parallel_resources):
        var95_col = 'var_95_' + str(id)
        var99_col = 'var_99_' + str(id)
        cols_to_merge = ['Date', var95_col, var99_col]
        df = pd.read_csv(var_results_url + str(id) + '.csv')
        df_res = df_res.merge(df[cols_to_merge], on='Date')  # To ensure integrity with Date
        df_res['var_95'] = df_res['var_95'] + df_res[var95_col]
        df_res['var_99'] = df_res['var_99'] + df_res[var99_col]
        df_res.drop(columns=[var95_col, var99_col], inplace=True)
    df_res['var_95'] = df_res['var_95'] / parallel_resources
    df_res['var_99'] = df_res['var_99'] / parallel_resources
    # Delete results from S3
    delete_s3_objects(s3_client, var_results)
    return df_res


def run(id, report_s3_url, parallel_resources, mc_samples_ec2):
    print('Assess risk started...')
    df = pd.read_csv(report_s3_url)
    calculate_var(df, mc_samples_ec2)
    if id == 0:
        df_res = merge_var_results(df, parallel_resources)
        df_res.to_csv(report_s3_url, index=False)
    else:
        df.to_csv(var_results_url + str(id) + '.csv', index=False)
    print('Done.')


if __name__ == "__main__":
    id = int(sys.argv[1])
    report_s3_url = str(sys.argv[2])
    parallel_resources = int(sys.argv[3])
    mc_samples_ec2 = int(sys.argv[4])
    run(id, report_s3_url, parallel_resources, mc_samples_ec2)
