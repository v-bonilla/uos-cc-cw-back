import json
import os

import numpy as np
import pandas as pd
import s3fs
import boto3

bucket_name = os.environ.get('S3_BUCKET_NAME')
credentials = '''[default]
aws_access_key_id=ASIARADQARUSGRRAEW5C
aws_secret_access_key=9yNCR2Uegcxm455OTOPTstHNWHB2CiXGVD7Q7Q9S
aws_session_token=FwoGZXIvYXdzEBMaDIQsIJyp+4A5IRowOCLDAab7JCPqSgV0z2rMD1vE1a6oyUstZKXj1FpiQKAmm8rsg/stTag4rWkBlCrl9SJKGtNfX/mzdI1KqrNEKEKMA3/cYuoea24Mo+r1AEAFDlk/9kOyHnM4251QvOyaqpBn6k8aSPQkFOnu9FUDsPmFZbximV2DNT1di90qgOt+Tq7AmCjfERYcDsveSPjFc0J5QspErJM65UEEc5eyNLoH5WhnsgmC2uNbK0xhE115ZOuFfEgD/MEav7UmFUt/YgTxLK9XJijJ3aP2BTIt1FJfgONMSTrOSRmAKObCVQ8z+/uBAa4gS7+zKeeUQkKFadIAIELqqSnf67QO'''
user_data_ec2 = '''#!/bin/bash
cd /root
mkdir .aws
cat > .aws/credentials << EOL
{}
EOL
aws s3 cp s3://uos-cc-cw/ec2_resources ./ec2_resources --recursive
yum install python3 -y
python3 -m venv ./ec2_resources/
cd ec2_resources
source bin/activate
pip3 install -r requirements.txt
python3 assess_risk_ec2.py {} {} {} {}
shutdown now'''


def calculate_var_window_stats(df, var_window):
    df['vw_mean'] = np.nan
    df['vw_std'] = np.nan
    var_win_scaled = var_window - 1
    for row in df.iterrows():
        index = row[0]
        if index >= var_win_scaled:  # To ensure window
            series = row[1]
            if series['sig'] != 0:
                price_window = df.loc[index - var_win_scaled:index, 'Adj Close']
                change = price_window.rolling(window=2).apply(lambda x: (x.iloc[1] - x.iloc[0]) / x.iloc[0]).dropna()
                df.loc[index, ['vw_mean', 'vw_std']] = [change.mean(), change.std()]


def calculate_var(df, mc_samples):
    df['var_95'] = np.nan
    df['var_99'] = np.nan
    for row in df.iterrows():
        index = row[0]
        series = row[1]
        if series['vw_mean'] != np.nan and series['vw_std'] != np.nan:
            mc_series = np.random.normal(series['vw_mean'], series['vw_std'], mc_samples)
            mc_series.sort()
            if series['sig'] == 1:
                var_quantiles = np.quantile(mc_series, [0.95, 0.99])
            else:
                var_quantiles = np.quantile(mc_series, [0.05, 0.01])
            df.loc[index, ['var_95', 'var_99']] = [(1 + x) * series['Adj Close'] for x in var_quantiles]
    df.drop(columns=['vw_mean', 'vw_std'], inplace=True)


def partitions_to_s3(partitions):
    for id, partition in enumerate(partitions):
        partition_s3_url = 's3://' + bucket_name + '/' + 'var_partitions/' + str(id) + '.csv'
        partition.to_csv(partition_s3_url, index=False)


def launch_cluster(parallel_resources, report_s3_url, mc_samples_ec2):
    ec2_client = boto3.resource('ec2')
    for id in range(parallel_resources):
        ec2_client.create_instances(InstanceType='t2.micro', MinCount=1, MaxCount=1, ImageId='ami-0323c3dd2da7fb37d',
                                    InstanceInitiatedShutdownBehavior='terminate', SecurityGroupIds=['sg-08f3f998506ef0548'],
                                    KeyName='us-east-1-kp',
                                    UserData=user_data_ec2.format(credentials, id, report_s3_url, parallel_resources,
                                                                  mc_samples_ec2))


def lambda_handler(event, context):
    ra_id = event.get('id')
    var_window = int(event.get('var_window'))
    mc_samples = int(event.get('mc_samples'))
    scalable_services = int(event.get('scalable_services'))
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
    report_s3_url = 's3://' + bucket_name + '/' + 'risk_analyses/' + ra.get('id') + '_' + ra.get('report_name') + '.csv'
    df = pd.read_json(json.dumps(ra.get('data')))
    calculate_var_window_stats(df, var_window)
    if not scalable_services:
        calculate_var(df, mc_samples)
        # Store results
        df.to_csv(report_s3_url, index=False)
    else:
        parallel_resources = int(event.get('parallel_resources'))
        # Store partial results for ec2 instances
        df.to_csv(report_s3_url, index=False)
        launch_cluster(parallel_resources, report_s3_url, int(mc_samples / parallel_resources))
    return {
        "status": 200
    }
