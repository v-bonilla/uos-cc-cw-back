import os

import boto3


def get_s3_objects(bucket_name, **base_kwargs):
    s3_client = boto3.client('s3')
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


def lambda_handler(event, context):
    bucket_name = os.environ.get('S3_BUCKET_NAME')
    prefix = event.get('prefix')
    return [s3_object.get('Key') for s3_object in get_s3_objects(bucket_name, Prefix=prefix) if
            not s3_object['Key'] == prefix]
