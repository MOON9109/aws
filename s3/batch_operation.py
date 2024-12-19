
import boto3
import datetime

def get_ETag(source_bucket, source_key):
    '''
    s3의 ETag를 받아오는 역할
    '''
    s3client = boto3.client('s3')

    versions = s3client.list_object_versions(
            Bucket=source_bucket,
            Prefix=source_key
        ).get('Versions', [])

    ETag=versions[0]['ETag'].strip('"')

    return ETag

def create_batch_operations_job(role_arn,bucket_name, account_id, manifest_path,region_name):
    '''
    manifest 파일에 있는 리스트를 copy하는 batch operation, operation을 변경하여 다른 역할 수행 가능
    '''
    print('start function')
    s3ControlClient = boto3.client('s3control', region_name=region_name)

    bucket_arn = 'arn:aws:s3:::%s' % bucket_name
    dst_bucket_arn=bucket_arn
    dst_prefix='destination'
    ETag=get_ETag(bucket_name, manifest_path)
    print(f'ETag:{ETag}')
    response = s3ControlClient.create_job(
        AccountId=account_id,
        ConfirmationRequired=False,
        Operation={
                'S3PutObjectCopy': {
                    "TargetResource": dst_bucket_arn,
                    'TargetKeyPrefix': dst_prefix,
                    "MetadataDirective": "COPY",
                    "RequesterPays": False,
                    "StorageClass": "STANDARD",
                },
            },
        Manifest={
            'Spec': {
                'Format': 'S3BatchOperations_CSV_20180820',
                'Fields': ['Bucket', 'Key']
            },
            'Location': {
                'ObjectArn': '%s/%s' % (bucket_arn, manifest_path),
                'ETag': ETag  # call generate etag

            }
        },
        Report={
            'Bucket': bucket_arn,
            'Format': 'Report_CSV_20180820',
            'Enabled': True,
            'Prefix': 'batch_operations/restore_reports',
            'ReportScope': 'AllTasks'
        },
        Description="Restore Request " + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        Priority=5,
        RoleArn=role_arn
    )
    print("Batch Job created")