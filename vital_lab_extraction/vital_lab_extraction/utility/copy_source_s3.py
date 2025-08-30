import boto3


def copy_object_to_s3(s3c, bucket, copy_source, copy_key,content_type=None ):
    if content_type:
        s3c.copy_object(CopySource=copy_source, Bucket=bucket,
                        Key=copy_key, ContentType=content_type,
                        MetadataDirective='REPLACE')
    else:
        s3c.copy_object(CopySource=copy_source, Bucket=bucket,
                        Key=copy_key,
                        MetadataDirective='REPLACE')
    print(f'copied {copy_source} to the key:  {copy_key}')
    return 'copied'
