import boto3
from static.static import ACCESS_KEY, SECRET_KEY, BUCKET_NAME, PREFIX
client = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)


def list_sub_folders():
    paginator = client.get_paginator("list_objects")
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX, Delimiter='/')

    bucket_object_list = []
    for page in page_iterator:
        if "Contents" in page:
            for key in page["Contents"]:
                key_string = key["Key"]
                bucket_object_list.append(key_string)

    print(bucket_object_list)


def get_matching_s3_objects():
    kwargs = {'Bucket': BUCKET_NAME, 'Prefix': PREFIX}
    while True:
        resp = client.list_objects_v2(**kwargs)
        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(PREFIX):
                yield key
                print(key)
        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


if __name__ == '__main__':
    for obj in get_matching_s3_objects():
        print(obj['Key'])
