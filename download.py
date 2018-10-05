import boto3
import os
import sys
from static import ACCESS_KEY, SECRET_KEY, BUCKET_NAME

client = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
resource = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)


def download_dir(prefix, local='../tmp'):
    paginator = client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=BUCKET_NAME, Delimiter='/', Prefix=prefix):
        if result.get('CommonPrefixes') is not None:
            for subdir in result.get('CommonPrefixes'):
                download_dir(subdir.get('Prefix'), local)

        if result.get('Contents') is not None:
            for file in result.get('Contents'):
                if not os.path.exists(os.path.dirname(local + os.sep + file.get('Key'))):
                    os.makedirs(os.path.dirname(local + os.sep + file.get('Key')))

                if not os.path.exists(local + os.sep + file.get('Key')):
                    print("(download...)\t", file.get('Key'))
                    resource.meta.client.download_file(BUCKET_NAME, file.get('Key'), local + os.sep + file.get('Key'))
                else:
                    print("(already exist)\t", file.get('Key'))


if __name__ == '__main__':
    # sys.argv = ['self', 'data/ulelmxu5-1595/', '../1595']
    if len(sys.argv) != 3:
        print("Invalid Arguments.")
        sys.exit(1)
    else:
        src_s3_url = sys.argv[1]
        dst_local_dir = sys.argv[2]

        if not os.path.exists(dst_local_dir):
            os.mkdir(dst_local_dir)

        print("src bucket url: ", src_s3_url)
        print("dst local path: ", dst_local_dir)
        download_dir(prefix=src_s3_url, local=dst_local_dir)

        print("Done")
