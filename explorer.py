import boto3
import sys
from static import ACCESS_KEY, SECRET_KEY, BUCKET_NAME, PREFIX
client = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)


def get_matching_s3_keys(keyword):
    matched_keys = []
    kwargs = {'Bucket': BUCKET_NAME, 'Prefix': PREFIX, 'Delimiter': '/'}
    while True:
        resp = client.list_objects_v2(**kwargs)
        try:
            common_prefixes = resp['CommonPrefixes']
        except KeyError:
            return

        for obj in common_prefixes:
            if obj['Prefix'].find(keyword) != -1:
                matched_keys.append(obj['Prefix'])

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return matched_keys


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Please enter the GameIndex to search.")
        sys.exit(1)

    elif not sys.argv[1].isdigit():
        print("Please enter the digits GameIndex to search.")
        sys.exit(1)

    else:
        keyword = sys.argv[1]
        print("target keyword: ", keyword, " searching ...")
        matched_urls = get_matching_s3_keys(keyword=keyword)
        print("result: ")
        for url in matched_urls:
            print(url)
