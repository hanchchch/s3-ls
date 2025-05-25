import boto3
from concurrent.futures import ProcessPoolExecutor, as_completed
import string

# https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
# len: 70
characters = sorted(string.ascii_letters + string.digits + "!-_.*'()/")


def _list_objects(
    bucket: str,
    prefix: str = "",
    offset: str = "",
    **s3_kwargs,
):
    s3 = boto3.client("s3", **s3_kwargs)
    limit = 1000
    return prefix, s3.list_objects_v2(
        Bucket=bucket,
        MaxKeys=limit,
        Prefix=prefix,
        StartAfter=offset,
    )


def list_objects(
    bucket: str,
    prefix: str = "",
    offset: str = "",
    max_workers: int = 30,
    **s3_kwargs,
):
    _prefix = prefix

    # _, response = _list_objects(bucket, _prefix, offset, **s3_kwargs)
    # if response.get("Contents") is not None:
    #     yield from response["Contents"]

    # if response.get("NextContinuationToken") is not None:
    #     offset = response["NextContinuationToken"]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for c in characters:
            futures.append(
                executor.submit(
                    _list_objects,
                    bucket,
                    _prefix + c,
                    offset,
                    **s3_kwargs,
                )
            )

        while len(futures) > 0:
            future = next(as_completed(futures))

            returned_prefix, response = future.result()
            if response.get("Contents") is not None:
                yield from response["Contents"]

            if response.get("NextContinuationToken") is not None:
                futures.append(
                    executor.submit(
                        _list_objects,
                        bucket,
                        returned_prefix,
                        response["NextContinuationToken"],
                        **s3_kwargs,
                    )
                )

            futures.remove(future)


if __name__ == "__main__":
    import os
    import time
    import dotenv
    import tqdm

    dotenv.load_dotenv()

    bucket = os.getenv("AWS_S3_BUCKET")
    prefix = os.getenv("AWS_S3_PREFIX")
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_kwargs = {
        "endpoint_url": endpoint_url,
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
    }

    total = 100_000
    start = time.time()
    i = 0
    for obj in tqdm.tqdm(
        list_objects(bucket, prefix, **s3_kwargs),
        total=total,
    ):
        i += 1
        if i > total:
            break
    end = time.time()
    print(f"Time taken: {end - start} seconds")

    start = time.time()
    offset = ""
    for _ in tqdm.tqdm(
        range(total // 1000),
    ):
        _, r = _list_objects(bucket, prefix, offset, **s3_kwargs)
        offset = r.get("NextContinuationToken")
    end = time.time()
    print(f"Time taken: {end - start} seconds")
