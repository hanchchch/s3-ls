import boto3
from concurrent.futures import ProcessPoolExecutor, as_completed
import string

# https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
# len: 70
characters = sorted(string.ascii_letters + string.digits + "!-_.*'()/")


def _get_common_prefix(paths: list[str]) -> str:
    if len(paths) < 2:
        return ""

    longest = max(len(p) for p in paths)
    common_prefix = ""
    for i in range(longest):
        try:
            if len(set([p[i] for p in paths])) == 1:
                common_prefix += paths[0][i]
            else:
                break
        except IndexError:
            break
    return common_prefix


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


def _spread(
    executor: ProcessPoolExecutor,
    bucket: str,
    prefix: str,
    existing_keys: list[str],
    s3_kwargs: dict,
):
    prefixes = [prefix + c for c in characters]
    if len(existing_keys) > 0:
        existing_keys.sort()
        offsets = [
            next((e for e in existing_keys if e.startswith(p)), "") for p in prefixes
        ]
    else:
        offsets = ["" for _ in prefixes]

    return [
        executor.submit(_list_objects, bucket, p, o, **s3_kwargs)
        for p, o in zip(prefixes, offsets)
    ]


def list_objects(
    bucket: str,
    prefix: str = "",
    max_workers: int = 30,
    existing_keys: list[str] = [],
    **s3_kwargs,
):
    executor = ProcessPoolExecutor(max_workers=max_workers)
    futures = _spread(executor, bucket, prefix, existing_keys, s3_kwargs)

    while len(futures) > 0:
        future = next(as_completed(futures))

        returned_prefix, response = future.result()
        contents = response.get("Contents", [])

        if len(contents) > 0:
            yield from contents

        keys = [c["Key"] for c in contents]
        common_prefix = _get_common_prefix(keys)
        futures.extend(_spread(executor, bucket, common_prefix, keys, s3_kwargs))

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

    executor.shutdown()


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
