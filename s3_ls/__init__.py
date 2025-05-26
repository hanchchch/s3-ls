import time
import boto3
from concurrent.futures import ProcessPoolExecutor, as_completed
import string

# https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
# len: 71
characters = sorted(string.ascii_letters + string.digits + "!-_.*'()" + "/")


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
    s3_kwargs: dict = {},
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
    offset: str,
    s3_kwargs: dict,
):
    start_letter = offset[len(prefix) :][0] if len(offset) > len(prefix) else ""
    prefixes = [prefix + c for c in characters if c >= start_letter]

    offsets = []
    for p in prefixes:
        if p[-1] == start_letter:
            offsets.append(offset)
        else:
            offsets.append("")

    return {
        p: executor.submit(_list_objects, bucket, p, o, s3_kwargs)
        for p, o in zip(prefixes, offsets)
    }


def _search(
    executor: ProcessPoolExecutor,
    bucket: str,
    prefix: str = "",
    offset: str = "",
    s3_kwargs: dict = {},
):
    tasks = _spread(executor, bucket, prefix, offset, s3_kwargs)

    while len(tasks.keys()) > 0:
        results = []
        for _prefix, future in list(tasks.items()):
            if future.done():
                results.append(future.result())
                tasks.pop(_prefix)

        for returned_prefix, response in results:
            contents = response.get("Contents", [])

            if len(contents) == 0:
                continue

            yield from contents

            if response.get("NextContinuationToken") is None:
                continue

            offset = response["NextContinuationToken"]

            keys = [c["Key"] for c in contents]
            common_prefix = _get_common_prefix(keys)
            if len(common_prefix) > len(returned_prefix):
                tasks.update(
                    _spread(executor, bucket, returned_prefix, offset, s3_kwargs)
                )
            else:
                tasks.update(
                    {
                        returned_prefix: executor.submit(
                            _list_objects, bucket, returned_prefix, offset, s3_kwargs
                        )
                    }
                )

        time.sleep(0.01)


def list_objects(
    bucket: str,
    prefix: str = "",
    offset: str = "",
    max_workers: int = 50,
    **s3_kwargs,
):
    executor = ProcessPoolExecutor(max_workers=max_workers)

    try:
        yield from _search(executor, bucket, prefix, offset, s3_kwargs)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
