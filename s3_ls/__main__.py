import os
import csv
import time
import dotenv
import argparse
import tqdm
import urllib.parse

from s3_ls import list_objects


dotenv.load_dotenv()


def readable_int(i: int, k: int = 1000) -> str:
    if i >= k**4:
        return f"{i / k**4:.2f} T"
    elif i >= k**3:
        return f"{i / k**3:.2f} G"
    elif i >= k**2:
        return f"{i / k**2:.2f} M"
    elif i >= k:
        return f"{i / k:.2f} K"
    else:
        return f"{i}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("s3_path", type=str)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output", type=str, default="output.csv")
    args = parser.parse_args()

    url = urllib.parse.urlparse(args.s3_path)
    if url.scheme != "s3":
        raise ValueError(f"Invalid S3 path: {args.s3_path}")

    bucket = url.netloc
    prefix = url.path[1:]  # remove leading slash

    endpoint_url = os.getenv("AWS_ENDPOINT_URL_S3")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    s3_kwargs = {
        "endpoint_url": endpoint_url,
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
    }

    f = open(args.output, "w")
    writer = csv.writer(f)
    writer.writerow(["etag", "last_modified", "s3_path", "size"])

    start = time.time()
    total_keys = 0
    total_size = 0

    for obj in tqdm.tqdm(
        list_objects(bucket, prefix, **s3_kwargs),
        total=args.limit,
    ):
        etag = obj["ETag"].strip('"')
        last_modified = obj["LastModified"].isoformat()
        key = obj["Key"]
        size = obj["Size"]

        total_keys += 1
        total_size += size

        if args.limit and total_keys > args.limit:
            break

        row = [
            etag,
            last_modified,
            "s3://" + bucket + "/" + key,
            size,
        ]
        writer.writerow(row)

    end = time.time()
    print(f"Time taken: {end - start} seconds")
    print(f"Total objects: {readable_int(total_keys)}")
    print(f"Total size: {readable_int(total_size)}B")


if __name__ == "__main__":
    main()
