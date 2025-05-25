# s3-ls

Fast S3 object listing

## Introduction

Listing S3 directory containing millions of objects is very painful because it has to be sequential, but maximum number of objects per one request is limited to 1000.

```python
# First 1k objects
response = s3.list_objects_v2(
    Bucket=bucket, Prefix=prefix, StartAfter=""
)
last_key = response["Contents"][-1]["Key"]

# Next 1k objects
response = s3.list_objects_v2(
    Bucket=bucket, Prefix=prefix, StartAfter=last_key
)
...
```

`s3-ls` parallelizes it by partitioning the prefix into multiple parts and listing them in parallel.

```
prefix = "path/to/dir/"

prefix_1 = "path/to/dir/a"
prefix_2 = "path/to/dir/b"
prefix_3 = "path/to/dir/c"
... (70 prefixes)
```

In this way, the listing process can be done **70x faster** in the ideal case.

## Quick Start

### Installation

```bash
pip install s3-ls
```

### Usage

```bash
s3-ls s3://bucket/prefix
```

```python
from s3_ls import list_objects

for obj in list_objects(bucket, prefix, **s3_kwargs):
    key = obj["Key"]
```
