

## OpenSearcClient

```
from opensearch_utils import OpenSearcClient
client = OpenSearcClient(HOST: str, PORT: int, REGION: str, AWS_ACCESSKEY: str, AWS_SECRETKEY: str)

```

## Make Index

```
make_index(index_name: str, index_mapping: dict, overwrite=True: bool) # If overwrite is true, overwrite the index if it already exists

```

## Running bulk indexing

```
run_bulk_index(document: list, batch_size=500: int, index_name='': str)

```



