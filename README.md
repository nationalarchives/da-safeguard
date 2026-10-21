# DA Safeguard

Utilities for working with S3 object inventories: `download.py` consumes S3
Object Created notifications from SQS and downloads the referenced objects,
while `restore.py` requests retrieval of archived S3 objects listed in the
database.

## Components

- `download.py` polls an SQS queue, downloads non-empty objects into a local
  work directory, acknowledges messages, and records completed keys in
  `safeguard_keys`.
- `restore.py` reads eligible objects from `s3_inventory` and requests a
  Bulk restore for objects that are not already being restored.
- `Dockerfile.downloader` builds the downloader image.
- `Dockerfile.db` builds a PostgreSQL image and loads compressed CSV files
  from `inventory-files/` into `versioned_s3_inventory` using `load.sql`.
- `Dockerfile.droid` builds a DROID container for identifying files in a
  mounted directory.

## Requirements

- Python 3 and the dependencies `boto3` and `psycopg2-binary` for the Python
  scripts.
- AWS credentials and permissions for the relevant SQS and S3 operations.
- PostgreSQL access and the database tables expected by the selected script.

The scripts read PostgreSQL connection settings from their `DB` configuration;
`DBHOST` overrides the host (`pg` by default in `download.py`, `localhost` in
`restore.py`). Configure the remaining connection details for your
environment before running them.

## Run the downloader

Set the required environment variables, then run:

```sh
export QUEUE_URL="https://sqs.<region>.amazonaws.com/<account>/<queue>"
export WORK_DIR="/path/to/downloads"
export CONCURRENCY=50 # optional; defaults to 50
export DBHOST="pg"    # optional; defaults to pg
python3 download.py
```

The downloader uses the standard AWS credential and region configuration
supported by Boto3. It stores each object under
`WORK_DIR/<bucket>/<object-key>`. `SIGTERM` and `SIGINT` request a graceful
shutdown.

## Run the restore utility

Configure PostgreSQL and AWS access, then run:

```sh
python3 restore.py
```

The bucket is currently selected in the `__main__` block of `restore.py`
(`source-das-28`). The script queries `s3_inventory`, processes results with a
100-process pool, and requests a one-day Bulk restore for each eligible key.
Change the bucket in the script when restoring a different inventory.

## Docker images

Build the downloader image from the repository root:

```sh
docker build -f Dockerfile.downloader -t da-safeguard-downloader .
```

The database image expects an `inventory-files/` directory containing
compressed CSV inventory files:

```sh
docker build -f Dockerfile.db -t da-safeguard-db .
```

Run the DROID image with the files directory mounted at `/files`; it writes
its report to `/files/out.csv`:

```sh
docker build -f Dockerfile.droid -t da-safeguard-droid .
docker run --rm -v "/path/to/files:/files" da-safeguard-droid
```

## Tests

Run the unit tests with:

```sh
python3 -m unittest test_download.py test_restore.py
```
