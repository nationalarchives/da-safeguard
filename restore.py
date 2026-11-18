import json
import os
from multiprocessing import Pool
from urllib.parse import unquote, unquote_plus

import boto3
import psycopg2
from botocore.config import Config


DB = dict(
    host=os.environ.get("DBHOST", "localhost"),
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password",
)

config = Config(region_name="eu-west-2")
s3 = boto3.client("s3", config=config)
sqs = boto3.client("sqs", config=config)

def process_result(result):
    errors = 0
    skipped = 0
    succeeded = 0
    bucket, encoded_key, version_id = result
    key = unquote(encoded_key)
    version = {"VersionId": version_id} if version_id else {}
    try:
        response = s3.head_object(Bucket=bucket, Key=key, **version)
        if response.get("Restore") != 'ongoing-request="true"' and response.get("StorageClass") == "GLACIER":
            s3.restore_object(
                Bucket=bucket,
                Key=key,
                RestoreRequest={
                    'Days': 1,
                    'GlacierJobParameters': {
                        'Tier': 'Bulk'
                    }
                }, **version)
            succeeded = 1
        elif not response.get("StorageClass") or response.get("StorageClass") == "STANDARD":
            message = {"Records": [
                {
                    "s3": {
                        "bucket": {"name": "source-tf-safeguard"},
                        "object": {"key": key, "size": response.get("Size", 0), "versionId": None}
                    }
                }
            ]}
            sqs.send_message(QueueUrl="https://sqs.eu-west-2.amazonaws.com/655103965971/sftp-s3-event-notification", MessageBody=json.dumps(message))
        else:
            skipped = 1
    except Exception as e:
        print(f"{result} {e}")
        errors = 1

    return {"errors": errors, "succeeded": succeeded, "skipped": skipped}

def run(bucket, prefix="", deleted=False, chunk=None):
    page_size = 100000
    errors = 0
    succeeded = 0
    skipped = 0
    with psycopg2.connect(**DB) as conn:
        with conn.cursor(name="restore_inventory") as cur:
            if chunk:
                cur.execute("select vsi.bucket, vsi.s3_key, null from versioned_s3_inventory vsi join sftp_chunks on start_with(vsi.s3_key, c.prefix) where vsi.bucket = %s and vsi.obj_size != 0 and vsi.is_latest and not vsi.is_deleted_marker;", (bucket, chunk))
            elif not deleted:
                # remove this as was just used for source-das-0's missed files?
                # cur.execute("select vsi.bucket, vsi.s3_key, null from versioned_s3_inventory vsi where obj_size != 0 and vsi.is_latest and not vsi.is_delete_marker and bucket = %s and starts_with(vsi.s3_key, %s) and not exists (select 1 from all_source_0_files asf where replace(replace(vsi.s3_key,':','%%3A'),'*','%%2A') = replace(asf.s3_key,'~','%%7E'));", (bucket, prefix))
                cur.execute("select vsi.bucket, vsi.s3_key, null from versioned_s3_inventory vsi where obj_size != 0 and vsi.is_latest and not vsi.is_delete_marker and bucket = %s and starts_with(vsi.s3_key, %s);", (bucket, prefix))
            else:
                cur.execute("select distinct on (vsi.s3_key) vsi.bucket, vsi.s3_key, vsi.version_id from versioned_s3_inventory vsi join versioned_s3_inventory marker on marker.bucket = vsi.bucket and marker.s3_key = vsi.s3_key and marker.is_latest and marker.is_delete_marker where vsi.obj_size != 0 and not vsi.is_delete_marker and vsi.bucket = %s and starts_with(vsi.s3_key, %s) order by vsi.s3_key, vsi.last_modified_date desc;", (bucket, prefix))
            with Pool(100) as p:
                while True:
                    results = cur.fetchmany(page_size)
                    if not results:
                        break
                    print(f"Processing {len(results)} results")
                    results = p.map(process_result, results)
                    succeeded += sum([x["succeeded"] for x in results])
                    errors += sum([x["errors"] for x in results])
                    skipped += sum([x["skipped"] for x in results])
                    print(f"Processed {len(results)} results with {succeeded} success, {errors} errors and {skipped} skipped")
    print(f"Succeeded {succeeded}")
    print(f"Skipped {skipped}")
    print(f"Errors {errors}")

def split_list(text):
    return [item.strip() for item in text.split(",") if item.strip()] or [""]


PREFIXES = split_list(os.environ.get("PREFIX", ""))
DELETED = os.environ.get("DELETED", "").lower() == "true"
CHUNK = os.environ.get("CHUNK")

if __name__ == "__main__":
    bucket = os.environ["BUCKET"]
    if CHUNK:
        print(f"Chunk {CHUNK} from sftp bucket")
        run(bucket, chunk=int(CHUNK))
    else:
        for prefix in PREFIXES:
            run(bucket, prefix, DELETED)

