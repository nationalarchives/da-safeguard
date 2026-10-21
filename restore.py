import os
from multiprocessing import Pool
from urllib.parse import unquote

import boto3
import psycopg2

DB = dict(
    host=os.environ.get("DBHOST", "localhost"),
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password",
)


s3 = boto3.client("s3")

def process_result(result):
    errors = 0
    skipped = 0
    succeeded = 0
    bucket, encoded_key = result
    key = unquote(encoded_key)
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        if response.get("Restore") != 'ongoing-request="true"':
            s3.restore_object(
                Bucket=bucket,
                Key=key,
                RestoreRequest={
                    'Days': 1,
                    'GlacierJobParameters': {
                        'Tier': 'Bulk'
                    }
                })
            succeeded = 1
        else:
            skipped = 1
    except Exception as e:
        print(f"{result} {e}")
        errors = 1

    return {"errors": errors, "succeeded": succeeded, "skipped": skipped}

def run(bucket):
    with psycopg2.connect(**DB) as conn:
        with conn.cursor() as cur:
            cur.execute("select vsi.bucket, vsi.s3_key from versioned_s3_inventory vsi where obj_size != 0 and vsi.is_latest and not vsi.is_delete_marker and bucket = %s;", (bucket,))
            results = cur.fetchall()
    errors = 0
    succeeded = 0
    skipped = 0
    with Pool(100) as p:
        results = p.map(process_result, results)
        succeeded += sum([x["succeeded"] for x in results])
        errors += sum([x["errors"] for x in results])
        skipped += sum([x["skipped"] for x in results])
    print(f"Succeeded {succeeded}")
    print(f"Skipped {skipped}")
    print(f"Errors {errors}")

if __name__ == "__main__":
    run(os.environ["BUCKET_NAME"])