import json
import os
import boto3

SOURCE_IDS = [0, 6, 20, 28, 34]
INVENTORY_BUCKET = "da-safeguard-inventory"
OUTPUT_DIR = "inventory-files"

s3 = boto3.client("s3")


def process_source(i: int) -> None:
    base_prefix = f"source-das-{i}/source-das-{i}-inventory/" if i != 42 else f"source-das-{i}/source-das-{i}/"
    latest = "2026-10-25T01-00Z"

    manifest_key = f"{base_prefix}{latest}/manifest.json"

    manifest_path = os.path.join(OUTPUT_DIR, f"{i}-manifest.json")
    s3.download_file(INVENTORY_BUCKET, manifest_key, manifest_path)

    with open(manifest_path) as f:
        manifest = json.load(f)

    dest_bucket = manifest.get("destinationBucket", INVENTORY_BUCKET).split(":::")[-1]
    files = manifest.get("files", [])

    for entry in files:
        key = entry["key"]
        print(f"aws s3 cp s3://{dest_bucket}/{key} .")
        local_path = os.path.join(OUTPUT_DIR, os.path.basename(key))
        if os.path.exists(local_path) and os.path.getsize(local_path) == entry.get("size", -1):
            print(f"  Skipping (already downloaded): {key}")
            continue
        print(f"  Downloading s3://{dest_bucket}/{key} ({entry.get('size', '?')} bytes)")
        s3.download_file(dest_bucket, key, local_path)


def main() -> None:
    for i in SOURCE_IDS:
        try:
            process_source(i)
        except Exception as e:
            print(f"Error processing source-das-{i}: {e}")


if __name__ == "__main__":
    main()