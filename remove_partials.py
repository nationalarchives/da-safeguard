import csv
import sys
from pathlib import Path
from urllib.parse import unquote

input_csv = Path(sys.argv[1])
root = Path(sys.argv[2]).resolve()
confirm = len(sys.argv) > 3 and sys.argv[3] == "confirm"

if not confirm:
    print("Dry run, nothing will be removed")

removed = 0
missing = 0
skipped = 0

with open(input_csv, newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        key = unquote(row["s3_key"])
        path = (root / key).resolve()

        # is the path inside the folder
        if not path.is_relative_to(root):
            print(f"Skipped: outside {root}: {key}")
            skipped += 1
            continue

        # is the file there
        if not path.is_file():
                    print(f"Missing {path}")
                    missing += 1
                    continue

        # is it already a complete file
        expected = row.get("expected_size")
        if expected and path.stat().st_size >= int(expected):
              print(f"Skipped (full size): {path}")
              skipped += 1
              continue

        if confirm:
              path.unlink()
              print(f"removed: {path}")
        else:
              print(f"would remove: {path}")
              removed += 1

action = "removed" if confirm else "would remove"
print(f"{action} {removed}, missing {missing}, skipped {skipped}")