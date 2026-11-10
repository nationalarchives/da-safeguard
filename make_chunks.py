import psycopg2
import sys

DB = dict(
    host="localhost",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="password"
)

BUCKET = "source-tf-safeguard"
INVENTORY_TABLE = "sftp_inventory"
CHUNKS_TABLE = "sftp_chunks"
LIMIT = 4_000_000_000_000

SIZES_SQL = f"""
SELECT %(prefix)s || split_part(substr(s3_key, length(%(prefix)s) + 1), '/', 1)
       || case when position('/' in substr(s3_key, length(%(prefix)s) + 1)) > 0 then '/' else '' end as item,
       sum(obj_size)::bigint as bytes
from {INVENTORY_TABLE}
where bucket = %(bucket)s and is_latest and not is_delete_marker and obj_size != 0
  and starts_with(s3_key, %(prefix)s)
group by 1
order by 1;
"""

def items_under(cur, prefix, limit=LIMIT):
    cur.execute(SIZES_SQL, {"prefix": prefix, "bucket": BUCKET})
    items = []
    for item, size in cur.fetchall():
        if size > limit and item.endswith("/"):
            print(f"splitting {item} ({size / 1e12:.2f} TB) into its subfolders")
            items += items_under(cur, item, limit)
        else:
            items.append((item,size))
    return items

def make_chunks(items, limit=LIMIT):
    chunks = []
    too_big = []
    current = []
    used = 0
    for item, size in items:
        if size > limit:
            too_big.append((item, size))
            continue
        if used + size > limit:
            chunks.append((current, used))
            current = []
            used = 0
        current.append(item)
        used += size
    if current:
        chunks.append((current, used))
    return chunks, too_big

def save_chunks(conn, chunks):
    with conn.cursor() as cur:
        cur.execute(f"create table if not exists {CHUNKS_TABLE} (chunk int, prefix text, chunk_bytes bigint)")
        cur.execute(f"delete from {CHUNKS_TABLE}")
        for number, (names, size) in enumerate(chunks, start=1):
            for name in names:
                cur.execute(f"insert into {CHUNKS_TABLE} (chunk, prefix, chunk_bytes) values (%s, %s, %s)", (number, name, size))
    conn.commit()
 
 
if __name__ == "__main__":
    save = len(sys.argv) > 1 and sys.argv[1] == "save" 
 
    conn = psycopg2.connect(**DB)
    try:
        with conn.cursor() as cur:
            items = items_under(cur, "")
  
        chunks, too_big = make_chunks(items)
 
        for item, size in too_big:
            print(f"TOO BIG FOR ONE CHUNK, a single file: {item} ({size / 1e12:.2f} TB)")
        for number, (names, size) in enumerate(chunks, start=1):
            print(f"Chunk {number}: {size / 1e12:.2f} TB, {len(names)} prefix(es)")
            print(f"  PREFIX={','.join(names)}")
 
        if save:
            save_chunks(conn, chunks)
            print(f"Saved {len(chunks)} chunk(s) to {CHUNKS_TABLE}")
    finally:
        conn.close()