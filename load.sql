CREATE TABLE IF NOT EXISTS versioned_s3_inventory (
	bucket text NULL,
	s3_key text NULL,
	version_id text NULL,
	is_latest bool NULL,
	is_delete_marker bool NULL,
	obj_size int8 NULL,
	last_modified_date timestamptz NULL,
	storage_class text NULL,
	is_multipart_uploaded bool NULL,
	replication_status text NULL,
	intelligent_tiering_access_tier text NULL
);

\copy versioned_s3_inventory FROM PROGRAM 'for f in /inventory/*.csv.gz; do gzip -dc -- "$f"; done' WITH (FORMAT csv, NULL '', FORCE_NULL (obj_size, last_modified_date, is_latest, is_delete_marker, is_multipart_uploaded));
