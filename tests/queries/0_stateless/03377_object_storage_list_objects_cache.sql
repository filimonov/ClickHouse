-- Tags: no-parallel, no-fasttest

SYSTEM DROP OBJECT STORAGE LIST OBJECTS CACHE;

INSERT INTO TABLE FUNCTION s3(s3_conn, filename='dir_a/dir_b/t_03377_sample.parquet', format='Parquet', structure='id UInt64') SETTINGS s3_truncate_on_insert=1 VALUES (1);

SELECT * FROM s3(s3_conn, filename='dir_**.parquet') Format Null SETTINGS use_object_storage_list_objects_cache=1, log_comment='cold_list_cache';
SELECT * FROM s3(s3_conn, filename='dir_**.parquet') Format Null SETTINGS use_object_storage_list_objects_cache=1, log_comment='warm_list_exact_cache';
SELECT * FROM s3(s3_conn, filename='dir_a/dir_b**.parquet') Format Null SETTINGS use_object_storage_list_objects_cache=1, log_comment='warm_list_prefix_match_cache';
SELECT * FROM s3(s3_conn, filename='dirr_**.parquet') Format Null SETTINGS use_object_storage_list_objects_cache=1, log_comment='warm_list_cache_miss'; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM s3(s3_conn, filename='d**.parquet') Format Null SETTINGS use_object_storage_list_objects_cache=1, log_comment='even_shorter_prefix';
SELECT * FROM s3(s3_conn, filename='dir_**.parquet') Format Null SETTINGS use_object_storage_list_objects_cache=1, log_comment='still_exact_match_after_shorter_prefix';
SYSTEM DROP OBJECT STORAGE LIST OBJECTS CACHE;
SELECT * FROM s3(s3_conn, filename='dir_**.parquet') Format Null SETTINGS use_object_storage_list_objects_cache=1, log_comment='after_drop';

SYSTEM FLUSH LOGS;

-- { echoOn }

SELECT  ProfileEvents['ObjectStorageListObjectsCacheMisses'] > 0 as miss
FROM system.query_log
where log_comment = 'cold_list_cache'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheHits'] > 0 as hit
FROM system.query_log
where log_comment = 'warm_list_exact_cache'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheExactMatchHits'] > 0 as hit
FROM system.query_log
where log_comment = 'warm_list_exact_cache'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCachePrefixMatchHits'] > 0 as prefix_match_hit
FROM system.query_log
where log_comment = 'warm_list_exact_cache'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheHits'] > 0 as hit
FROM system.query_log
where log_comment = 'warm_list_prefix_match_cache'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheExactMatchHits'] > 0 as exact_match_hit
FROM system.query_log
where log_comment = 'warm_list_prefix_match_cache'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCachePrefixMatchHits'] > 0 as prefix_match_hit
FROM system.query_log
where log_comment = 'warm_list_prefix_match_cache'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheHits'] > 0 as hit
FROM system.query_log
where log_comment = 'even_shorter_prefix'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheMisses'] > 0 as miss
FROM system.query_log
where log_comment = 'even_shorter_prefix'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheHits'] > 0 as hit
FROM system.query_log
where log_comment = 'still_exact_match_after_shorter_prefix'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheExactMatchHits'] > 0 as exact_match_hit
FROM system.query_log
where log_comment = 'still_exact_match_after_shorter_prefix'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheHits'] > 0 as hit
FROM system.query_log
where log_comment = 'after_drop'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;

SELECT ProfileEvents['ObjectStorageListObjectsCacheMisses'] > 0 as miss
FROM system.query_log
where log_comment = 'after_drop'
AND type = 'QueryFinish'
ORDER BY event_time desc
LIMIT 1;
