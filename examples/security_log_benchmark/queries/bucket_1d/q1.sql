-- Sigma chained correlation: repeated explicit-credential logons by the same account
-- within 24h. Window (86400s) equals the bucket width, so dilation is +/-1 day bucket.
-- Partition key: bucket_1d.
SELECT e1.bucket_1d, e1.src_user_id, e1.ts_epoch AS first_ts, e2.ts_epoch AS second_ts
FROM auth_events AS e1, auth_events AS e2
WHERE e1.event_code = 4648
  AND e1.auth_type = 'MSV1_0'
  AND e1.logon_type = 'NewCredentials'
  AND e2.event_code = 4648
  AND e2.auth_type = 'MSV1_0'
  AND e2.logon_type = 'NewCredentials'
  AND e2.src_user_id = e1.src_user_id
  AND e2.dst_host_id <> e1.dst_host_id
  AND e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 86400;
