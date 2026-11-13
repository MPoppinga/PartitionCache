-- Sigma correlation "Failed Logins Followed by Successful Login" (temporal_ordered, 1h),
-- re-keyed to the temporal partition key.
-- Partition key: bucket_1h. The 1h window can cross a bucket boundary, so the cached
-- bucket sets must be dilated by ceil(3600/3600) = 1 bucket before intersection.
SELECT e1.bucket_1h, e1.src_user_id, e1.ts_epoch AS fail_ts, e2.ts_epoch AS success_ts
FROM auth_events AS e1, auth_events AS e2
WHERE e1.event_code = 4625
  AND e1.auth_type = 'NTLM'
  AND e1.logon_type = 'Network'
  AND e2.event_code = 4624
  AND e2.auth_type = 'NTLM'
  AND e2.logon_type = 'Network'
  AND e2.src_user_id = e1.src_user_id
  AND e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 3600;
