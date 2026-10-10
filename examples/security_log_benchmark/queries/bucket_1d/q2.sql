-- Three-stage chained correlation: brute force -> success -> lateral movement.
-- Windows chain to a 1h + 30min graph diameter, still inside one day bucket.
SELECT e1.bucket_1d, e1.src_user_id, e3.dst_host_id AS lateral_host
FROM auth_events AS e1, auth_events AS e2, auth_events AS e3
WHERE e1.event_code = 4625
  AND e1.auth_type = 'NTLM'
  AND e1.logon_type = 'Network'
  AND e2.event_code = 4624
  AND e2.auth_type = 'NTLM'
  AND e2.src_user_id = e1.src_user_id
  AND e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 3600
  AND e3.event_code = 4769
  AND e3.auth_type = 'Kerberos'
  AND e3.src_user_id = e2.src_user_id
  AND e3.ts_epoch BETWEEN e2.ts_epoch AND e2.ts_epoch + 1800;
