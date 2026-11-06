-- Cross-source chained correlation over a day bucket: tool spawn followed by
-- service-ticket requests, then an explicit-credential logon within 24h.
SELECT e1.bucket_1d, p1.src_host_id, e1.dst_host_id, e2.src_user_id
FROM process_events AS p1, auth_events AS e1, auth_events AS e2
WHERE p1.process_name = 'psexesvc.exe'
  AND e1.event_code = 4769
  AND e1.auth_type = 'Kerberos'
  AND e1.src_host_id = p1.src_host_id
  AND e1.ts_epoch BETWEEN p1.ts_epoch AND p1.ts_epoch + 900
  AND e2.event_code = 4648
  AND e2.auth_type = 'MSV1_0'
  AND e2.src_user_id = e1.src_user_id
  AND e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 86400;
