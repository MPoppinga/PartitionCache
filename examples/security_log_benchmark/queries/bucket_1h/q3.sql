-- Lateral movement keyed on the temporal bucket (30 min window).
SELECT e1.bucket_1h, e1.src_user_id, e1.dst_host_id AS foothold_host, e2.dst_host_id AS lateral_host
FROM auth_events AS e1, auth_events AS e2
WHERE e1.event_code = 4624
  AND e1.auth_type = 'NTLM'
  AND e1.logon_type = 'Network'
  AND e2.event_code = 4769
  AND e2.auth_type = 'Kerberos'
  AND e2.src_user_id = e1.src_user_id
  AND e2.dst_host_id <> e1.dst_host_id
  AND e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 1800;
