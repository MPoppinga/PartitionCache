-- MITRE CAR: a host receiving failed NTLM network logons and then a successful one,
-- i.e. password spraying observed from the target host's perspective (timespan 1h).
SELECT e1.src_host_id, e1.dst_host_id, e2.src_user_id
FROM auth_events AS e1, auth_events AS e2
WHERE e1.event_code = 4625
  AND e1.auth_type = 'NTLM'
  AND e1.logon_type = 'Network'
  AND e2.event_code = 4624
  AND e2.auth_type = 'NTLM'
  AND e2.src_host_id = e1.src_host_id
  AND e2.src_user_id <> e1.src_user_id
  AND e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 3600;
