-- Sigma correlation "Failed Logins Followed by Successful Login"
-- (SigmaHQ correlation type: temporal_ordered, group-by: user, timespan: 1h)
-- Partition key: src_user_id -- the group-by field of the correlation rule.
SELECT e1.src_user_id, e1.ts_epoch AS fail_ts, e2.ts_epoch AS success_ts, e2.dst_host_id
FROM auth_events AS e1, auth_events AS e2
WHERE e1.event_code = 4625
  AND e1.auth_type = 'NTLM'
  AND e1.logon_type = 'Network'
  AND e2.event_code = 4624
  AND e2.auth_type = 'NTLM'
  AND e2.logon_type = 'Network'
  AND e2.src_user_id = e1.src_user_id
  AND e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 3600;
