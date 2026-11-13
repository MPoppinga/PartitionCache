-- Sigma correlation: explicit-credential logon (4648) followed by a remote-exec tool spawn.
-- Cross-source correlation (auth telemetry x process telemetry), group-by user, timespan 10s.
SELECT e1.src_user_id, e1.src_host_id, p1.command_line
FROM auth_events AS e1, process_events AS p1
WHERE e1.event_code = 4648
  AND e1.auth_type = 'MSV1_0'
  AND e1.logon_type = 'NewCredentials'
  AND p1.process_name = 'psexesvc.exe'
  AND p1.src_user_id = e1.src_user_id
  AND p1.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 10;
