-- Sigma exploit-chain correlation: suspicious authentication immediately followed by a
-- remote-exec tool spawn on the SAME host (timespan 10s). Cross-source (auth x process).
-- Partition key: src_host_id -- present on both fact tables.
SELECT e1.src_host_id, e1.src_user_id, p1.command_line
FROM auth_events AS e1, process_events AS p1
WHERE e1.event_code = 4648
  AND e1.auth_type = 'MSV1_0'
  AND e1.logon_type = 'NewCredentials'
  AND p1.process_name = 'psexesvc.exe'
  AND p1.src_host_id = e1.src_host_id
  AND p1.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 10;
