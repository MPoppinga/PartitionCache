-- Elastic detection-rules EQL sequence: remote-exec tool spawn followed by outbound
-- service-ticket requests from the same host (timespan 15 min).
SELECT p1.src_host_id, p1.command_line, e1.dst_host_id
FROM process_events AS p1, auth_events AS e1
WHERE p1.process_name = 'psexesvc.exe'
  AND e1.event_code = 4769
  AND e1.auth_type = 'Kerberos'
  AND e1.src_host_id = p1.src_host_id
  AND e1.ts_epoch BETWEEN p1.ts_epoch AND p1.ts_epoch + 900;
