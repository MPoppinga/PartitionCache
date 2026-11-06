#!/usr/bin/env python3
"""
Security log data generator for the PartitionCache security/log-analysis benchmark.

Generates a synthetic enterprise authentication and process-execution log with planted
red-team campaigns and a ground-truth table. The schema deliberately mirrors the LANL
"Comprehensive, Multi-Source Cyber-Security Events" (Kent, 2015) layout so that the real
LANL dump can later be loaded into the same tables without touching the queries.

Why synthetic first: the point of this workload is to test whether detection-rule queries
(Sigma correlations, Elastic EQL sequences) have the structure PartitionCache needs. That
question is about query shape and partition-key behaviour, so a reproducible generator with
known ground truth is worth more than a 12 GB download.

Tables:
    auth_events     - authentication events (LANL auth.txt shape, flattened)
    process_events  - process creation events (host telemetry)
    redteam_events  - ground truth: which events belong to a planted campaign

Bucket columns (bucket_1h, bucket_1d) are precomputed temporal partition keys. Campaigns are
deliberately placed so that a controlled share of their event pairs straddles a bucket
boundary - that is what makes the temporal-dilation experiment deterministic instead of luck.

Usage:
    # Small smoke-test load
    python generate_security_log_data.py --scale 0.01

    # Full load (~20M auth events)
    python generate_security_log_data.py

    # Recreate tables from scratch
    python generate_security_log_data.py --scale 0.1 --drop-existing
"""

import argparse
import os
import random
import sys
from datetime import datetime, timedelta

try:
    import psycopg
except ImportError:
    print("ERROR: psycopg is required. Install with: pip install -e '.[db]'", file=sys.stderr)
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# --- Campaign / corpus shape -------------------------------------------------

DAYS = 58  # matches the LANL 2015 collection window
SECONDS_PER_DAY = 86400
TOTAL_SECONDS = DAYS * SECONDS_PER_DAY
EPOCH_START = datetime(2015, 1, 1)

AUTH_EVENTS_AT_SCALE_1 = 20_000_000
PROCESS_EVENTS_AT_SCALE_1 = 4_000_000
USERS_AT_SCALE_1 = 10_000
HOSTS_AT_SCALE_1 = 1_000
CAMPAIGNS_AT_SCALE_1 = 40

BATCH_ROWS = 50_000

# Background event-code mix. Failed logons are kept rare on purpose: in a corpus where every
# user fails constantly, "failed then successful logon" matches by chance everywhere and the
# planted campaigns drown in noise.
AUTH_EVENT_CODES = [4624, 4634, 4769, 4768, 4625, 4648]
AUTH_EVENT_WEIGHTS = [55.0, 20.0, 12.0, 8.0, 4.5, 0.5]

AUTH_TYPES = ["Kerberos", "Negotiate", "NTLM", "MSV1_0"]
AUTH_TYPE_WEIGHTS = [60.0, 25.0, 14.0, 1.0]

LOGON_TYPES = ["Network", "Service", "Batch", "Interactive", "NewCredentials"]
LOGON_TYPE_WEIGHTS = [50.0, 20.0, 15.0, 12.0, 3.0]

AUTH_ORIENTS = {4624: "LogOn", 4634: "LogOff", 4769: "TGS", 4768: "TGT", 4625: "LogOn", 4648: "LogOn"}

# Office-hours weighting, so buckets are not uniformly loaded.
DIURNAL_WEIGHTS = [
    2.0, 1.5, 1.0, 1.0, 1.0, 1.5, 4.0, 9.0, 18.0, 24.0, 26.0, 25.0,
    22.0, 24.0, 26.0, 25.0, 20.0, 14.0, 9.0, 7.0, 6.0, 5.0, 4.0, 3.0,
]

COMMON_PROCESSES = [
    "svchost.exe", "explorer.exe", "chrome.exe", "outlook.exe", "cmd.exe",
    "powershell.exe", "taskhostw.exe", "conhost.exe", "msedge.exe", "teams.exe",
    # Admin tooling: legitimately used, which is what makes it a realistic detection
    # anchor. A signature with zero benign occurrences would turn the correlation query
    # into a pure lookup and make its selectivity meaningless.
    "wmiprvse.exe", "psexesvc.exe",
]
COMMON_PROCESS_WEIGHTS = [25.0, 15.0, 14.0, 12.0, 9.0, 7.0, 7.0, 5.0, 4.0, 2.0, 0.4, 0.15]
PARENT_PROCESSES = ["services.exe", "explorer.exe", "userinit.exe", "wininit.exe"]

# Campaign signatures
CAMPAIGN_TOOL_PROCESS = "psexesvc.exe"
CAMPAIGN_TOOL_CMDLINE = "psexesvc.exe -accepteula -s cmd.exe /c whoami"

SCHEMA_SQL = """
CREATE TABLE auth_events (
    event_id     BIGINT PRIMARY KEY,
    ts_epoch     BIGINT   NOT NULL,
    ts           TIMESTAMP NOT NULL,
    bucket_1h    INTEGER  NOT NULL,
    bucket_1d    INTEGER  NOT NULL,
    src_user_id  INTEGER  NOT NULL,
    dst_user_id  INTEGER  NOT NULL,
    src_host_id  INTEGER  NOT NULL,
    dst_host_id  INTEGER  NOT NULL,
    auth_type    TEXT     NOT NULL,
    logon_type   TEXT     NOT NULL,
    auth_orient  TEXT     NOT NULL,
    outcome      TEXT     NOT NULL,
    event_code   INTEGER  NOT NULL
);

CREATE TABLE process_events (
    event_id       BIGINT PRIMARY KEY,
    ts_epoch       BIGINT   NOT NULL,
    ts             TIMESTAMP NOT NULL,
    bucket_1h      INTEGER  NOT NULL,
    bucket_1d      INTEGER  NOT NULL,
    src_host_id    INTEGER  NOT NULL,
    src_user_id    INTEGER  NOT NULL,
    parent_process TEXT     NOT NULL,
    process_name   TEXT     NOT NULL,
    command_line   TEXT     NOT NULL,
    event_code     INTEGER  NOT NULL
);

CREATE TABLE redteam_events (
    event_id         BIGINT  NOT NULL,
    source           TEXT    NOT NULL,
    campaign_id      INTEGER NOT NULL,
    technique        TEXT    NOT NULL,
    straddles_1h     BOOLEAN NOT NULL DEFAULT FALSE,
    straddles_1d     BOOLEAN NOT NULL DEFAULT FALSE
);
"""

INDEX_SQL = """
CREATE INDEX idx_auth_ts          ON auth_events (ts_epoch);
CREATE INDEX idx_auth_user_ts     ON auth_events (src_user_id, ts_epoch);
CREATE INDEX idx_auth_host_ts     ON auth_events (src_host_id, ts_epoch);
CREATE INDEX idx_auth_code_ts     ON auth_events (event_code, ts_epoch);
CREATE INDEX idx_auth_bucket_1h   ON auth_events (bucket_1h);
CREATE INDEX idx_auth_bucket_1d   ON auth_events (bucket_1d);
CREATE INDEX idx_proc_ts          ON process_events (ts_epoch);
CREATE INDEX idx_proc_host_ts     ON process_events (src_host_id, ts_epoch);
CREATE INDEX idx_proc_name_ts     ON process_events (process_name, ts_epoch);
CREATE INDEX idx_proc_bucket_1h   ON process_events (bucket_1h);
CREATE INDEX idx_proc_bucket_1d   ON process_events (bucket_1d);
CREATE INDEX idx_redteam_event    ON redteam_events (event_id, source);
"""


def ts_columns(ts_epoch: int) -> tuple[str, int, int]:
    """Return (timestamp literal, bucket_1h, bucket_1d) for an epoch offset."""
    return (
        (EPOCH_START + timedelta(seconds=ts_epoch)).isoformat(sep=" "),
        ts_epoch // 3600,
        ts_epoch // SECONDS_PER_DAY,
    )


def random_timestamp(rng: random.Random) -> int:
    """Draw an epoch offset with an office-hours diurnal profile."""
    day = rng.randrange(DAYS)
    hour = rng.choices(range(24), weights=DIURNAL_WEIGHTS, k=1)[0]
    return day * SECONDS_PER_DAY + hour * 3600 + rng.randrange(3600)


# Hours whose diurnal weight is lowest, i.e. the sparsest buckets.
QUIET_HOURS = (2, 3, 4)


def quiet_hour_boundary(rng: random.Random) -> int:
    """Return an hour boundary inside the quiet part of the night.

    Straddling campaigns are anchored here for two reasons. It is realistic -- off-hours
    activity is the norm for hands-on-keyboard intrusion. And it keeps the boundary effect
    observable: in a densely populated bucket some unrelated event usually satisfies the
    other side of the correlation by chance, so a naive bucket intersection survives even
    though it is unsound. Sparse buckets expose the missing dilation instead of hiding it.
    """
    day = rng.randrange(DAYS)
    hour = rng.choice(QUIET_HOURS)
    return day * SECONDS_PER_DAY + hour * 3600


class EstateProfile:
    """Per-entity behaviour, so attribute filters are actually selective.

    Drawing the user uniformly at random for every event makes every user look alike: over 58
    days each one accumulates thousands of events covering every auth_type and logon_type, so
    a fragment like "users with a failed NTLM network logon" matches the entire estate and the
    cache prunes nothing. Real enterprise telemetry is heavy-tailed -- most accounts never use
    NTLM, most never fail a logon, and activity concentrates on a minority of hosts.

    Each user therefore gets a dominant auth type, a dominant logon type, a home host, and a
    flag for whether it ever fails authentication at all. Selectivity is a property of the data
    distribution, not of the cache, so this modelling choice is what the benchmark measures
    against.
    """

    # Fraction of accounts that ever produce a failed logon.
    FAILING_USER_SHARE = 0.12
    # Probability an event uses the user's dominant attribute rather than a random one.
    DOMINANT_SHARE = 0.93

    def __init__(self, rng: random.Random, n_users: int, n_hosts: int) -> None:
        self.auth_type = rng.choices(AUTH_TYPES, weights=AUTH_TYPE_WEIGHTS, k=n_users + 1)
        self.logon_type = rng.choices(LOGON_TYPES, weights=LOGON_TYPE_WEIGHTS, k=n_users + 1)
        self.home_host = [rng.randrange(1, n_hosts + 1) for _ in range(n_users + 1)]
        self.can_fail = [rng.random() < self.FAILING_USER_SHARE for _ in range(n_users + 1)]

    def draw_user(self, rng: random.Random, n_users: int) -> int:
        """Heavy-tailed user draw: a minority of accounts generate most of the traffic."""
        return min(n_users, 1 + int(n_users * rng.random() ** 2.2))


class Corpus:
    """Holds the generated rows and assigns event ids."""

    def __init__(self, auth_start_id: int = 1, process_start_id: int = 1) -> None:
        self.auth: list[tuple] = []
        self.process: list[tuple] = []
        self.redteam: list[tuple] = []
        self._next_auth_id = auth_start_id
        self._next_process_id = process_start_id

    def add_auth(
        self,
        ts_epoch: int,
        src_user: int,
        dst_user: int,
        src_host: int,
        dst_host: int,
        auth_type: str,
        logon_type: str,
        event_code: int,
    ) -> int:
        event_id = self._next_auth_id
        self._next_auth_id += 1
        ts, b1h, b1d = ts_columns(ts_epoch)
        self.auth.append(
            (
                event_id, ts_epoch, ts, b1h, b1d, src_user, dst_user, src_host, dst_host,
                auth_type, logon_type, AUTH_ORIENTS[event_code],
                "Fail" if event_code == 4625 else "Success", event_code,
            )
        )
        return event_id

    def add_process(
        self, ts_epoch: int, host: int, user: int, parent: str, name: str, cmdline: str
    ) -> int:
        event_id = self._next_process_id
        self._next_process_id += 1
        ts, b1h, b1d = ts_columns(ts_epoch)
        self.process.append((event_id, ts_epoch, ts, b1h, b1d, host, user, parent, name, cmdline, 4688))
        return event_id

    def mark(
        self, event_id: int, source: str, campaign_id: int, technique: str,
        straddles_1h: bool = False, straddles_1d: bool = False,
    ) -> None:
        self.redteam.append((event_id, source, campaign_id, technique, straddles_1h, straddles_1d))


def stream_background_auth(copy, rng: random.Random, profile: EstateProfile, n_auth: int, n_users: int, n_hosts: int, start_id: int) -> int:
    """Stream benign authentication traffic straight into COPY.

    Rows are written as they are drawn rather than accumulated, so peak memory stays flat
    regardless of scale. Random draws are batched because per-row ``random.choices`` calls
    dominate the runtime at tens of millions of rows.
    """
    print(f"  background auth events:    {n_auth:,}")
    event_id = start_id
    remaining = n_auth
    while remaining > 0:
        batch = min(BATCH_ROWS, remaining)
        hours = rng.choices(range(24), weights=DIURNAL_WEIGHTS, k=batch)
        codes = rng.choices(AUTH_EVENT_CODES, weights=AUTH_EVENT_WEIGHTS, k=batch)
        off_auth = rng.choices(AUTH_TYPES, weights=AUTH_TYPE_WEIGHTS, k=batch)
        off_logon = rng.choices(LOGON_TYPES, weights=LOGON_TYPE_WEIGHTS, k=batch)
        for i in range(batch):
            ts_epoch = rng.randrange(DAYS) * SECONDS_PER_DAY + hours[i] * 3600 + rng.randrange(3600)
            ts, b1h, b1d = ts_columns(ts_epoch)
            user = profile.draw_user(rng, n_users)
            code = codes[i]
            # Accounts that never fail authentication log a success instead.
            if code == 4625 and not profile.can_fail[user]:
                code = 4624
            # Attributes follow the account's own habits most of the time.
            auth_type = profile.auth_type[user] if rng.random() < profile.DOMINANT_SHARE else off_auth[i]
            logon_type = profile.logon_type[user] if rng.random() < profile.DOMINANT_SHARE else off_logon[i]
            src_host = profile.home_host[user] if rng.random() < 0.75 else rng.randrange(1, n_hosts + 1)
            # Most authentication is host-local; the rest fans out across the estate.
            dst_host = src_host if rng.random() < 0.6 else rng.randrange(1, n_hosts + 1)
            copy.write_row(
                (
                    event_id, ts_epoch, ts, b1h, b1d, user, user, src_host, dst_host,
                    auth_type, logon_type, AUTH_ORIENTS[code],
                    "Fail" if code == 4625 else "Success", code,
                )
            )
            event_id += 1
        remaining -= batch
        if (n_auth - remaining) % 20_000_000 == 0:
            print(f"    ... {n_auth - remaining:,}")
    return event_id


def stream_background_process(copy, rng: random.Random, profile: EstateProfile, n_process: int, n_users: int, n_hosts: int, start_id: int) -> int:
    """Stream benign process-creation telemetry straight into COPY."""
    print(f"  background process events: {n_process:,}")
    event_id = start_id
    remaining = n_process
    while remaining > 0:
        batch = min(BATCH_ROWS, remaining)
        hours = rng.choices(range(24), weights=DIURNAL_WEIGHTS, k=batch)
        names = rng.choices(COMMON_PROCESSES, weights=COMMON_PROCESS_WEIGHTS, k=batch)
        parents = rng.choices(PARENT_PROCESSES, k=batch)
        for i in range(batch):
            ts_epoch = rng.randrange(DAYS) * SECONDS_PER_DAY + hours[i] * 3600 + rng.randrange(3600)
            ts, b1h, b1d = ts_columns(ts_epoch)
            name = names[i]
            puser = profile.draw_user(rng, n_users)
            copy.write_row(
                (
                    event_id, ts_epoch, ts, b1h, b1d,
                    profile.home_host[puser], puser,
                    parents[i], name, f"{name} --session {rng.randrange(1000, 9999)}", 4688,
                )
            )
            event_id += 1
        remaining -= batch
    return event_id


def generate_campaigns(corpus: Corpus, rng: random.Random, n_campaigns: int, n_users: int, n_hosts: int) -> dict[str, int]:
    """Plant red-team campaigns with ground truth.

    Half of the campaigns are deliberately positioned across a bucket boundary so that the
    temporal-dilation experiment has a deterministic set of pairs that a naive bucket
    intersection must lose.
    """
    counts = {
        "brute_force_then_success": 0, "lateral_movement": 0, "exploit_then_spawn": 0,
        "post_exploitation_tickets": 0, "chained_24h": 0, "straddling_1h": 0, "straddling_1d": 0,
    }

    for campaign_id in range(1, n_campaigns + 1):
        user = rng.randrange(1, n_users + 1)
        target_host = rng.randrange(1, n_hosts + 1)
        straddle = campaign_id % 2 == 0

        # --- Technique 1: many failed NTLM/Network logons, then a success (Sigma temporal_ordered)
        if straddle:
            # Anchor just before an hour boundary, success just after -> different bucket_1h
            boundary = quiet_hour_boundary(rng)
            first_fail = boundary - rng.randrange(60, 240)
            success_ts = boundary + rng.randrange(30, 180)
        else:
            first_fail = random_timestamp(rng)
            success_ts = first_fail + rng.randrange(300, 1800)

        for _ in range(rng.randrange(8, 26)):
            eid = corpus.add_auth(
                first_fail + rng.randrange(0, 120), user, user,
                rng.randrange(1, n_hosts + 1), target_host, "NTLM", "Network", 4625,
            )
            corpus.mark(eid, "auth", campaign_id, "brute_force_then_success", straddle)
        eid = corpus.add_auth(success_ts, user, user, rng.randrange(1, n_hosts + 1), target_host, "NTLM", "Network", 4624)
        corpus.mark(eid, "auth", campaign_id, "brute_force_then_success", straddle)
        counts["brute_force_then_success"] += 1
        if straddle:
            counts["straddling_1h"] += 1

        # --- Technique 2: the compromised account fans out across the estate
        lateral_start = success_ts + rng.randrange(60, 900)
        for _ in range(rng.randrange(8, 16)):
            eid = corpus.add_auth(
                lateral_start + rng.randrange(0, 1800), user, user,
                target_host, rng.randrange(1, n_hosts + 1), "Kerberos", "Network", 4769,
            )
            corpus.mark(eid, "auth", campaign_id, "lateral_movement")
        counts["lateral_movement"] += 1

        # --- Technique 3: explicit-credential logon followed by a remote-exec tool spawn (10s)
        if straddle:
            boundary = quiet_hour_boundary(rng)
            exploit_ts = boundary - rng.randrange(2, 6)
            spawn_ts = boundary + rng.randrange(1, 5)
        else:
            exploit_ts = random_timestamp(rng)
            spawn_ts = exploit_ts + rng.randrange(1, 9)

        exec_host = rng.randrange(1, n_hosts + 1)
        eid = corpus.add_auth(exploit_ts, user, user, exec_host, exec_host, "MSV1_0", "NewCredentials", 4648)
        corpus.mark(eid, "auth", campaign_id, "exploit_then_spawn", straddle)
        pid = corpus.add_process(spawn_ts, exec_host, user, "services.exe", CAMPAIGN_TOOL_PROCESS, CAMPAIGN_TOOL_CMDLINE)
        corpus.mark(pid, "process", campaign_id, "exploit_then_spawn", straddle)
        counts["exploit_then_spawn"] += 1

        # Post-exploitation: the tool host starts requesting service tickets outward.
        for _ in range(rng.randrange(3, 8)):
            eid = corpus.add_auth(
                spawn_ts + rng.randrange(5, 900), user, user,
                exec_host, rng.randrange(1, n_hosts + 1), "Kerberos", "Network", 4769,
            )
            corpus.mark(eid, "auth", campaign_id, "post_exploitation_tickets")
        counts["post_exploitation_tickets"] += 1

        # --- Technique 4: a second burst by the same account within 24h (chained correlation)
        if straddle:
            day_boundary = exploit_ts // SECONDS_PER_DAY * SECONDS_PER_DAY + SECONDS_PER_DAY
            second_burst = day_boundary + rng.randrange(60, 3600)
            first_anchor = day_boundary - rng.randrange(60, 3600)
            counts["straddling_1d"] += 1
        else:
            first_anchor = exploit_ts
            second_burst = exploit_ts + rng.randrange(3600, 20 * 3600)

        eid = corpus.add_auth(first_anchor, user, user, exec_host, exec_host, "MSV1_0", "NewCredentials", 4648)
        corpus.mark(eid, "auth", campaign_id, "chained_24h", False, straddle)
        eid = corpus.add_auth(second_burst, user, user, exec_host, rng.randrange(1, n_hosts + 1), "MSV1_0", "NewCredentials", 4648)
        corpus.mark(eid, "auth", campaign_id, "chained_24h", False, straddle)
        counts["chained_24h"] += 1

    return counts


def copy_rows(cur, table: str, columns: list[str], rows: list[tuple]) -> None:
    """Bulk-load rows with COPY."""
    if not rows:
        return
    stmt = f"COPY {table} ({', '.join(columns)}) FROM STDIN"
    with cur.copy(stmt) as copy:
        for i in range(0, len(rows), BATCH_ROWS):
            for row in rows[i : i + BATCH_ROWS]:
                copy.write_row(row)


AUTH_COLUMNS = [
    "event_id", "ts_epoch", "ts", "bucket_1h", "bucket_1d", "src_user_id", "dst_user_id",
    "src_host_id", "dst_host_id", "auth_type", "logon_type", "auth_orient", "outcome", "event_code",
]
PROCESS_COLUMNS = [
    "event_id", "ts_epoch", "ts", "bucket_1h", "bucket_1d", "src_host_id", "src_user_id",
    "parent_process", "process_name", "command_line", "event_code",
]
REDTEAM_COLUMNS = ["event_id", "source", "campaign_id", "technique", "straddles_1h", "straddles_1d"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic security log data for the PartitionCache benchmark")
    parser.add_argument("--scale", type=float, default=1.0, help="Scale factor; 1.0 = ~20M auth events (default: 1.0)")
    parser.add_argument("--seed", type=int, default=20260819, help="RNG seed for reproducibility")
    parser.add_argument("--drop-existing", action="store_true", help="Drop and recreate the tables")
    parser.add_argument("--env-file", type=str, default="examples/security_log_benchmark/.env", help="Path to .env file")
    args = parser.parse_args()

    if load_dotenv is not None and os.path.exists(args.env_file):
        load_dotenv(args.env_file, override=True)

    if args.scale <= 0:
        print("ERROR: --scale must be positive", file=sys.stderr)
        return 1

    n_auth = max(1000, int(AUTH_EVENTS_AT_SCALE_1 * args.scale))
    n_process = max(200, int(PROCESS_EVENTS_AT_SCALE_1 * args.scale))
    # Entity counts grow as sqrt(scale) while events grow linearly. Holding the estate fixed
    # while multiplying events would inflate per-user event density, and the correlation
    # fragments self-join within a user -- so population cost would grow quadratically in
    # scale rather than tracking the data size.
    n_users = max(100, int(USERS_AT_SCALE_1 * args.scale**0.5))
    n_hosts = max(20, int(HOSTS_AT_SCALE_1 * args.scale**0.5))
    n_campaigns = max(10, int(CAMPAIGNS_AT_SCALE_1 * args.scale**0.5))

    print(f"Generating security log corpus (scale={args.scale}, seed={args.seed})")
    print(f"  users={n_users:,}  hosts={n_hosts:,}  campaigns={n_campaigns}  window={DAYS} days")

    conn_kwargs = {
        "host": os.environ.get("DB_HOST", "127.0.0.1"),
        "port": int(os.environ.get("DB_PORT", "5436")),
        "user": os.environ.get("DB_USER", "seclog_user"),
        "password": os.environ.get("DB_PASSWORD", "seclog_password"),
        "dbname": os.environ.get("DB_NAME", "security_log_db"),
    }
    print(f"Connecting to postgresql://{conn_kwargs['user']}@{conn_kwargs['host']}:{conn_kwargs['port']}/{conn_kwargs['dbname']}")

    rng = random.Random(args.seed)
    profile = EstateProfile(rng, n_users, n_hosts)
    with psycopg.connect(**conn_kwargs) as conn:
        with conn.cursor() as cur:
            if args.drop_existing:
                cur.execute("DROP TABLE IF EXISTS auth_events, process_events, redteam_events CASCADE")
            cur.execute("SELECT to_regclass('auth_events')")
            if cur.fetchone()[0] is not None:
                print("ERROR: tables already exist; rerun with --drop-existing", file=sys.stderr)
                return 1
            cur.execute(SCHEMA_SQL)
            conn.commit()

            # Background traffic is streamed directly into COPY so peak memory stays flat.
            with cur.copy(f"COPY auth_events ({', '.join(AUTH_COLUMNS)}) FROM STDIN") as copy:
                next_auth_id = stream_background_auth(copy, rng, profile, n_auth, n_users, n_hosts, 1)
            with cur.copy(f"COPY process_events ({', '.join(PROCESS_COLUMNS)}) FROM STDIN") as copy:
                next_process_id = stream_background_process(copy, rng, profile, n_process, n_users, n_hosts, 1)
            conn.commit()

            # Campaigns are small enough to build in memory; their ids continue after the
            # background rows so both share one id space per table.
            print("  planting red-team campaigns")
            corpus = Corpus(next_auth_id, next_process_id)
            counts = generate_campaigns(corpus, rng, n_campaigns, n_users, n_hosts)
            for name, value in counts.items():
                print(f"    {name}: {value}")

            print(f"Loading {len(corpus.auth):,} campaign auth events")
            copy_rows(cur, "auth_events", AUTH_COLUMNS, corpus.auth)
            print(f"Loading {len(corpus.process):,} campaign process events")
            copy_rows(cur, "process_events", PROCESS_COLUMNS, corpus.process)
            print(f"Loading {len(corpus.redteam):,} ground-truth rows")
            copy_rows(cur, "redteam_events", REDTEAM_COLUMNS, corpus.redteam)
            conn.commit()

            print("Creating indexes")
            cur.execute(INDEX_SQL)
            for table in ("auth_events", "process_events", "redteam_events"):
                cur.execute(f"ANALYZE {table}")
            conn.commit()

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
