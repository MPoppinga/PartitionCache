#!/usr/bin/env python3
"""Load an LDBC SNB BI data set into DuckDB and/or PostgreSQL for PartitionCache benchmarking.

The LDBC data generator emits ``Post`` and ``Comment`` separately, plus ``Place``
and ``Organisation`` as polymorphic tables. The BI queries, however, are written
against the *merged* schema (``Message``, ``City``, ``Country``, ...). This script
reproduces the official derivations from the LDBC reference implementation
(``umbra/load.py``, ``umbra/dml/maintain-views.sql``,
``umbra/dml/create-static-materialized-views.sql``):

* ``Message``            = ``Post`` plus ``Comment``, where each comment inherits
  ``RootPostId`` / ``RootPostLanguage`` / ``ContainerForumId`` from the post at the
  root of its reply chain. This is what keeps ``ContainerForumId`` non-NULL for
  comments -- important, because a NULL there would silently defeat any
  ``ContainerForumId IN (...)`` cache filter.
* ``Message_hasTag_Tag`` = ``Post_hasTag_Tag`` plus ``Comment_hasTag_Tag``
* ``Person_likes_Message`` = ``Person_likes_Post`` plus ``Person_likes_Comment``
* ``Person_knows_Person`` is stored **symmetrically** (both directions), as the
  reference loader does.

DuckDB does the extraction and the transformation; PostgreSQL is populated through
DuckDB's ``postgres`` extension.

Usage::

    python generate_ldbc_data.py --data-dir /path/to/.../composite-merged-fk
    python generate_ldbc_data.py --skip-postgres          # DuckDB only (Track B)
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

import duckdb

logger = logging.getLogger("ldbc_loader")

# Raw entities: (entity directory, target table, explicit SELECT list or None)
STATIC_ENTITIES = ["Organisation", "Place", "Tag", "TagClass"]
DYNAMIC_ENTITIES = [
    "Comment",
    "Comment_hasTag_Tag",
    "Forum",
    "Forum_hasMember_Person",
    "Forum_hasTag_Tag",
    "Person",
    "Person_hasInterest_Tag",
    "Person_knows_Person",
    "Person_likes_Comment",
    "Person_likes_Post",
    "Person_studyAt_University",
    "Person_workAt_Company",
    "Post",
    "Post_hasTag_Tag",
]

# Tables that end up in PostgreSQL, in dependency-free order (no FKs are declared).
FINAL_TABLES = [
    "country",
    "city",
    "company",
    "university",
    "tag",
    "tagclass",
    "person",
    "forum",
    "message",
    "message_hastag_tag",
    "person_likes_message",
    "person_knows_person",
    "person_hasinterest_tag",
    "forum_hasmember_person",
    "forum_hastag_tag",
    "person_studyat_university",
    "person_workat_company",
]

INDEXES = [
    ("message", "creatorpersonid"),
    ("message", "containerforumid"),
    ("message", "locationcountryid"),
    ("message", "rootpostid"),
    ("message", "parentmessageid"),
    ("message", "creationdate"),
    ("message_hastag_tag", "messageid"),
    ("message_hastag_tag", "tagid"),
    ("person", "locationcityid"),
    ("city", "partofcountryid"),
    ("tag", "typetagclassid"),
    ("forum", "moderatorpersonid"),
    ("forum_hasmember_person", "personid"),
    ("forum_hasmember_person", "forumid"),
    ("person_knows_person", "person1id"),
    ("person_knows_person", "person2id"),
    ("person_hasinterest_tag", "personid"),
    ("person_hasinterest_tag", "tagid"),
    ("person_likes_message", "personid"),
    ("person_likes_message", "messageid"),
]


def _snapshot_dir(data_dir: Path) -> Path:
    """Locate the initial_snapshot directory, tolerating an outer wrapper directory."""
    candidates = [data_dir / "initial_snapshot", data_dir]
    candidates += list(data_dir.glob("graphs/csv/bi/composite-merged-fk/initial_snapshot"))
    candidates += list(data_dir.glob("*/graphs/csv/bi/composite-merged-fk/initial_snapshot"))
    for candidate in candidates:
        if (candidate / "static").is_dir() and (candidate / "dynamic").is_dir():
            return candidate
    raise SystemExit(f"Could not find an initial_snapshot with static/ and dynamic/ under {data_dir}")


def load_raw(con: duckdb.DuckDBPyConnection, snapshot: Path) -> None:
    """Read every raw CSV entity into a DuckDB table of the same name."""
    for group, entities in (("static", STATIC_ENTITIES), ("dynamic", DYNAMIC_ENTITIES)):
        for entity in entities:
            pattern = snapshot / group / entity / "part-*.csv.gz"
            start = time.time()
            con.execute(
                f'CREATE OR REPLACE TABLE "raw_{entity}" AS '
                f"SELECT * FROM read_csv('{pattern}', delim='|', header=true, nullstr='')"
            )
            rows = con.execute(f'SELECT count(*) FROM "raw_{entity}"').fetchone()[0]
            logger.info("  %-28s %10d rows  (%.1fs)", entity, rows, time.time() - start)


def derive(con: duckdb.DuckDBPyConnection) -> None:
    """Build the merged BI schema from the raw entities."""
    logger.info("Deriving static tables (Country/City/Company/University)")
    con.execute("CREATE OR REPLACE TABLE country AS SELECT id, name, url, PartOfPlaceId AS PartOfContinentId FROM raw_Place WHERE type = 'Country'")
    con.execute("CREATE OR REPLACE TABLE city    AS SELECT id, name, url, PartOfPlaceId AS PartOfCountryId   FROM raw_Place WHERE type = 'City'")
    con.execute("CREATE OR REPLACE TABLE company    AS SELECT id, name, url, LocationPlaceId AS LocatedInCountryId FROM raw_Organisation WHERE type = 'Company'")
    con.execute("CREATE OR REPLACE TABLE university AS SELECT id, name, url, LocationPlaceId AS LocatedInCityId    FROM raw_Organisation WHERE type = 'University'")

    logger.info("Deriving Message from Post")
    con.execute(
        """
        CREATE OR REPLACE TABLE message AS
        SELECT creationDate,
               id                 AS MessageId,
               id                 AS RootPostId,
               language           AS RootPostLanguage,
               content, imageFile, locationIP, browserUsed, length,
               CreatorPersonId,
               ContainerForumId,
               LocationCountryId,
               CAST(NULL AS BIGINT) AS ParentMessageId
        FROM raw_Post
        """
    )

    # Comments inherit RootPostId / RootPostLanguage / ContainerForumId from the root post.
    logger.info("Deriving Message from Comment (recursive reply-tree walk)")
    con.execute(
        """
        INSERT INTO message
        WITH RECURSIVE tree(CommentId, RootPostId, RootPostLanguage, ContainerForumId) AS (
            SELECT c.id, m.MessageId, m.RootPostLanguage, m.ContainerForumId
            FROM raw_Comment c
            JOIN message m ON m.MessageId = c.ParentPostId
          UNION ALL
            SELECT c.id, t.RootPostId, t.RootPostLanguage, t.ContainerForumId
            FROM raw_Comment c
            JOIN tree t ON t.CommentId = c.ParentCommentId
        )
        SELECT c.creationDate,
               c.id                AS MessageId,
               t.RootPostId,
               t.RootPostLanguage,
               c.content,
               CAST(NULL AS VARCHAR) AS imageFile,
               c.locationIP, c.browserUsed, c.length,
               c.CreatorPersonId,
               t.ContainerForumId,
               c.LocationCountryId,
               coalesce(c.ParentPostId, c.ParentCommentId) AS ParentMessageId
        FROM tree t
        JOIN raw_Comment c ON c.id = t.CommentId
        """
    )

    logger.info("Deriving Message_hasTag_Tag / Person_likes_Message")
    con.execute(
        """
        CREATE OR REPLACE TABLE message_hastag_tag AS
            SELECT creationDate, PostId    AS MessageId, TagId FROM raw_Post_hasTag_Tag
        UNION ALL
            SELECT creationDate, CommentId AS MessageId, TagId FROM raw_Comment_hasTag_Tag
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE person_likes_message AS
            SELECT creationDate, PersonId, PostId    AS MessageId FROM raw_Person_likes_Post
        UNION ALL
            SELECT creationDate, PersonId, CommentId AS MessageId FROM raw_Person_likes_Comment
        """
    )

    # The reference loader inserts each knows-edge in both directions.
    logger.info("Symmetrising Person_knows_Person")
    con.execute(
        """
        CREATE OR REPLACE TABLE person_knows_person AS
            SELECT creationDate, Person1Id, Person2Id FROM raw_Person_knows_Person
        UNION ALL
            SELECT creationDate, Person2Id AS Person1Id, Person1Id AS Person2Id FROM raw_Person_knows_Person
        """
    )

    logger.info("Renaming remaining entities to their BI names")
    con.execute("CREATE OR REPLACE TABLE person AS SELECT creationDate, id, firstName, lastName, gender, birthday, locationIP, browserUsed, LocationCityId, language AS speaks, email FROM raw_Person")
    con.execute("CREATE OR REPLACE TABLE forum AS SELECT * FROM raw_Forum")
    con.execute("CREATE OR REPLACE TABLE tag AS SELECT * FROM raw_Tag")
    con.execute("CREATE OR REPLACE TABLE tagclass AS SELECT * FROM raw_TagClass")
    con.execute("CREATE OR REPLACE TABLE person_hasinterest_tag AS SELECT creationDate, personId AS PersonId, interestId AS TagId FROM raw_Person_hasInterest_Tag")
    con.execute("CREATE OR REPLACE TABLE forum_hasmember_person AS SELECT * FROM raw_Forum_hasMember_Person")
    con.execute("CREATE OR REPLACE TABLE forum_hastag_tag AS SELECT * FROM raw_Forum_hasTag_Tag")
    con.execute("CREATE OR REPLACE TABLE person_studyat_university AS SELECT * FROM raw_Person_studyAt_University")
    con.execute("CREATE OR REPLACE TABLE person_workat_company AS SELECT * FROM raw_Person_workAt_Company")

    remap_ids(con)

    for entity in STATIC_ENTITIES + DYNAMIC_ENTITIES:
        con.execute(f'DROP TABLE IF EXISTS "raw_{entity}"')



# Every id column, grouped by the entity whose id space it draws on. LDBC ids are opaque
# surrogates in the 10^13 range; PartitionCache's postgresql_array handler stores an
# integer[] (int4, max 2.1e9), so the raw ids cannot be cached at all. Remapping every
# entity to a dense integer id is semantics-preserving (ids are surrogates) and is also
# what makes the bit-based cache backends usable at all.
ID_SPACES: dict[str, list[tuple[str, str]]] = {
    "person": [("person", "id"), ("forum", "moderatorpersonid"), ("message", "creatorpersonid"),
               ("message_hastag_tag", None), ("person_likes_message", "personid"),
               ("person_knows_person", "person1id"), ("person_knows_person", "person2id"),
               ("person_hasinterest_tag", "personid"), ("forum_hasmember_person", "personid"),
               ("person_studyat_university", "personid"), ("person_workat_company", "personid")],
    "forum": [("forum", "id"), ("message", "containerforumid"),
              ("forum_hasmember_person", "forumid"), ("forum_hastag_tag", "forumid")],
    "message": [("message", "messageid"), ("message", "rootpostid"), ("message", "parentmessageid"),
                ("message_hastag_tag", "messageid"), ("person_likes_message", "messageid")],
    "tag": [("tag", "id"), ("message_hastag_tag", "tagid"), ("person_hasinterest_tag", "tagid"),
            ("forum_hastag_tag", "tagid")],
    "tagclass": [("tagclass", "id"), ("tagclass", "subclassoftagclassid"), ("tag", "typetagclassid")],
    "place": [("city", "id"), ("city", "partofcountryid"), ("country", "id"),
              ("country", "partofcontinentid"), ("person", "locationcityid"),
              ("message", "locationcountryid"), ("company", "locatedincountryid"),
              ("university", "locatedincityid")],
    "organisation": [("company", "id"), ("university", "id"),
                     ("person_studyat_university", "universityid"), ("person_workat_company", "companyid")],
}
# id-space source: the table/column that enumerates the space
ID_SOURCES: dict[str, str] = {
    "person": "SELECT id FROM person",
    "forum": "SELECT id FROM forum",
    "message": "SELECT messageid AS id FROM message",
    "tag": "SELECT id FROM tag",
    "tagclass": "SELECT id FROM tagclass",
    "place": "SELECT id FROM raw_Place",
    "organisation": "SELECT id FROM raw_Organisation",
}


def remap_ids(con: duckdb.DuckDBPyConnection) -> None:
    """Rewrite every entity id to a dense integer, consistently across all references."""
    logger.info("Remapping ids to dense integers (LDBC ids exceed int4, which the cache stores)")
    for space, source in ID_SOURCES.items():
        con.execute(
            f"CREATE OR REPLACE TABLE map_{space} AS "
            f"SELECT id AS old_id, CAST(row_number() OVER (ORDER BY id) AS BIGINT) AS new_id FROM ({source})"
        )
        n = con.execute(f"SELECT count(*), max(new_id) FROM map_{space}").fetchone()
        logger.info("  %-14s %8d ids -> 1..%d", space, n[0], n[1])

    for space, columns in ID_SPACES.items():
        for table, column in columns:
            if column is None:
                continue
            con.execute(
                f"UPDATE {table} SET {column} = "
                f"(SELECT new_id FROM map_{space} WHERE map_{space}.old_id = {table}.{column}) "
                f"WHERE {column} IS NOT NULL"
            )
    for space in ID_SOURCES:
        con.execute(f"DROP TABLE map_{space}")


def sanity_check(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("Sanity checks")
    for table in FINAL_TABLES:
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        logger.info("  %-28s %10d rows", table, rows)

    too_big = []
    for table, column in (("message", "creatorpersonid"), ("message", "containerforumid"),
                          ("message", "locationcountryid"), ("person_knows_person", "person1id"),
                          ("message", "messageid"), ("tag", "id")):
        biggest = con.execute(f"SELECT max({column}) FROM {table}").fetchone()[0]
        if biggest is not None and biggest > 2_147_483_647:
            too_big.append(f"{table}.{column}={biggest}")
    if too_big:
        raise SystemExit(
            "id remapping failed -- these exceed int4 and cannot be stored in an integer[] cache: "
            + ", ".join(too_big)
        )
    logger.info("  all partition-key candidates fit in int4")

    posts, comments = con.execute(
        "SELECT count(*) FILTER (WHERE ParentMessageId IS NULL), count(*) FILTER (WHERE ParentMessageId IS NOT NULL) FROM message"
    ).fetchone()
    orphans = con.execute("SELECT count(*) FROM message WHERE ContainerForumId IS NULL").fetchone()[0]
    logger.info("  message: %d posts, %d comments, %d rows with NULL ContainerForumId", posts, comments, orphans)
    if orphans:
        logger.warning("  %d messages have a NULL ContainerForumId -- a ContainerForumId cache filter would drop them", orphans)


def copy_to_postgres(con: duckdb.DuckDBPyConnection, dsn: str) -> None:
    logger.info("Copying to PostgreSQL")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres)")
    for table in FINAL_TABLES:
        start = time.time()
        # DuckDB preserves the CSV's mixed-case column names. Carrying those into
        # PostgreSQL would make every column a quoted identifier, and PartitionCache's
        # clean_query strips double quotes unconditionally -- so force lower case here.
        columns = [row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()]
        select_list = ", ".join(f'"{c}" AS {c.lower()}' for c in columns)
        con.execute(f"DROP TABLE IF EXISTS pg.{table}")
        con.execute(f"CREATE TABLE pg.{table} AS SELECT {select_list} FROM {table}")
        logger.info("  %-28s (%.1fs)", table, time.time() - start)

    logger.info("Creating indexes")
    for table, column in INDEXES:
        con.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{column} ON pg.{table} ({column})")
    # ANALYZE has to run inside PostgreSQL; DuckDB's VACUUM/ANALYZE only covers its own tables.
    for table in FINAL_TABLES:
        con.execute(f"CALL postgres_execute('pg', 'ANALYZE {table}')")
    con.execute("DETACH pg")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", default=os.environ.get("LDBC_DATA_DIR", "/home/poppinga/ldbc-data"),
                        help="Directory containing the extracted LDBC data set")
    parser.add_argument("--duckdb-path", default=None, help="Path of the DuckDB file to build (default: examples/ldbc_snb_bi/ldbc_sf{SF}.duckdb)")
    parser.add_argument("--scale-factor", default="1", help="Scale factor label used in the DuckDB filename")
    parser.add_argument("--pg-dsn", default="dbname=ldbc host=127.0.0.1 port=55433 user=ldbc",
                        help="libpq DSN for the PostgreSQL target")
    parser.add_argument("--skip-postgres", action="store_true", help="Only build the DuckDB file")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

    snapshot = _snapshot_dir(Path(args.data_dir))
    logger.info("Snapshot: %s", snapshot)

    duckdb_path = Path(args.duckdb_path) if args.duckdb_path else Path(__file__).parent / f"ldbc_sf{args.scale_factor}.duckdb"
    if duckdb_path.exists():
        duckdb_path.unlink()
    logger.info("DuckDB file: %s", duckdb_path)

    overall = time.time()
    con = duckdb.connect(str(duckdb_path))
    logger.info("Loading raw entities")
    load_raw(con, snapshot)
    derive(con)
    sanity_check(con)
    if not args.skip_postgres:
        copy_to_postgres(con, args.pg_dsn)
    con.close()
    logger.info("Total: %.1fs", time.time() - overall)


if __name__ == "__main__":
    main()
