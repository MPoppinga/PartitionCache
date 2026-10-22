#!/usr/bin/env python3
"""Instantiate flattened LDBC SNB BI query templates with substitution parameters.

The benchmark runner has no templating: it loads one ``.sql`` file per entry in
``query_flights``. This script therefore materialises *template x parameter binding*
into ``queries/original/``.

Two regimes are supported:

``official``
    Bindings are taken from LDBC's own curated substitution parameters, in order.
    This is the regime LDBC designed -- bindings differ from run to run so that a
    *result* cache cannot help.

``recurring``
    The same bindings, but each is issued twice (a dashboard-style workload where
    users revisit the same filters). This is the regime PartitionCache targets.

The templates themselves are hand-flattened rewrites of the reference queries in
https://github.com/ldbc/ldbc_snb_bi/tree/main/umbra/queries. Each rewrite is
documented in the header comment written into the generated file. PartitionCache
cannot decompose CTEs (its FROM-list parser is regex based), rejects top-level
outer joins outright, and treats un-flattened EXISTS blocks as opaque, so every
template below is a single flat conjunctive SELECT.
"""

from __future__ import annotations

import argparse
import csv
import json
import textwrap
from pathlib import Path

# Each template: source query, the partition keys it can serve, its notes, and the SQL.
# NOTE ON PARTITION KEYS: PartitionCache only produces sound fragments when the
# partition key participates in a join of the form <fact>.<pk> = <dim>.<col>
# ("attachment join"). Without one it synthesises a pairwise <a>.<pk> = <b>.<pk>
# clique across every table in the fragment, which is nonsense for tables that do
# not carry the column. That is why each template below is paired with specific keys.
TEMPLATES: dict[str, dict] = {
    "q3": {
        "source": "bi-3 (Popular topics in a country)",
        "params": "bi-3",
        "rewrite": "EXISTS over Message_hasTag_Tag/Tag/TagClass promoted to inner joins; "
                   "count(Message.MessageId) -> count(DISTINCT m.messageid) because a message "
                   "may carry several tags of the class; ORDER BY/LIMIT dropped.",
        "sql": """
            SELECT f.id AS forum_id, f.title AS forum_title, f.moderatorpersonid AS person_id,
                   count(DISTINCT m.messageid) AS messagecount
            FROM message AS m, forum AS f, person AS modp, city AS ci, country AS co,
                 message_hastag_tag AS mt, tag AS t, tagclass AS tc
            WHERE f.id = m.containerforumid
              AND modp.id = f.moderatorpersonid
              AND ci.id = modp.locationcityid
              AND co.id = ci.partofcountryid
              AND co.name = '{country}'
              AND mt.messageid = m.messageid
              AND t.id = mt.tagid
              AND tc.id = t.typetagclassid
              AND tc.name = '{tagClass}'
            GROUP BY f.id, f.title, f.moderatorpersonid
        """,
    },
    "q9": {
        "source": "bi-9 (Top thread initiators)",
        "params": "bi-9",
        "rewrite": "Post_View inlined as m.parentmessageid IS NULL; the MPP pre-aggregation CTE "
                   "removed (it collapses into the same flat join); ORDER BY/LIMIT dropped.",
        "sql": """
            SELECT p.id AS person_id, p.firstname, p.lastname,
                   count(DISTINCT m.messageid) AS threadcount,
                   count(DISTINCT r.messageid) AS messagecount
            FROM message AS m, person AS p, message AS r
            WHERE p.id = m.creatorpersonid
              AND m.parentmessageid IS NULL
              AND m.creationdate >= DATE '{startDate}' AND m.creationdate <= DATE '{endDate}'
              AND r.rootpostid = m.messageid
              AND r.creationdate >= DATE '{startDate}' AND r.creationdate <= DATE '{endDate}'
            GROUP BY p.id, p.firstname, p.lastname
        """,
    },
    "q16": {
        "source": "bi-16 (Fake news detection)",
        "params": "bi-16a",
        "rewrite": "The two subgraph CTEs are inlined as two symmetric conjunctive cores joined on "
                   "the person; creationDate::date = :date rewritten as a half-open timestamptz range so the "
                   "creationdate index is usable and the planner can estimate it (the cast form forced a "
                   "sequential scan and a nested loop with a rows=1 misestimate); "
                   "the person; the LEFT JOIN + HAVING degree constraint (maxKnowsLimit) is dropped "
                   "because a top-level outer join is rejected outright by the fragment generator; "
                   "ORDER BY/LIMIT dropped.",
        "sql": """
            SELECT p.id AS person_id,
                   count(DISTINCT m.messageid)  AS messagecounta,
                   count(DISTINCT mb.messageid) AS messagecountb
            FROM message AS m, message_hastag_tag AS mta, tag AS ta, person AS p,
                 message AS mb, message_hastag_tag AS mtb, tag AS tb
            WHERE m.creatorpersonid = p.id
              AND mta.messageid = m.messageid AND ta.id = mta.tagid AND ta.name = '{tagA}'
              AND m.creationdate >= TIMESTAMPTZ '{dateA} 00:00:00+00'
              AND m.creationdate <  TIMESTAMPTZ '{dateA} 00:00:00+00' + INTERVAL '1 day'
              AND mb.creatorpersonid = p.id
              AND mtb.messageid = mb.messageid AND tb.id = mtb.tagid AND tb.name = '{tagB}'
              AND mb.creationdate >= TIMESTAMPTZ '{dateB} 00:00:00+00'
              AND mb.creationdate <  TIMESTAMPTZ '{dateB} 00:00:00+00' + INTERVAL '1 day'
            GROUP BY p.id
        """,
    },
    "q17": {
        "source": "bi-17 (Information propagation analysis)",
        "params": "bi-17",
        "rewrite": "The MyMessage CTE is inlined three times as explicit message/message_hastag_tag/tag "
                   "joins; the NOT EXISTS anti-join is kept verbatim (nested negation is supported); "
                   "the non-equi conjuncts (interval comparison, two disequalities) are kept; "
                   "ORDER BY/LIMIT dropped.",
        "sql": """
            SELECT m.creatorpersonid AS person1_id, count(DISTINCT m2.messageid) AS messagecount
            FROM message AS m, message_hastag_tag AS mt1, tag AS t,
                 message AS m2, message_hastag_tag AS mt2,
                 message AS c, message_hastag_tag AS mt3,
                 forum_hasmember_person AS f2, forum_hasmember_person AS f3
            WHERE mt1.messageid = m.messageid AND t.id = mt1.tagid AND t.name = '{tag}'
              AND mt2.messageid = m2.messageid AND mt2.tagid = t.id
              AND mt3.messageid = c.messageid AND mt3.tagid = t.id
              AND m.creationdate + INTERVAL '{delta} hour' < m2.creationdate
              AND c.parentmessageid = m2.messageid
              AND f2.forumid = m.containerforumid AND f2.personid = c.creatorpersonid
              AND f3.forumid = m.containerforumid AND f3.personid = m2.creatorpersonid
              AND m.containerforumid <> m2.containerforumid
              AND f2.personid <> f3.personid
              AND NOT EXISTS (SELECT 1 FROM forum_hasmember_person AS f1
                              WHERE f1.forumid = m2.containerforumid
                                AND f1.personid = m.creatorpersonid)
            GROUP BY m.creatorpersonid
        """,
    },
    "q11": {
        "source": "bi-11 (Friend triangles)",
        "params": "bi-11",
        "rewrite": "The Persons_of_country_w_friends CTE is inlined three times, once per triangle "
                   "corner. Structurally unchanged otherwise - bi-11 is already a pure conjunctive "
                   "self-join with AND-ed ordering and date-range predicates.",
        "sql": """
            SELECT count(*) AS triangle_count
            FROM person_knows_person AS k1, person AS p1, city AS ci1, country AS co1,
                 person_knows_person AS k2, person AS p2, city AS ci2, country AS co2,
                 person_knows_person AS k3, person AS p3, city AS ci3, country AS co3
            WHERE k1.person1id = p1.id AND ci1.id = p1.locationcityid
              AND co1.id = ci1.partofcountryid AND co1.name = '{country}'
              AND k2.person1id = p2.id AND ci2.id = p2.locationcityid
              AND co2.id = ci2.partofcountryid AND co2.name = '{country}'
              AND k3.person1id = p3.id AND ci3.id = p3.locationcityid
              AND co3.id = ci3.partofcountryid AND co3.name = '{country}'
              AND k1.person2id = k2.person1id
              AND k2.person2id = k3.person1id
              AND k3.person2id = k1.person1id
              AND k1.person1id < k2.person1id
              AND k2.person1id < k3.person1id
              AND k1.creationdate >= DATE '{startDate}' AND k1.creationdate <= DATE '{endDate}'
              AND k2.creationdate >= DATE '{startDate}' AND k2.creationdate <= DATE '{endDate}'
              AND k3.creationdate >= DATE '{startDate}' AND k3.creationdate <= DATE '{endDate}'
        """,
    },
    "q18": {
        "source": "bi-18 (Friend recommendation)",
        "params": "bi-18",
        "rewrite": "The two CTEs are inlined; the NOT EXISTS anti-join is kept verbatim; "
                   "ORDER BY/LIMIT dropped.",
        "sql": """
            SELECT i1.personid AS person1_id, i2.personid AS person2_id,
                   count(*) AS mutualfriendcount
            FROM person_knows_person AS k1, person_hasinterest_tag AS i1, tag AS t,
                 person_knows_person AS k2, person_hasinterest_tag AS i2
            WHERE t.id = i1.tagid AND t.name = '{tag}'
              AND t.id = i2.tagid
              AND k1.person1id = i1.personid
              AND k2.person1id = i2.personid
              AND k1.person2id = k2.person2id
              AND i1.personid < i2.personid
              AND NOT EXISTS (SELECT 1 FROM person_knows_person AS k3
                              WHERE k3.person1id = i1.personid AND k3.person2id = i2.personid)
            GROUP BY i1.personid, i2.personid
        """,
    },
}


def read_bindings(params_dir: Path, name: str) -> list[dict[str, str]]:
    path = params_dir / f"{name}.csv"
    with path.open() as handle:
        rows = list(csv.DictReader(handle, delimiter="|"))
    return [{key.split(":")[0]: value for key, value in row.items()} for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--params-dir", default="/home/poppinga/ldbc-data/params/parameters/parameters-sf1")
    parser.add_argument("--out-dir", default=str(Path(__file__).parent / "queries" / "original"))
    parser.add_argument("--regime", choices=["official", "recurring"], default="official")
    parser.add_argument("--selected", default=None,
                        help="selected_bindings.json from select_bindings.py; when given, bindings are taken "
                             "from there instead of straight off the official CSV")
    parser.add_argument("--count", type=int, default=6, help="queries generated per template")
    args = parser.parse_args()

    params_dir = Path(args.params_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    preselected = json.loads(Path(args.selected).read_text()) if args.selected else None

    for name, spec in TEMPLATES.items():
        if preselected is not None:
            bindings = [{k: v for k, v in b.items() if not k.startswith("_")} for b in preselected.get(name, [])]
        else:
            bindings = read_bindings(params_dir, spec["params"])
        if not bindings:
            print(f"{name}: no usable bindings, skipped")
            continue
        if args.regime == "official":
            chosen = bindings[: args.count]
        else:
            # dashboard style: half as many distinct bindings, each issued twice
            distinct = bindings[: max(1, args.count // 2)]
            chosen = [distinct[i // 2] for i in range(args.count)]

        for index, binding in enumerate(chosen, start=1):
            sql = textwrap.dedent(spec["sql"]).strip().format(**binding)
            shown = ", ".join(f"{k}={v}" for k, v in binding.items())
            header = (
                f"-- Derived from LDBC SNB BI {spec['source']}\n"
                f"-- Parameters ({args.regime}): {shown}\n"
                f"-- Rewrite: {spec['rewrite']}\n"
            )
            (out_dir / f"{name}_{index}.sql").write_text(header + sql + "\n")
        print(f"{name}: {len(chosen)} queries from {spec['params']}.csv ({args.regime})")


if __name__ == "__main__":
    main()
