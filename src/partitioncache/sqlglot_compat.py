"""Keep sqlglot's parsing of PostGIS point constructors stable.

Fragment hashes are computed from sqlglot's rendering of a query, and the rendered fragments
are executed on PostgreSQL. Since sqlglot 26, ``ST_Point`` and ``ST_MakePoint`` are both parsed
into one ``exp.StPoint`` node that is printed as ``ST_POINT``. That changes the hash of every
query using ``ST_MakePoint``, and it is wrong for PostGIS: the third argument of ``ST_Point`` is
the SRID, so ``ST_MakePoint(x, y, z)`` would silently become a 2D point with SRID ``z``.

Removing both names from the function table of the default and the PostgreSQL parser makes
sqlglot keep them as anonymous functions with their original name and arguments, as before.
Other dialects (e.g. DuckDB, whose spatial extension expects ``ST_Point``) are left untouched.
"""

from sqlglot.dialects.dialect import Dialect

_PASS_THROUGH_FUNCTIONS = ("ST_POINT", "ST_MAKEPOINT")


def _keep_point_constructors_anonymous() -> None:
    for dialect in (None, "postgres"):
        functions = Dialect.get_or_raise(dialect).parser_class.FUNCTIONS
        for name in _PASS_THROUGH_FUNCTIONS:
            functions.pop(name, None)


_keep_point_constructors_anonymous()
