"""Keep sqlglot's handling of PostGIS point constructors and of regex match flags correct.

Fragment hashes are computed from sqlglot's rendering of a query, and the rendered fragments
are executed on PostgreSQL. Since sqlglot 26, ``ST_Point`` and ``ST_MakePoint`` are both parsed
into one ``exp.StPoint`` node that is printed as ``ST_POINT``. That changes the hash of every
query using ``ST_MakePoint``, and it is wrong for PostGIS: the third argument of ``ST_Point`` is
the SRID, so ``ST_MakePoint(x, y, z)`` would silently become a 2D point with SRID ``z``.

Removing both names from the function table of the default and the PostgreSQL parser makes
sqlglot keep them as anonymous functions with their original name and arguments, as before.
Other dialects (e.g. DuckDB, whose spatial extension expects ``ST_Point``) are left untouched.

Second, sqlglot renders ``REGEXP_LIKE(a, 'p', 'i')`` for PostgreSQL as ``a ~ 'p'``: the flag is lost and the
case-insensitive match becomes case-sensitive, which drops rows from queries rendered for PostgreSQL (the lazy
cache paths). The PostgreSQL rendering keeps it: ``~*`` for the flag ``i``, the ``REGEXP_LIKE`` function
(PostgreSQL 15+) for any other flag. The default rendering, which the hashes use, is unchanged.
"""

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator

_PASS_THROUGH_FUNCTIONS = ("ST_POINT", "ST_MAKEPOINT")


def _keep_point_constructors_anonymous() -> None:
    for dialect in (None, "postgres"):
        functions = Dialect.get_or_raise(dialect).parser_class.FUNCTIONS
        for name in _PASS_THROUGH_FUNCTIONS:
            functions.pop(name, None)


def _postgres_regexp_like(self: Generator, expression: exp.RegexpLike) -> str:
    flag = expression.args.get("flag")
    if flag is None:
        return self.binary(expression, "~")
    if isinstance(flag, exp.Literal) and flag.is_string and flag.this == "i":
        return self.binary(expression, "~*")
    return self.func("REGEXP_LIKE", expression.this, expression.expression, flag)


def _keep_regexp_like_flags_for_postgres() -> None:
    Dialect.get_or_raise("postgres").generator_class.TRANSFORMS[exp.RegexpLike] = _postgres_regexp_like


_keep_point_constructors_anonymous()
_keep_regexp_like_flags_for_postgres()
