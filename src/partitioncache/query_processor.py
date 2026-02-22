"""
Methods for splitting and reassembling queries
"""

import hashlib
import itertools
import logging
import re
import warnings
from collections import defaultdict
from itertools import combinations
from typing import Any

import networkx as nx  # type: ignore
import sqlglot
import sqlglot.expressions as exp
import sqlglot.optimizer
import sqlglot.optimizer.canonicalize
import sqlglot.optimizer.normalize
import sqlglot.optimizer.simplify

logger = logging.getLogger("PartitionCache")

# Mapping of deprecated parameter names to their new names
_DEPRECATED_PARAM_ALIASES: dict[str, str] = {
    "star_join_table": "partition_join_table",
    "auto_detect_star_join": "auto_detect_partition_join",
}


def _handle_deprecated_kwargs(kwargs: dict[str, Any], func_name: str) -> dict[str, Any]:
    """Handle backward-compatible deprecated parameter names.

    Translates old star_join_* parameter names to partition_join_* equivalents,
    emitting a DeprecationWarning for each one used.
    """
    result: dict[str, Any] = {}
    for old_name, new_name in _DEPRECATED_PARAM_ALIASES.items():
        if old_name in kwargs:
            warnings.warn(
                f"Parameter '{old_name}' is deprecated in {func_name}(), "
                f"use '{new_name}' instead.",
                DeprecationWarning,
                stacklevel=3,
            )
            result[new_name] = kwargs.pop(old_name)
    if kwargs:
        raise TypeError(f"{func_name}() got unexpected keyword arguments: {list(kwargs.keys())}")
    return result


def normalize_joins_to_cross_join(query: str) -> str:
    """
    Convert explicit JOIN ON syntax to comma-separated FROM with conditions in WHERE.

    This normalization ensures that queries using JOIN ON syntax produce the same
    cache fragments as equivalent queries using comma joins with WHERE conditions.

    Example:
        SELECT t.id FROM trips t JOIN pois p ON ST_DWithin(t.geom, p.geom, 500) WHERE t.fare > 10
        →
        SELECT t.id FROM trips AS t, pois AS p WHERE ST_DWithin(t.geom, p.geom, 500) AND t.fare > 10

    Args:
        query: SQL query string to normalize

    Returns:
        Normalized query with JOINs converted to comma joins
    """
    try:
        parsed = sqlglot.parse_one(query)
    except Exception:
        return query

    select = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
    if not select:
        return query

    # Only collect JOINs at the current SELECT scope — skip JOINs inside subqueries/CTEs/EXISTS
    joins = [
        j
        for j in select.find_all(exp.Join)
        if not j.find_ancestor(exp.Subquery) and not j.find_ancestor(exp.Exists) and not j.find_ancestor(exp.In)
    ]
    if not joins:
        return query  # No JOINs to convert

    # Only process joins that have ON conditions (explicit joins)
    join_conditions = []
    for join in joins:
        on_clause = join.args.get("on")
        if on_clause:
            # The on arg is the condition expression directly
            join_conditions.append(on_clause)
            # Convert to comma join by clearing ON, kind, and side
            join.set("on", None)
            join.set("kind", None)
            join.set("side", None)
        elif join.args.get("kind") or join.args.get("side"):
            # JOIN without ON but with kind (e.g., CROSS JOIN) — just strip kind
            join.set("kind", None)
            join.set("side", None)

    # Add collected ON conditions to WHERE clause
    if join_conditions:
        existing_where = select.args.get("where")
        if existing_where:
            combined = existing_where.this
            for cond in join_conditions:
                combined = exp.And(this=combined, expression=cond)
            select.set("where", exp.Where(this=combined))
        else:
            if len(join_conditions) == 1:
                select.set("where", exp.Where(this=join_conditions[0]))
            else:
                combined = join_conditions[0]
                for cond in join_conditions[1:]:
                    combined = exp.And(this=combined, expression=cond)
                select.set("where", exp.Where(this=combined))

    return parsed.sql()


def clean_query(query: str) -> str:
    """
    Perform query cleaning to ensure stable output for cache variant generation.

    This function normalizes queries and removes clauses that don't affect which partition keys
    are accessed, ensuring that semantically equivalent queries for caching purposes generate
    the same hash variants.

    Normalizes:
    - JOIN ON syntax → comma-separated FROM with conditions in WHERE

    Removes:
    - SELECT expressions (replaced with *, don't affect partition key access patterns)
    - ORDER BY clauses (don't affect partition key access patterns)
    - LIMIT clauses (don't affect partition key access patterns)
    - GROUP BY clauses (don't affect partition key access patterns)
    - HAVING clauses (don't affect partition key access patterns)
    - Comments
    - Trailing semicolons
    - Unnecessary parentheses in WHERE clauses

    Args:
        query (str): SQL query to clean

    Returns:
        str: Cleaned and normalized query suitable for cache variant generation
    """

    # Remove single-line comments before whitespace normalization,
    # otherwise collapsing newlines makes `--.*` wipe out everything
    # from the first comment to the end of the (now single-line) string.
    query = re.sub(r"--.*", "", query)

    query = re.sub(r"\s+", " ", query)
    query = re.sub(r"\s*=\s*", "=", query)

    # Remove trailing semicolons (they're not part of the SQL statement)
    query = query.rstrip().rstrip(";")

    # Normalize JOINs to comma syntax before parsing for optimization
    query = normalize_joins_to_cross_join(query)

    # Parse the query
    parsed = sqlglot.parse_one(query)

    # Normalize the query (building CNF)
    parsed = sqlglot.optimizer.normalize.normalize(parsed)

    # Simplification e.g. Sort comparison operators to ensure consistent order
    parsed = sqlglot.optimizer.simplify.simplify(parsed)

    # Remove clauses that don't affect which partition keys are accessed
    for select_stmt in parsed.find_all(exp.Select):
        # Remove ORDER BY
        if select_stmt.args.get("order"):
            select_stmt.set("order", None)
            logger.debug("Removed ORDER BY clause for cache variant generation")

        # Remove LIMIT
        if select_stmt.args.get("limit"):
            select_stmt.set("limit", None)
            logger.debug("Removed LIMIT clause for cache variant generation")

        # Remove GROUP BY
        if select_stmt.args.get("group"):
            select_stmt.set("group", None)
            logger.debug("Removed GROUP BY clause for cache variant generation")

        # Remove HAVING
        if select_stmt.args.get("having"):
            select_stmt.set("having", None)
            logger.debug("Removed HAVING clause for cache variant generation")

    # Replace only the outermost SELECT expressions with * to avoid confusing
    # downstream regex-based parsing (e.g., aliases containing SQL keywords like FROM)
    # Only the outermost SELECT is replaced; subqueries/CTEs keep their columns
    outermost_select = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
    if outermost_select:
        outermost_select.set("expressions", [exp.Star()])

    query = parsed.sql()

    # Removing tailing semicolon
    query = re.sub(r";\s*$", "", query)

    # Remove all double quotes(postgresql) and backticks(mysql) from the normalized query
    query = query.replace('"', "")  # TODO currently double quotes are not supported in identifiers in PartitionCache
    query = query.replace("`", "")  # TODO currently backticks are not supported in identifiers in PartitionCache

    return query


def all_connected_subgraphs(G: nx.Graph, min_comp_size: int, max_comp_size: int):
    """
    Get all connected subgraphs of the given graph.
    """
    con_comp = sorted(nx.connected_components(G), key=len, reverse=True)

    def recursive_local_expand(node_set, possible, excluded, results, max_size):
        """
        Recursive function to add an extra node to the subgraph being formed
        """
        results.append(node_set)
        if len(node_set) == max_size:
            return
        for j in possible - excluded:
            new_node_set = node_set | {j}
            excluded = excluded | {j}
            new_possible = (possible | set(G.neighbors(j))) - excluded
            recursive_local_expand(new_node_set, new_possible, excluded, results, max_size)

    results: list = []
    for cc in con_comp:
        max_size = len(cc)

        excluded = set()
        for i in G:
            excluded.add(i)
            recursive_local_expand({i}, set(G.neighbors(i)) - excluded, excluded, results, max_size)

    result_set: set = {frozenset(r) for r in results}

    ret: dict[int, Any] = {}

    for r in result_set:
        r_len = len(r)
        # print(r, r_len, min_comp_size, max_comp_size)

        if min_comp_size <= r_len <= max_comp_size:
            if r_len in ret:
                ret[len(r)].append(r)
            else:
                ret[len(r)] = [r]

    return ret


def generate_tuples(
    edges: list[tuple[str, str]],
    table_aliases: list,
    min_component_size: int,
    follow_graph: bool,
    max_component_size: int = 15,
) -> dict[int, list[tuple[str, str]]]:
    """
    Generates sets of tuples of table aliases. Grouped by the number of tables in the tuple.
    If follow_graph is True, the function will only return partial queries that are connected to each other.
    If follow_graph is False, it will return all possible combinations of tables.
    """
    result: dict = {}
    if follow_graph:
        # Get all variants with are connected to each other
        g = nx.Graph(edges)
        # Add all table aliases as nodes, even if there are no edges
        g.add_nodes_from(table_aliases)
        result = all_connected_subgraphs(g, min_component_size, max_component_size)
    else:
        # Get all Permutations
        for i in range(min_component_size, min(max_component_size + 1, len(table_aliases) + 1)):
            result[i] = list(combinations(table_aliases, i))

    return result


def remove_single_conditions(
    conditions: dict[str, list[str]],
) -> list[dict[str, list[str]]]:
    """
    If in one condition more than one attribute is used, remove one of the attributes (yielding all possible outcomes)
    Returns all new conditions together with the original conditions
    """
    ret = [conditions]
    for key in conditions.keys():
        if len(conditions[key]) > 1:
            for i in range(len(conditions[key])):
                new_conditions = conditions.copy()
                new_conditions[key] = [conditions[key][i]]
                ret.append(new_conditions)
    return ret


def extract_conjunctive_conditions(sql: str) -> list[str]:
    """
    Returns all conjunctive conditions from a query.
    """
    parsed = sqlglot.parse_one(sql)
    where_clause = parsed.find(exp.Where)
    conditions = []

    def extract_conditions_from_expression(expression):
        if isinstance(expression, exp.And):
            extract_conditions_from_expression(expression.left)
            extract_conditions_from_expression(expression.right)
        else:
            conditions.append(expression.sql())

    if where_clause:
        extract_conditions_from_expression(where_clause.this)

    return conditions


def extract_and_group_query_conditions(
    query, partition_key
) -> tuple[
    dict[str, list[str]],
    dict[tuple[str, str], list[str]],
    dict[tuple, list[str]],
    dict[str, list[str]],
    dict[tuple, list[str]],
    list[str],
    dict[str, str],
    dict[tuple[str, str], list[str]],
]:
    """
    Extracts all conditions from a query
    Splits it by distance functions, attributes and subqueries for the partition key
    """
    attribute_conditions: dict[str, list[str]] = {}  # {table_alias: [conditions(w/alias)]}
    distance_conditions: dict[tuple[str, str], list[str]] = defaultdict(list)  # {(table_alias1, table_alias2): [conditions]}
    other_functions: dict[tuple, list[str]] = defaultdict(list)  # {(table_alias1, ...): [conditions]}
    partition_key_conditions: dict[str, list[str]] = defaultdict(list)  # {table_alias: [conditions(w/alias)]}
    or_conditions: dict[tuple, list[str]] = defaultdict(list)  # {(table_alias1, table_alias2, ...): [conditions(w/alias)]}
    partition_key_joins: dict[tuple[str, str], list[str]] = defaultdict(list)  # Track PK joins for star detection

    # get tables
    tables = re.split("FROM|WHERE", query, flags=re.IGNORECASE)[1]
    tables = tables.split(",")
    tables = [x.strip() for x in tables]

    # Create mapping of aliases to table names
    alias_to_table_map: dict[str, str] = {}
    table_aliases = []

    for table_spec in tables:
        parts = re.split(r"\s+(?:AS\s+)?", table_spec, flags=re.IGNORECASE)
        if len(parts) >= 2:
            table_name = parts[0]
            alias = parts[-1]
        else:
            table_name = parts[0]
            alias = parts[0]

        table_aliases.append(alias)
        alias_to_table_map[alias] = table_name

    # warn if more than one table is used
    unique_tables = set(alias_to_table_map.values())
    if len(unique_tables) > 1:
        #  This may cause unexpected behavior if tables are not joined by partition key
        logger.debug(f"More than one table is used in the query ({', '.join(unique_tables)}).")

    # define emtpy list for attribute_conditions for each tablealias
    for ta in table_aliases:
        attribute_conditions[ta] = []

    # get all conditions from where clause
    condition_list: list[str] = extract_conjunctive_conditions(query)

    # Iterate through all conditions and sort them into table_conditions and distance_conditions
    for condition in condition_list:
        # find partition_key join_conditions and track them
        if re.match(rf"\w*\.{partition_key}\s=\s\w*\.{partition_key}", condition):
            # Extract the two aliases involved in the join
            parts = condition.split("=")
            left_alias = parts[0].strip().split(".")[0]
            right_alias = parts[1].strip().split(".")[0]
            if left_alias in table_aliases and right_alias in table_aliases:
                partition_key_joins[(min(left_alias, right_alias), max(left_alias, right_alias))].append(condition)
            continue  # Skip adding to other conditions
        elif condition.count(partition_key) >= 1 and (
            sqlglot.parse_one(condition).find(exp.In) or any(op in condition for op in ["BETWEEN", ">", "<", "=", "!=", "<>"])
        ):
            # Partition key condition (IN, BETWEEN, comparison, etc.) — store with alias intact
            pk_alias_match = re.match(r'(?:NOT\s+)?(\w+)\.', condition)
            if pk_alias_match:
                pk_alias = pk_alias_match.group(1)
            elif table_aliases:
                pk_alias = table_aliases[0]
            else:
                pk_alias = "unknown"
            partition_key_conditions[pk_alias].append(condition)
            continue

        # count how many distinct table aliases are in the condition
        aliases_present: set[str] = set()
        for alias in table_aliases:
            if f"{alias}." in condition:
                aliases_present.add(alias)
        nr_alias_in_condition = len(aliases_present)

        # if only one table alias, it is an attribute condition — store with alias intact
        # Regex remap handles single-ref and multi-ref conditions correctly during reassembly
        if nr_alias_in_condition == 1 and "." in condition:
            alias = next(iter(aliases_present))
            attribute_conditions[alias].append(condition)
            continue

        # Multi-reference, multi-alias, or zero-alias conditions need alias substitution
        # Filter against outer FROM aliases to exclude inner subquery table references
        all_alias: set = set(re.findall(r"[a-zA-Z_]\w*(?=\.)", condition)) & set(table_aliases)
        if sqlglot.parse_one(condition).find(exp.Func):
            if len(all_alias) == 2:
                all_alias_list = sorted(all_alias)
                distance_conditions[(all_alias_list[0], all_alias_list[1])].append(condition)
                continue
            else:
                parsed = sqlglot.parse_one(condition)
                table_identifiers = tuple(sorted({col.table for col in parsed.find_all(exp.Column) if col.table}))
                other_functions[table_identifiers].append(condition)
                continue

        elif sqlglot.parse_one(condition).find(exp.Or):
            parsed = sqlglot.parse_one(condition)
            table_identifiers = tuple(sorted({col.table for col in parsed.find_all(exp.Column) if col.table}))
            or_conditions[table_identifiers].append(condition)
            continue
        else:
            if len(all_alias) == 2:
                # get all aliases with sqlglot
                parsed = sqlglot.parse_one(condition)
                table_identifiers = tuple(sorted({col.table for col in parsed.find_all(exp.Column) if col.table}))
                distance_conditions[(table_identifiers[0], table_identifiers[1])].append(condition)
                continue
            else:
                parsed = sqlglot.parse_one(condition)
                table_identifiers = tuple(sorted({col.table for col in parsed.find_all(exp.Column) if col.table}))
                other_functions[table_identifiers].append(condition)
                continue

    return (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        alias_to_table_map,
        partition_key_joins,
    )


def detect_partition_join_table(
    table_aliases: list[str],
    alias_to_table_map: dict[str, str],
    attribute_conditions: dict[str, list[str]],
    distance_conditions: dict[tuple[str, str], list[str]],
    partition_key_joins: dict[tuple[str, str], list[str]],
    partition_key: str,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
) -> str | None:
    """
    Detect partition-join table based on explicit specification or auto-detection patterns.

    Partition-join tables (formerly "p0 tables") are special tables that serve as central join
    points in star-schema patterns. They are automatically detected and handled specially
    to optimize query variant generation.

    Detection Process:
    1. **Explicit specification**: Tables can be marked as partition-join via partition_join_table parameter
       - Matches by alias first, then by table name
       - Only one partition-join table is used per query

    2. **Auto-detection** (when auto_detect_partition_join=True, default):
       a. Tables with names starting with 'p0' AND no attribute conditions
       b. Smart detection: Tables that join ALL other tables AND have ONLY partition key conditions

    Args:
        table_aliases: List of table aliases in the query
        alias_to_table_map: Mapping from alias to table name
        attribute_conditions: Conditions for each table alias
        distance_conditions: Distance conditions between table pairs
        partition_key_joins: Explicit partition key joins between tables
        partition_key: The partition key column name
        auto_detect_partition_join: Whether to auto-detect partition-join tables
        partition_join_table: Explicitly specified partition-join table alias or name

    Returns:
        The alias of the detected partition-join table, or None if no partition-join table is found

    Raises:
        None - function handles all edge cases gracefully
    """
    detected_partition_join_alias = None

    # First, check explicitly specified partition-join table if provided
    if partition_join_table:
        # Try to match by alias first, then by table name
        if partition_join_table in table_aliases:
            detected_partition_join_alias = partition_join_table
            logger.info(f"Using explicit partition-join table by alias: {partition_join_table}")
        else:
            # Try to match by table name
            for alias, table_name in alias_to_table_map.items():
                if table_name == partition_join_table:
                    detected_partition_join_alias = alias
                    logger.info(f"Using explicit partition-join table by name: {partition_join_table} -> alias {alias}")
                    break

            if not detected_partition_join_alias:
                logger.warning(f"Could not match partition-join table specification '{partition_join_table}' to any alias or table name")
                logger.warning(f"Available aliases: {table_aliases}")
                logger.warning(f"Available tables: {list(alias_to_table_map.values())}")

    # If no explicit partition-join specified and auto-detection is enabled
    if not detected_partition_join_alias and auto_detect_partition_join:
        candidates = []

        # 1. Check naming convention (p0_*)
        for alias in table_aliases:
            has_no_attrs = alias not in attribute_conditions or len(attribute_conditions.get(alias, [])) == 0
            table_name = alias_to_table_map.get(alias, "")
            is_p0_table = table_name.lower().startswith("p0")

            if has_no_attrs and is_p0_table:
                candidates.append((alias, "naming convention"))

        # 2. Smart detection: tables that join to all others with only PK conditions
        if len(table_aliases) > 2:  # Need at least 3 tables for star schema
            for alias in table_aliases:
                # Check if already a candidate
                if any(c[0] == alias for c in candidates):
                    continue

                # Check conditions - should only have partition key conditions
                has_only_pk_conditions = True
                for cond in attribute_conditions.get(alias, []):
                    if partition_key not in cond:
                        has_only_pk_conditions = False
                        break

                if has_only_pk_conditions:
                    # Count tables joined via partition key
                    joined_tables = set()

                    # Check explicit partition key joins
                    for (t1, t2), _ in partition_key_joins.items():
                        if t1 == alias:
                            joined_tables.add(t2)
                        elif t2 == alias:
                            joined_tables.add(t1)

                    # Check distance conditions for partition key joins
                    for (t1, t2), conds in distance_conditions.items():
                        if t1 == alias:
                            for cond in conds:
                                if f"{t1}.{partition_key}" in cond and f"{t2}.{partition_key}" in cond:
                                    joined_tables.add(t2)
                        elif t2 == alias:
                            for cond in conds:
                                if f"{t1}.{partition_key}" in cond and f"{t2}.{partition_key}" in cond:
                                    joined_tables.add(t1)

                    # Check if joins all other tables
                    expected_joins = len(table_aliases) - 1
                    if len(joined_tables) == expected_joins:
                        candidates.append((alias, "smart detection"))

        # Use first candidate found
        if candidates:
            detected_partition_join_alias, detection_method = candidates[0]
            logger.info(
                f"Auto-detected partition-join table via {detection_method}: {detected_partition_join_alias} -> {alias_to_table_map.get(detected_partition_join_alias, detected_partition_join_alias)}"
            )
            if len(candidates) > 1:
                logger.info(f"Multiple partition-join candidates found: {[c[0] for c in candidates]}. Using '{detected_partition_join_alias}'")

    return detected_partition_join_alias


def detect_partition_join_from_query(
    query: str,
    partition_key: str,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
) -> str | None:
    """
    Public wrapper to detect partition-join table from a SQL query.

    This function extracts query conditions and uses the internal partition-join detection
    logic to identify partition-join tables in the query.

    Args:
        query: SQL query to analyze
        partition_key: The partition key column name
        auto_detect_partition_join: Whether to auto-detect partition-join tables
        partition_join_table: Explicitly specified partition-join table alias or name

    Returns:
        The alias of the detected partition-join table, or None if no partition-join table is found
    """
    (
        attribute_conditions,
        distance_conditions,
        _other_functions,
        _partition_key_conditions,
        _or_conditions,
        table_aliases,
        alias_to_table_map,
        partition_key_joins,
    ) = extract_and_group_query_conditions(query, partition_key)

    return detect_partition_join_table(
        table_aliases,
        alias_to_table_map,
        attribute_conditions,
        distance_conditions,
        partition_key_joins,
        partition_key,
        auto_detect_partition_join,
        partition_join_table,
    )


def _build_select_clause(
    strip_select: bool,
    original_select_clause: str | None,
    table_aliases: list[str],
    original_to_new_alias_mapping: dict[str, str],
    partition_key: str,
    partition_join_alias: str | None = None,
    geometry_column: str | None = None,
) -> str:
    """
    Build the SELECT clause for a partial query.

    Args:
        strip_select: If True, use stripped SELECT (partition key only)
        original_select_clause: Original SELECT clause expressions if available
        table_aliases: List of new table aliases in the partial query (e.g., ['t1', 't2'])
        original_to_new_alias_mapping: Mapping from original aliases to new aliases
        partition_key: The partition key column name
        partition_join_alias: Alias of the partition-join table if present (e.g., 'p1')
        geometry_column: If set, use this column instead of partition_key in SELECT (for spatial queries)

    Returns:
        Complete SELECT clause string (with SELECT keyword)
    """
    if strip_select or original_select_clause is None:
        # Determine which column to select
        select_column = geometry_column if geometry_column else partition_key

        # For partition-join queries, prefer selecting from the partition-join table
        if partition_join_alias:
            return f"SELECT DISTINCT {partition_join_alias}.{select_column}"
        else:
            # Default behavior: select only partition key from first table
            return f"SELECT DISTINCT {table_aliases[0]}.{select_column}"

    # Preserve original SELECT clause with alias mapping
    try:
        # Replace original aliases with new aliases in the SELECT clause
        mapped_select_clause = original_select_clause
        for original_alias, new_alias in original_to_new_alias_mapping.items():
            # Replace table alias references (e.g., "cd.pdb_id" -> "t1.pdb_id")
            pattern = rf"\b{re.escape(original_alias)}\."
            mapped_select_clause = re.sub(pattern, f"{new_alias}.", mapped_select_clause)

        return f"SELECT {mapped_select_clause}"
    except Exception as e:
        logger.warning(f"Failed to map SELECT clause aliases: {e}. Falling back to stripped SELECT.")
        return f"SELECT DISTINCT {table_aliases[0]}.{partition_key}"


def generate_partial_queries(
    query: str,
    partition_key: str,
    min_component_size: int = 1,
    follow_graph: bool = True,
    keep_all_attributes: bool = True,
    other_functions_as_distance_conditions: bool = True,
    auto_detect_partition_join: bool = True,
    max_component_size: int | None = None,
    partition_join_table: str | None = None,
    warn_no_partition_key: bool = True,
    strip_select: bool = True,
    skip_partition_key_joins: bool = False,
    geometry_column: str | None = None,
    pre_clean_select_clause: str | None = None,
) -> list[str]:
    """
    This function takes a query and returns the list of all possible partial queries.
    Args:
        query: str: The query to be split
        partition_key: str: The identifier of the search space
        min_component_size: int: The minimum number of tables in the partial queries (connected components)
        follow_graph: bool: If True, only generate variants from tables that form connected subgraphs via
            multi-table predicates (e.g., distance conditions, non-equijoin conditions between tables).
            If False, generate all possible combinations of tables regardless of connectivity
        keep_all_attributes: bool: If True, the function will only return partial queries with the original attributes.
            If False, it will return also partial queries with fewer attributes
        other_functions_as_distance_conditions: bool: Whether to treat other functions as distance conditions
        auto_detect_partition_join: bool: If True, automatically detect partition-join tables based on pattern
            (joins all tables with only partition key conditions). If False, only use partition_join_table
        max_component_size: int: Maximum number of tables in generated variants (excluding partition-join)
        partition_join_table: str: Explicit table alias or name to treat as the partition-join table.
            Only one partition-join table is allowed per query
        warn_no_partition_key: bool: If True, emit warnings for tables not using the partition key
        strip_select: bool: If True (default), strip SELECT clause to only select partition keys.
            If False, preserve original SELECT clause with proper alias mapping for partial queries.
        skip_partition_key_joins: bool: If True, skip generating partition_key equijoin conditions between tables.
            Used for spatial queries where tables are linked by distance conditions, not equijoins.
        geometry_column: str | None: If set, use this geometry column instead of partition_key in SELECT clause.
            Used for spatial cache handlers (H3, BBox) where the SELECT needs the geometry column.

    Returns:
        List[str]: List of all possible partial queries

    """

    # init variables
    ret: list[str] = []  # List of all possible partial queries for return

    # Extract original SELECT clause if strip_select=False.
    # Use pre_clean_select_clause if provided (e.g., extracted before clean_query replaces SELECT with *).
    original_select_clause = pre_clean_select_clause
    if not strip_select and original_select_clause is None:
        try:
            parsed_query = sqlglot.parse_one(query)
            if parsed_query and parsed_query.find(exp.Select):
                select_expr = parsed_query.find(exp.Select)
                if select_expr and select_expr.expressions:
                    # Extract SELECT expressions (columns) without the SELECT keyword
                    original_select_clause = ", ".join(expr.sql() for expr in select_expr.expressions)
        except Exception as e:
            logger.warning(f"Failed to extract original SELECT clause: {e}. Falling back to stripped SELECT.")
            original_select_clause = None

    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,  # TODO Fix # TODO what to fix here?
        table_aliases,
        alias_to_table_map,
        partition_key_joins,
    ) = extract_and_group_query_conditions(query, partition_key)

    # Detect partition-join tables - special tables used for partition key joins
    # These tables are excluded from variant generation and re-added to each variant
    detected_partition_join_alias = detect_partition_join_table(
        table_aliases,
        alias_to_table_map,
        attribute_conditions,
        distance_conditions,
        partition_key_joins,
        partition_key,
        auto_detect_partition_join,
        partition_join_table,
    )

    # Emit warnings for tables not using partition key if requested
    # Skip warning in spatial mode — partition_key is a namespace, not a column
    if warn_no_partition_key and not geometry_column:
        for alias in table_aliases:
            if alias == detected_partition_join_alias:
                continue  # Partition-join tables are expected to use partition key

            # Check if this table uses the partition key
            uses_partition_key = False

            # Check in attribute conditions
            for cond in attribute_conditions.get(alias, []):
                if partition_key in cond:
                    uses_partition_key = True
                    break

            # Check in distance conditions
            if not uses_partition_key:
                for (t1, t2), conds in distance_conditions.items():
                    if t1 == alias or t2 == alias:
                        for cond in conds:
                            if partition_key in cond:
                                uses_partition_key = True
                                break
                    if uses_partition_key:
                        break

            # Check in partition key joins
            if not uses_partition_key:
                for (t1, t2), _conds in partition_key_joins.items():
                    if t1 == alias or t2 == alias:
                        uses_partition_key = True
                        break

            if not uses_partition_key:
                logger.warning(f"Table '{alias}' ({alias_to_table_map.get(alias, alias)}) does not use partition key '{partition_key}'")

    # Filter aliases for variant generation - always exclude partition-join table
    if detected_partition_join_alias:
        aliases_for_variants = [a for a in table_aliases if a != detected_partition_join_alias]
    else:
        aliases_for_variants = table_aliases

    # Filter distance conditions to exclude partition-join table
    distance_conditions_filtered = {}
    if detected_partition_join_alias:
        for (alias1, alias2), conditions in distance_conditions.items():
            if alias1 != detected_partition_join_alias and alias2 != detected_partition_join_alias:
                distance_conditions_filtered[(alias1, alias2)] = conditions
    else:
        distance_conditions_filtered = distance_conditions

    # Get all possible combinations of tables (grouped by number of tables in the tuple)
    query_combinations = generate_tuples(
        list(distance_conditions_filtered.keys()),
        aliases_for_variants,
        min_component_size,
        follow_graph,
        max_component_size if max_component_size else 15,  # Default max
    )

    all_query_combinations = []
    for v in query_combinations.values():
        all_query_combinations.extend(v)

    # Make sure Table conditions are sorted
    for key in attribute_conditions.keys():
        attribute_conditions[key].sort()

    # Create all possible partial queries based on query_combinations and table_conditions
    # Each loop iteration creates a new partial query
    for combination in all_query_combinations:
        if keep_all_attributes:
            table_conditions_vaiants = [attribute_conditions]
        else:
            table_conditions_vaiants = remove_single_conditions(attribute_conditions)  # create variants with less attributes

        # If multiple variants of table conditions are available, create a partial query for each variant
        for var_attribute_conditions in table_conditions_vaiants:
            new_table_list = []  # List of all table aliases in this partial query

            # Put table condition keys in the right order based on table conditions item lists
            table_conditions_keys = sorted(
                [y for y in var_attribute_conditions.keys() if y in combination],
                key=lambda x: "".join(var_attribute_conditions[x]),
            )
            if len(table_conditions_keys) == 0:
                continue

            # Filter distance conditions that are relevant for the current combination
            relvant_conditions_for_combination: list[str] = []
            for x in distance_conditions.keys():
                if x[0] in combination and x[1] in combination:
                    dcond: list[str] = distance_conditions[x]
                    relvant_conditions_for_combination.extend(dcond)

            if other_functions_as_distance_conditions:
                for x in other_functions.keys():
                    if all(key in combination for key in x):
                        ocond: list[str] = other_functions[x]
                        relvant_conditions_for_combination.extend(ocond)

            if or_conditions:
                for x in or_conditions.keys():
                    if all(key in combination for key in x):
                        orcond: list[str] = or_conditions[x]
                        relvant_conditions_for_combination.extend(orcond)

            relvant_conditions_for_combination.sort()

            # Build the query
            query_where: list[str] = []
            i = 0  # Index for new table aliases
            for table_condition_key in table_conditions_keys:
                i += 1
                table_alias = f"t{i}"  # Enumerate table aliases
                new_table_list.append(table_alias)
                query_where.extend([re.sub(rf'\b{re.escape(table_condition_key)}\.', f'{table_alias}.', x) for x in var_attribute_conditions[table_condition_key]])

                # set the correct table alias in the distance conditions
                pattern = rf"\b{re.escape(table_condition_key)}\b"
                relvant_conditions_for_combination = [re.sub(pattern, table_alias, cc) for cc in relvant_conditions_for_combination]

            # Add distance conditions to the query
            for rdist in relvant_conditions_for_combination:
                query_where.append(rdist)

            # Add join conditions for given partition_key (skip for spatial queries)
            if not skip_partition_key_joins:
                for i in range(1, len(new_table_list)):
                    for j in range(i + 1, len(new_table_list) + 1):
                        query_where.append(f"{new_table_list[i - 1]}.{partition_key} = {new_table_list[j - 1]}.{partition_key}")

            # Build table list with correct table names from alias_to_table_map
            new_table_list_with_alias = []
            original_to_new_alias_mapping = {}
            for i, table_condition_key in enumerate(table_conditions_keys):
                original_table = alias_to_table_map.get(table_condition_key, table_condition_key)
                new_alias = f"t{i + 1}"
                new_table_list_with_alias.append(f"{original_table} AS {new_alias}")
                # Build mapping from original alias to new alias for SELECT clause mapping
                original_to_new_alias_mapping[table_condition_key] = new_alias

            # Re-add partition-join table if one was detected
            if detected_partition_join_alias:
                # ONLY create variant with partition-join table re-added
                # (no base variant without partition-join for star-schema queries)
                partition_join_table_name = alias_to_table_map.get(detected_partition_join_alias, detected_partition_join_alias)
                # Use p1 as default alias for partition-join table (for backward compatibility)
                # but check for conflicts and use alternative if needed
                partition_join_new_alias = "p1"
                if partition_join_new_alias in new_table_list or partition_join_new_alias in table_aliases:
                    # If p1 conflicts, use a unique alias
                    partition_join_new_alias = f"partition_join_{abs(hash(partition_join_table_name)) % 10000}"
                partition_join_table_spec = f"{partition_join_table_name} AS {partition_join_new_alias}"
                # Add partition-join alias mapping
                original_to_new_alias_mapping[detected_partition_join_alias] = partition_join_new_alias

                # Build combined table list
                combined_table_list = new_table_list_with_alias + [partition_join_table_spec]

                # Add joins from each table to partition-join table
                partition_join_joins = []
                if skip_partition_key_joins:
                    # Spatial mode: re-add original distance/spatial conditions
                    # between partition-join table and each variant table
                    for orig_alias in table_conditions_keys:
                        new_alias = original_to_new_alias_mapping[orig_alias]
                        for (a1, a2), conds in distance_conditions.items():
                            if (a1 == detected_partition_join_alias and a2 == orig_alias) or \
                               (a2 == detected_partition_join_alias and a1 == orig_alias):
                                for cond in conds:
                                    mapped = cond
                                    mapped = re.sub(rf'\b{re.escape(detected_partition_join_alias)}\b', partition_join_new_alias, mapped)
                                    mapped = re.sub(rf'\b{re.escape(orig_alias)}\b', new_alias, mapped)
                                    partition_join_joins.append(mapped)
                        # Also re-add other_functions between partition-join and variant tables
                        for keys, oconds in other_functions.items():
                            if detected_partition_join_alias in keys and orig_alias in keys:
                                for cond in oconds:
                                    mapped = cond
                                    mapped = re.sub(rf'\b{re.escape(detected_partition_join_alias)}\b', partition_join_new_alias, mapped)
                                    mapped = re.sub(rf'\b{re.escape(orig_alias)}\b', new_alias, mapped)
                                    partition_join_joins.append(mapped)
                else:
                    for table_alias in new_table_list:
                        partition_join_joins.append(f"{table_alias}.{partition_key} = {partition_join_new_alias}.{partition_key}")

                # Add any partition-join table conditions if they exist
                partition_join_conditions_list = []
                if detected_partition_join_alias in attribute_conditions:
                    for cond in attribute_conditions[detected_partition_join_alias]:
                        partition_join_conditions_list.append(re.sub(rf'\b{re.escape(detected_partition_join_alias)}\.', f'{partition_join_new_alias}.', cond))
                # Also include single-table other_functions that only reference the partition-join table
                for keys, oconds in other_functions.items():
                    if len(keys) == 1 and keys[0] == detected_partition_join_alias:
                        for cond in oconds:
                            mapped_cond = re.sub(rf'\b{re.escape(detected_partition_join_alias)}\b', partition_join_new_alias, cond)
                            partition_join_conditions_list.append(mapped_cond)

                # Build the combined query with partition-join table
                # IMPORTANT: Remove direct joins between tables when partition-join is present
                # Only keep attribute conditions and distance conditions, not partition key joins
                query_where_without_pk_joins = []
                for condition in query_where:
                    # Check for partition key equality joins between tables
                    # Pattern: table1.partition_key = table2.partition_key
                    is_pk_join = False
                    if "=" in condition and partition_key in condition:
                        # Split on = and check both sides contain table.partition_key pattern
                        parts = condition.split("=")
                        if len(parts) == 2:
                            left_side = parts[0].strip()
                            right_side = parts[1].strip()
                            # Check if both sides are table.partition_key references
                            left_match = re.match(rf"^t\d+\.{re.escape(partition_key)}$", left_side)
                            right_match = re.match(rf"^t\d+\.{re.escape(partition_key)}$", right_side)
                            if left_match and right_match:
                                is_pk_join = True

                    if is_pk_join:
                        # This is a partition key join between tables, skip it
                        continue
                    # Keep all other conditions (attribute conditions, distance conditions, etc.)
                    query_where_without_pk_joins.append(condition)

                combined_where = query_where_without_pk_joins + partition_join_joins + partition_join_conditions_list
                select_clause = _build_select_clause(
                    strip_select, original_select_clause, new_table_list, original_to_new_alias_mapping, partition_key, partition_join_new_alias, geometry_column
                )
                q_with_partition_join = f"{select_clause} FROM {', '.join(combined_table_list)} WHERE {' AND '.join(combined_where)}"
                ret.append(q_with_partition_join)

                # If there are partition key conditions (like IN subqueries), create variants
                # Flatten partition_key_conditions dict for combinatorial generation
                all_pk_conditions_pj = [(alias, cond) for alias, conds in partition_key_conditions.items() for cond in conds]
                if all_pk_conditions_pj:
                    # For partition-join tables with partition key conditions, apply them to the partition-join alias
                    for i in range(1, len(all_pk_conditions_pj) + 1):
                        for comb in itertools.combinations(all_pk_conditions_pj, i):
                            query_where_comb = combined_where.copy()
                            for orig_alias, subquery in comb:
                                remapped = re.sub(rf'\b{re.escape(orig_alias)}\.', f'{partition_join_new_alias}.', subquery)
                                query_where_comb.append(remapped)
                            # Build WHERE clause only if conditions exist
                            select_clause = _build_select_clause(
                                strip_select, original_select_clause, new_table_list, original_to_new_alias_mapping, partition_key, partition_join_new_alias, geometry_column
                            )
                            if query_where_comb:
                                q = f"{select_clause} FROM {', '.join(combined_table_list)} WHERE {' AND '.join(query_where_comb)}"
                            else:
                                q = f"{select_clause} FROM {', '.join(combined_table_list)}"
                            ret.append(q)
            else:
                # Normal case without p0 exclusion
                # Build WHERE clause only if conditions exist
                select_clause = _build_select_clause(strip_select, original_select_clause, new_table_list, original_to_new_alias_mapping, partition_key, None, geometry_column)
                if query_where:
                    q = f"{select_clause} FROM {', '.join(new_table_list_with_alias)} WHERE {' AND '.join(query_where)}"
                else:
                    q = f"{select_clause} FROM {', '.join(new_table_list_with_alias)}"
                ret.append(q)

            # Flatten partition_key_conditions for combinatorial generation
            all_pk_conditions = [(alias, cond) for alias, conds in partition_key_conditions.items() for cond in conds]
            if all_pk_conditions:  # TODO handle CTE expressions
                # Create variants of the query with partition key conditions
                for i in range(1, len(all_pk_conditions) + 1):
                    for comb in itertools.combinations(all_pk_conditions, i):
                        query_where_comb = query_where.copy()
                        for orig_alias, subquery in comb:
                            remapped = re.sub(rf'\b{re.escape(orig_alias)}\.', f'{new_table_list[0]}.', subquery)
                            query_where_comb.append(remapped)
                        # Build WHERE clause only if conditions exist
                        select_clause = _build_select_clause(
                            strip_select, original_select_clause, new_table_list, original_to_new_alias_mapping, partition_key, None, geometry_column
                        )
                        if query_where_comb:
                            q = f"{select_clause} FROM {', '.join(new_table_list_with_alias)} WHERE {' AND '.join(query_where_comb)}"
                        else:
                            q = f"{select_clause} FROM {', '.join(new_table_list_with_alias)}"
                        ret.append(q)

    # Add also the raw partition_key queries
    for pk_conds in partition_key_conditions.values():
        for partition_key_part in pk_conds:
            # Get part in brackets
            part = re.search(r"\((.*)\)", partition_key_part)
            if part:
                spart = part.group(1)
            else:
                continue
            sublist = spart.split("INTERSECT")
            for sub in sublist:
                sub = sub.strip()
                ret.append(sub)

    # Process each query with sqlglot, skipping non-SQL fragments
    result = []
    for q in ret:
        try:
            parsed = sqlglot.parse_one(q)
            simplified = sqlglot.optimizer.simplify.simplify(parsed)
            sql_result = simplified.sql()
            result.append(sql_result)
        except Exception:
            # If parsing fails, skip this fragment
            logger.error(f"Failed to parse query: {q}")
            continue

    return result


def is_distance_function(condition: str) -> bool:
    if re.match(
        # r"\w*\([a-zA-Z0-9_\.]*[,\+]\s*[a-zA-Z0-9_\.]*\)", condition
        r"\w*\(.*[,\+].*\)",
        condition,
    ):  # TODO make more robust
        return True
    else:
        return False


def normalize_distance_conditions(original_query: str, bucket_steps: float = 1.0, restrict_to_dist_functions=True) -> str:
    """
    This function takes a query and normalizes the distance conditions.
    It replaces the distance conditions with lower bound bucket bracket  and upper bound bucket bracket
    of the BETWEEN clause.

    Min values gets replaced by the next lower bucket value
    Max values gets replaced by the next higher bucket value
    eg.
    1.6 - 3.6 -> 1 - 4 WITH bucket_steps = 1
    1.6 - 3.6 -> 0 - 4 WITH bucket_steps = 2

    query: str: The query to be normalized
    partition_key: str: The identifier of the search space
    """
    bucket_steps = float(bucket_steps)

    # If bucket_steps is 0 or negative, return original query without distance normalization
    if bucket_steps <= 0:
        return original_query

    original_query = sqlglot.parse_one(original_query).sql()
    condition_list = extract_conjunctive_conditions(original_query)

    query = original_query
    distance_conditions_between = []
    distance_conditions_smaller = []
    distance_conditions_greater = []

    for condition in condition_list:
        if " BETWEEN " in condition.upper():
            distance_conditions_between.append(condition)
        elif "<" in condition or "<=" in condition:
            distance_conditions_smaller.append(condition)
        elif ">" in condition or ">=" in condition:
            distance_conditions_greater.append(condition)
        else:
            pass

    # TODO Ensure one of the values is a literal number
    # TODO Ensure number is on second position
    # TODO Ensure number is not negative (verify that this is needed)

    # Check if at least one value is a number
    for distance_condition in list(distance_conditions_between + distance_conditions_smaller + distance_conditions_greater):
        if not any(str(x).replace(".", "", 1).isdigit() for x in distance_condition.split()):
            logger.warning(f"No numeric value found in distance condition: {distance_condition}")
            if distance_condition in distance_conditions_between:
                distance_conditions_between.remove(distance_condition)
            if distance_condition in distance_conditions_smaller:
                distance_conditions_smaller.remove(distance_condition)
            if distance_condition in distance_conditions_greater:
                distance_conditions_greater.remove(distance_condition)

    # Check if number is on right side of comparison operator for distance functions # TODO check if this is needed, should be handled by sqlglot
    for distance_condition in list(distance_conditions_between + distance_conditions_smaller + distance_conditions_greater):
        if is_distance_function(distance_condition):
            if "<" in distance_condition or ">" in distance_condition:
                parts = re.split(r"(<=|>=|<|>)", distance_condition)
                if len(parts) == 2 and not str(parts[1].strip()).replace(".", "", 1).isdigit():
                    logger.warning(f"Numeric value not on right side of comparison in distance condition: {distance_condition}")
                    if distance_condition in distance_conditions_between:
                        distance_conditions_between.remove(distance_condition)
                    if distance_condition in distance_conditions_smaller:
                        distance_conditions_smaller.remove(distance_condition)
                    if distance_condition in distance_conditions_greater:
                        distance_conditions_greater.remove(distance_condition)

    # Check for negative numbers
    for distance_condition in list(distance_conditions_between + distance_conditions_smaller + distance_conditions_greater):
        numbers = [float(x) for x in re.findall(r"-?\d*\.?\d+", distance_condition)]
        if any(n < 0 for n in numbers):
            logger.warning(f"Negative value found in distance condition: {distance_condition}")
            if distance_condition in distance_conditions_between:
                distance_conditions_between.remove(distance_condition)
            if distance_condition in distance_conditions_smaller:
                distance_conditions_smaller.remove(distance_condition)
            if distance_condition in distance_conditions_greater:
                distance_conditions_greater.remove(distance_condition)

    for distance_condition in distance_conditions_between:
        if restrict_to_dist_functions and not is_distance_function(distance_condition):
            continue
        lower_bound, upper_bound = distance_condition.split(" BETWEEN ")[1].split(" AND ")

        lower_bound = lower_bound.strip()
        lower_bound = lower_bound.replace(";", "")
        upper_bound = upper_bound.strip()
        upper_bound = upper_bound.replace(";", "")

        # do nothing if either is negative
        if float(lower_bound) < 0 or float(upper_bound) < 0:
            continue

        # Floor lower bound
        lower_boundf = int(float(lower_bound) / bucket_steps) * bucket_steps

        # Ceil upper bound
        if int(float(upper_bound) / bucket_steps) * bucket_steps == float(upper_bound):
            upper_boundf = int(float(upper_bound) / bucket_steps) * bucket_steps
        else:
            upper_boundf = int(float(upper_bound) / bucket_steps) * bucket_steps + bucket_steps

        new_condition = f"{distance_condition.split('BETWEEN')[0]} BETWEEN {lower_boundf:g} AND {upper_boundf:g}"
        query = query.replace(distance_condition, new_condition)

    # TODO ensure robustness with flipped conditions
    for distance_condition in distance_conditions_smaller:
        if restrict_to_dist_functions and not is_distance_function(distance_condition):
            continue

        # More robust parsing: use regex to extract the numeric value correctly
        if "<=" in distance_condition:
            # Use regex to find the number after <=, avoiding parsing issues with subsequent AND clauses
            match = re.search(r"<=\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)", distance_condition)
            if not match:
                logger.warning(f"Could not extract numeric value from condition: {distance_condition}")
                continue

            value_str = match.group(1)
            try:
                value = float(value_str)
            except ValueError:
                logger.warning(f"Could not parse numeric value '{value_str}' from condition: {distance_condition}")
                continue

            if value < 0:
                continue
            # For <= operator: round up to next bucket boundary
            bucket_floor = int(value / bucket_steps) * bucket_steps
            if bucket_floor == value:
                # Value is exactly on bucket boundary, keep it
                valuef = value
            else:
                # Round up to next bucket boundary
                valuef = bucket_floor + bucket_steps
            left_part = distance_condition.split("<=")[0]
            new_condition = f"{left_part} <= {valuef:g}"
        else:
            # Similar robust parsing for < operator
            match = re.search(r"<\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)", distance_condition)
            if not match:
                logger.warning(f"Could not extract numeric value from condition: {distance_condition}")
                continue

            value_str = match.group(1)
            try:
                value = float(value_str)
            except ValueError:
                logger.warning(f"Could not parse numeric value '{value_str}' from condition: {distance_condition}")
                continue

            if value < 0:
                continue
            b = int(value / bucket_steps) * bucket_steps
            if b == value:
                valuef = b
            else:
                valuef = b + bucket_steps
            left_part = distance_condition.split("<")[0]
            new_condition = f"{left_part} < {valuef:g}"
        query = query.replace(distance_condition, new_condition)

    for distance_condition in distance_conditions_greater:
        if restrict_to_dist_functions and not is_distance_function(distance_condition):
            continue

        # More robust parsing for >= and > operators
        if ">=" in distance_condition:
            match = re.search(r">=\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)", distance_condition)
            if not match:
                logger.warning(f"Could not extract numeric value from condition: {distance_condition}")
                continue

            value_str = match.group(1)
            try:
                value = float(value_str)
            except ValueError:
                logger.warning(f"Could not parse numeric value '{value_str}' from condition: {distance_condition}")
                continue

            if value < 0:
                continue
            valuef = int(value / bucket_steps) * bucket_steps
            left_part = distance_condition.split(">=")[0]
            new_condition = f"{left_part} >= {valuef:g}"
        else:
            match = re.search(r">\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)", distance_condition)
            if not match:
                logger.warning(f"Could not extract numeric value from condition: {distance_condition}")
                continue

            value_str = match.group(1)
            try:
                value = float(value_str)
            except ValueError:
                logger.warning(f"Could not parse numeric value '{value_str}' from condition: {distance_condition}")
                continue

            if value < 0:
                continue
            valuef = int(value / bucket_steps) * bucket_steps
            left_part = distance_condition.split(">")[0]
            new_condition = f"{left_part} > {valuef:g}"
        query = query.replace(distance_condition, new_condition)

    return " ".join(query.split())


def _add_constraints_to_query(query: str, add_constraints: dict[str, str]) -> str:
    """
    Add constraints to specific tables in a query.

    Args:
        query: The SQL query to modify
        add_constraints: Dict mapping table names to constraints (e.g. {"points_table": "size = 4"})

    Returns:
        Modified query with constraints added
    """
    if not add_constraints:
        return query

    try:
        parsed = sqlglot.parse_one(query)

        # Check if we have any of the target tables in the query
        has_target_table = False
        for table in parsed.find_all(exp.Table):
            if table.name in add_constraints:
                has_target_table = True
                break

        if not has_target_table:
            return query

        # Find the main SELECT statement
        select_stmt = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
        if not select_stmt:
            return query

        # Build combined constraint for all matching tables
        new_constraints = []
        for table_name, constraint in add_constraints.items():
            # Check if this table exists in the query
            table_exists = any(table.name == table_name for table in parsed.find_all(exp.Table))
            if table_exists:
                try:
                    # Parse the constraint as an expression
                    constraint_parsed = sqlglot.parse_one(f"SELECT * FROM t WHERE {constraint}")
                    where_clause = constraint_parsed.find(exp.Where)
                    if where_clause and where_clause.this:
                        constraint_expr = where_clause.this
                        new_constraints.append(constraint_expr)
                except Exception as e:
                    logger.warning(f"Failed to parse constraint '{constraint}': {e}")

        if not new_constraints:
            return query

        # Get existing WHERE clause
        existing_where = select_stmt.args.get("where")

        if existing_where:
            # Combine with existing WHERE clause using AND
            combined_condition = existing_where.this
            for constraint_expr in new_constraints:
                combined_condition = exp.And(this=combined_condition, expression=constraint_expr)
            select_stmt.set("where", exp.Where(this=combined_condition))
        else:
            # Create new WHERE clause
            if len(new_constraints) == 1:
                where_condition = new_constraints[0]
            else:
                where_condition = new_constraints[0]
                for constraint_expr in new_constraints[1:]:
                    where_condition = exp.And(this=where_condition, expression=constraint_expr)
            select_stmt.set("where", exp.Where(this=where_condition))

        return parsed.sql()
    except Exception as e:
        logger.warning(f"Failed to add constraints to query: {e}")
        return query


def _remove_orphaned_tables(parsed: exp.Expression) -> exp.Expression:
    """Remove tables from FROM that have no remaining WHERE conditions connecting them.

    When a condition (e.g., ST_DWithin) linking a dimension table to the main table is removed,
    the dimension table becomes "orphaned" — it has no remaining conditions connecting it.
    This function detects and removes such orphaned tables and their stale conditions.

    Uses connectivity analysis: builds a graph of aliases connected by shared conditions,
    then removes aliases not reachable from the first (anchor) table.
    Reconstructs SQL from scratch to avoid dangling AST references.
    """
    select = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
    if not select:
        return parsed

    # 1. Get all table aliases from FROM (outer-level only, not subquery tables)
    from_clause = select.find(exp.From)
    if not from_clause:
        return parsed

    table_info: list[tuple[str, exp.Table]] = []  # (lowercase_alias, Table node)
    from_table = from_clause.this
    if isinstance(from_table, exp.Table):
        alias = str(from_table.alias or from_table.name).lower()
        table_info.append((alias, from_table))

    # Comma-joined tables appear as Join nodes in sqlglot
    for join in select.find_all(exp.Join):
        if join.find_ancestor(exp.Subquery):
            continue
        if isinstance(join.this, exp.Table):
            alias = str(join.this.alias or join.this.name).lower()
            table_info.append((alias, join.this))

    alias_set = {a for a, _ in table_info}
    if len(alias_set) <= 1:
        return parsed

    # 2. Get remaining WHERE conditions
    where = select.args.get("where")
    if not where:
        return parsed

    conditions = []
    where_expr = where.this
    if isinstance(where_expr, exp.And):
        conditions = list(where_expr.flatten())
    else:
        conditions = [where_expr]

    # 3. Build connectivity graph
    G = nx.Graph()
    G.add_nodes_from(alias_set)

    for condition in conditions:
        referenced = set()
        for col in condition.find_all(exp.Column):
            if col.table and col.table.lower() in alias_set:
                referenced.add(col.table.lower())
        for exists_node in condition.find_all(exp.Exists):
            for col in exists_node.find_all(exp.Column):
                if col.table and col.table.lower() in alias_set:
                    referenced.add(col.table.lower())

        ref_list = list(referenced)
        for i in range(len(ref_list)):
            for j in range(i + 1, len(ref_list)):
                G.add_edge(ref_list[i], ref_list[j])

    # 4. Connected component from anchor (first table)
    anchor = table_info[0][0]
    connected = set(nx.node_connected_component(G, anchor)) if anchor in G else {anchor}

    # 5. Orphaned aliases
    orphaned = alias_set - connected
    if not orphaned:
        return parsed

    # 6. Reconstruct SQL without orphaned tables (avoids dangling AST commas)
    kept_tables = [(a, t) for a, t in table_info if a not in orphaned]

    new_conditions = []
    for condition in conditions:
        referenced = set()
        for col in condition.find_all(exp.Column):
            if col.table:
                referenced.add(col.table.lower())
        if not referenced or (referenced - orphaned):
            new_conditions.append(condition)

    select_clause = ", ".join(expr.sql() for expr in select.expressions)
    distinct_keyword = "DISTINCT " if select.args.get("distinct") else ""
    from_parts = [t.sql() for _, t in kept_tables]
    sql = f"SELECT {distinct_keyword}{select_clause} FROM {', '.join(from_parts)}"
    if new_conditions:
        where_parts = [c.sql() for c in new_conditions]
        sql += f" WHERE {' AND '.join(where_parts)}"

    for clause_name in ["group", "order", "limit", "having"]:
        clause = select.args.get(clause_name)
        if clause:
            sql += f" {clause.sql()}"

    return sqlglot.parse_one(sql)


def _remove_constraints_from_query(query: str, attributes_to_remove: list[str]) -> str:
    """
    Remove constraints involving specific attributes from a query.

    Args:
        query: The SQL query to modify
        attributes_to_remove: List of attribute names to remove from constraints

    Returns:
        Modified query with specified constraints removed
    """
    if not attributes_to_remove:
        return query

    try:
        parsed = sqlglot.parse_one(query)

        # Function to check if an expression contains any of the attributes to remove
        def contains_attribute(expr, attrs):
            for col in expr.find_all(exp.Column):
                if col.name in attrs:
                    return True
            return False

        # Process WHERE clause
        select = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
        if select and select.args.get("where"):
            where = select.args["where"].this

            if isinstance(where, exp.And):
                # Filter out conditions that contain the attributes
                new_conditions = []
                for condition in where.flatten():
                    if not contains_attribute(condition, attributes_to_remove):
                        new_conditions.append(condition)

                if new_conditions:
                    # Rebuild the WHERE clause
                    if len(new_conditions) == 1:
                        select.set("where", exp.Where(this=new_conditions[0]))
                    else:
                        # Combine remaining conditions with AND
                        combined = new_conditions[0]
                        for cond in new_conditions[1:]:
                            combined = exp.And(this=combined, expression=cond)
                        select.set("where", exp.Where(this=combined))
                else:
                    # Remove WHERE clause entirely if no conditions remain
                    select.set("where", None)
            else:
                # Single condition in WHERE
                if contains_attribute(where, attributes_to_remove):
                    select.set("where", None)

        # Remove orphaned tables (tables with no remaining conditions connecting them)
        parsed = _remove_orphaned_tables(parsed)

        return parsed.sql()
    except Exception as e:
        logger.warning(f"Failed to remove constraints from query: {e}")
        return query


def _apply_constraint_modifications(
    queries: set[str],
    add_constraints: dict[str, str] | None = None,
    remove_constraints_all: list[str] | None = None,
    remove_constraints_add: list[str] | None = None,
) -> set[str]:
    """
    Apply constraint modifications to a set of queries.

    Args:
        queries: Set of SQL queries to modify
        add_constraints: Constraints to add to specific tables
        remove_constraints_all: Attributes to remove from all queries
        remove_constraints_add: Attributes to remove, creating additional variants

    Returns:
        Set of modified queries
    """
    result_queries = set()

    # First, handle remove_constraints_all (modifies all queries)
    if remove_constraints_all:
        modified_queries = set()
        for q in queries:
            modified_q = _remove_constraints_from_query(q, remove_constraints_all)
            modified_queries.add(modified_q)
        queries = modified_queries

    # Second, handle remove_constraints_add (creates additional variants)
    if remove_constraints_add:
        # Keep original queries and add variants with constraints removed
        result_queries.update(queries)
        for q in queries:
            modified_q = _remove_constraints_from_query(q, remove_constraints_add)
            result_queries.add(modified_q)
    else:
        result_queries = queries.copy()

    # Finally, handle add_constraints (can create multiple variants per query)
    if add_constraints:
        final_queries = set()
        for q in result_queries:
            # For each query, create a variant with constraints added
            modified_q = _add_constraints_to_query(q, add_constraints)
            final_queries.add(modified_q)
            # Also keep the original
            final_queries.add(q)
        result_queries = final_queries

    return result_queries


def generate_all_query_hash_pairs(
    query: str,
    partition_key: str,
    min_component_size: int = 1,
    follow_graph: bool = True,
    keep_all_attributes: bool = True,
    canonicalize_queries: bool = False,
    auto_detect_partition_join: bool = True,
    max_component_size: int | None = None,
    partition_join_table: str | None = None,
    warn_no_partition_key: bool = True,
    strip_select: bool = True,
    bucket_steps: float = 1.0,
    add_constraints: dict[str, str] | None = None,
    remove_constraints_all: list[str] | None = None,
    remove_constraints_add: list[str] | None = None,
    skip_partition_key_joins: bool = False,
    geometry_column: str | None = None,
    **kwargs: Any,
) -> list[tuple[str, str]]:
    """
    Generate all query hash pairs for a given query with configurable variant generation.

    Args:
        query: The SQL query to process
        partition_key: The partition key identifier
        min_component_size: Minimum size for query components
        follow_graph: Whether to follow multi-point non-equality joins (e.g. spatial constraints)
        keep_all_attributes: Whether to keep all attributes in variants fixed
        canonicalize_queries: Whether to canonicalize queries for consistent hashing
        auto_detect_partition_join: Automatically detect partition join patterns
        max_component_size: Maximum size for query components
        partition_join_table: Specific table to use as partition join center
        warn_no_partition_key: Whether to warn if partition key is missing
        strip_select: Whether to strip SELECT clause
        bucket_steps: Step size for normalizing distance conditions (e.g., 1.0, 0.5, etc.)
        add_constraints: Dict mapping table names to constraints to add (e.g., {"table": "col = val"})
        remove_constraints_all: List of attribute names to remove from all query variants
        remove_constraints_add: List of attribute names to remove, creating additional variants

    Returns:
        List of tuples containing (query_text, query_hash) pairs
    """
    # Handle deprecated star_join_* parameter names (only apply if explicit param is at default)
    deprecated = _handle_deprecated_kwargs(kwargs, "generate_all_query_hash_pairs")
    if "partition_join_table" in deprecated:
        if partition_join_table is not None:
            raise TypeError("Cannot pass both 'partition_join_table' and deprecated 'star_join_table'")
        partition_join_table = deprecated["partition_join_table"]
    if "auto_detect_partition_join" in deprecated:
        auto_detect_partition_join = deprecated["auto_detect_partition_join"]

    query_set: set[str] = set()

    # If strip_select=False, extract the original SELECT clause BEFORE clean_query()
    # replaces it with *, so downstream generate_partial_queries can preserve it.
    pre_clean_select_clause: str | None = None
    if not strip_select:
        try:
            _parsed = sqlglot.parse_one(query)
            _sel = _parsed.find(exp.Select) if not isinstance(_parsed, exp.Select) else _parsed
            if _sel and _sel.expressions and not (len(_sel.expressions) == 1 and isinstance(_sel.expressions[0], exp.Star)):
                pre_clean_select_clause = ", ".join(e.sql() for e in _sel.expressions)
        except Exception:
            pre_clean_select_clause = None

    # Clean the query
    query = clean_query(query)

    # Create all possible partial queries
    query_set_diff_combinations = set(
        generate_partial_queries(
            query,
            partition_key,
            min_component_size,
            follow_graph,
            keep_all_attributes,
            other_functions_as_distance_conditions=True,
            auto_detect_partition_join=auto_detect_partition_join,
            max_component_size=max_component_size,
            partition_join_table=partition_join_table,
            warn_no_partition_key=warn_no_partition_key,
            strip_select=strip_select,
            skip_partition_key_joins=skip_partition_key_joins,
            geometry_column=geometry_column,
            pre_clean_select_clause=pre_clean_select_clause,
        )
    )
    query_set.update(query_set_diff_combinations)

    # Create bucket variant with normalized distances (Example: 1.6 - 3.6 -> 1 - 4 WITH bucket_steps = 1)
    nor_dist_query = normalize_distance_conditions(query, bucket_steps=bucket_steps)

    query_set.update(
        set(
            generate_partial_queries(
                nor_dist_query,
                partition_key,
                min_component_size,
                follow_graph,
                keep_all_attributes,
                other_functions_as_distance_conditions=True,  # TODO evaluate if how performance is affected if is turned off
                auto_detect_partition_join=auto_detect_partition_join,
                max_component_size=max_component_size,
                partition_join_table=partition_join_table,
                warn_no_partition_key=warn_no_partition_key,
                strip_select=strip_select,
                skip_partition_key_joins=skip_partition_key_joins,
                geometry_column=geometry_column,
                pre_clean_select_clause=pre_clean_select_clause,
            )
        )
    )

    # Apply constraint modifications to all generated queries
    query_set = _apply_constraint_modifications(
        query_set, add_constraints=add_constraints, remove_constraints_all=remove_constraints_all, remove_constraints_add=remove_constraints_add
    ) # TODO This would sometime create a difefrent ordering nessesary as the reassignment of aliase in the fragmentcreation could be different

    # If we have constraint modifications, also apply them to normalized distance variants
    if add_constraints or remove_constraints_add:
        # Generate normalized distance variants for modified queries
        additional_normalized_queries = set()
        for q in query_set:
            if q != query and q != nor_dist_query:  # Avoid re-processing original queries
                nor_q = normalize_distance_conditions(q, bucket_steps=bucket_steps)
                if nor_q != q:  # Only add if normalization changed something
                    additional_normalized_queries.add(nor_q)
        query_set.update(additional_normalized_queries)

    if canonicalize_queries:
        # canonicalize each query (to make sure that the hash is unique for the same query)
        # Not needed if the query is already canonicalized (e.g. if the query is always generated by the same functions)
        can_query_set = set()
        for baseq in query_set:  # TODO Performance needs to be improved
            sqlglot_query = sqlglot.parse_one(baseq)
            nquery = sqlglot.optimizer.canonicalize.canonicalize(sqlglot_query)
            q = nquery.sql()
            can_query_set.add(q)
        return [(q, hash_query(q)) for q in can_query_set]
    else:
        return [(q, hash_query(q)) for q in query_set]


def hash_query(query: str) -> str:
    return hashlib.sha1(query.encode()).hexdigest()



def extract_distance_constraints(query: str) -> list[tuple[str, str, float]]:
    """Extract (alias1, alias2, distance) from all distance constraints in the query.

    Handles two kinds of distance constraints:
      1. ST_DWithin(a.geom, b.geom, distance) function calls
      2. Comparison-based distance expressions detected by is_distance_function(),
         e.g. SQRT(POWER(p1.x - p2.x, 2)) < 0.008 or DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6

    Args:
        query: SQL query string to parse.

    Returns:
        List of (alias1, alias2, distance) tuples, sorted by alias pair.
    """
    try:
        parsed = sqlglot.parse_one(query)
    except sqlglot.ParseError:
        return []

    results: list[tuple[str, str, float]] = []
    seen_pairs: set[tuple[str, str]] = set()

    # --- Pass 1: ST_DWithin function calls ---
    for func in parsed.find_all(exp.Anonymous):
        if func.name.upper() != "ST_DWITHIN":
            continue
        args = func.args.get("expressions", [])
        if len(args) < 3:
            continue

        alias1 = None
        alias2 = None
        for col in args[0].find_all(exp.Column):
            if col.table:
                alias1 = col.table
                break
        for col in args[1].find_all(exp.Column):
            if col.table:
                alias2 = col.table
                break

        distance_arg = args[2]
        distance = None
        literal = distance_arg.find(exp.Literal)
        if literal is None and isinstance(distance_arg, exp.Literal):
            literal = distance_arg
        if literal is not None:
            try:
                distance = float(literal.this)
            except (ValueError, TypeError):
                pass

        if alias1 is not None and alias2 is not None and distance is not None:
            a1, a2 = sorted([alias1, alias2])
            results.append((a1, a2, distance))
            seen_pairs.add((a1, a2))

    # --- Pass 2: Comparison-based distance functions ---
    try:
        conditions = extract_conjunctive_conditions(query)
    except Exception:
        conditions = []

    for condition in conditions:
        if not is_distance_function(condition):
            continue

        # Extract table aliases from column references in the condition
        try:
            cond_parsed = sqlglot.parse_one(f"SELECT 1 WHERE {condition}")
        except sqlglot.ParseError:
            continue

        aliases: set[str] = set()
        for col in cond_parsed.find_all(exp.Column):
            if col.table:
                aliases.add(col.table)

        if len(aliases) != 2:
            continue

        a1, a2 = sorted(aliases)

        # Skip if already captured by ST_DWithin pass
        if (a1, a2) in seen_pairs:
            continue

        # Extract upper-bound distance from the condition
        distance = None
        condition_upper = condition.upper()

        if " BETWEEN " in condition_upper:
            # BETWEEN x AND y → extract y (upper bound)
            try:
                between_part = condition.split(" BETWEEN ")[1] if " BETWEEN " in condition else condition.split(" between ")[1]
                upper_str = between_part.split(" AND ")[1] if " AND " in between_part else between_part.split(" and ")[1]
                distance = float(upper_str.strip())
            except (IndexError, ValueError):
                pass
        elif "<=" in condition:
            match = re.search(r"<=\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)", condition)
            if match:
                try:
                    distance = float(match.group(1))
                except ValueError:
                    pass
        elif "<" in condition:
            match = re.search(r"<\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)", condition)
            if match:
                try:
                    distance = float(match.group(1))
                except ValueError:
                    pass
        elif ">=" in condition or ">" in condition:
            # Lower bound only — not relevant for buffer distance
            continue

        if distance is not None:
            results.append((a1, a2, distance))
            seen_pairs.add((a1, a2))

    return sorted(results)


def compute_buffer_distance(query: str) -> float:
    """Compute buffer distance as the weighted graph diameter of distance constraints.

    Builds a weighted graph where nodes are table aliases and edges are distance
    constraints (ST_DWithin calls and comparison-based distance expressions).
    Returns the diameter (longest shortest path weight) of the graph.

    For star patterns (all distances from one hub), the diameter is the sum of the
    two largest distances through the hub. For chain patterns, it's the sum along
    the longest path.

    Args:
        query: SQL query string to parse.

    Returns:
        The weighted graph diameter in the same units as the distance constraints.
        Returns 0.0 if no distance constraints are found or the graph has fewer than 2 nodes.
    """
    edges = extract_distance_constraints(query)
    if not edges:
        return 0.0

    G = nx.Graph()
    for alias1, alias2, distance in edges:
        # If multiple ST_DWithin between same pair, use the max distance
        if G.has_edge(alias1, alias2):
            current_weight = G[alias1][alias2]["weight"]
            G[alias1][alias2]["weight"] = max(current_weight, distance)
        else:
            G.add_edge(alias1, alias2, weight=distance)

    if G.number_of_nodes() < 2:
        return 0.0

    # Compute weighted diameter: longest shortest path across all pairs
    max_distance = 0.0
    for component in nx.connected_components(G):
        subgraph = G.subgraph(component)
        if subgraph.number_of_nodes() < 2:
            continue
        # All-pairs shortest paths (weighted)
        for source in subgraph.nodes():
            lengths = nx.single_source_dijkstra_path_length(subgraph, source, weight="weight")
            for _target, length in lengths.items():
                if length > max_distance:
                    max_distance = length

    return max_distance

def generate_all_hashes(
    query: str,
    partition_key: str,
    min_component_size=1,
    follow_graph=True,
    fix_attributes=True,
    canonicalize_queries=False,
    auto_detect_partition_join: bool = True,
    max_component_size: int | None = None,
    partition_join_table: str | None = None,
    warn_no_partition_key: bool = True,
    strip_select: bool = True,
    bucket_steps: float = 1.0,
    add_constraints: dict[str, str] | None = None,
    remove_constraints_all: list[str] | None = None,
    remove_constraints_add: list[str] | None = None,
    skip_partition_key_joins: bool = False,
    geometry_column: str | None = None,
    **kwargs: Any,
) -> list[str]:
    """
    Generates all hashes for the given query.
    """
    # Handle deprecated star_join_* parameter names (only apply if explicit param is at default)
    deprecated = _handle_deprecated_kwargs(kwargs, "generate_all_hashes")
    if "partition_join_table" in deprecated:
        if partition_join_table is not None:
            raise TypeError("Cannot pass both 'partition_join_table' and deprecated 'star_join_table'")
        partition_join_table = deprecated["partition_join_table"]
    if "auto_detect_partition_join" in deprecated:
        auto_detect_partition_join = deprecated["auto_detect_partition_join"]

    qh_pairs = generate_all_query_hash_pairs(
        query=query,
        partition_key=partition_key,
        min_component_size=min_component_size,
        follow_graph=follow_graph,
        keep_all_attributes=fix_attributes,
        canonicalize_queries=canonicalize_queries,
        auto_detect_partition_join=auto_detect_partition_join,
        max_component_size=max_component_size,
        partition_join_table=partition_join_table,
        warn_no_partition_key=warn_no_partition_key,
        strip_select=strip_select,
        bucket_steps=bucket_steps,
        add_constraints=add_constraints,
        remove_constraints_all=remove_constraints_all,
        remove_constraints_add=remove_constraints_add,
        skip_partition_key_joins=skip_partition_key_joins,
        geometry_column=geometry_column,
    )
    return [x[1] for x in qh_pairs]


if __name__ == "__main__":
    print("To use the query processor, please refer to the documentation")
