"""
Methods for splitting and reassembling queries
"""

import hashlib
import itertools
import logging
import re
from collections import defaultdict
from itertools import combinations
from typing import Any

import networkx as nx  # type: ignore
import sqlglot
import sqlglot.expressions
import sqlglot.optimizer
import sqlglot.optimizer.canonicalize
import sqlglot.optimizer.normalize
import sqlglot.optimizer.simplify

logger = logging.getLogger("PartitionCache")


def clean_query(query: str) -> str:
    """
    Perform some basic cleaning of the query to ensure a stable output even if the query is formatted differently.
    """

    query = re.sub(r"\s+", " ", query)
    query = re.sub(r"\s*=\s*", "=", query)

    # Remove trailing semicolons (they're not part of the SQL statement)
    query = query.rstrip().rstrip(";")

    # Normalize the query
    sqlglot_query = sqlglot.parse_one(query)

    # Normalize the query (including building DNF)
    nquery = sqlglot.optimizer.normalize.normalize(sqlglot_query)
    query = nquery.sql()


    # Use sqlglot to properly parse and flatten WHERE clause
    try:
        parsed = sqlglot.parse_one(query)
        where_clause = parsed.find(sqlglot.expressions.Where)

        if where_clause and where_clause.this:
            # Check if the WHERE clause is wrapped in unnecessary parentheses
            where_expr = where_clause.this

            # If the top-level expression is wrapped in Paren, unwrap it if it's just a container
            if isinstance(where_expr, sqlglot.expressions.Paren):
                # The parentheses are unnecessary - extract the inner expression
                inner_expr = where_expr.this
                where_clause.set("this", inner_expr)
                query = parsed.sql()
                logger.debug("Flattened unnecessary parentheses around WHERE clause")

    except Exception as e:
        logger.debug(f"Could not flatten WHERE parentheses using sqlglot: {e}")
        # Fallback to regex approach for simple cases

    # Removing all comments
    query = re.sub(r"--.*", "", query)

    # Removing LIMIT
    query = re.sub(r"LIMIT\s\d+", "", query)

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
    If in one condition more thand one attribute is used, remove one of the attributes (all possible outcomes)
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
    where_clause = parsed.find(sqlglot.expressions.Where)
    conditions = []

    def extract_conditions_from_expression(expression):
        if isinstance(expression, sqlglot.expressions.And):
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
    list[str],
    dict[tuple, list[str]],
    list[str],
    dict[str, str],
    dict[tuple[str, str], list[str]],
]:
    """
    Extracts all conditions from a query
    Splits it by distance functions, attributes and subqueries for the partition key
    """
    attribute_conditions: dict[str, list[str]] = {}  # {table_alias: [conditions]}
    distance_conditions: dict[tuple[str, str], list[str]] = defaultdict(list)  # {(table_alias1, table_alias2): [conditions]}
    other_functions: dict[tuple, list[str]] = defaultdict(list)  # {(table_alias1, ...): [conditions]}
    partition_key_conditions: list[str] = []  # List of all subqueries
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
            sqlglot.parse_one(condition).find(sqlglot.expressions.In) or
            any(op in condition for op in ["BETWEEN", ">", "<", "=", "!=", "<>"])
        ):
            # if partition_key is in condition, it is a partition key condition (IN, BETWEEN, comparison, etc.)
            # Preserve the full condition including NOT, BETWEEN, comparison operators
            # Find the partition key part after the table alias
            if "." in condition:
                parts = condition.split(".")
                if len(parts) >= 2:
                    # Preserve everything from the partition key onwards, including NOT prefix if present
                    table_part = parts[0].strip()
                    condition_part = ".".join(parts[1:])

                    # Handle NOT prefix properly
                    if table_part.upper().startswith("NOT "):
                        # Extract table alias and preserve NOT
                        con = f"NOT {condition_part}"
                    else:
                        con = condition_part
                    partition_key_conditions.append(con)
                    continue
            # Fallback: use the condition as-is if parsing fails
            partition_key_conditions.append(condition)
            continue

        # count how many times a table alias is in the condition
        nr_alias_in_condition = 0
        for alias in table_aliases:
            nr_alias_in_condition += condition.count(f"{alias}.")  # TODO Needs to be more robust

        # if only one table alias is in the condition, it is an attribute condition
        if nr_alias_in_condition == 1 and "." in condition:
            al, *cons = condition.split(".")
            con = ".".join(cons)
            attribute_conditions[al].append(con)
            continue

        # if two table aliases are in the condition
        else:
            all_alias: set = set(re.findall(r"[a-zA-Z_]\w*(?=\.)", condition))
            if sqlglot.parse_one(condition).find(sqlglot.expressions.Func):
                if len(all_alias) == 2:
                    all_alias_list = sorted(all_alias)
                    distance_conditions[(all_alias_list[0], all_alias_list[1])].append(condition)
                    continue
                else:
                    parsed = sqlglot.parse_one(condition)
                    table_identifiers = tuple(sorted({col.table for col in parsed.find_all(sqlglot.expressions.Column) if col.table}))
                    other_functions[table_identifiers].append(condition)
                    continue

            elif sqlglot.parse_one(condition).find(sqlglot.expressions.Or):
                parsed = sqlglot.parse_one(condition)
                table_identifiers = tuple(sorted({col.table for col in parsed.find_all(sqlglot.expressions.Column) if col.table}))
                or_conditions[table_identifiers].append(condition)
                continue
            else:
                if len(all_alias) == 2:
                    # get all aliases with sqlglot
                    parsed = sqlglot.parse_one(condition)
                    table_identifiers = tuple(sorted({col.table for col in parsed.find_all(sqlglot.expressions.Column) if col.table}))
                    distance_conditions[(table_identifiers[0], table_identifiers[1])].append(condition)
                    continue
                else:
                    parsed = sqlglot.parse_one(condition)
                    table_identifiers = tuple(sorted({col.table for col in parsed.find_all(sqlglot.expressions.Column) if col.table}))
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


def detect_star_join_table(
    table_aliases: list[str],
    alias_to_table_map: dict[str, str],
    attribute_conditions: dict[str, list[str]],
    distance_conditions: dict[tuple[str, str], list[str]],
    partition_key_joins: dict[tuple[str, str], list[str]],
    partition_key: str,
    auto_detect_star_join: bool = True,
    star_join_table: str | None = None,
) -> str | None:
    """
    Detect star-join table based on explicit specification or auto-detection patterns.

    Star-join tables (formerly "p0 tables") are special tables that serve as central join
    points in star-schema patterns. They are automatically detected and handled specially
    to optimize query variant generation.

    Detection Process:
    1. **Explicit specification**: Tables can be marked as star-join via star_join_table parameter
       - Matches by alias first, then by table name
       - Only one star-join table is used per query

    2. **Auto-detection** (when auto_detect_star_join=True, default):
       a. Tables with names starting with 'p0' AND no attribute conditions
       b. Smart detection: Tables that join ALL other tables AND have ONLY partition key conditions

    Args:
        table_aliases: List of table aliases in the query
        alias_to_table_map: Mapping from alias to table name
        attribute_conditions: Conditions for each table alias
        distance_conditions: Distance conditions between table pairs
        partition_key_joins: Explicit partition key joins between tables
        partition_key: The partition key column name
        auto_detect_star_join: Whether to auto-detect star-join tables
        star_join_table: Explicitly specified star-join table alias or name

    Returns:
        The alias of the detected star-join table, or None if no star-join table is found

    Raises:
        None - function handles all edge cases gracefully
    """
    detected_star_join_alias = None

    # First, check explicitly specified star-join table if provided
    if star_join_table:
        # Try to match by alias first, then by table name
        if star_join_table in table_aliases:
            detected_star_join_alias = star_join_table
            logger.info(f"Using explicit star-join table by alias: {star_join_table}")
        else:
            # Try to match by table name
            for alias, table_name in alias_to_table_map.items():
                if table_name == star_join_table:
                    detected_star_join_alias = alias
                    logger.info(f"Using explicit star-join table by name: {star_join_table} -> alias {alias}")
                    break

            if not detected_star_join_alias:
                logger.warning(f"Could not match star-join table specification '{star_join_table}' to any alias or table name")

    # If no explicit star-join specified and auto-detection is enabled
    if not detected_star_join_alias and auto_detect_star_join:
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
            detected_star_join_alias, detection_method = candidates[0]
            logger.info(
                f"Auto-detected star-join table via {detection_method}: {detected_star_join_alias} -> {alias_to_table_map.get(detected_star_join_alias, detected_star_join_alias)}"
            )
            if len(candidates) > 1:
                logger.info(f"Multiple star-join candidates found: {[c[0] for c in candidates]}. Using '{detected_star_join_alias}'")

    return detected_star_join_alias


def generate_partial_queries(
    query: str,
    partition_key: str,
    min_component_size: int = 1,
    follow_graph: bool = True,
    keep_all_attributes: bool = True,
    other_functions_as_distance_conditions: bool = True,
    auto_detect_star_join: bool = True,
    max_component_size: int | None = None,
    star_join_table: str | None = None,
    warn_no_partition_key: bool = True,
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
        auto_detect_star_join: bool: If True, automatically detect star-join tables based on pattern
            (joins all tables with only partition key conditions). If False, only use star_join_table
        max_component_size: int: Maximum number of tables in generated variants (excluding star-join)
        star_join_table: str: Explicit table alias or name to treat as the star-join table.
            Only one star-join table is allowed per query
        warn_no_partition_key: bool: If True, emit warnings for tables not using the partition key

    Returns:
        List[str]: List of all possible partial queries

    """

    # init variables
    ret: list[str] = []  # List of all possible partial queries for return

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

    # Detect star-join tables - special tables used for partition key joins
    # These tables are excluded from variant generation and re-added to each variant
    detected_star_join_alias = detect_star_join_table(
        table_aliases,
        alias_to_table_map,
        attribute_conditions,
        distance_conditions,
        partition_key_joins,
        partition_key,
        auto_detect_star_join,
        star_join_table,
    )

    # Emit warnings for tables not using partition key if requested
    if warn_no_partition_key:
        for alias in table_aliases:
            if alias == detected_star_join_alias:
                continue  # Star-join tables are expected to use partition key

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

            if not uses_partition_key:
                logger.warning(f"Table '{alias}' ({alias_to_table_map.get(alias, alias)}) does not use partition key '{partition_key}'")

    # No need to limit - we already have only one detected_star_join_alias

    # Filter aliases for variant generation - always exclude star-join table
    if detected_star_join_alias:
        aliases_for_variants = [a for a in table_aliases if a != detected_star_join_alias]
    else:
        aliases_for_variants = table_aliases

    # Filter distance conditions to exclude star-join table
    distance_conditions_filtered = {}
    if detected_star_join_alias:
        for (alias1, alias2), conditions in distance_conditions.items():
            if alias1 != detected_star_join_alias and alias2 != detected_star_join_alias:
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
                query_where.extend([f"{table_alias}.{x}" for x in var_attribute_conditions[table_condition_key]])

                # set the correct table alias in the distance conditions
                pattern = rf"\b{re.escape(table_condition_key)}\b"
                relvant_conditions_for_combination = [re.sub(pattern, table_alias, cc) for cc in relvant_conditions_for_combination]

            # Add distance conditions to the query
            for rdist in relvant_conditions_for_combination:
                query_where.append(rdist)

            # Add join conditions for given partition_key
            for i in range(1, len(new_table_list)):
                for j in range(i + 1, len(new_table_list) + 1):
                    query_where.append(f"{new_table_list[i - 1]}.{partition_key} = {new_table_list[j - 1]}.{partition_key}")

            # Build table list with correct table names from alias_to_table_map
            new_table_list_with_alias = []
            for i, table_condition_key in enumerate(table_conditions_keys):
                original_table = alias_to_table_map.get(table_condition_key, table_condition_key)
                new_alias = f"t{i + 1}"
                new_table_list_with_alias.append(f"{original_table} AS {new_alias}")

            # Re-add star-join table if one was detected
            if detected_star_join_alias:
                # ONLY create variant with star-join table re-added
                # (no base variant without star-join for star-schema queries)
                star_join_table_name = alias_to_table_map.get(detected_star_join_alias, detected_star_join_alias)
                star_join_new_alias = "p1"
                star_join_table_spec = f"{star_join_table_name} AS {star_join_new_alias}"

                # Build combined table list
                combined_table_list = new_table_list_with_alias + [star_join_table_spec]

                # Add partition key joins from each table to star-join table
                star_join_joins = []
                for table_alias in new_table_list:
                    star_join_joins.append(f"{table_alias}.{partition_key} = {star_join_new_alias}.{partition_key}")

                # Add any star-join table conditions if they exist
                star_join_conditions = []
                if detected_star_join_alias in attribute_conditions:
                    for cond in attribute_conditions[detected_star_join_alias]:
                        star_join_conditions.append(f"{star_join_new_alias}.{cond}")

                # Build the combined query with star-join table
                # IMPORTANT: Remove direct joins between tables when star-join is present
                # Only keep attribute conditions and distance conditions, not partition key joins
                query_where_without_pk_joins = []
                for condition in query_where:
                    # Skip partition key equality joins between tables (t1.pk = t2.pk)
                    if f".{partition_key} = " in condition and condition.count("=") == 1:
                        # This is a simple partition key join, skip it
                        continue
                    # Keep all other conditions (attribute conditions, distance conditions, etc.)
                    query_where_without_pk_joins.append(condition)

                combined_where = query_where_without_pk_joins + star_join_joins + star_join_conditions
                q_with_star_join = (
                    f"SELECT DISTINCT {new_table_list[0]}.{partition_key} FROM {', '.join(combined_table_list)} WHERE {' AND '.join(combined_where)}"
                )
                ret.append(q_with_star_join)

                # If there are partition key conditions (like IN subqueries), create variants
                if partition_key_conditions:
                    # For star-join tables with partition key conditions, apply them to the star-join alias
                    for i in range(1, len(partition_key_conditions) + 1):
                        for comb in itertools.combinations(partition_key_conditions, i):
                            query_where_comb = combined_where.copy()
                            # Apply partition key conditions to the star-join table
                            for subquery in comb:
                                # Handle NOT conditions properly to maintain valid SQL syntax
                                if subquery.startswith("NOT "):
                                    # NOT condition: place NOT before the table alias
                                    condition_part = subquery[4:]  # Remove "NOT "
                                    star_join_subquery = f"NOT {star_join_new_alias}.{condition_part}"
                                else:
                                    # Regular condition: place table alias normally
                                    star_join_subquery = f"{star_join_new_alias}.{subquery}"
                                query_where_comb.append(star_join_subquery)
                            # Build WHERE clause only if conditions exist
                            if query_where_comb:
                                q = (
                                    f"SELECT DISTINCT {new_table_list[0]}.{partition_key} FROM "
                                    + f"{', '.join(combined_table_list)} WHERE {' AND '.join(query_where_comb)}"
                                )
                            else:
                                q = (
                                    f"SELECT DISTINCT {new_table_list[0]}.{partition_key} FROM "
                                    + f"{', '.join(combined_table_list)}"
                                )
                            ret.append(q)
            else:
                # Normal case without p0 exclusion
                # Build WHERE clause only if conditions exist
                if query_where:
                    q = f"SELECT DISTINCT {new_table_list[0]}.{partition_key} FROM {', '.join(new_table_list_with_alias)} WHERE {' AND '.join(query_where)}"
                else:
                    q = f"SELECT DISTINCT {new_table_list[0]}.{partition_key} FROM {', '.join(new_table_list_with_alias)}"
                ret.append(q)

            if partition_key_conditions:  # TODO handle CTE expressions
                # Create variants of the query with subqueries
                for i in range(1, len(partition_key_conditions) + 1):
                    for comb in itertools.combinations(partition_key_conditions, i):
                        query_where_comb = query_where.copy()
                        # For each combination of subqueries, create a new partial query
                        for subquery in comb:  # Add all partition_key_conditions of this combination to the query
                            # Handle NOT conditions properly to maintain valid SQL syntax
                            if subquery.startswith("NOT "):
                                # NOT condition: place NOT before the table alias
                                condition_part = subquery[4:]  # Remove "NOT "
                                p_subquery = f"NOT {new_table_list[0]}.{condition_part}"
                            else:
                                # Regular condition: place table alias normally
                                p_subquery = f"{new_table_list[0]}.{subquery}"
                            query_where_comb.append(p_subquery)
                        # Build WHERE clause only if conditions exist
                        if query_where_comb:
                            q = (
                                f"SELECT DISTINCT {new_table_list[0]}.{partition_key} FROM "
                                + f"{', '.join(new_table_list_with_alias)} WHERE {' AND '.join(query_where_comb)}"
                            )
                        else:
                            q = (
                                f"SELECT DISTINCT {new_table_list[0]}.{partition_key} FROM "
                                + f"{', '.join(new_table_list_with_alias)}"
                            )
                        ret.append(q)

    # Add also the raw partition_key queries
    for partition_key_part in partition_key_conditions:
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

    return ret


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

    # TODO Ensure one of the values is a number
    # TODO Ensure number is on second position
    # TODO Ensure number is not negative

    # Check if at least one value is a number
    for distance_condition in distance_conditions_between + distance_conditions_smaller + distance_conditions_greater:
        if not any(str(x).replace(".", "", 1).isdigit() for x in distance_condition.split()):
            logger.warning(f"No numeric value found in distance condition: {distance_condition}")

    # Check if number is on right side of comparison operator for distance functions
    for distance_condition in distance_conditions_between + distance_conditions_smaller + distance_conditions_greater:
        if is_distance_function(distance_condition):
            if "<" in distance_condition or ">" in distance_condition:
                parts = re.split(r"(<=|>=|<|>)", distance_condition)
                if len(parts) == 2 and not str(parts[1].strip()).replace(".", "", 1).isdigit():
                    logger.warning(f"Numeric value not on right side of comparison in distance condition: {distance_condition}")

    # Check for negative numbers
    for distance_condition in distance_conditions_between + distance_conditions_smaller + distance_conditions_greater:
        numbers = [float(x) for x in re.findall(r"-?\d*\.?\d+", distance_condition)]
        if any(n < 0 for n in numbers):
            logger.warning(f"Negative value found in distance condition: {distance_condition}")

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
            match = re.search(r'<=\s*([+-]?(?:\d+\.?\d*|\.\d+))', distance_condition)
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
            match = re.search(r'<\s*([+-]?(?:\d+\.?\d*|\.\d+))', distance_condition)
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
            match = re.search(r'>=\s*([+-]?(?:\d+\.?\d*|\.\d+))', distance_condition)
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
            match = re.search(r'>\s*([+-]?(?:\d+\.?\d*|\.\d+))', distance_condition)
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


def generate_all_query_hash_pairs(
    query: str,
    partition_key: str,
    min_component_size: int = 1,
    follow_graph: bool = True,
    keep_all_attributes: bool = True,
    canonicalize_queries: bool = False,
    auto_detect_star_join: bool = True,
    max_component_size: int | None = None,
    star_join_table: str | None = None,
    warn_no_partition_key: bool = True,
) -> list[tuple[str, str]]:
    query_set: set[str] = set()

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
            auto_detect_star_join=auto_detect_star_join,
            max_component_size=max_component_size,
            star_join_table=star_join_table,
            warn_no_partition_key=warn_no_partition_key,
        )
    )
    query_set.update(query_set_diff_combinations)

    # Create bucket variant with normalized distances (Example: 1.6 - 3.6 -> 1 - 4 WITH bucket_steps = 1)
    nor_dist_query = normalize_distance_conditions(query)

    query_set.update(
        set(
            generate_partial_queries(
                nor_dist_query,
                partition_key,
                min_component_size,
                follow_graph,
                keep_all_attributes,
                other_functions_as_distance_conditions=True,  # TODO evaluate if how performance is affected if is turned off
                auto_detect_star_join=auto_detect_star_join,
                max_component_size=max_component_size,
                star_join_table=star_join_table,
                warn_no_partition_key=warn_no_partition_key,
            )
        )
    )

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


def generate_all_hashes(
    query: str,
    partition_key: str,
    min_component_size=1,
    follow_graph=True,
    fix_attributes=True,
    canonicalize_queries=False,
    auto_detect_star_join: bool = True,
    max_component_size: int | None = None,
    star_join_table: str | None = None,
    warn_no_partition_key: bool = True,
) -> list[str]:
    """
    Generates all hashes for the given query.
    """
    qh_pairs = generate_all_query_hash_pairs(
        query=query,
        partition_key=partition_key,
        min_component_size=min_component_size,
        follow_graph=follow_graph,
        keep_all_attributes=fix_attributes,
        canonicalize_queries=canonicalize_queries,
        auto_detect_star_join=auto_detect_star_join,
        max_component_size=max_component_size,
        star_join_table=star_join_table,
        warn_no_partition_key=warn_no_partition_key,
    )
    return [x[1] for x in qh_pairs]


if __name__ == "__main__":
    print("To use the query processor, please refer to the documentation")
